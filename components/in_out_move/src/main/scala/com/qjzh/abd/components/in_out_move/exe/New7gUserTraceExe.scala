package com.qjzh.abd.components.in_out_move.exe

import com.qjzh.abd.components.comp_common.common.{HdfsUtils, RddUtils, RedisBusUtils}
import com.qjzh.abd.components.in_out_move.caseview.{InAndOutFix, UserMacPointTrajectory, UserStatics}
import com.qjzh.abd.components.in_out_move.conf.{HbaseIncrTableConf, New7gUserTraceConf}
import com.qjzh.abd.components.in_out_move.dao.{HbaseDao, HbaseIncrDao, RedisDao}
import com.qjzh.abd.components.in_out_move.service.New7gUserTraceService
import com.qjzh.abd.control.common.view.Report
import com.qjzh.abd.control.exe.CommonExe
import com.qjzh.abd.function.common.DateUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.dstream.DStream

/**
  * Created by 92025 on 2016/12/27.
  */
object New7gUserTraceExe extends CommonExe{

  //执行(实时)
  override def exe[T: ClassManifest](line: DStream[T]): Unit = {

    if(line.isInstanceOf[DStream[Report]]) {
      val stream: DStream[Report] = line.asInstanceOf[DStream[Report]]

      val paseRdd: DStream[Report] = stream.map(New7gUserTraceService.parseSrc(_)).filter(xx => {
        "000000000000".equals(xx.apMac)
      }).filter(_.isFake == 0)  //格式化数据源数据  添加区域信息  和服务器时间

      //{"userMac":"0008223181UN","apMac":"000000000009","rssi":"0","timestamp":1482719345327,"apType":"hs_irg","brand":"SZJ-SZJ","areaNo":"2","isFake":0,
      // "pointX":272.0,"pointY":102.0,"pointZ":0.0,"mapID":0,"floor":1,"serverTimeStamp":201612261030,"coordinateX":0.0,"coordinateY":0.0,"coordinateZ":0.0}

      //对一分钟内的用户进行去重  留下时间最大的那个
      val reduceByKey: DStream[(String, Report)] = paseRdd.map(xx => {
        (xx.userMac, xx)
      }).reduceByKey((last, later) => {
        if ((last.timeStamp - later.timeStamp) > 0) {
          last
        } else {
          later
        }
      })

      //构造hbase迁入迁出轨迹
      reduceByKey.foreachRDD(rdd => {
        rdd.foreachPartition(p  => {
          p.foreach(x => {
            val _2: Report = x._2
            val stamp: Long = _2.timeStamp
            _2.timeStamp = _2.serverTimeStamp
            _2.serverTimeStamp = stamp
            //单人过关记录详细  计算逻辑
            New7gUserTraceService.computerUserDetail(_2)
          })
        })
      })

      //构造运动轨迹对象
      val allUserMacPoint: DStream[UserMacPointTrajectory] = reduceByKey.map(x => {
        New7gUserTraceService.dealUserMacPointTrajectory(x._2)
      }).filter(x => {
        (x != null && x.size >0)
      }).flatMap(_.toList).filter(_ != null)

      allUserMacPoint.cache()

      allUserMacPoint.map(x => {

        //统计用户描述，存到hbase
        val rawStatics :UserStatics = HbaseIncrDao.updateUserStatistics(x)
        New7gUserTraceService.updateUserDesc(rawStatics)

        //格式: 日期(yyyyddmm),手机mac,当次区域开始时间戳,当次区域结束时间戳,手机品牌,本次是否新增入关次数
        val curDay = DateUtils.getCurTime("yyyyMMdd")
        (curDay+","+x.userMac+","+x.startTime+","+x.endTime+","+x.brand+","+x.isNewTime+","+x.areaNo)
      }).repartition(1).foreachRDD(rdd => {
        RddUtils.saveToHdfs(rdd, HdfsUtils.getSaveFilePath(HdfsUtils.duration_hdfs_file_path))
      })

//      val sqlContext = new SQLContext(allUserMacPoint.ssc.sparkContext)

     /* allUserMacPoint.foreachRDD(xx => {
        xx.foreach(yy => {
          if(yy.isFirst == 1){//当天首次
            New7gUserTraceService.createUserDayAndArea(yy)
          }else if(yy.isNewTime == 1 ){ //新区域
            New7gUserTraceService.createUserArea(yy)
          }else{//不是新区域
            New7gUserTraceService.updateUserArea(yy)
          }
        })
      })*/

    }
  }

  //执行(离线)
  override def exe(sparkcontent: SparkContext): Unit = {

  }

  //执行前预处理
  override def init(args: Array[String]): Unit = {
    HbaseDao.createTable(New7gUserTraceConf.Hbase_Table_Name_Trace_IN_OUT)  //检查表是否存在
    HbaseDao.createTable(New7gUserTraceConf.Hbase_Table_Name_Trace_gather)  //检查表是否存在

    //用户描述增量表和展示表初始化
    HbaseIncrDao.createIncrTable()
    HbaseDao.createTable(HbaseIncrTableConf.descShowTable)
    //"abd:in_out_move:point:traj:"+DateUtils.getCurTime("yyyyMMdd")+":"+userMac  // 35 + 12

    if(false){//是否需要清楚redis中和本业务相关的redisKey
      RedisDao.delRedisKeyByRexKey(s"abd:in_out_move:point:traj:${DateUtils.getCurTime("yyyyMMdd")}*",47)
    }
  }
}
