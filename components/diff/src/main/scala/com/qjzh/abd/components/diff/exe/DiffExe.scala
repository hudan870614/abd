package com.qjzh.abd.components.diff.exe


import com.qjzh.abd.components.comp_common.caseciew.{MessageKafka}
import com.qjzh.abd.components.comp_common.common.KafkaUtils
import com.qjzh.abd.components.comp_common.utils.{ComUtils}
import com.qjzh.abd.components.diff.caseview.HbaseDiffCruBean
import com.qjzh.abd.components.diff.conf.DiffConf
import com.qjzh.abd.components.diff.dao.redis.RedisClientUtils
import com.qjzh.abd.components.diff.service.DiffService
import com.qjzh.abd.control.common.view.Report
import com.qjzh.abd.control.exe.CommonExe
import com.qjzh.abd.function.common.DateUtils
import com.qjzh.abd.function.hbase.utils.HbaseUtils
import org.apache.spark.{SparkContext}
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream

/**
  * 区块骤增接口
  * 区块预警告警计算
  * Created by 92025 on 2016/10/24.
  */
object DiffExe extends CommonExe{


  val batchTime = 1 * 60  //streaming 批次时间
  val windowWith = 1 * 60 * 10  //窗口大小
  val spilTime = 1 * 60   //滑动时间



  //执行前预处理
  override def init(args: Array[String]): Unit = {
    HbaseUtils.createTable(DiffConf.HBASE_DIFF_TABLE)   //检查表是否存在
  }

  //执行(实时)
  override def exe[T: ClassManifest](line: DStream[T]): Unit = {

    if(line.isInstanceOf[DStream[Report]]) {

      val stream: DStream[Report] = line.asInstanceOf[DStream[Report]]

      val parserRdd: DStream[Report] = stream.filter(xx => {
        //"000000000000".equals(xx.apMac)
        var flag = false
        if (xx != null && xx.pointX > 0) {
          flag = true
        }
        flag
      }).map(report => {
        report.timeStamp = DateUtils.getCurTime("yyyyMMddHHmm").toLong
        report.areaNo = null
        val pointX: Double = report.pointX
        val pointY: Double = report.pointY
        val areaList = RedisClientUtils.getRedisAreaMap().filter(x => {
           ComUtils.isContainInArea(report, x) != -1
        }).take(1)
        if (!areaList.isEmpty) {
          report.areaNo = areaList(0).id + ""
        } else {
          report.areaNo = "-1"
        }
        report
      })
      //==============================区域骤增--修改版
      val reduceByKey1: DStream[(String, Report)] = parserRdd.map(xx => {
        //分组去重
        (xx.floor + "_" + xx.areaNo + "_" + xx.userMac, xx)
      }).reduceByKey((laster, later) => {
        later
      })
      val reduceByKeyAndWindow: DStream[(String, (Report, Int))] = reduceByKey1.map(xx => {
        (xx._2.floor + "_" + xx._2.areaNo, (xx._2, 1))
      }).reduceByKeyAndWindow((x: (Report, Int), y: (Report, Int)) => {
        (y._1, y._2 + x._2)
      }, Seconds(windowWith), Seconds(spilTime))



      val map: DStream[MessageKafka[HbaseDiffCruBean]] = reduceByKeyAndWindow.map(DiffService.computerDiff(_))


      val data: DStream[MessageKafka[HbaseDiffCruBean]] = map.map(xx => {
        (xx._type, xx)
      }).groupByKey().flatMap(xx => {
        //        val _type = xx._1
        DiffService.compuerLossAreaData(xx)
      })



      ComUtils.sendEaWaMessageToKafkaRdd(data,KafkaUtils.oline_one_minute_mub)



    }

  }

  //执行
  override def exe(sparkcontent: SparkContext): Unit = {

  }
}
