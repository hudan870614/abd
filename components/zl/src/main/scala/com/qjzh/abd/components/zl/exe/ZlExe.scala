package com.qjzh.abd.components.zl.exe


import com.qjzh.abd.components.comp_common.caseciew.{MessageKafka, SynAreaBean}
import com.qjzh.abd.components.comp_common.common.{KafkaUtils, RedisBusUtils}
import com.qjzh.abd.components.comp_common.utils.{ComUtils, CompRedisClientUtils, GsonTools}
import com.qjzh.abd.components.zl.caseview.{EwAndAwRedisData, RedisZLData}
import com.qjzh.abd.components.zl.conf.ZlConf
import com.qjzh.abd.components.zl.service.ZlService
import com.qjzh.abd.control.common.view.Report
import com.qjzh.abd.control.exe.CommonExe
import com.qjzh.abd.function.common.DateUtils
import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.DStream

/**
  * Created by 92025 on 2016/12/21.
  */
object ZlExe extends CommonExe{
  //执行前预处理
  override def init(args: Array[String]): Unit = {

  }

  //执行
  override def exe[T: ClassManifest](line: DStream[T]): Unit = {
    if(line.isInstanceOf[DStream[Report]]){

      val stream: DStream[Report] = line.asInstanceOf[DStream[Report]]


      val parserRdd: DStream[Report] = stream.filter(xx => {
        var flag = false
        if (xx != null && xx.pointX > 0) {
          flag = true
        }
        flag
      }).map(report => {
        //      report.timeStamp = DateUtils.getCurTime("yyyyMMddHHmm").toLong
        report.areaNo = null
        val pointX: Double = report.pointX
        val pointY: Double = report.pointY

        val areaList = CompRedisClientUtils.getRedisAreaMap().filter(x => {
           ComUtils.isContainInArea(report, x) != -1
        }).take(1)

        if (!areaList.isEmpty) {
          report.areaNo = areaList(0).id + ""
        } else {
          report.areaNo = "-1"
        }
        report

      })
      val dStream: DStream[(String, Report)] = parserRdd.map(xx => {
        //分组去重
        (xx.floor + "_" + xx.areaNo + "_" + xx.userMac, xx)
      })

      //=================================计算滞留告警消息==========================
      val timeDay: String = DateUtils.getCurTime("yyyyMMdd")
      val reduceByKey: DStream[(String, Report)] = dStream.reduceByKey((xx,yy) => yy)   //每分钟去重后的数据
      val filter: DStream[(String, Report)] = reduceByKey.filter(yy => {
        //      println("===================" + yy)
        var flag = false
        val _1: String = yy._1
        val _2: Report = yy._2
        val REDIS_KEY =  ZlConf.REDIS_KEY_ZL  + "_" + timeDay

        val hget: String = RedisBusUtils.hget(REDIS_KEY, _2.userMac)
        if (hget == null || "".equals(hget)) {
          //没有出现过   存入最新值
          RedisBusUtils.hset(REDIS_KEY, _2.userMac, GsonTools.gson.toJson(RedisZLData(_2.timeStamp, _2.timeStamp, _2.areaNo.toLong, _2.floor, 0)),ZlConf.DRUATION_DAT)
          false
        }else{
          val redisZLData: RedisZLData = GsonTools.gson.fromJson(hget, classOf[RedisZLData])
          val drution: Long = (_2.timeStamp - redisZLData.endTime)/1000/60 //当前的和历史已有的值之间的在线时间
          val areaBean: SynAreaBean = CompRedisClientUtils.getAreaInfoByFloorAndAreaNo(_2.floor, _2.areaNo.toLong) //区块信息

          if (_2.areaNo != "-1" && redisZLData.areaId != _2.areaNo.toLong) {
            //这个用户已经跑到其他区域   //或者是没有探测的区域
            //1.如果用户的区域id = 不等于-1   立即跟新该mac
            RedisBusUtils.hset(REDIS_KEY, _2.userMac, GsonTools.gson.toJson(RedisZLData(_2.timeStamp, _2.timeStamp, _2.areaNo.toLong, _2.floor, 0)),ZlConf.DRUATION_DAT)
            false
          }else{
            if(areaBean != null && redisZLData.areaId == _2.areaNo.toLong && redisZLData.floor == _2.floor && areaBean.rtTimeThreshold < drution && drution < ZlConf.ZL_HEAT_TIME_DRUATION) {
              //修改该对象在redis中的endTime 和在线时长
              RedisBusUtils.hset(REDIS_KEY, _2.userMac, GsonTools.gson.toJson(RedisZLData(redisZLData.startTime, _2.timeStamp, redisZLData.areaId, redisZLData.floor, redisZLData.duration + drution)),ZlConf.DRUATION_DAT)
              true
            }else{
              RedisBusUtils.hset(REDIS_KEY, _2.userMac, GsonTools.gson.toJson(RedisZLData(_2.timeStamp, _2.timeStamp, _2.areaNo.toLong, _2.floor, 0)),ZlConf.DRUATION_DAT)
              false
            }
          }
        }
      })

      val dStream1: DStream[(String, (Report, Int))] = filter.map(xx => {//聚合每个区域的人数
        //滞留用户入库
        ZlService.zlUserMacToRedis(xx)
        //
        (xx._2.floor + "_" + xx._2.areaNo, (xx._2, 1))
      }).reduceByKey((xx, yy) => {
        (xx._1, (xx._2 + yy._2))
      })

      /*val dStream2: DStream[MessageKafka[EwAndAwRedisData]] = dStream1.map(xx => {
        ZlService.computerZl(xx)
      })

      ComUtils.sendEaWaMessageToKafkaRdd(dStream2,KafkaUtils.oline_one_minute_mub)*/

      dStream1.foreachRDD(rdd => {
        rdd.foreach(yy => {
          ZlService.computerZl(yy)
        })
      })


    }
  }

  //执行(离线)
  override def exe(sparkcontent: SparkContext): Unit = {

  }
}
