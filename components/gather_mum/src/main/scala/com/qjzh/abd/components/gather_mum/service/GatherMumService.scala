package com.qjzh.abd.components.gather_mum.service

import com.qjzh.abd.components.comp_common.caseciew.MessageKafka
import com.qjzh.abd.components.comp_common.common._
import com.qjzh.abd.components.comp_common.conf.CommonConf
import com.qjzh.abd.components.comp_common.utils.{ComUtils, GsonTools, LonLatUtils, SparkWriteToHbaseHelpler}
import com.qjzh.abd.components.gather_mum.caseview._
import com.qjzh.abd.components.gather_mum.dao.GatherMumDao
import com.qjzh.abd.control.common.view.Report
import com.qjzh.abd.function.common.DateUtils
import org.apache.parquet.it.unimi.dsi.fastutil.objects.ObjectArrayList
import org.apache.spark.streaming.dstream.DStream

/**
  * Created by Fred on 2016/12/26.
  */
object GatherMumService {


  def main(args: Array[String]): Unit = {
    //最大边长
    val area_side_dis = RedisBusUtils.getSideLen()
    println(area_side_dis)

    // 只在行政级别计算热力图
    RedisBusUtils.getRedisAreaMap().sortBy(-_.areaLevel).foreach(println)
  }


  /**
    * 处理实时人数汇总
    * @param disReportRdd
    * @param curMin
    */
  def dealMemSta(disReportRdd :  DStream[Report], curMin : String): Unit ={
    //当前点位
    val mapUserMacRdd = disReportRdd.filter(_.mapID != null).flatMap(r =>{
      r.mapID.split("\\;").map(rid => {
        ((0,rid),r)
      })
    })
    //当前区域
    val areaUserMacRdd = disReportRdd.filter(_.floor != null).flatMap(r => {
      r.floor.split("\\;").map(rid => {
        ((1,rid),r)
      })
    })
    //合并
    val allMinUserStaRdd = mapUserMacRdd.union(areaUserMacRdd)

    //汇总当前天＼小时
    allMinUserStaRdd.filter(_._2.isFake == 0).map(r => {
      (r._1,r._2.userMac)
    }).foreachRDD(rdd => {
      rdd.foreachPartition(p => {
        p.foreach(data => {
          val busType = data._1._2
          val user_mac = data._2

          val curHour = DateUtils.getCurTime("yyyyMMddHH")
          val curDay = DateUtils.getCurTime("yyyyMMdd")

          val day_reids_key = RedisBusUtils.redis_spark_stamen_minu_prefix + busType + "_" + curDay
          val hour_reids_key = RedisBusUtils.redis_spark_stamen_minu_prefix + busType + "_" + curHour

          RedisBusUtils.hset(day_reids_key, user_mac, user_mac, RedisBusUtils.second_one_day)
          RedisBusUtils.hset(hour_reids_key, user_mac, user_mac, RedisBusUtils.second_one_hour)

        })
      })
    })

    //指定格式
    val saveHbaseRdd = allMinUserStaRdd.groupByKey().map(r => {
      val stype = r._1._1
      val busType = r._1._2
      val mSum = r._2.map(_.userMac).toList.distinct.size

      val curHour = DateUtils.getCurTime("yyyyMMddHH")
      val curDay = DateUtils.getCurTime("yyyyMMdd")
      val day_reids_key = RedisBusUtils.redis_spark_stamen_minu_prefix + busType + "_" + curDay
      val hour_reids_key = RedisBusUtils.redis_spark_stamen_minu_prefix + busType + "_" + curHour
      val hSum = RedisBusUtils.hlen(hour_reids_key)
      val dSum = RedisBusUtils.hlen(day_reids_key)

      StaMemberCaseInfo(curMin, busType, mSum, hSum, dSum, 0, 0, 0,busType,stype)

    })

    //发送kafka
    saveHbaseRdd.foreachRDD(rdd => {
      if(!rdd.isEmpty()){
        rdd.foreachPartition(p => {
          val list: ObjectArrayList[StaMemberCaseInfo] = new ObjectArrayList[StaMemberCaseInfo]()
          p.foreach(list.add(_))
          KafkaUtils.sendMsgToKafka(KafkaUtils.oline_one_minute_mub,MessageKafka[ObjectArrayList[StaMemberCaseInfo]](CommonConf.WARN_AREA_NUM,
            curMin.toLong,list))
        })
      }
    })

    //入库 hbase
    SparkWriteToHbaseHelpler.saveDStreamToHbase[StaMemberCaseInfo](saveHbaseRdd,HbaseBusUtils.sta_impr_day_nber,GatherMumDao.staMemBerBatchWirteToHbaseFun)

  }


  /**
    * 处理热力点位
    * @param disReportRdd
    * @param curMin
    */
  def dealHotPoint(disReportRdd :  DStream[Report], curMin : String): Unit ={

    // 只在行政级别计算热力图
    RedisBusUtils.getRedisAreaMap().filter(_.areaLevel == 3)
      .map(a => {
        //当前行政区域最大＼小经纬度
        (a,ComUtils.getMinMaxLngLat(a.points))
      }).foreach(area => {
      //区域编码
      val areaId = area._1.id
      //最大边长
      val area_side_dis = area._1.gridLength

      //当前区域下栅格后的多个中心点位
      val centerPoinsList = LonLatUtils.getCenterPointsByMinMaxLngLat(area._2._1,area._2._2,area._2._3,area._2._4,area_side_dis)

      val pointMemberRdd = disReportRdd.filter(report => {
        report.pointX >= area._2._1 && report.pointX <= area._2._2 && report.pointY >= area._2._3 && report.pointY <= area._2._4
      }).map(r => {

        val centerPoint = centerPoinsList.map(cp => {
          (cp,LonLatUtils.getDistanceByPoint(r.pointX,r.pointY,cp._1,cp._2))
        }).minBy(_._2)._1
        (centerPoint,r.userMac)
      }).groupByKey().map(r => {
        val point = r._1
        val mSum = r._2.toList.distinct.size
        PointMember(areaId+"",point._1,point._2,area_side_dis/2,mSum,curMin)
      })

      //发送kafka
      pointMemberRdd.foreachRDD(rdd => {
        if(!rdd.isEmpty()){
          rdd.foreachPartition(p => {
            val pointMemList: ObjectArrayList[PointMember] = new ObjectArrayList[PointMember]()
            p.foreach(pointMemList.add(_))
            KafkaUtils.sendMsgToKafka(KafkaUtils.oline_one_minute_mub,MessageKafka[ObjectArrayList[PointMember]](CommonConf.WARN_AREA_HEAT_NUM,
              curMin.toLong,pointMemList))
          })

        }

      })

      //入库 hbase
      SparkWriteToHbaseHelpler.saveDStreamToHbase[PointMember](pointMemberRdd,HbaseBusUtils.HBASE_7gG_STA_HEAT_AREA_HIS,GatherMumDao.pointMemberBatchWirteToHbaseFun)


    })

  }

}
