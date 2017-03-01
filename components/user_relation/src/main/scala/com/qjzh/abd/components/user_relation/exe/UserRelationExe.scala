package com.qjzh.abd.components.user_relation.exe


import java.util

import com.qjzh.abd.components.comp_common.caseciew.MessageKafka
import com.qjzh.abd.components.comp_common.common.KafkaUtils
import com.qjzh.abd.components.comp_common.conf.CommonConf
import com.qjzh.abd.components.comp_common.utils.{ComUtils, CompRedisClientUtils}
import com.qjzh.abd.components.user_relation.caseview.G7UserRelateInfoToKafka
import com.qjzh.abd.components.user_relation.conf.New7gUserRelationConf
import com.qjzh.abd.components.user_relation.dao.New7gUserRelationHbaseDao
import com.qjzh.abd.components.user_relation.service.New7gUserRelationService
import com.qjzh.abd.control.common.view.Report
import com.qjzh.abd.control.exe.CommonExe
import com.qjzh.abd.function.common.DateUtils
import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable.ListBuffer
import collection.JavaConversions._
import scala.util.Random
/**
  * Created by 92025 on 2017/1/11.
  */
object UserRelationExe extends CommonExe{
  //执行前预处理
  override def init(args: Array[String]): Unit = {
    New7gUserRelationHbaseDao.createTable(New7gUserRelationConf.g7_user_relate_info)
    New7gUserRelationHbaseDao.createTable(New7gUserRelationConf.g7_user_relate_info_detail)
    New7gUserRelationHbaseDao.createTable(New7gUserRelationConf.g7_user_relate_info_log_table)
  }

  //执行(实时)
  override def exe[T: ClassManifest](line: DStream[T]): Unit = {
    if(line.isInstanceOf[DStream[Report]]) {
      val stream: DStream[Report] = line.asInstanceOf[DStream[Report]]

      val paseRdd: DStream[Report] = stream.map(New7gUserRelationService.parseSrc(_)).filter(xx => {
        "000000000000".equals(xx.apMac)
      }).filter(_.isFake == 0)  //格式化数据源数据  添加区域信息

      //对一分钟内的用户进行去重  留下时间最大的那个
      val reduceByKey: DStream[(String, Report)] = paseRdd.map(xx => {
        (xx.areaNo + "_" + xx.userMac, xx)
      }).reduceByKey((last, later) => {
        if ((last.timeStamp - later.timeStamp) > 0) {
          last
        } else {
          later
        }
      })

      val userMacAndUserMacRelationDStream: DStream[(Report, Report)] = reduceByKey.map(xx => {
        (xx._1.split("_")(0), xx._2)
      }).groupByKey().flatMap(element => {
        //已经按照所有的区域分组了
        val element1: ListBuffer[(Report, Report)] = New7gUserRelationService.combination2Element(element._2.toArray)
        element1
      })


      userMacAndUserMacRelationDStream.foreachRDD(_.foreach(New7gUserRelationService.userRelationComputer(_))) //移动终端关联关系计算

      val partitions: DStream[(Report, Report)] = userMacAndUserMacRelationDStream.repartition(10).mapPartitions(partition => {
        //测试方法
        var i = 5
        partition.map(xx => {
          if (i > 0) {
            i = i - 1
            xx
          } else {
            i = i - 1
            null
          }
        })
      }).filter(_ != null)

      val flatMap: DStream[(String, G7UserRelateInfoToKafka)] = partitions.
        filter(New7gUserRelationService.haveCorePerson(_)). //判断元祖中是否有重点人
        flatMap(New7gUserRelationService.relationCorePersonAndAreaGroup(_)).
        filter(xx => {xx._2.sinTimes >= New7gUserRelationConf.sinTimes || xx._2.reTimes >= New7gUserRelationConf.reTimes}).//加入重点人过滤条件
        reduceByKey((x: G7UserRelateInfoToKafka, y: G7UserRelateInfoToKafka) => {x.reMacs.addAll(y.reMacs);x})  //实时推送重点人终端关联关系到kafka

      val dStream: DStream[MessageKafka[G7UserRelateInfoToKafka]] = flatMap.map(xx => {
        MessageKafka[G7UserRelateInfoToKafka](CommonConf.WARN_AREA_CORE_NUM, DateUtils.getTime(System.currentTimeMillis(), "yyyyMMddHHmm").toLong, xx._2)
      })  //真实计算方法

      /*val filter: DStream[(String, G7UserRelateInfoToKafka)] = flatMap.repartition(10).mapPartitions(partition => {   //测试方法
        var i = 5
        partition.map(xx => {
          if (i > 0) {
            i = i - 1
            xx
          } else {
            i = i - 1
            null
          }
        })
      }).filter(_ != null)
     val dStream: DStream[MessageKafka[G7UserRelateInfoToKafka]] = filter.map(xx => {
        MessageKafka[G7UserRelateInfoToKafka](CommonConf.WARN_AREA_CORE_NUM, DateUtils.getTime(System.currentTimeMillis(), "yyyyMMddHHmm").toLong, xx._2)
      })*/

//      CompRedisClientUtils.getRedisImpUserMacs()    //重点人库


      New7gUserRelationService.insertG7UserRelateInfoToKafkaLog(dStream)   //保存发送日志
//      ComUtils.sendEaWaMessageToKafkaRdd(dStream,KafkaUtils.oline_one_minute_mub)//发送重点人相关消息到kafka
      ComUtils.sendEaWaMessageToKafkaRddPrrocess(dStream,KafkaUtils.oline_one_minute_mub,
        MessageKafka[G7UserRelateInfoToKafka](CommonConf.WARN_AREA_CORE_NUM,
          DateUtils.getTime(System.currentTimeMillis(), "yyyyMMddHHmm").toLong,null)
        )
    }
  }

  //执行(离线)
  override def exe(sparkContext: SparkContext): Unit = {

  }
}
