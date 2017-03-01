package com.qjzh.abd.components.location.dao

import com.google.gson.Gson
import com.qjzh.abd.components.location.common.caseview.CaseClass.SynApDeviceBean
import com.qjzh.abd.components.location.common.conf.CommonConf
import com.qjzh.abd.function.common.PropertiesUtils
import com.qjzh.abd.function.redis.utils.RedisUtils
import com.qjzh.abd.function.hbase.utils._
import com.qjzh.abd.function.kafka.utils.KafkaProUtils


/**
  * Created by yufeiwang on 19/12/2016.
  */
object HgDaoImpl {

  /**
    * 从redis里拿到海关项目所有的AP信息
    *
    * @return
    */
  def getApDevicesFromRedis(): List[SynApDeviceBean] = {
    val result = RedisUtils.getRedisClient().withClient[List[SynApDeviceBean]](x => {
      x.hgetall("RKP_AP").toList.flatMap(map => {
        map.map(value => {
          val gson: Gson = new Gson
          gson.fromJson(value._2, classOf[SynApDeviceBean])
        })
      })
    })
    result
  }

  /**
    * write the contented to location hbase table
    *
    * @param rowKey
    * @param colName
    * @param value
    */
  def hbaseWriteTable(rowKey: String, colName: String, value: String): Unit = {
    HbaseUtils.writeTable(CommonConf.hs_location_test, rowKey, colName, value)
  }

  /**
    * Send message to kafka using utilities in function module
    *
    * @param topic
    * @param msg
    */
  def kafkaSendMsg(topic: String, msg: String): Unit = {
    KafkaProUtils.sendMessages(topic, msg)
  }

  /**
    * For testing purpose
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val properties = PropertiesUtils.init("hgLocationRules.properties")
    println(properties.get(""))
  }

}
