package com.qjzh.abd.function.kafka.conf

import com.qjzh.abd.function.common.PropertiesUtils


/**
  * Created by 92025 on 2016/12/16.
  */
object KafkaConfigAndProperties {

  val  properties= PropertiesUtils.init()
  val kafka_defalt_topic = "kafka.defalt.topic"     //hbase默认使用的zookeeper地址

  /**
    * @return
    */
  def getProValue( key: String): String = {
    return properties.getProperty(key)
  }

  def main(args: Array[String]): Unit = {
    println(getProValue("kafka.defalt.topic"))
  }


}
