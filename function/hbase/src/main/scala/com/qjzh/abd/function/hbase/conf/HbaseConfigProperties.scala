package com.qjzh.abd.function.hbase.conf

import java.io.FileInputStream
import java.util.Properties

import com.qjzh.abd.function.common.PropertiesUtils

/**
  * Created by 92025 on 2016/12/15.
  */
object HbaseConfigProperties {
  val incrTableName ="incr_table"

  val  properties= PropertiesUtils.init()
  val hbase_zookeeper_host = "hbase.zookeeper.host"     //hbase默认使用的zookeeper地址
  val defaultFamily="data"
  val defaultQualifier="data"
  /**
    * @return
    */
  def getProValue( key: String): String = {
    return properties.getProperty(key)
  }

  def main(args: Array[String]): Unit = {
    println(HbaseConfigProperties.getProValue(HbaseConfigProperties.hbase_zookeeper_host))
  }
}
