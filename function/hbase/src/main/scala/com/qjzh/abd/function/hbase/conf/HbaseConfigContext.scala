package com.qjzh.abd.function.hbase.conf

/**
  * Created by 92025 on 2016/12/15.
  */
object HbaseConfigContext {
  val hbase_zookeeper_host = HbaseConfigProperties.getProValue(HbaseConfigProperties.hbase_zookeeper_host)
}
