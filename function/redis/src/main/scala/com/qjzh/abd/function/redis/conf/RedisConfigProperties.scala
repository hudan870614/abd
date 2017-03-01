package com.qjzh.abd.function.redis.conf

import com.qjzh.abd.function.common.PropertiesUtils


/**
  * Created by 92025 on 2016/12/15.
  */
object RedisConfigProperties {

  val redis_host_ip = "redis.host.name"     //redis主机地址
  val redis_host_port = "redis.host.port"     //redis主机端口

  val properties = PropertiesUtils.init()


  /**
    *
    * @return
    */
  def getProValue( key: String): String = {
    return properties.getProperty(key)
  }

  def main(args: Array[String]): Unit = {
    println(properties)
  }
}
