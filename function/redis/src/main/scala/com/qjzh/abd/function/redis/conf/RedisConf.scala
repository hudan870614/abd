package com.qjzh.abd.function.redis.conf

/**
  * Created by 92025 on 2016/12/16.
  */
object RedisConf {
  val redis_host = RedisConfigProperties.getProValue(RedisConfigProperties.redis_host_ip)

  val redis_port = RedisConfigProperties.getProValue(RedisConfigProperties.redis_host_port).toInt

}
