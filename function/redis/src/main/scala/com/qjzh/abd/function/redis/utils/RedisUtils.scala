package com.qjzh.abd.function.redis.utils

import com.qjzh.abd.function.redis.conf.RedisConf
import com.redis.RedisClientPool

/**
  * Created by hudan
  */
class RedisUtils private(val host:String, val port:Int) extends Serializable{

  val redisClient = new RedisClientPool(host,port)
  def getRedisClient() : RedisClientPool = redisClient

}

object RedisUtils {
  private val redisClient : RedisUtils = new RedisUtils(RedisConf.redis_host,RedisConf.redis_port)

  def getRedisClient() : RedisClientPool = redisClient.getRedisClient()
}
