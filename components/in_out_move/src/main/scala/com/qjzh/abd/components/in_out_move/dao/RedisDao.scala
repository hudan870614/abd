package com.qjzh.abd.components.in_out_move.dao

import com.qjzh.abd.components.comp_common.common.RedisBusUtils
import com.qjzh.abd.function.redis.utils.RedisUtils

import scala.collection.mutable

/**
  * Created by 92025 on 2016/12/26.
  */
object RedisDao{
  val redisUtils = RedisBusUtils
  val redisClient = RedisUtils.getRedisClient()

  val one_hour = 60 * 60

  val one_minute = 60

  val one_day = 60 * 60 * 24
  /**
    *
    * @param redisKey
    * @param mapKV
    */
  def hsetMap(redisKey: String,mapKV: mutable.HashMap[String,String],ttl: Int = 0): Unit ={
    mapKV.foreach(xx => {
      RedisDao.hset(redisKey,xx._1,xx._2,ttl)
    })
  }
  /**
    *
    * @param redisKey
    * @param mapKV
    */
  def hsetMuMap(redisKey: String,mapKV: Map[String,String],ttl: Int = 0): Unit ={
    mapKV.foreach(xx => {
      RedisDao.hset(redisKey,xx._1,xx._2,ttl)
    })
  }

  /**
    * 封装hashmap的存储
    * @param redisKey
    * @param field
    * @param value
    * @param ttl
    */
  def hset(redisKey: String,field: String ,value: String,ttl: Int = 0): Unit ={
    if(ttl > 0){
      redisUtils.hset(redisKey,field,value,ttl)
    }else{
      redisUtils.hset(redisKey,field,value)
    }
  }

  /**
    * 删除redis中指定的hash字段
    * @param redisKey
    * @param field
    */
  def hdel(redisKey: String,field: String): Unit ={
    redisClient.withClient[Unit](x => {
      x.hdel(redisKey,field)
    })
  }

  /**
    * 根据正则删除redis中的key
    * @param keyRex   redisKey的正则
    * @param keyLength   要删除的redis可以的长度
    */
  def delRedisKeyByRexKey(keyRex: String,keyLength: Int): Unit ={

    redisClient.withClient[Unit]( x => {
      val maybeStrings: List[Option[String]] = x.keys(keyRex).get
      maybeStrings.foreach(xx => {
        xx match {
          case Some(key) => if (key.length == keyLength) x.del(key)
          case None =>
        }
      })
    })
  }
  def main(args: Array[String]): Unit = {
    RedisDao.hdel("11","11")
  }

}
