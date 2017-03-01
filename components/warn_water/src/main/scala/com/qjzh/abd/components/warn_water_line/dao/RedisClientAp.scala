package com.qjzh.abd.components.warn_water_line.dao

import com.qjzh.abd.components.comp_common.common.RedisBusUtils
import com.qjzh.abd.components.comp_common.utils.GsonTools
import com.qjzh.abd.components.warn_water_line.caseview.WarnWaterLineApCacheInfoToRedis
import com.qjzh.abd.function.redis.utils.RedisUtils
import org.apache.commons.lang3.StringUtils

/**
  * Created by 92025 on 2017/2/20.
  */
object RedisClientAp {
  val redisBusSession = RedisBusUtils
  val redisSession = RedisUtils

  /**
    * 获取ap的历史最大值
    * @param redisHashKey
    * @return
    */
  def queryOrUpdateApHisMaxSumByApMac(redisHashKey:String,cacheInfoToRedis: WarnWaterLineApCacheInfoToRedis): WarnWaterLineApCacheInfoToRedis ={
    val hget: String = redisBusSession.hget(redisHashKey,cacheInfoToRedis.apMac)
    var warnWaterLineApCacheInfoToRedis: WarnWaterLineApCacheInfoToRedis = null
    if(StringUtils.isNotEmpty(hget)){
      warnWaterLineApCacheInfoToRedis = GsonTools.gson.fromJson(hget,classOf[WarnWaterLineApCacheInfoToRedis])
      if(warnWaterLineApCacheInfoToRedis.hisMaxSum < cacheInfoToRedis.hisMaxSum){
        redisBusSession.hset(redisHashKey,cacheInfoToRedis.apMac,GsonTools.gson.toJson(cacheInfoToRedis))
      }
    }else{
      redisBusSession.hset(redisHashKey,cacheInfoToRedis.apMac,GsonTools.gson.toJson(cacheInfoToRedis))
      warnWaterLineApCacheInfoToRedis = cacheInfoToRedis
    }
    warnWaterLineApCacheInfoToRedis
  }




}
