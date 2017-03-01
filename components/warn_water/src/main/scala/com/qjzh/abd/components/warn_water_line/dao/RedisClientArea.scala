package com.qjzh.abd.components.warn_water_line.dao

import com.qjzh.abd.components.comp_common.caseciew.SynAreaBean
import com.qjzh.abd.components.comp_common.common.RedisBusUtils
import com.qjzh.abd.components.comp_common.utils.{ComUtils, GsonTools}
import com.qjzh.abd.components.warn_water_line.caseview.WarnWaterLineAreaCacheInfoToRedis
import com.qjzh.abd.control.common.view.Report
import com.qjzh.abd.function.redis.utils.RedisUtils
import org.apache.commons.lang3.StringUtils

/**
  * Created by 92025 on 2017/2/20.
  */
object RedisClientArea {
  /**
    * 获取区域的历史最大值
    * @param redisHashKey
    * @return
    */
  def queryOrUpdateAreaHisMaxSumByAreaMac(redisHashKey: String, cacheInfoToRedis: WarnWaterLineAreaCacheInfoToRedis): WarnWaterLineAreaCacheInfoToRedis = {
    val hget: String = redisBusSession.hget(redisHashKey,cacheInfoToRedis.areaNo)
    var warnWaterLineAreaCacheInfoToRedis: WarnWaterLineAreaCacheInfoToRedis = null
    if(StringUtils.isNotEmpty(hget)){
      warnWaterLineAreaCacheInfoToRedis = GsonTools.gson.fromJson(hget,classOf[WarnWaterLineAreaCacheInfoToRedis])
      if(warnWaterLineAreaCacheInfoToRedis.hisMaxSum < cacheInfoToRedis.hisMaxSum){
        redisBusSession.hset(redisHashKey,cacheInfoToRedis.areaNo,GsonTools.gson.toJson(cacheInfoToRedis))
      }
    }else{
      redisBusSession.hset(redisHashKey,cacheInfoToRedis.areaNo,GsonTools.gson.toJson(cacheInfoToRedis))
      warnWaterLineAreaCacheInfoToRedis = cacheInfoToRedis
    }
    warnWaterLineAreaCacheInfoToRedis
  }


  val redisBusSession = RedisBusUtils
  val redisSession = RedisUtils
  val utils = ComUtils
  /**
    * 获取Area配置信息
    * @return RDD[(String, ApCase)]  K:APMAC   V:AP对象
    */
  def getAreaList(): List[SynAreaBean] ={
    redisBusSession.getRedisAreaMap()
  }

  /**
    * 判断当前点位是否在所属区域下
    * @param point 目标点位
    * @param area 关联区域
    * @return (1:属于区域内;0:在区域边线上;-1:在区域之外)
    */
  def isContainInArea(point : Report, area: SynAreaBean) : Int = {
    utils.isContainInArea(point,area)
  }

  def main(args: Array[String]): Unit = {
    val json = "{\"userMac\":\"006RL26T736W\",\"apMac\":\"000000000000\",\"rssi\":\"0\",\"timestamp\":1487641850203,\"apType\":\"test_kafka\",\"brand\":\"test_kafka\",\"projectNo\":\"sk\",\"areaNo\":\"4\",\"isFake\":0,\"pointX\":95.0,\"pointY\":38.0,\"pointZ\":0.0,\"mapID\":0,\"floor\":1,\"serverTimeStamp\":1487641860450,\"coordinateX\":0.0,\"coordinateY\":0.0,\"coordinateZ\":0.0}"
    val json1: Report = GsonTools.gson.fromJson(json,classOf[Report])
    println(json)
    val areaList: List[SynAreaBean] = getAreaList()//.filter(_.id != 4)
    val synAreaBeans: List[SynAreaBean] = areaList.filter(xx => {
      isContainInArea(json1, xx) != -1
    })
    println(synAreaBeans)


  }
}
