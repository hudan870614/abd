package com.qjzh.abd.components.comp_common.utils

import com.qjzh.abd.components.comp_common.caseciew.{ImportUserMac, SettingRedisData, SynAreaBean, SynBaseRuleBean}
import com.qjzh.abd.components.comp_common.conf.CommonConf
import com.qjzh.abd.function.redis.utils.RedisUtils

/**
  * Created by 92025 on 2016/12/21.
  */
object CompRedisClientUtils {
  /**
    * 加载区域缓存表
    * @return
    */
  def getRedisAreaMap(): List[SynAreaBean] = {

    val result = RedisUtils.getRedisClient().withClient[List[SynAreaBean]]( x => {

      x.hgetall(CommonConf.tBaseAreaRedisKey).toList.flatMap(map => {
        map.map(value => {
          GsonTools.gson.fromJson(value._2, classOf[SynAreaBean])
        })
      })
    })
    result
  }

  /**
    * 获取一个区域的信息
    * @param floor
    * @param areaId
    * @return
    */
  def getAreaInfoByFloorAndAreaNo(floor:String,areaId:Long): SynAreaBean ={
    val redisAreaMap: List[SynAreaBean] = getRedisAreaMap()
    val filter: List[SynAreaBean] = redisAreaMap.filter(xx => {
//      xx.floor == floor && xx.id == areaId
        xx.id == areaId
    })
    if(filter != null && filter.size > 0){
      return filter(0)
    }
    null
  }

  /**
    * 加载规则缓存表
    * @return
    */
  def getRedisRuleMacs(): List[SynBaseRuleBean] = {

    val result = RedisUtils.getRedisClient().withClient[List[SynBaseRuleBean]](x => {

      x.hgetall(CommonConf.tBaseRuleRedisKey).toList.flatMap(map => {
        map.map(value => {
          GsonTools.gson.fromJson(value._2, classOf[SynBaseRuleBean])
        })
      })
    })
    result
  }

  /**
    * 加载关口配置缓存表
    * @return
    */
  def getRedisSetting(): List[SettingRedisData] = {

    val result = RedisUtils.getRedisClient().withClient[List[SettingRedisData]](x => {

      x.hgetall(CommonConf.tBaseLocationRedisKey).toList.flatMap(map => {
        map.map(value => {
          GsonTools.gson.fromJson(value._2, classOf[SettingRedisData])
        })
      })
    })
    result
  }

  /**
    * 加载基础配置项
    */
  def getHotAreaMaxConf(): (Int,Int,Int) ={
    val confSet = CompRedisClientUtils.getRedisSetting().head
    val maxMapPixel = confSet.maxMapPixel
    val max_point_x = maxMapPixel.split(",")(0).toInt
    val max_point_y = maxMapPixel.split(",")(1).toInt
    val area_side_dis = confSet.htGridSideLen.toInt
    (max_point_x,max_point_y,area_side_dis)
  }


  /**
    * 加载重点ＭＡＣ缓存表
    * @return
    */
  def getRedisImpUserMacs(): List[ImportUserMac] = {

    val result = RedisUtils.getRedisClient().withClient[List[ImportUserMac]](x => {

      x.hgetall(CommonConf.redis_imp_user_name_key).toList.flatMap(map => {
        map.map(value => {
          GsonTools.gson.fromJson(value._2, classOf[ImportUserMac])
        })
      })
    })
    result
  }


}
