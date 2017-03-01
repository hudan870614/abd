package com.qjzh.abd.components.diff.dao.redis

import com.qjzh.abd.components.comp_common.caseciew.SynAreaBean
import com.qjzh.abd.components.comp_common.utils.GsonTools
import com.qjzh.abd.components.diff.conf.CommonConf
import com.qjzh.abd.function.redis.utils.RedisUtils

/**
  * Created by 92025 on 2016/12/19.
  */
object RedisClientUtils {
  /**
    * 获取一个区域的信息
    * @param floor
    * @param areaId
    * @return
    */
  def getAreaInfoByFloorAndAreaNo(floor:Long,areaId:Long): SynAreaBean ={
    val redisAreaMap: List[SynAreaBean] = getRedisAreaMap()
    val filter: List[SynAreaBean] = redisAreaMap.filter(xx => {
       xx.id == areaId
    })
    if(filter != null && filter.size > 0){
      return filter(0)
    }
    null
  }

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

}
