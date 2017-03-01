package com.qjzh.abd.components.warn_water_line.caseview

/**
  * Created by 92025 on 2017/2/20.
  */
object RedisCaseResult {

}

/**
  * AP点位历史最大值缓存信息
  * @param hisMaxSum        该Ap历史最大值
  * @param serverTimeStamp  历史最大值的时间
  *    @param apMac           apMac
  */
case class WarnWaterLineApCacheInfoToRedis(hisMaxSum: Long,serverTimeStamp: Long,apMac: String) extends Serializable

/**
  * 区域的历史最大值缓存信息
  * @param hisMaxSum        该区域历史最大值
  * @param serverTimeStamp  历史最大值的时间
  * @param areaNo           区域ID
  */
case class WarnWaterLineAreaCacheInfoToRedis(hisMaxSum: Long,serverTimeStamp: Long,areaNo: String) extends Serializable