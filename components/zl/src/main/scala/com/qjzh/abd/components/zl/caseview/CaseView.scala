package com.qjzh.abd.components.zl.caseview


/**
  * Created by 92025 on 2016/12/21.
  */
object CaseView {}
/**
  * //redis中存储的滞留人信息
  * @param startTime 起始时间
  * @param endTime  结束时间
  * @param areaId  区域id
  * @param floor  楼层
  * @param duration 多长时间(分中)
  */
case class RedisZLData(val startTime: Long = 0,val endTime: Long = 0,val areaId:Long = 0,val floor:String = null,val duration: Long = 0) extends Serializable
/**
  * 预警告警redis
  * @param areaId
  * @param time
  * @param count
  * @param _type
  * @param level
  * @param levelValue
  */
case class EwAndAwRedisData(val areaId:Long = 0,val time:Long = 0,var count: Long = 0,val _type:Long = 0,var level: String,var levelValue: Long = 0,val floor:Long = 1) extends Serializable
