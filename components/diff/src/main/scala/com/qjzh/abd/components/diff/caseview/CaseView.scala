package com.qjzh.abd.components.diff.caseview

/**
  * Created by 92025 on 2016/12/20.
  */
object CaseView {

}


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



//============================骤增用到的cass
//存入redis的上十分钟骤增对象
case class RedisDiffLasterBean(val areaId:Long = 0,val dateTime:Long = 0,val sum:Long = 0,val floor:Long = 0) extends Serializable


//存入hbase的骤增对象
case class HbaseDiffCruBean(val areaId:Long = 0,val dateTime:Long = 0,val sum:Long = 0,val floor:Long = 0
                            ,val lSum:Long = 0,val diff:Long = 0,diffRate:Double = 0.0,
                            val isEwWarn:Boolean = false,val isSurgeThresholdWarn:Boolean = false) extends Serializable