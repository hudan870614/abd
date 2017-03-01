package com.qjzh.abd.components.label.view

/**
  * Created by hushuai on 16/12/27.
  */
object LabelDigView {

}


case class ImportCrowd(var times : Long,var duration:Double,var brand : String = "",var details : String = "", var curDay : String = "") extends Serializable


/**
  *
  * 在线时长结构说明
  * @param curDay 日期(格式yyyyMMdd)
  * @param dayNum 天数
  * @param userMac 手机mac
  * @param times 当天频次
  * @param duration 当天时长
  * @param differDay 跟执行日相隔天数
  *
  */
case class label_view_duration(val curDay : String = "",val dayNum : Long = 0,
                               val userMac : String = "",val times : Long = 0,
                               val duration : Double = 0.0D,val differDay : Int = 0,
                               val brand : String = "") extends Serializable



case class HbaseImpStaCountCase(val count : Long) extends Serializable

case class HbaseBrandCountCase(val tSum : Long,val bSum: Long, val brand : String = "") extends Serializable

case class RedisCountCase(val passTypeId : String,val curCount: Long) extends Serializable

/**
  * @param total 总人数
  * @param keyPass 重点旅客
  * @param firstPass 首次旅客
  * @param occasionalPass 偶尔旅客
  * @param activePass 活跃旅客
  * @param staff 工作人员
  */
case class HbaseAllPassDetailCase(val total : Long,val keyPass: Long, val firstPass: Long, val occasionalPass:Long, val activePass : Long , val staff:Long) extends Serializable