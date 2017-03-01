package com.qjzh.abd.components.comp_common.caseciew

/**
  * Created by hushuai on 16/12/26.
  */
object HdfsView {

}


/**
  * 每分钟维度统计人数表
  * @param curTimeMinute 当前分钟(格式:yyyyMMddHHmm)
  * @param apMac 当前设备＼区域编码＼楼层＼类型 等
  * @param mSum 当前分钟值
  * @param hSum 当前小时值
  * @param dSum 当前天值
  * @param dSum2 无用
  * @param hisMax 当天历史最大值
  * @param hisMaxTime 当天历史最大值时间分钟(格式:yyyyMMddHHmm)
  */
case class hdfs_view_umac_nber(val curTimeMinute : String ,val apMac : String ,
                           val mSum : Long, val hSum : Long, val dSum : Long, val dSum2 : Long,
                               val hisMax : Long, val hisMaxTime : Long) extends Serializable

/**
  *
  * 用户在线时长表结构说明
  * @param curDay 日期(格式yyyyMMdd)
  * @param userMac 手机mac
  * @param startTime 当次区域开始时间戳
  * @param endTime 当次区域结束时间戳
  * @param brand 手机品牌
  * @param isNewTime 是否当前新增入关入次(0:否,1:是)
  * @param areaId 当次区域编码
  *
  */
case class hdfs_view_duration(val curDay : String,val userMac : String,val startTime : String,
                               val endTime : String, val brand : String = "",
                              val isNewTime : Int = 0,val areaId : String = "") extends Serializable


/**
  * 分钟旅客类型汇总表
  * @param time       创建日期
  * @param usermac    用户MAC
  * @param usertype   标签
  * @param stype      区域块
  */
case class hdfs_user_type(var time:String=null,var usermac:String=null,var usertype:String=null,var stype:String=null) extends Serializable

/**
  * 用户标签表
  * @param curDay 创建日期
  * @param userMac 用户MAC
  * @param label 标签
  * @param brand 手机品牌
  */
case class hdfs_view_label(val curDay : String ,val userMac : String ,
                           val label : String, val brand : String = "") extends Serializable
