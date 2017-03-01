package com.qjzh.abd.components.gather_mum.caseview

/**
  * Created by 92025 on 2016/12/19.
  */
object CaseView {

}

/**
  * @param x 虚拟区块的中心点位x值
  * @param y 虚拟区块的中心点位y值
  * @param mac 重点人mac
  * @param areaId 当前所属区域主键
  */
case class Point(val x : Double, val y: Double, val mac : String = "",val areaId : String = "",val floor : String = "",val radius : Int = 0) extends Serializable

/**
  *
  * @param areaId 当前行政区域主键
  * @param x 虚拟点位经度
  * @param y 虚拟点位纬度
  * @param radius 边长
  * @param value 当前虚拟点人数
  * @param curMinue 当前分钟
  */
case class PointMember(val areaId:String,val x : Double, val y: Double, val radius : Double = 0,val value : Long = 0,val curMinue: String) extends Serializable

//===========================警戒水位用到的cass
//一小时最大最小实体
case class AreaHourData(val hourMaxSum:Long = 0,val maxDateTime:Long = 0,val hourMinSum:Long = 0,val minDateTime:Long = 0) extends Serializable
//历史最大实体
case class AreaHisMax(val hisMaxSum:Long = 0,val hisDateTime:Long = 0)
//Hbase中对应的警戒水位实体类
case class HbaseWarnData(val areaHisMax: AreaHisMax,val areaHourData: AreaHourData,
                         val areaId:Long = 0,val dateTime:Long = 0,val sum:Long = 0,val floor:Long = 0)



/**
  * //redis中存储的滞留人信息
  * @param startTime 起始时间
  * @param endTime  结束时间
  * @param areaId  区域id
  * @param floor  楼层
  * @param duration 多长时间(分中)
  */
case class RedisZLData(val startTime: Long = 0,val endTime: Long = 0,val areaId:Long = 0,val floor:Long = 0,val duration: Long = 0) extends Serializable

/**
  * 小时分钟汇总被探测到的用户   rowKey    时间(小时) + _ + apMac
  * @param userMac
  * @param timeStamp
  * @param isFake
  * @param apMac
  * @param serverTime
  */
case class HbaseBeanUserMacDetailMinuteAndHour(userMac:String = null,timeStamp:Long = 0,apTimeStamp:Long = 0,isFake:String = null,apMac:String = null,serverTime:Long = 0) extends Serializable

/**
  *根据用户的mac  找出历史所有被探测的记录
  */
case class HbaseBeanApMacDetail(userMac:String = null,timeStamp:Long = 0,apTimeStamp:Long = 0,isFake:String = null,apMac:String = null,serverTime:Long = 0) extends Serializable
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


//汇总人数
/**
  *
  * @param curMinue 当前分钟
  * @param busNo 当前业务编码(stype为0设备mac，stype为1设备区域编码)
  * @param mSum 当前分钟值
  * @param hSum 当前小时值
  * @param dSum 当天最大值
  * @param hisMax  历史最大值
  * @param hisMaxTime 历史最大值时间
  * @param holdTimes 平均停驻时间
  * @param rowKey 当前主建
  * @param stype(0:设备mac,1:区域编码)
  */
case class StaMemberCaseInfo(val curMinue: String,val busNo: String,val mSum: Long,val hSum: Long,val dSum:Long,val hisMax:Long,val hisMaxTime:Long,val holdTimes:Long,val rowKey : String = "",val stype: Int = 0) extends Serializable

