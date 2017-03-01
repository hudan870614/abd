package com.qjzh.abd.components.comp_common.caseciew

import org.apache.parquet.it.unimi.dsi.fastutil.objects.ObjectArrayList


/**
  * Created by 92025 on 2016/12/23.
  */
object CaseView {

}
//redis中区块数据cass class
/**
  *

  * @param id
  * @param areaLevel 区块类别编码 如：3:区
  * @param projectNo
  * @param points
  * @param rtPnumThreshold
  * @param rtTimeThreshold
  * @param surgeThreshold
  * @param waHouStage
  * @param waMinStage
  * @param paMinStage
  * @param preSurgeThreshold
  * @param preRtPnumThreshold
  * @param importantsThreshold
  * @param preImportantsThreshold
  * @param impRetThreshold
  * @param preImpRetThreshold
  * @param active
  * @param gridLength 栅格边长
  *
  */
case class SynAreaBean(val id:Int,val areaLevel:Int = 1,val projectNo:String = null,val points:String = null,
                       val rtPnumThreshold :Float = -1, val rtTimeThreshold:Float = -1,val surgeThreshold:Float = -1,
                       val waHouStage:Float = -1,val waMinStage:Float = -1,val paMinStage: Long = 0,
                       val preSurgeThreshold :Float = -1 ,val preRtPnumThreshold:Long = 0,val importantsThreshold :Long = 0,
                       val preImportantsThreshold : Long = 0,val impRetThreshold : Long =0 , val preImpRetThreshold : Long = 0,
                       val active : Int = 1, val gridLength : Long = 500) extends Serializable

/**
  * @param x 虚拟区块的中心点位x值
  * @param y 虚拟区块的中心点位y值
  * @param mac 重点人mac
  * @param areaId 当前所属区域主键
  */
case class Point(val x : Double, val y: Double, val mac : String = "",val areaId : String = "",val floor : String = "",val radius : Int = 0) extends Serializable


/**
  *
  * @param id 主键
  * @param apStatus 状态(0:有效 1:失效)
  * @@param ruleType 规则类型  1:重点旅客  2:工作人员 3:普通旅客 val ruleType:Int = -1,
  *
  * @param isUseFi 是否启用规则一(0:启用 1:不启用)
  * @param fiLaterDay 规则一最近天数
  * @param fiEnDayBegin 规则一入境天数开始
  * @param fiEnDayEnd  规则一入境天数结束
  * @param fiCheckedSet 规则一设置在线时长/频次(0:选中 1:不选中)
  *
  * @param isUseSe  是否启用规则二(0:启用 1:不启用)
  * @param seLaterDay 规则二最近天数
  * @param seSigDayBegin 规则二单日入境次数开始
  * @param seSigDayEnd  规则二单日入境次数结束
  * @param seCheckedSet 规则二是否设置在线时长(0:选中 1:不选中)
  *
  * @param isUseTh 是否启用规则三(0:启用 1:不启用)
  * @param thLaterDay 规则三今日入境次数
  * @param thCheckedSet 规则三是否设置在线时长(0:选中 1:不选中)
  *
  * @param onlDHourBegin 每天在线时长开始
  * @param onlDHourEnd 每天在线时长结束
  * @param onlDAppearBegin 每天出现频次开始
  * @param onlDAppearEnd 每天出现频次结束
  *
  * @param dateType 日期类型(1:所有日期{默认},2:工作日,3:周末)
  * @param timeSegs 多个区间
  *
  * @param isUseFo
  *
  *
  */
case class SynBaseRuleBean(val id:Int, val apStatus:String,
                           val isUseFi:Int = -1, val fiLaterDay:Int = -1,val fiEnDayBegin:Int = -1,val fiEnDayEnd:Int = -1, val fiCheckedSet:Int = -1,
                           val isUseSe:Int = -1,val seLaterDay:Int = -1,val seSigDayBegin:Int = -1,val seSigDayEnd:Int = -1,val seCheckedSet:Int = -1,
                           val isUseTh:Int = -1,val thLaterDay:Int = -1,val thCheckedSet:Int = -1,
                           val onlDHourBegin : Int = -1, val onlDHourEnd : Int = -1,val onlDAppearBegin: Int = -1 ,val onlDAppearEnd: Int = -1,
                           val dateType : Int = 1, val timeSegs : ObjectArrayList[SynBaseRuleTimeSegsBean],val isUseFo : Int = 1) extends Serializable


/**
  * 规则区间
  * @param begin
  * @param end
  */
case class SynBaseRuleTimeSegsBean(val begin:String, val end:String ) extends Serializable


/**
  * @param alarm 总客流警戒值
  * @param htGridSideLen 边长
  * @param kpAlarm 重点旅客警戒值
  * @param maxMapPixel 最大像素值
  * @param preAlarm 总客流预警值
  * @param preKpAlarm 重点旅客预警值
  */
case class SettingRedisData(val alarm:Long = 0,val htGridSideLen:Double = 0.0D,
                            val kpAlarm:Long = 0,val maxMapPixel:String = null,
                            val preAlarm : Long = 0,val preKpAlarm : Long = 0,
                            val projectNo : String) extends Serializable


/**
  * json字符串：{“mac”: “58696C36F8F1”, “type”:, 1,”source”:1}
          mac: MAC地址
          type: 人员类型  1:重点旅客，2:工作人员 3:普通旅客
          source: 来源  1:手工添加，2:规则判断
  * @param mac
  * @param rpType
  * @param source
  */
case class ImportUserMac(val mac : String,val rpType: Int, val source : Int) extends Serializable


/**
  * 消息推送格式
  * @param _type:类型
  * @param timeStamp:时间(格式:yyyyMMddHHmm)
  * @param value(简单可json属性化对象)
  */
case class MessageKafka[T](val _type: Long,val timeStamp: Long, val value: T) extends Serializable


/**
  * 消息推送格式
  * @param topic:主题
  * @param sendMsg:多个MessageKafka对象
  */
case class ToKafka[T](val topic: String, val sendMsg: Array[MessageKafka[T]]) extends Serializable

/**
  * 基础点位视图
  * @param apLocCode 点位编码
  * @param apMac mac地址
  * @param lon 点位经度
  * @param lat 点位纬度
  * @param paMinStage 预警水位(人/min)
  * @param paHouStage 预警水位(人/h)
  * @param waMinStage 警戒水位(人/min)
  * @param waHouStage 警戒水位(人/h)
  * @param surgeThreshold 骤增警戒阈值
  * @param preSurgeThreshold 骤增预警阈值
  */
case class SynApDeviceBean(apLocCode:String, apMac:String,lon:Double = -1,lat : Double = -1,
                           paMinStage:Int = 0,paHouStage:Int = 0,waMinStage:Int = 0,waHouStage:Int = 0,
                           surgeThreshold:Int = 0,preSurgeThreshold:Int = 0) extends Serializable


/**
  * @param mac 重点人MAC String
  * @param lastDate 最后上报时间(yyyyDDMM) String
  * @param times 总次数 Int
  * @param duration 总在线时长(分钟单位) Double
  * @param status 状态[0:正常，1：失效(超过指定日期再也没有扫描到)] Int
  * @param labels 关联标签（规则外键）
  */
case class RedisImpDetailCase(val mac : String,val lastDate: String, val times: Int, val duration:Double, val status : Int = 0, val labels : String = "-1") extends Serializable


