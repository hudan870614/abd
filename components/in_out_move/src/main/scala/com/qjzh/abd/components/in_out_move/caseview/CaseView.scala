package com.qjzh.abd.components.in_out_move.caseview

import java.util.ArrayList

/**
  * Created by 92025 on 2016/12/26.
  */
object CaseView {

}

case class InAndOutHbase(val inTime: Long, val outTime: Long, val areaId: Long) extends Serializable

case class Days(days: ArrayList[String]) extends Serializable

case class InAndOutFix(val simpleTime: Long, isCruDay: String, isSimple: String, userMac: String, areaNo: Long, startTime: Long, endTime: Long) extends Serializable

/**
  * 单点轨迹记录视图
  *
  * @param curSTMin  当次开始分钟(HHmm)
  * @param curETMin  当次开始分钟(HHmm)
  * @param isFirst   是否当天首次(0:否,1:是)
  * @param isNewTime 是否为新增当次(0:否,1:是)
  * @param userMac   手机mac
  * @param areaNo    当前区域
  * @param startTime 当次区域开始时间戳
  * @param endTime   当次区域结束时间戳
  * @param brand     手机品牌
  */
case class UserMacPointTrajectory(val curSTMin: String, var curETMin: String,
                                  val isFirst: Int = 0, val isNewTime: Int = 0,
                                  val userMac: String, val areaNo: String,
                                  val startTime: Long, var endTime: Long, val brand: String = "") extends Serializable


/**
  *
  * @param userMac
  * @param countTotal        总次数
  * @param dayCountTotal     总天数
  * @param mostFreqTSlot     最频繁出现时间段
  * @param minuteTotal       总时长，单位分钟
  * @param lastApprDate      最新出现时间
  * @param firstApprDate     首次出现时间
  * @param countAvgByWeek    周平均次数
  * @param dayCountAvgByWeek 周平均天数
  * @param maxByDay          单日最大日期
  * @param avgDay            日均次数
  * @param isFirst           是否当天新入关
  */
case class UserStatics(userMac: String, countTotal: String, dayCountTotal: String, mostFreqTSlot: String,
                       minuteTotal: String, lastApprDate: String,
                       firstApprDate: String, countAvgByWeek: String, dayCountAvgByWeek: String,
                       maxByDay: String, avgDay: String, isFirst: String)

/**
  *
  * @param labels      标识[人群标签]
  * @param sourceId    来源关联标签(为规则ID，默认为-1(其它标签))
  * @param oftenHour   频繁出现时段
  * @param dSum        计出现天数
  * @param minSum      累计出现分钟数
  * @param lastTime    最新探测时间[时间戳]
  * @param firstTime   首次探测时间[时间戳]
  * @param avgWeTiNum  周平均(总次数)
  * @param avgWeDaNum  周平均(总天数)
  * @param dayHigTiNum 单日出现最高次数
  * @param avgDayTiNum 日均出现次数
  * @param lastUpdate  服务器最后更新时间[时间戳]
  */
case class UserStaticsJson(val labels: String, val sourceId: String, val oftenHour: String, val dSum: String, val minSum : String, val lastTime: String, val firstTime: String,
                           val avgWeTiNum: String, val avgWeDaNum: String, val dayHigTiNum: String, val avgDayTiNum: String, val lastUpdate: String)
