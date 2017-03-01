package com.qjzh.abd.components.user_relation.caseview


/**
  * Created by 92025 on 2017/1/10.
  */
object CaseView {

}

/**
  * 关联对象
  * @param times  关联次数
  * @param lastUpdate  上次更新时间
  * @param firstTime   第一次关联时间
  * @param reMac        被关联userMac
  */
case class G7UserRelateInfo(val times: String,val lastUpdate: Long,val firstTime:Long,val reMac: String) extends Serializable

/**
  * 关联对象
  * @param times  关联次数
  * @param lastUpdate  上次更新时间
  * @param firstTime   第一次关联时间
  */
case class G7UserRelateInfoDetail(val times: String,val lastUpdate: Long,val firstTime:Long) extends Serializable

/**
  *
  * @param mac           当前重点人MAC
  * @param reMacs         关联的userMac
  * @param x              X坐标
  * @param y              Y坐标
  * @param areaId        所属区域ID
  * @param reTimes       最近关联次数
  * @param sinTimes      单日关联次数
  */
case class G7UserRelateInfoToKafka(val mac: String,
                                   val reMacs: java.util.List[RaletionMacInfo],
                                   val x: Float,
                                   val y: Float,
                                   val areaId: Int,
                                   val reTimes: Int,
                                   val sinTimes: Int) extends Serializable

/**
  * 被关联mac信息
  * @param reMac
  * @param ralTimes
  */
case class RaletionMacInfo(val reMac: String,val ralTimes: Int) extends Serializable