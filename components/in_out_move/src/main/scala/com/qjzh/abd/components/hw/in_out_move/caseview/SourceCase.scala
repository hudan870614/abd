package com.qjzh.abd.components.hw.in_out_move.caseview

import scala.collection.mutable.ListBuffer

/**
  * Created by 92025 on 2017/2/22.
  */
object SourceCase {

}

/**
  * 区块级别维表信息
  * @param areaId              区域ID
  * @param areaLevel          区域级别
  * @param parentAreaId       父区块id
  * @param isMigrateArea      区块是否做迁徙计算
  */
case class AreaInfoJoin(var areaId: Long,var areaLevel: Long,parentAreaId: Long,isMigrateArea: Long) extends Serializable

//级别  1    2    3
//一级   1
//二级   2 3 4
//三级   5 6 7 8
//关系   1 ---->  2 3 4
//       2 ---->  5
//       3 ---->  6 7
//       4 ---->  8

/**
  * redis中临时缓存的人的上一次定位信息
  * @param serverTimeStamp
  * @param lastAreaNo
  * @param userMac
  */
case class ReportRedisCache(var serverTimeStamp:Long,var lastAreaNo:Long,var userMac: String) extends Serializable

/**
  * 分析出的迁入迁出信息
  * @param serverTimeStamp
  * @param inAreaNo
  * @param outAreaNo
  * @param userMac
  * @param areaNoAndLevel
  */
case class ReportInOutMove(var serverTimeStamp:Long,var inAreaNo:Long,var outAreaNo: Long,var userMac: String,var areaNoAndLevel: String) extends Serializable

/**
  * 最热迁入视图
  * @param serverTimeStamp   服务器时间
  * @param inAreaNo           迁入区域ID
  * @param sum                 迁入人数
  * @param perce               占比
  * @param view                视图
  * @param level               级别
  * @param allSumIn            所有迁入人数
  */
case class WriteHeatInToHbase(var serverTimeStamp:Long,var inAreaNo:Long,var sum: Long,perce:Double,var view: Long,var level: Long,var allSumIn: Long) extends Serializable

/**
  * 迁出最热视图
  * @param serverTimeStamp   服务器时间
  * @param outAreaNo          迁出区号
  * @param sum                 人数
  * @param perce               占比
  * @param view                视角
  * @param level               级别
  * @param allSumOut          所有迁出人之和
  */
case class WriteHeatOutToHbase(var serverTimeStamp:Long,var outAreaNo:Long,var sum: Long,perce:Double,var view: Long,var level: Long,var allSumOut: Long) extends Serializable

/**
  * 最热路线视图
  * @param serverTimeStamp  服务器时间
  * @param inAreaNo          迁入区号
  * @param outAreaNo         迁出区号
  * @param sum                人数
  * @param view               视角
  * @param level              级别
  */
case class WriteHeatInOutToHbase(var serverTimeStamp:Long,var inAreaNo:Long,var outAreaNo:Long,var sum: Long,var view: Long,var level: Long) extends Serializable


//------------------------单点查询
/**
  * 单点查询时的实体
  * @param viewAndLevel     视图级别
  * @param inAreaNo          迁入的目标区域
  * @param inAllSum           总迁入人数
  * @param inListArea       从哪些区域迁入或迁出来的列表
  */
case class WriteInSimple(var viewAndLevel: String,var inAreaNo: Long,inAllSum: Long,var inListArea: List[SimpleInOut]) extends Serializable

/**
  *
  * @param viewAndLevel  视图级别
  * @param outAreaNo     迁出的目标区域
  * @param outAllSum     总迁出人数
  * @param outListArea   迁出到哪些目标区域去了
  */
case class WriteOutSimple(var viewAndLevel: String,var outAreaNo: Long,outAllSum: Long,var outListArea: List[SimpleInOut]) extends Serializable

/**
  *
  * @param areaNo   区域ID
  * @param sum      人数
  * @param perce    占比
  */
case class SimpleInOut(var areaNo: Long,var sum: Long, var perce: Double) extends Serializable

/**
  * 单点迁入迁出趋势点
  * @param areaNo               区域ID
  * @param sum                  人数
  * @param serverTimeStamp     时间
  * @param viewAndLevel                 视图级别
  */
case class SimpleInOutTrenPoint(var viewAndLevel: String,var areaNo: Long,var sum: Long,var serverTimeStamp: Long) extends Serializable