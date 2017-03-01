package com.qjzh.abd.components.forecast.view

/**
  * Created by damon on 2017/1/6.
  */
object ForecastView {

}

/**
  * 解析对象
  * @param time  YYYYMMDDHHmm年月日时分
  * @param ymd    年月日
  * @param hourmin   时分
  * @param hour      时
  * @param minute    分
  * @param stype  区域块
  * @param daynum    天人数
  * @param hournum   时人数
  * @param minutenum 分人数
  */
case class ForecastCass(var time:String = null,var ymd:String = null,var week:Int = 2,
                   var hourmin:String = null, var hour:String = null,var minute:String = null,
                   var stype:String = null, var minutenum:Int=0,var hournum:Int=0,var daynum:Int=0) extends Serializable
case class ForecastHbaseCass(var mSum:Long = 0 ) extends Serializable
case class SendForecastCass(var rowKey:String="",var mSum:Long = 0 ) extends Serializable
/**
  *
  * @param date     日期YMD
  * @param hour     小时点
  * @param stype    区域
  * @param flownum  预测人流数
  */
case class ForecastRddCass(var date:String=null,var hour:String=null,var stype:String=null,
                           var flownum:Long=0)

/**
  *
  * @param hour
  * @param flownum
  */
case class HourFlowCass(var hour:String,var flownum:Int)

/**
  *
  * @param similar  欧式距离相似度
  * @param pearson  皮尔森相关系数
  * @param residual 残差和
  * @param rmse     均方根误差
  */
case class AnalyseHbaseCass(var similar:Double,var pearson:Double,var residual:Double,var rmse:Double)extends Serializable
