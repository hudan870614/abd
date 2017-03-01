package com.qjzh.abd.control.common.utils

/**
  * Created by hushuai on 16/12/15.
  */
object ExeType extends Enumeration {

  //一分钟
  val ON_ONE_MIN = "ON_ONE_MIN"

  //预清洗
  val ON_PRE_SHUFFLE = "ON_PRE_SHUFFLE"

  //定位
  val ON_POSITION = "ON_POSITION"

  //离线一小时
  val OF_ONE_HOUR = "OF_ONE_HOUR"

  //离线每天
  val OF_ONE_DAY = "OF_ONE_DAY"

  //预测
  val OF_FORECAST = "OF_FORECAST"

  //分钟在线预测
  val ON_FORECAST = "ON_FORECAST"
}
