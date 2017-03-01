package com.qjzh.abd.components.in_out_move.utils

import com.qjzh.abd.components.in_out_move.conf.HbaseIncrTableConf
import com.qjzh.abd.components.in_out_move.dao.HbaseIncrDao

/**
  * Created by yufeiwang on 12/01/2017.
  */
object IncrTableUtils {


  //历史最大次数
  def getHistoryMaxCount(userMac : String) : String ={
    HbaseIncrDao.HbaseSessionUtils.readTable(
      HbaseIncrTableConf.incrTableName, userMac, HbaseIncrTableConf.max_fName, HbaseIncrTableConf.max_previous_qualifier)
  }

  //最近15天总天数
  def getHalfMonthDayCount(userMac : String) : String ={
    HbaseIncrDao.HbaseSessionUtils.countColumnOfFamily(
      HbaseIncrTableConf.incrTableName, userMac, HbaseIncrTableConf.fifteen_dayCount_fName).toString
  }

}
