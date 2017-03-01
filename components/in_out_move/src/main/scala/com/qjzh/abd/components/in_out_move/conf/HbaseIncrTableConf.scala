package com.qjzh.abd.components.in_out_move.conf

import com.qjzh.abd.components.comp_common.common.HbaseBusUtils

/**
  * Created by yufeiwang on 29/12/2016.
  */
object HbaseIncrTableConf {


  /** ------------------------------------------累加表------------------------------------------------*/
  val weekRecordTTL = 90*24*60*60 //seconds


  val ffteenTTL = 15*24*60*60 //seconds
  // 最近15天天数统计
  val fifteen_dayCount_fName : String = "ffteen_dayCount_ttl"

  //累计分钟
  val min_fName :String ="total"
  val min_qualifier : String ="min"

  //完成首次出现时间，最后一次出现时间
  val time_stamp_fName: String = "time_stamp"
  val ts_first_qualifier: String = "first_appear"
  val ts_last_qualifier: String = "last_appear"


  //出现次数天数汇总
  val appear_fName : String= "appear"
  val appear_count_qualifer: String = "count"
  val appear_dayCount_qualifier: String = "dayCount"

  //时间段汇总
  val time_slot_fName : String= "time_slot"
  val slot_prefix : String= ""//slot_"

  //单天最大出现次数
  val max_fName : String= "max"
  val max_cur_qualifier : String="current"
  val max_previous_qualifier: String ="previous"

  //周次数, ttl:90*24*60*60
  val week_count_fName: String="count_ttl"


  //周天数, ttl:90*24*60*60
  val week_dayCount_fName: String="dayCount_ttl"



  //累计表
  val incrTableName : String ="7g_user_desc_incr"


  /** ------------------------------------------展示表------------------------------------------------*/
  val descShowTable = HbaseBusUtils.user_gather_descri
//  val descShowTable = "7g_gather_desc_show"
  val default_column = HbaseBusUtils.hbase_common_cloumn_family



}
