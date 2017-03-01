package com.qjzh.abd.components.in_out_move.conf

import com.qjzh.abd.components.comp_common.common.HbaseBusUtils

/**
  * Created by 92025 on 2016/12/24.
  */
object New7gUserTraceConf {
  val batchTime: Long = 60
  val kafkaBrokerList: String = "slave1:9092,slave2:9092,slave3:9092"
  val kafkaTopicSet: String = "7g_mac_flow"
  val kafkaOffsetAuto: String = "largest"




  val Hbase_Table_Name_Trace_gather = HbaseBusUtils.user_gather_descri     //移动终端迁入迁出明细表
  val Hbase_Table_Name_Trace_IN_OUT = HbaseBusUtils.user_inout_descri     //存储用户出入关记录表

}
