package com.qjzh.abd.components.warn_water_line.conf

/**
  * Created by 92025 on 2016/12/21.
  */
object HwWarnWaterConf {
  //HBASE---------CONSTANTS
  val ColumnFamilyName = "data"  //列族的默认名称
  val ColumnName = "data"        //列的默认名称
  val PREFIX = "_"

  //表1(存储区块警戒水位详细记录表):  用于保存区块人数和时间的没分钟详细记录
  val HBASE_TABLE_AREA_HISTORY_DETAIL = "hw_hw_sparkstream_warnwaterline"
  val HBASE_TABLE_AREA_HISTORY_DETAIL_ROWKEY_PREFIX = "_area_history_detail"

  //表2(存储区块警戒水位预警告警日志表): 用于保存每分钟区块的告警预警信息
  val HBASE_TABLE_AREA_LOG_DETAIL = "hw_hw_sparkstream_warnwaterline"
  val HBASE_TABLE_AREA_LOG_DETAIL_ROWKEY_PREFIX = "_area_log_detail"


  //表3(存储ap警戒水位详细记录表):  用于保存ap人数和时间的没分钟详细记录
  val HBASE_TABLE_AP_HISTORY_DETAIL = "hw_hw_sparkstream_warnwaterline"
  val HBASE_TABLE_AP_HISTORY_DETAIL_ROWKEY_PREFIX = "_ap_history_detail"
  //表4(存储ap警戒水位预警告警日志表): 用于保存每分钟ap的告警预警信息
  val HBASE_TABLE_AP_LOG_DETAIL = "hw_hw_sparkstream_warnwaterline"
  val HBASE_TABLE_AP_LOG_DETAIL_ROWKEY_PREFIX = "_ap_log_detail"


  //REDIS---------CONSTANTS
  //存储区块的历史峰值redis_key
  val REDIS_HW_HW_SPARK_WARNWATERLINE = "hw_hw_sparkstream_warnwaterline"
  val REDIS_CACHE_TTL_SECONDS = 1 * 60 * 5   //redis的缓存时长



  val test_path = "F:\\123\\"
}
