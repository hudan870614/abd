package com.qjzh.abd.components.hw.in_out_move.conf

/**
  * Created by 92025 on 2017/2/22.
  */
object HwHwInOutMoveConf {
  val SYS_PREFIX = "_"
  //Hbase迁入迁出表名
  val HBASE_IN_OUT_MOVE_TABLE_NAME = "hw_hw_sparkstream_in_out_move"

  //-----------最热迁入迁出配置
  //最热迁入迁出rowkey后缀
  val HBASE_HEAT_IN_OUT_MOVE_PREFIX = "_in_out_heat"
  //最热迁出rowkey后缀
  val HBASE_HEAT_OUT_MOVE_PREFIX = "_out_heat"
  //最热迁入rowkey后缀
  val HBASE_HEAT_IN_MOVE_PREFIX = "_in_heat"

  //-----------单点查询配置
  //单点迁入rowkey后缀
  val HBASE_SINGLE_POINT_IN_MOVE_PREFIX = "_in_single"
  //单点迁入所有人数列名后缀
  val HBASE_SINGLE_POINT_IN_MOVE_SUM_PREFIX = "_ALL_IN"

  //单点迁出rowkey后缀
  val HBASE_SINGLE_POINT_OUT_MOVE_PREFIX = "_out_single"
  //单点迁入所有人数列名后缀
  val HBASE_SINGLE_POINT_OUT_MOVE_SUM_PREFIX = "_ALL_OUT"

  //迁入迁出的时间序列人数
  val HBASE_STREAM_IN_OUT_MOVE_PREFIX = "_in_out_stream"



  //redis缓存信息
  //1.-------------迁入迁出缓存信息
  val REDIS_HW_HW_SPARKSTREAM_IN_OUT_MOVE = "hw_hw_sparkstream_in_out_move_"


  //2.redis数据缓存时间设置
  val REDIS_TTL_FOR_REPORT_REDISCACHE = 5 * 60


  //redis取出key
  //1.最热路线key
  val REDIS__KEY_IN_OUT_HEAT_PREFIX = "_in_out_heat"
  //2.最热迁出路线
  val REDIS__KEY_OUT_HEAT_PREFIX =  "_out_heat"
  //3.最热迁入路线
  val REDIS__KEY_IN_HEAT_PREFIX =  "_in_heat"
  //4.单点迁入查询
  val REDIS__KEY_IN_SINGLE_PREFIX = "_in_single"
  //5.单点迁出查询
  val REDIS__KEY_OUT_SINGLE_PREFIX = "_out_single"

  //6.单点迁入趋势
  val REDIS__KEY_IN_SINGLE_TREN_PREFIX = "_in_tren_single"
  //6.单点迁出趋势
  val REDIS__KEY_OUT_SINGLE_TREN_PREFIX = "_out_tren_single"

  val REDIS_KEY_TREN_LENTH = 24 * 60
}
