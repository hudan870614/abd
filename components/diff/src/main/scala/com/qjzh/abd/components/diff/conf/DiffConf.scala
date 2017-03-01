package com.qjzh.abd.components.diff.conf

/**
  * Created by 92025 on 2016/12/28.
  */
object DiffConf {
  //============区块骤增配置
  val REDIS_LASTER_DIFF_KEY = "sparkStreaming:diff:module:laster:tenminutedata"  //保存上十分数据的key
  val REDIS_CRU_DIFF_KEY = "sparkStreaming:diff:module:cru:tenminutedata"   //保存当前值的key


  //  val HBASE_DIFF_TABLE = "t_test"   //保存在hbase的骤增表
  val HBASE_DIFF_TABLE = "7g_warnwaterlevel"   //保存在hbase的骤增表

  val COLUMNE = "data"   //hbase保存数据的列名
  val OUTER = "OUTER"    //室外的标记

  val DIFF_MODULE_NAME = "diff"  //模块的名称
}
