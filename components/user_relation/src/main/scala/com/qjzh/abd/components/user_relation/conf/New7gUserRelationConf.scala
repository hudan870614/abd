package com.qjzh.abd.components.user_relation.conf

/**
  * Created by 92025 on 2017/1/9.
  */
object New7gUserRelationConf {
  val g7_user_relate_info = "7g_user_relate_info"    //前端查询表
  val g7_user_relate_info_detail = "7g_user_relate_info_detail"   //内部自己使用表    用于存全量的关联关系
  val g7_user_relate_info_ten_five_data = "g7_user_relate_info_ten_five_data" //内部自己使用的表,建十五天有效期ttl在列上
  val g7_user_relate_info_log_table = "7g_user_relate_info_log_table"   //重点人关联关系推送个日志表

  val sinTimes: Int = 2    //重点人历史单日次数过滤阀值
  val reTimes: Int = 2     //当前重点人近期十五天过关天数过滤阀值


  //type_date_userMac

  val col_name = "data"    //固定的列名
  val rowKey = "rowKey"   //固定的rowkey

  val prefix_xiahuaxian = "_"     //固定的符号

  val batchTime = 60
  val hbaseTableColTime = 60 * 60 * 24 * 15
}
