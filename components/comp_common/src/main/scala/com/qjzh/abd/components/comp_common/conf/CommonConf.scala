package com.qjzh.abd.components.comp_common.conf

/**
  * Created by 92025 on 2016/12/21.
  */
object CommonConf {


  val project_no = "sw"

  /**
    * 基础配置
    */
  var hdfsDic = "/new7g"
  //重点人员
  var redis_imp_user_name_key: String = project_no+"_RKP_RP"

  var LABELS = "_LABELS"


  //新需求 区域表数据 ap表数据 关口配置信息
  var tBaseAreaRedisKey = project_no+"_RKP_AREA"
  var tBaseApRedisKey = project_no+"_RKP_AP"
  var tBaseRuleRedisKey =project_no+"_RKP_RULE"

  /***
    * 基础配置表
    */
  var tBaseLocationRedisKey = project_no+"_RKP_LOCATION"

  /**
    * 边长
    */
  var area_side_dis : Double =  500

  /**
    * 告警类型。1：警戒告警，2：骤增告警，3：滞留告警；4：重点旅客告警；5：总客流告警；6：总重点旅客告警；7：预测告警；8：重点滞留告警
    */
  val WARN_AREA = "1"
  val WARN_AREA_DIFF = "2"
  val WARN_AREA_DELAY = "3"
  val WARN_AREA_CORE_PERSON = "4"

  val WARN_TOTAL_PERSON = "5"
  val WARN_TOTAL_CORE_PERSON = "6"
  val WARN_TOTAL_PREDI_PERSON = "7"
  val WARN_TOTAL_CORE_DELAY_PERSON = "8"

  val WARN_LEVEL_EW = "ew"
  val WARN_LEVEL_AW = "aw"

  /**
    * 警戒告警类型
    */
  //1.骤增人数
  val WARN_AREA_DIFF_NUM: Long = 1
  //2.警戒水位人数、
  val WARN_AREA_WATER_LINE_NUM: Long = 2
  //3.实时人数、
  val WARN_AREA_NUM: Long = 3
  //  4.热力区块图人数、
  val WARN_AREA_HEAT_NUM: Long = 4
  //  5.热力图重点旅客人数
  val WARN_AREA_CORE_NUM: Long = 5

  //  6.重点旅客明细推送
  val WARN_AREA_CORE_DETAIL:Long = 6

  //  8.按天实际值推送
  val OFFLINE_STA_MEM_BY_DAY:Long = 8

  val WARN_AREA_DELAY_NUM : Long = 7

  //  9.按天重点旅客分类汇总
  val OFFLINE_IMP_STA_BY_DAY: Long = 9



  /**
    * 离线预测
    */
  val FORECAST_DELAY_NUM: Long = 7
  /**
    * 实时重点人全量key
    * 实时全量key
    * 重点人
    * 滞留重点人
    */
  val IMP_ALL = "IMP_ALL"
  val ALL = "ALL"
  val IMP = "IMP"
  val RET = "RET"
  val FIR = "FIR"
  val OCC = "OCC"
  val STA = "STA"
  val ACT = "ACT"


  // 全量手机mac关联 标签
  //1.1  工作人员
  val LABLE_WORRKER_DATA = "WORKER"  //工作人员
  //1.2 重点旅客
  val LABLE_CORE_DATA = "CORE"   //重点旅客
  val LABLE_RULE_DATA = "RULE"   //自定义规则
  val LABLE_HAND_DATA = "HAND"    //手工添加
  val LABLE_NEW_DATA = "NEW"    //新增
  val LABLE_ACT_DATA = "ACT"    //活跃
  val LABLE_OCC_DATA = "OCC"    //沉默
  val LABLE_ALL_DATA = "ALL"    //全部

  val LABLE_NORMAL_DATA = "NORMAL"  //常规

  //1.3 普通旅客
  val LABLE_GUEST_DATA = "GUEST" //普通旅客
  val LABLE_FIR_DATA = "FIR"  //首次
  val LABLE_ONALLY_DATA = "ONALLY"  //偶尔
  val LABLE_OFTEN_DATA = "OFTEN"  //频繁

  val LABLE_WORRKER_HAND_DATA = LABLE_WORRKER_DATA+"_"+LABLE_HAND_DATA//工作人员—人工
  val LABLE_WORRKER_NORMAL_DATA = LABLE_WORRKER_DATA+"_"+LABLE_NORMAL_DATA//工作人员－常规

  val LABLE_CORE_HAND_DATA = LABLE_CORE_DATA+"_"+LABLE_HAND_DATA//重点人员—人工
  val LABLE_CORE_ALL_DATA = LABLE_CORE_DATA+"_"+LABLE_ALL_DATA//重点人员—全部
  val LABLE_CORE_RULE_DATA = LABLE_CORE_DATA+"_"+LABLE_RULE_DATA//重点人员—规则
  val LABLE_CORE_NEW_DATA = LABLE_CORE_DATA+"_"+LABLE_NEW_DATA//重点人员-新增
  val LABLE_CORE_ACT_DATA = LABLE_CORE_DATA+"_"+LABLE_ACT_DATA//重点人员—活跃
  val LABLE_CORE_ACC_DATA = LABLE_CORE_DATA+"_"+LABLE_OCC_DATA//重点人员—沉默

  val LABLE_GUEST_ALL_DATA = LABLE_GUEST_DATA+"_"+LABLE_ALL_DATA//普通旅客—全部
  val LABLE_GUEST_FIR_DATA = LABLE_GUEST_DATA+"_"+LABLE_FIR_DATA//普通旅客—首次
  val LABLE_GUEST_ONALLY_DATA = LABLE_GUEST_DATA+"_"+LABLE_ONALLY_DATA//普通旅客—偶然
  val LABLE_GUEST_ACT_DATA = LABLE_GUEST_DATA+"_"+LABLE_OFTEN_DATA//普通旅客—频繁








}
