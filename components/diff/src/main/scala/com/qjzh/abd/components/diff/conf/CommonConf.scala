package com.qjzh.abd.components.diff.conf

/**
  * Created by 92025 on 2016/12/20.
  */
object CommonConf {
  //新需求 区域表数据 ap表数据 关口配置信息
  var tBaseAreaRedisKey = "RKP_AREA"
  var tBaseApRedisKey = "RKP_AP"
  var tBaseRuleRedisKey ="RKP_RULE"

  //============预警告警
  val EW_AW_REDIS_KEY = "sparkStreaming:info:staDetail"
  /**
    * 告警类型。1：警戒告警，2：骤增告警，3：滞留告警；4：重点旅客告警；5：总客流告警；6：总重点旅客告警；7：预测告警；8：重点滞留告警
    */

}
