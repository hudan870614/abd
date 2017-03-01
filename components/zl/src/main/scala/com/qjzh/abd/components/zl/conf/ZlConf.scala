package com.qjzh.abd.components.zl.conf

import com.qjzh.abd.components.comp_common.common.RedisBusUtils

/**
  * Created by 92025 on 2016/12/21.
  */
object ZlConf {
  //滞留配置
  val REDIS_KEY_ZL = "sparkStreaming:warnwaterlevel:module:zl"   //计算滞留用的临时usermac
  val REDIS_KEY_ZL_USER_MAC = "sparkStreaming:warnwaterlevel:module:zl:user:mac"    //滞留的用户mac前缀   实际===  REDIS_KEY_ZL_USER_MAC + "_" + yyyyMMdd
  val ZL_HEAT_TIME_DRUATION = 60   //分钟单位        允许多长时间没有探测到滞留的mac

  //常量
  val DRUATION_DAT = RedisBusUtils.second_one_day   //一天时长
  val DRUATION_HOUR = RedisBusUtils.second_one_hour    //一小时时长
}
