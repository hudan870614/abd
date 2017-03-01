package com.qjzh.abd.components.zl.service

import com.qjzh.abd.components.comp_common.caseciew.{MessageKafka, SynAreaBean}
import com.qjzh.abd.components.comp_common.conf.CommonConf
import com.qjzh.abd.components.comp_common.utils.{ CompRedisClientUtils, GsonTools}
import com.qjzh.abd.components.zl.caseview.EwAndAwRedisData
import com.qjzh.abd.components.zl.conf.ZlConf
import com.qjzh.abd.control.common.view.Report
import com.qjzh.abd.function.common.DateUtils
import com.qjzh.abd.components.comp_common.common.RedisBusUtils

/**
  * Created by 92025 on 2016/12/21.
  */
object ZlService {

  def computerZl(yy: (String, (Report, Int))): MessageKafka[EwAndAwRedisData] = {

    //          println("xxxxxxxxxxxxxxxxxxxxxx" + yy)
    val _1: Report = yy._2._1      //
    val _2: Int = yy._2._2         //该区域的滞留人数总和
    val areaNo: SynAreaBean = CompRedisClientUtils.getAreaInfoByFloorAndAreaNo(_1.floor,_1.areaNo.toLong)
   /* SendAlarmMsgToKafkaServiceImpl.sendAlarmMsgToKafka(CommonConf.oline_one_minute_mub,
      GsonTools.gson.toJson(
        EaWaMessageToKafka(CommonConf.WARN_AREA_DELAY_NUM,DateUtils.getTime(_1.serverTimeStamp,"yyyyMMddHHmm").toLong,
          EwAndAwRedisData(_1.areaNo.toLong,_1.timeStamp,_2,CommonConf.WARN_AREA_DELAY_NUM,"",areaNo.rtPnumThreshold.toLong,areaNo.floor))))*/
    /*EaWaMessageToKafka(CommonConf.WARN_AREA_DELAY_NUM,DateUtils.getTime(_1.serverTimeStamp,"yyyyMMddHHmm").toLong,
      ComUtils.getEaWaMessageInfoList(EwAndAwRedisData(_1.areaNo.toLong,_1.timeStamp,_2,CommonConf.WARN_AREA_DELAY_NUM,"",areaNo.rtPnumThreshold.toLong,areaNo.floor)))*/

    MessageKafka[EwAndAwRedisData](CommonConf.WARN_AREA_DELAY_NUM,DateUtils.getTime(_1.serverTimeStamp,"yyyyMMddHHmm").toLong,EwAndAwRedisData(_1.areaNo.toLong,_1.timeStamp,_2,CommonConf.WARN_AREA_DELAY_NUM,"",areaNo.rtPnumThreshold.toLong,areaNo.floor))
  }
  /**
    * 滞留旅客入库
    * @param xx
    */
  def zlUserMacToRedis(xx: (String, Report)) = {
    val timeDay: String = DateUtils.getCurTime("yyyyMMdd")
    val key: String = ZlConf.REDIS_KEY_ZL_USER_MAC + "_" + timeDay
    RedisBusUtils.hset(key,xx._2.userMac,GsonTools.gson.toJson(xx._2),RedisBusUtils.second_one_day)
  }

}
