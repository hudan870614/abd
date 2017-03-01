package com.qjzh.abd.components.comp_common.common

import com.qjzh.abd.components.comp_common.caseciew.{MessageKafka, ToKafka}
import com.qjzh.abd.components.comp_common.conf.CommonConf
import com.qjzh.abd.components.comp_common.utils.GsonTools
import com.qjzh.abd.function.kafka.utils.KafkaProUtils

/**
  * Created by hushuai on 16/12/28.
  */
object KafkaUtils {

  /**
    * topic 名称
    *
    */
  //实时告警topic
  val oline_one_minute_mub = CommonConf.project_no+"_oline_one_minute_mub"
  //离线详细topic
  val off_line_one_day_mub = CommonConf.project_no+"_off_line_one_day_mub"

  /**
    * 发送kafka
    * @param kafkaTrait
    */
  def sendMsgToKafka[T](kafkaTrait : ToKafka[T]): Unit = {
    val topic = kafkaTrait.topic
    val sendMsg = kafkaTrait.sendMsg
      if(sendMsg.length > 0){
        KafkaProUtils.sendMessages(topic,GsonTools.gson.toJson(sendMsg))
      }
  }

  /**
    * 发送到kafka
    * @param topic
    * @param msg
    * @tparam T
    */
  def sendMsgToKafka[T](topic: String,msg: MessageKafka[T]): Unit ={
    KafkaProUtils.sendMessages(topic,GsonTools.gson.toJson(msg))
  }

}
