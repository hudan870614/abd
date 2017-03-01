package com.qjzh.abd.function.kafka.utils

import java.util.Properties

import com.qjzh.abd.function.kafka.conf.{KafkaConfContext, KafkaConfigAndProperties}
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

/**
  * Created by 92025 on 2016/12/16.
  */
class KafkaProUtils {
  def getProducerConfig(brokerAddr: String) : Properties = {

    /*val props = new Properties()
    props.put("metadata.broker.list", brokerAddr)
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("key.serializer.class", "kafka.serializer.StringEncoder")
    props*/

    KafkaConfigAndProperties.properties
  }

  def sendMessages(topic: String, messages: String, kafkaUrl:String) {
    val producer = new Producer[String, String](new ProducerConfig(getProducerConfig(kafkaUrl)))
    producer.send(new KeyedMessage[String, String](topic, scala.util.Random.nextString(5) ,messages))
    producer.close()
  }
}

object KafkaProUtils{
  var kafkaUtils =  new KafkaProUtils()

  def sendMessages(topic: String, messages: String): Unit ={
    kafkaUtils.sendMessages(topic, messages, KafkaConfContext.kafka_defalt_topic)
  }
}
