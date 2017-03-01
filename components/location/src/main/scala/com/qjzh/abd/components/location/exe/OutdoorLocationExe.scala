package com.qjzh.abd.components.location.exe

import com.qjzh.abd.components.location.common.conf.CommonConf
import com.qjzh.abd.components.location.service.OutdoorLocationService
import com.qjzh.abd.control.common.utils.ExeType
import com.qjzh.abd.control.common.view.Report
import com.qjzh.abd.control.exe.CommonExe
import com.qjzh.abd.function.hbase.utils.HbaseUtils
import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

/**
  * Created by yufeiwang on 24/02/2017.
  */
object OutdoorLocationExe extends CommonExe {
  //执行类型
  override def exeType: String = ExeType.ON_POSITION

  //优先级
  override def priority: Int = super.priority

  //执行
  override def exe[T: ClassTag](line: DStream[T]): Unit = {

    //格式化
    if (line.isInstanceOf[DStream[Report]]) {
      var data = line.asInstanceOf[DStream[Report]].map(report => {
        (report.userMac, report)
      })

      //过滤出相关ap
      data = data.filter(OutdoorLocationService.isAPInList)
        .filter(_._2.pointX == 0)

      //Using userMac as key to group all rssi info
      val userRssiList = data.groupByKey()

      //Calculate the location coordinate of the user
      val userLocation = OutdoorLocationService.calculateLoc(userRssiList)


      //Send user's location detail to Kafka message queue
      OutdoorLocationService.sendToKafka(userLocation)
    }
  }

  //执行(离线)
  override def exe(sparkcontent: SparkContext): Unit = ???

  //执行前预处理
  override def init(args: Array[String]): Unit = {
    HbaseUtils.createTable(CommonConf.testHWOrWestTable)
  }
}
