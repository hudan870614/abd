package com.qjzh.abd.components.location.exe

import com.qjzh.abd.components.location.service.LocationService
import com.qjzh.abd.control.common.utils.ExeType
import com.qjzh.abd.control.common.view.Report
import com.qjzh.abd.control.exe.CommonExe
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

/**
  * Created by yufeiwang on 19/12/2016.
  */
object LocationExe extends CommonExe {

  //执行类型
  override def exeType: String = ExeType.ON_POSITION

  //优先级
  override def priority: Int = super.priority

  //执行
  override def exe[T: ClassTag](line: DStream[T]): Unit = {

    if (line.isInstanceOf[DStream[Report]]) {
      var data = line.asInstanceOf[DStream[Report]].map(report => {
        (report.userMac + "_" + report.apMac, report)
      })

      //filter the data with AP macs known to this project
      data = data.filter(_._2.projectNo.equalsIgnoreCase("hg_sk"))
        //.filter(LocationService.isApInList)
        .filter(LocationService.isApInList2)
        .filter(_._2.pointX == 0)

      //Collect data using spark streaming window
      val windowData = data.groupByKeyAndWindow(Seconds(30), Seconds(10))

      //Perfomr Kalman filter on collected RSSI data
      //TODO Anonymous function converible to a method value
      val filteredData = windowData.mapValues(LocationService.sampleFilter(_))

      //Input formating : userMac as key, ap info as value
      val userRssiList = filteredData.map(x => {
        (x._2.userMac, x._2)
      }).groupByKey()

      //Calculate the location coordinate of the user
      val userLocation = LocationService.calculateLoc(userRssiList)

      //Send user's location detail to Kafka message queue
//      LocationService.sendToKafka(userLocation)
    }
  }

  //执行(离线)
  override def exe(sparkcontent: SparkContext): Unit = ???

  //执行前预处理
  override def init(args: Array[String]): Unit = ???
}
