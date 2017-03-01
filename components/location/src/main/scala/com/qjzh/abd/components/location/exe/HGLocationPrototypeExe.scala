package com.qjzh.abd.components.location.exe

import com.qjzh.abd.components.comp_common.common.RedisBusUtils
import com.qjzh.abd.components.location.common.conf.CommonConf
import com.qjzh.abd.components.location.common.utils.LocationPropertiesUtils
import com.qjzh.abd.components.location.service.{LocationCommonService, LocationService}
import com.qjzh.abd.control.common.utils.ExeType
import com.qjzh.abd.control.common.view.Report
import com.qjzh.abd.control.exe.CommonExe
import com.qjzh.abd.function.common.PropertiesUtils
import com.qjzh.abd.function.hbase.utils.HbaseUtils
import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

/**
  * Created by yufeiwang on 21/12/2016.
  * HG prototype for location. Used only for prototype demo.
  */
object HGLocationPrototypeExe extends CommonExe {
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

      //找出Properties文件中定位模块启动的项目
      val projNames = LocationPropertiesUtils.getSelectedProjectNames()

      //过滤出相关ap
      data = data.filter(x => projNames.contains(x._2.projectNo))//.filter(_._2.projectNo.equalsIgnoreCase("hg_sk"))
//        .filter(LocationService.isApInList)
          .filter(LocationService.isApInList2)
        .filter(_._2.pointX == 0)

      //Using userMac as key to group all rssi info
      val userRssiList = data.groupByKey()

      //调用借口方法
      val userLocation = LocationCommonService.calculatePos(userRssiList,projNames)

      LocationService.saveAreaCount(userLocation)

      //Send user's location detail to Kafka message queue
      LocationService.sendToKafka(userLocation)
    }
  }

  //执行(离线)
  override def exe(sparkcontent: SparkContext): Unit = ???

  //执行前预处理
  override def init(args: Array[String]): Unit = {
    HbaseUtils.createTableInTTL(CommonConf.eastOrWestTable,60*10)
    RedisBusUtils.set(CommonConf.forceSwitch,"default")
  }
}
