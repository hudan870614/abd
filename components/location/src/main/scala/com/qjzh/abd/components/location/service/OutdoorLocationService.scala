package com.qjzh.abd.components.location.service

import com.qjzh.abd.components.comp_common.caseciew.SynApDeviceBean
import com.qjzh.abd.components.comp_common.common.RedisBusUtils
import com.qjzh.abd.components.comp_common.conf.CommDataTest
import com.qjzh.abd.components.location.common.LocateUtil
import com.qjzh.abd.components.location.common.caseview.CaseClass.{APCircle, Coordinate}
import com.qjzh.abd.components.location.common.conf.CommonConf
import com.qjzh.abd.components.location.common.utils.MobileUtils
import com.qjzh.abd.components.location.dao.HgDaoImpl
import com.qjzh.abd.control.common.view.Report
import com.qjzh.abd.function.hbase.utils.HbaseUtils
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable

/**
  * Created by yufeiwang on 24/02/2017.
  */
object OutdoorLocationService {

  val apInfoList : mutable.HashMap[String,SynApDeviceBean] = new mutable.HashMap[String,SynApDeviceBean]()
  for (elem <- CommDataTest.SynApDeviceBeanTest) {
    apInfoList.put(elem.apMac,elem)
  }
  /**
    * calculate the user's location
    *
    * @param userRssiList
    * @return (userMac, Coordinate)
    */
  def calculateLoc(userRssiList: DStream[(String, Iterable[Report])]): DStream[(String, String, Coordinate)] = {
    val userLoc = userRssiList.map(x => {
      val brand = ""
      val pointMap = x._2.toList.sortBy(_.rssi).map(x => {
        val apMac = x.apMac
        val rssi = x.rssi.toFloat
        val brand = x.brand

        //Calculate the distance between mobile and AP
        val dist = MobileUtils.getDistWithBrand(brand, rssi)

        //Shape the circle with center of AP location and r of distance
//        val apDevicesInfo = HgDaoImpl.getApDevicesFromRedis.filter(_.mac.equalsIgnoreCase(apMac))
        val apDeviceInfo = apInfoList.get(apMac)
        var apCircle: APCircle = null
        if (apDeviceInfo.nonEmpty) {
          val apMacInfo = apDeviceInfo.head
          apCircle = APCircle(apMacInfo.lon, apMacInfo.lat, dist, apMac, 0, Nil)
        }
        apCircle

      }).distinct.toArray

      //calculate the coordiante for this user's location
      val col: Coordinate = LocateUtil.calculateOutdoorLoc(pointMap)

      (x._1, brand, col)
    })
    userLoc
  }

  /**
    * Check if AP is in this project≈
    * @param arg
    * @return
    */
  def isAPInList(arg: (String, Report)): Boolean = {
    apInfoList.contains(arg._2.apMac)
  }

  /**
    * send the user's location coordinates to kafka
    *
    * @param stream
    */
  def sendToKafka(stream:  DStream[(String, String, Coordinate)]) = {

    stream.filter(_._3 != null).foreachRDD(rdd => {
      rdd.foreachPartition(d => {
        d.foreach(x => {

          val userMac = x._1
          val brand = x._2
          val lon = x._3.x
          val lat = x._3.y

          //点位信息
          val apMac : String = x._3.apMac
          val apPoint = RedisBusUtils.getApPointByApMac(apMac)

          //根据当前经纬度找到所属多个区域
          val areaNos = RedisBusUtils.getAreaInfoByLonLat(apPoint.lon,apPoint.lat)


          val report = new Report()
          report.userMac = userMac
          report.apMac = apMac
          report.coordinateX = apPoint.lon
          report.coordinateY = apPoint.lat
          report.timeStamp = System.currentTimeMillis()
          report.mapID = apPoint.apLocCode
          report.brand = brand
          report.projectNo = com.qjzh.abd.components.comp_common.conf.CommonConf.project_no
          if(!areaNos.isEmpty && areaNos.size > 0){
            report.floor = areaNos.mkString(";")
            report.areaNo = areaNos(0)
          }
          HbaseUtils.writeTable(CommonConf.testHWOrWestTable,System.currentTimeMillis().toString,"data",report.toString)
          HgDaoImpl.kafkaSendMsg(CommonConf.topics, report.toString)
        })
      })
    })
  }
}
