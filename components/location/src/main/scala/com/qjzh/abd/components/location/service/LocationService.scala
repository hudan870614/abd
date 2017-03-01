package com.qjzh.abd.components.location.service

import com.qjzh.abd.components.comp_common.common.{HbaseBusUtils, RedisBusUtils}
import com.qjzh.abd.components.location.common.LocateUtil
import com.qjzh.abd.components.location.common.caseview.CaseClass.{APCircle, Coordinate}
import com.qjzh.abd.components.location.common.conf.CommonConf
import com.qjzh.abd.components.location.common.utils.{CommonUtils, KalmanFilter, MobileUtils, PositionUtil}
import com.qjzh.abd.components.location.dao.HgDaoImpl
import com.qjzh.abd.control.common.view.Report
import com.qjzh.abd.function.hbase.utils.HbaseUtils
import org.apache.spark.streaming.dstream.DStream


/**
  * Created by yufeiwang on 19/12/2016.
  */
object LocationService {

  /**
    * calculate the user's location
    *
    * @param userRssiList
    * @return (userMac, Coordinate)
    */
  def calculateLoc(userRssiList: DStream[(String, Iterable[Report])]): DStream[(String, String, Coordinate)] = {
    val userLoc = userRssiList.map(x => {
      var brand = ""
      val pointMap = x._2.toList.sortBy(_.rssi).map(x => {
        val apMac = x.apMac
        val rssi = x.rssi.toFloat
        val brand = x.brand

        //Calculate the distance between mobile and AP
        val dist = MobileUtils.getDistWithBrand(brand, rssi)

        //Shape the circle with center of AP location and r of distance
        val apDevicesInfo = HgDaoImpl.getApDevicesFromRedis.filter(_.mac.equalsIgnoreCase(apMac))
        var apCircle: APCircle = null
        if (apDevicesInfo.nonEmpty) {
          val apMacInfo = apDevicesInfo.head
          apCircle = APCircle(apMacInfo.lat, apMacInfo.lon, dist, apMac, 0)
        }
        apCircle

      }).distinct.toArray

      //calculate the coordiante for this user's location
      val col: Coordinate = LocateUtil.calculateLoc(pointMap)

      //Save the pre-calculated ap point map and the calculated user's location to hbase for testing
      val ce = (Long.MaxValue - System.currentTimeMillis()).toString
      HgDaoImpl.hbaseWriteTable(ce, x._1 + "_1", CommonUtils.gson.toJson(pointMap))
      HgDaoImpl.hbaseWriteTable(ce, x._1 + "_2", CommonUtils.gson.toJson(col))

      (x._1, brand, col)
    })
    userLoc
  }

  /**
    * calculate the user's position using prototype algorithm
    *
    * @param userRssiList
    * @return
    */
  def calculatePos(userRssiList: DStream[(String, Iterable[Report])]): DStream[(String, String,String, (Coordinate,Coordinate))] = {
    val userLoc = userRssiList.map(x => {
      var brand = ""
      val pointMap = x._2.toList.sortBy(_.rssi).map(x => {
        val apMac = x.apMac
        val rssi = x.rssi.toFloat
        brand = x.brand

        //当前场强的距离
        val range = CommonConf.pointRange.elements().filter(x => {
          var isRun = false
          if (x != null && x._1._1 >= rssi && x._1._2 <= rssi) {
            isRun = true
          }
          isRun
        }).head._2

        //得到当前AP的点位
        //        val apDevicesInfo = HgDaoImpl.getApDevicesFromRedis.filter(_.mac.equalsIgnoreCase(apMac))
        //        var apCircle: APCircle = null
        //        if (apDevicesInfo.nonEmpty) {
        //          val apMacInfo = apDevicesInfo.head
        //          apCircle = APCircle(apMacInfo.lon, apMacInfo.lat, range, apMac, 0)
        //        }
        //        apCircle
        var apCircle: APCircle = null
        if (CommonConf.apDevices.containsKey(apMac)) {
          val x = CommonConf.apDevices.get(apMac)._1.toInt
          val y = CommonConf.apDevices.get(apMac)._2.toInt
          apCircle = APCircle(x, y, range, apMac, 0)
        }
        apCircle
      }).distinct.toArray

      //calculate the coordiante for this user's location
      val col: (Coordinate,Coordinate) = PositionUtil.tcl2(pointMap)

      //Save the pre-calculated ap point map and the calculated user's location to hbase for testing
      val ce = (Long.MaxValue - System.currentTimeMillis()).toString
      HgDaoImpl.hbaseWriteTable(ce, x._1 + "_1", CommonUtils.gson.toJson(pointMap))
      HgDaoImpl.hbaseWriteTable(ce, x._1 + "_2", CommonUtils.gson.toJson(col._1))

      val area = getDistinctArea(col._2.x, col._2.y)

      (x._1, brand, area, col)
    })
    userLoc
  }

  /**
    * for testing
    *
    * @param userRssiList
    * @return
    */
  def calculatePosTest(userRssiList: DStream[(String, Iterable[Report])]): DStream[(String, String, String, Coordinate)] = {
    val userLoc = userRssiList.map(x => {
      var brand = ""
      val pointMap = x._2.toList.sortBy(_.rssi).map(x => {
        val apMac = x.apMac
        val rssi = x.rssi.toFloat
        brand = x.brand

        //当前场强的距离
        val range = CommonConf.pointRange.elements().filter(x => {
          var isRun = false
          if (x != null && x._1._1 >= rssi && x._1._2 <= rssi) {
            isRun = true
          }
          isRun
        }).head._2

        //得到当前AP的点位
        //        val apDevicesInfo = HgDaoImpl.getApDevicesFromRedis.filter(_.mac.equalsIgnoreCase(apMac))
        //        var apCircle: APCircle = null
        //        if (apDevicesInfo.nonEmpty) {
        //          val apMacInfo = apDevicesInfo.head
        //          apCircle = APCircle(apMacInfo.lon, apMacInfo.lat, range, apMac, 0)
        //        }
        //        apCircle
        var apCircle: APCircle = null
        if (CommonConf.apDevices.containsKey(apMac)) {
          val x = CommonConf.apDevices.get(apMac)._1.toInt
          val y = CommonConf.apDevices.get(apMac)._2.toInt
          apCircle = APCircle(x, y, range, apMac, 0)
        }
        apCircle
      }).distinct.toArray

      //calculate the coordiante for this user's location
      val col: Coordinate = PositionUtil.tcl2Test(pointMap)

      //Save the pre-calculated ap point map and the calculated user's location to hbase for testing
      val ce = (Long.MaxValue - System.currentTimeMillis()).toString
      HgDaoImpl.hbaseWriteTable(ce, x._1 + "_1", CommonUtils.gson.toJson(pointMap))
      HgDaoImpl.hbaseWriteTable(ce, x._1 + "_2", CommonUtils.gson.toJson(col))

      //      val area = getArea(col.x,col.y)
      val area = getDistinctArea(col.x, col.y)
      (x._1, brand, area, col)
    })
    userLoc
  }

  private def getDistinctArea(x: Double, y: Double): String = {
    var area = CommonConf.DEFAULT
    var coors: List[Double] = CommonConf.dongtongArea
    if (x >= coors(0) && x <= coors(1) && y >= coors(2) && y <= coors(3)) {
      area = CommonConf.DONGTONG
    }
    coors = CommonConf.xitongArea
    if (x >= coors(0) && x <= coors(1) && y >= coors(2) && y <= coors(3)) {
      area = CommonConf.XITONG
    }
    area
  }

  /**
    * 根据坐标判断区域
    *
    * @param x
    * @param y
    * @return
    */
  private def getArea(x: Double, y: Double): String = {
    var area = ""
    val keyIt = CommonConf.areaCoors.keySet().iterator()
    while (keyIt.hasNext) {
      val key = keyIt.next()
      val coors: List[Double] = CommonConf.areaCoors.get(key)(0)
      if (x >= coors(0) && x <= coors(1) && y >= coors(2) && y <= coors(3)) {
        area = key
      }
    }
    area
  }

  /**
    * 将区域人数统计保存到redis
    *
    * @param stream
    */
  def saveAreaCount(stream: DStream[(String, String, String, (Coordinate,Coordinate))]): Unit = {
    val keyIt = CommonConf.areaCoors.keySet().iterator()

    while (keyIt.hasNext) {
      val key = keyIt.next()
      stream.filter(x => x._3.equalsIgnoreCase(key)).count().foreachRDD(rdd => {
        rdd.foreachPartition(p => {
          p.foreach(x => {
             if(key.equals(CommonConf.DONGTONG)){
               RedisBusUtils.lpush(CommonConf.eastRedisKey, x.toString())
               determinEastOrWest()
            }else if(key.equals(CommonConf.XITONG)){
               RedisBusUtils.lpush(CommonConf.westRedisKey, x.toString())
               determinEastOrWest()
             }
          })
        })
      })
    }
  }

  def determinEastOrWest() : Unit={
    RedisBusUtils.ltrim(CommonConf.eastRedisKey,0,4)
    RedisBusUtils.ltrim(CommonConf.westRedisKey,0,4)

    val eastCounts : List[String] = RedisBusUtils.lrange(CommonConf.eastRedisKey,0,4)
    val westCounts : List[String] = RedisBusUtils.lrange(CommonConf.westRedisKey,0,4)
    val forceSwitch : String = RedisBusUtils.get(CommonConf.forceSwitch)

    var eastTotal : Long = 0l
    var westTotal : Long = 0l

    for (elem <- eastCounts) {
      eastTotal=eastTotal+elem.toLong
    }
    for (elem <- westCounts) {
      westTotal=westTotal+elem.toLong
    }

    if(eastCounts.length != 0 && westCounts.length != 0){
      eastTotal = eastTotal/eastCounts.length
      westTotal = westTotal/westCounts.length

      if (!forceSwitch.equalsIgnoreCase("east") && !forceSwitch.equalsIgnoreCase("west")){
        if(eastTotal<=westTotal){
          CommonConf.westNotEast=true
        }else{
          CommonConf.westNotEast=false
        }
      }else{
        if (forceSwitch.equalsIgnoreCase("east")){
          CommonConf.westNotEast=false
        }else if (forceSwitch.equalsIgnoreCase("west")){
          CommonConf.westNotEast=true
        }
      }

      RedisBusUtils.set("count_advice",CommonConf.westNotEast.toString,20)
      HbaseUtils.writeTable(CommonConf.eastOrWestTable,System.currentTimeMillis().toString,"data",CommonConf.westNotEast.toString)
    }
  }


  /**
    * send the user's location coordinates to kafka
    *
    * @param stream
    */
  def sendToKafka(stream: DStream[(String, String, String,(Coordinate,Coordinate))]) = {
    stream.filter(_._4._1 != null).foreachRDD(rdd => {
      rdd.foreachPartition(d => {
        d.foreach(x => {

          val userMac = x._1
          val brand = x._2
          val pointX = x._4._1.x
          val pointY = x._4._1.y

          val report = new Report()
          report.userMac = userMac
          report.apMac = "00:00:00:00:00:00"
          report.coordinateX = pointX
          report.coordinateY = pointY
          report.timeStamp = System.currentTimeMillis()
          report.floor = null
          report.mapID = "1"
          report.brand = brand
          report.projectNo = com.qjzh.abd.components.comp_common.conf.CommonConf.project_no
          report.areaNo = ""

          HgDaoImpl.kafkaSendMsg(CommonConf.topics, report.toString)
        })
      })
    })
  }

  /**
    * 判断AP mac是否在此项目AP列表中
    *
    * @param arg
    * @return
    */
  def isApInList(arg: (String, Report)): Boolean = {
    var r: Boolean = false
    val macs = HgDaoImpl.getApDevicesFromRedis().filter(_.mac.equalsIgnoreCase(arg._2.apMac))
    if (macs.length > 0) {
      r = true
    }
    r
  }

  def isApInList2(arg: (String, Report)): Boolean = {
    CommonConf.apDevices.containsKey(arg._2.apMac)
  }

  /**
    * Perform Kalman filter on the batch of reports
    *
    * @param args
    * @return
    */
  def sampleFilter(args: Iterable[Report]): Report = {
    var rssiList: List[Double] = Nil
    for (elem <- args) {
      rssiList = rssiList :+ (elem.rssi).toDouble
    }
    val KFestimate = KalmanFilter.filter(rssiList)
    val r: Report = args.head
    r.rssi = KFestimate.toString
    r
  }


}
