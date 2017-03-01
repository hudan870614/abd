package com.qjzh.abd.components.comp_common.utils

import com.qjzh.abd.components.comp_common.caseciew.{MessageKafka, _}
import com.qjzh.abd.components.comp_common.common.{KafkaUtils, RedisBusUtils}
import com.qjzh.abd.control.common.view.Report
import com.qjzh.abd.function.common.DateUtils
import org.apache.parquet.it.unimi.dsi.fastutil.objects.ObjectArrayList
import org.apache.spark.HashPartitioner
import org.apache.spark.streaming.dstream.DStream
import org.json4s.ShortTypeHints
import org.json4s.jackson.{JsonMethods, Serialization}

import scala.reflect.ClassTag

/**
  * Created by 92025 on 2016/12/19.
  */
object ComUtils {

  val upstep_num = 1000000

  /**
    * 发送某个模块下的所有区域告警信息
    */
  def sendEaWaMessageToKafkaRddPrrocess[T:ClassTag](xx: DStream[MessageKafka[T]],topic: String,callBackData: MessageKafka[T]): Unit ={
    val combineByKey: DStream[(Int, MessageKafka[ObjectArrayList[T]])] = xx.map((1, _)).combineByKey(x => {
      val list: ObjectArrayList[T] = new ObjectArrayList[T]()
      list.add(x.value)
      MessageKafka(x._type, x.timeStamp, list)
    }, (x: MessageKafka[ObjectArrayList[T]], y: MessageKafka[T]) => {
      x.value.add(y.value)
      x
    }, (x: MessageKafka[ObjectArrayList[T]], y: MessageKafka[ObjectArrayList[T]]) => {
      x.value.addAll(y.value)
      x
    }, new HashPartitioner(1))

    combineByKey.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        rdd.foreach(ka =>{
          if(ka._2 != null && ka._2.value.size() > 0){
            val _2: MessageKafka[ObjectArrayList[T]] = ka._2
            KafkaUtils.sendMsgToKafka(topic,_2)
          }
        })
      }else{
        KafkaUtils.sendMsgToKafka(topic,MessageKafka(callBackData._type,DateUtils.getTime(System.currentTimeMillis(), "yyyyMMddHHmm").toLong,callBackData.value))
      }
    })
  }

  /**
    * 发送某个模块下的所有区域告警信息
    */
  def sendEaWaMessageToKafkaRdd[T:ClassTag](xx: DStream[MessageKafka[T]],topic: String): Unit ={
    xx.map((1,_)).combineByKey(x => {
      val list: ObjectArrayList[T] = new ObjectArrayList[T]()
      list.add(x.value)
      MessageKafka(x._type, x.timeStamp, list)
    }, (x: MessageKafka[ObjectArrayList[T]], y: MessageKafka[T]) => {
      x.value.add(y.value)
      x
    }, (x: MessageKafka[ObjectArrayList[T]], y: MessageKafka[ObjectArrayList[T]]) => {
      x.value.addAll(y.value)
      x
    }, new HashPartitioner(1)).foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
         rdd.foreach(ka =>{
           if(ka._2 != null && ka._2.value.size() > 0){
             val _2: MessageKafka[ObjectArrayList[T]] = ka._2
             KafkaUtils.sendMsgToKafka(topic,_2)
           }
         })
      }
    })

  }

  def getReportByJsonStr(str : String):Report = {
    implicit val formats = Serialization.formats(ShortTypeHints(List()))
    val reportJsonObject = JsonMethods.parse(str)

    val report = new Report
    report.apMac = (reportJsonObject \ "apMac").extractOrElse[String](null)
    report.areaNo = (reportJsonObject \ "areaID").extractOrElse[String](null)
    report.pointX = (reportJsonObject \ "coordinateX").extractOrElse[Float](0)
    report.pointY = (reportJsonObject \ "coordinateY").extractOrElse[Float](0)
    report.pointZ = (reportJsonObject \ "coordinateZ").extractOrElse[Float](0)
    report.rssi = (reportJsonObject \ "rssi").extractOrElse[String]("0")
    report.timeStamp = (reportJsonObject \ "timeStamp").extractOrElse[String]("0").toLong
    report.userMac = (reportJsonObject \ "userMac").extractOrElse[String](null)
    report.apType = (reportJsonObject \ "type").extractOrElse[String](null)
    report.brand = (reportJsonObject \ "brand").extractOrElse[String](null)
    report.projectNo = (reportJsonObject \ "projectNo").extractOrElse[String](null)
    report.mapID = (reportJsonObject \ "mapID").extractOrElse[String](null)
    report.floor = (reportJsonObject \ "floor").extractOrElse[String](null)

    report.userMac = ComUtils.trimMac(report.userMac)
    report.apMac = ComUtils.trimMac(report.apMac)
    report.isFake = ComUtils.isFakeMac(report.userMac)

    report

  }

  def trimMac(mac:String):String ={
    val apMacTag = mac.replaceAll("\\:","")
    apMacTag.trim.toUpperCase()
  }

  def isFakeMac(mac:String):Int ={
    val containFakeList = Array("2", "3", "6", "7", "A", "B", "E", "F")

    var str : Int = 0
    if(!mac.isEmpty){
      val tag = mac.substring(1,2)
      val results = containFakeList.filter(_.equalsIgnoreCase(tag))
      if(results.length > 0){
        str = 1
      }
    }
    str
  }
  /**
    * 判断当前点位是否在所属区域下
    * @param point 目标点位
    * @param area 关联区域
    * @return (1:属于区域内;0:在区域边线上;-1:在区域之外)
    */
  def isContainInArea(point : Report, area: SynAreaBean) : Int = {

    var flag = -1
    val points = area.points
    val points_arr = points.split("\\;")
    val maxX = points_arr.map(_.split("\\,")(0).toDouble).max
    val minX = points_arr.map(_.split("\\,")(0).toDouble).min
    val maxY = points_arr.map(_.split("\\,")(1).toDouble).max
    val minY = points_arr.map(_.split("\\,")(1).toDouble).min

    if(point.pointX >= minX && point.pointX <= maxX && point.pointY >= minY && point.pointY <= maxY ){
      flag = 1
    }
    flag
  }
  def main(args: Array[String]): Unit = {

    // 114.122759,22.550609
    val report = new Report()
    report.pointX = 114.122759D
    report.pointY = 22.550609D

//    RedisBusUtils.getAreaInfoByLonLat(report.pointX,report.pointY).foreach(println)

    println(getMinMaxLngLat("11,22;13,23;44,45"))

//    RedisBusUtils.getRedisAreaMap().filter(x => {
//      ComUtils.isContainInArea2(report.pointX,report.pointY, x.points)
//    }).foreach(println)

//    RedisBusUtils.getRedisAreaMap().map(x => {
//      (x.id,ComUtils.isContainInArea2(report.pointX,report.pointY, x.points))
//    }).filter(_._2 != -1).foreach(println)

//    println(ComUtils.isContainInArea2(report.pointX,report.pointY,
//      "114.119166,22.55136;114.123101,22.553647;114.127664,22.550125;114.123047,22.547839"))


  }
  /**
    * 判断当前点位是否在所属区域下
    * @param lon 经度
    * @param lat 纬度
    * @param points 关联区域范围
    * @return (true:在区域边线上;false:在区域之外)
    */
  def isContainInArea2(lon : Double,lat : Double, points: String) : Boolean = {

    val points_arr = points.split("\\;")

    val lonList = points_arr.map(_.split("\\,")(0).toDouble).toList
    val latList = points_arr.map(_.split("\\,")(1).toDouble).toList

    LonLatUtils.isPointInPolygon(lon,lat,lonList,latList)

  }

  /**
    * 在main方法中获取手动设置的日期参数，为空则取上一个小时的日期
    * @param args
    * @return
    */
  def getBeforeDayByPrams(args: Array[String]): String ={
    var beforeDay = DateUtils.getBeforeDay("yyyy/MM/dd", -1)
    if(args != null && args.length > 0){
      val Array(generateDay) = args
      if(generateDay != null && generateDay.indexOf("/") != -1){
        beforeDay = generateDay
      }
    }
    beforeDay
  }


  def getMinMaxLngLat(points: String):(Double,Double,Double,Double) = {
    val points_arr = points.split("\\;")
    val minX = points_arr.map(_.split("\\,")(0).toDouble).min
    val maxX = points_arr.map(_.split("\\,")(0).toDouble).max
    val minY = points_arr.map(_.split("\\,")(1).toDouble).min
    val maxY = points_arr.map(_.split("\\,")(1).toDouble).max
    (minX,maxX,minY,maxY)
  }






}
