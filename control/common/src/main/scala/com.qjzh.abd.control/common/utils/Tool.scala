package com.qjzh.abd.control.common.utils

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import com.qjzh.abd.components.comp_common.caseciew.MessageKafka
import com.qjzh.abd.components.comp_common.utils.GsonTools
import com.qjzh.abd.control.common.view.{Report}
import org.apache.parquet.it.unimi.dsi.fastutil.objects.ObjectArrayList
import org.json4s.ShortTypeHints
import org.json4s.jackson.{JsonMethods, Serialization}
/**
  * Created by hushuai on 16/3/5.
  */
object Tool {

  val gson = new Gson()


  def getReportByJsonStr(str : String):Report = {
    implicit val formats = Serialization.formats(ShortTypeHints(List()))

    val reportJsonObject = JsonMethods.parse(str)
    val report = new Report
    report.apMac = (reportJsonObject \ "apMac").extractOrElse[String](null)
    report.areaNo = (reportJsonObject \ "areaNo").extractOrElse[String](null)
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

    report.userMac = Tool.trimMac(report.userMac)
    report.apMac = Tool.trimMac(report.apMac)
    report.isFake = Tool.isFakeMac(report.userMac)

    report

  }


  def getInfoByJsonStr[T](str : String):MessageKafka[ObjectArrayList[T]] = {
    val data = GsonTools.gson.fromJson[MessageKafka[ObjectArrayList[T]]](str,
      new TypeToken[MessageKafka[ObjectArrayList[T]]](){}.getType());
    data
  }
  /**
    * 统一去分号，全大写
    * @param mac
    * @return
    */
  private def trimMac(mac:String):String ={
    val apMacTag = mac.replaceAll("\\:","")
    apMacTag.trim.toUpperCase()
  }


  /**
    * 是否伪码
    * @param mac
    * @return
    */
  private def isFakeMac(mac:String):Int ={
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





}
