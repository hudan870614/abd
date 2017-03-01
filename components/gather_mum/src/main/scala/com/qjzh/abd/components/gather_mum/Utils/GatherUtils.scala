package com.qjzh.abd.components.gather_mum.Utils

import java.util

import com.google.gson.{JsonArray, JsonElement, JsonParser}
import com.qjzh.abd.components.comp_common.caseciew.SynAreaBean
import com.qjzh.abd.components.comp_common.common.RedisBusUtils
import com.qjzh.abd.components.comp_common.conf.CommonConf
import com.qjzh.abd.components.comp_common.utils.{ComUtils, GsonTools, LabelTools}
import com.qjzh.abd.components.gather_mum.caseview.Point
import com.qjzh.abd.control.common.view.{BatchReportCollect, Report}
import com.qjzh.abd.function.common.DateUtils
import it.unimi.dsi.fastutil.objects.ObjectArrayList

/**
  * Created by yufeiwang on 26/12/2016.
  */
object GatherUtils {


  /**
    * Call ComUtils to check if the report is in a specific area
    * @param report
    * @param area
    * @return
    */
  def isContainInArea(report : Report, area: SynAreaBean) : Boolean ={
    ComUtils.isContainInArea2(report.pointX,report.pointY, area.points)
  }


  /**
    * JSON转成list集合
    *
    * @param json
    * @param clazz
    * @return
    */
  def jsonStringToList[T](json: String, clazz: Class[T]): util.ArrayList[T] = {

    val lst: util.ArrayList[T] = new util.ArrayList[T]()
    if (json == null || "".equals(json)) {
      return lst
    }

    val array: JsonArray = new JsonParser().parse(json).getAsJsonArray()
    if (array != null && array.size() != 0) {
      for (i <- 0 to array.size() - 1) {
        val element: JsonElement = array.get(i)
        val json1: T = GsonTools.gson.fromJson(element, clazz)
        lst.add(json1)
      }
    }
    lst
  }


  /**
    * 根据最大点位获取虚拟点位的中心点
    *
    * @param colletion
    * @param aimPoint
    * @return
    */
  def getNearAreaCenter(colletion: ObjectArrayList[Point], aimPoint: Point): Point = {
    val resPoint = colletion.elements().filter(_ != null).map(x => {
      val _x = Math.abs(x.x - aimPoint.x)
      val _y = Math.abs(x.y - aimPoint.y)
      val distance = Math.sqrt(_x * _x + _y * _y)
      (x, distance)
    }).minBy(_._2)

    resPoint._1
  }

  /**
    * 根据最大点位获取虚拟点位的中心点
    *
    * @param maxPoint
    * @param side
    * @return
    */
  def getLittleAreaCenter(maxPoint: Point, side: Float): ObjectArrayList[Point] = {
    val reportList = new ObjectArrayList[Point]()
    var isXRun = true
    var x: Float = 1
    while (isXRun) {
      val center_x = x * (side / 2)
      if (center_x >= maxPoint.x) {
        isXRun = false
      } else {
        var y: Float = 1
        var isYRun = true
        while (isYRun) {
          val center_y = y * (side / 2)
          if (center_y >= maxPoint.y) {
            isYRun = false
          } else {
            //            println("x="+center_x+",y="+center_y)
            val point = Point(center_x, center_y)
            reportList.add(point)
            y = y + 1
          }
        }
        x = x + 1
      }

    }
    reportList
  }

  /**
    * 按点位及区域汇总
    *
    * @param report
    * @return
    */
  def getAllAreaInfoByRecode(report: Report): ObjectArrayList[BatchReportCollect] = {

    val staList = new ObjectArrayList[BatchReportCollect]()

    //当前设备
    val cur_report = new Report()
    cur_report.userMac = report.userMac
    cur_report.apMac = report.apMac
    cur_report.timeStamp = report.timeStamp
    cur_report.apType = report.apType
    cur_report.isFake = report.isFake
    cur_report.serverTimeStamp = report.serverTimeStamp

    val batchData = new BatchReportCollect
    batchData.report = cur_report
    batchData.reportMap.put(cur_report.userMac, cur_report)

    staList.add(batchData)


    RedisBusUtils.getRedisAreaMap().filter(x => {
      GatherUtils.isContainInArea(report, x)
    }).foreach(x => {
      //当前区域的
      val hs_al_report = new Report()
      hs_al_report.userMac = report.userMac
      hs_al_report.timeStamp = report.timeStamp
      hs_al_report.apType = x.id.toString + "_" + CommonConf.ALL
      hs_al_report.apMac = x.id.toString + "_" + CommonConf.ALL
      hs_al_report.isFake = report.isFake
      hs_al_report.serverTimeStamp = report.serverTimeStamp

      val hs_all_report = new BatchReportCollect
      hs_all_report.report = hs_al_report
      hs_all_report.reportMap.put(hs_al_report.userMac, hs_al_report)
      staList.add(hs_all_report)

    })

    //总数据
    val al_report = new Report()
    al_report.userMac = report.userMac
    al_report.timeStamp = report.timeStamp
    al_report.apType = CommonConf.ALL
    al_report.apMac = CommonConf.ALL
    al_report.isFake = report.isFake
    al_report.serverTimeStamp = report.serverTimeStamp

    val all_report = new BatchReportCollect
    all_report.report = al_report
    all_report.reportMap.put(al_report.userMac, al_report)
    staList.add(all_report)


    staList
  }


}
