package com.qjzh.abd.components.comp_common.utils

/**
  * Created by hushuai on 17/2/20.
  */
object LonLatUtils {

  val R : Double =  6378137D

  /**
    * 计算地球上任意两点(经纬度)距离
    * @param long1 第一点经度
    * @param lat1 第一点纬度
    * @param long2 第二点经度
    * @param lat2 第二点纬度
    * @return 返回距离 单位：米
    */
  def getDistanceByPoint(long1:Double,lat1:Double,long2:Double,lat2:Double): Double ={
    val latOne = lat1 * Math.PI / 180.0
    val latTwo = lat2 * Math.PI / 180.0
    val a = latOne - latTwo
    val b = (long1 - long2) * Math.PI / 180.0
    val sa2 = Math.sin(a / 2.0)
    val sb2 = Math.sin(b / 2.0)
    2 * R * Math.asin(Math.sqrt(sa2 * sa2 + Math.cos(latOne) * Math.cos(latTwo) * sb2 * sb2))
  }

  /**
    * 根据当前点位＼距离＼角度找到目标点位
    * @param lng1
    * @param lat1
    * @param distance 距离(km)
    * @param angle(角度，以正北向为0度，顺时钟)
    * @return
    */
  def getPointByDistance(lng1:Double,lat1:Double,distance : Double, angle : Double) : (Double,Double) = {
    //将距离转换成经度的计算公式
    val lon = lng1 + (distance * Math.sin(angle* Math.PI / 180)) / (111 * Math.cos(lat1 * Math.PI / 180))
    //将距离转换成纬度的计算公式
    val lat = lat1 + (distance * Math.cos(angle* Math.PI / 180)) / 111
    (lon,lat)
  }

  /**
    * 根据当前最小最大的经纬度＼距离＼方位算
    * @param min_point_lng
    * @param max_point_lng
    * @param min_point_lat
    * @param max_point_lat
    * @param distance
    * @return
    */
  def getCenterPointsByMinMaxLngLat(min_point_lng:Double,max_point_lng:Double,min_point_lat:Double,max_point_lat:Double, distance:Double) : List[(Double,Double)] = {
    var result : List[(Double,Double)] = Nil
    //最大纬度边长
    val lng_dis = LonLatUtils.getDistanceByPoint(min_point_lng,max_point_lat,max_point_lng,max_point_lat)
    val lat_dis = LonLatUtils.getDistanceByPoint(min_point_lng,max_point_lat,min_point_lng,min_point_lat)

    val lng_it = lng_dis / distance
    val lat_it = lat_dis / distance

    val lng_batch = Math.abs(max_point_lng - min_point_lng)/lng_it
    val lat_batch = Math.abs(min_point_lat - max_point_lat) / lat_it


    for( lat_index <-  0 to lat_it.ceil.toInt - 1){
      val point_lat = max_point_lat - lat_batch * lat_index
      for( lng_index <-  0 to lng_it.ceil.toInt) {
        val point_lng = min_point_lng + lng_batch * lng_index
        val dis = Math.sqrt(Math.pow(distance/2,2)*2)
        val centPoint = LonLatUtils.getPointByDistance(point_lng,point_lat,dis/1000,135D)
        result = result :+ (centPoint._1,centPoint._2)
      }
    }

    result
  }

  /**
    * 是否有 横断 参数为四个点的坐标
    *
    * @param px1
    * @param py1
    * @param px2
    * @param py2
    * @param px3
    * @param py3
    * @param px4
    * @param py4
    * @return
    */
  private def isIntersect(px1 : Double, py1 : Double,
                  px2 : Double, py2 : Double,
                  px3 : Double, py3 : Double,
                  px4 : Double, py4 : Double) : Boolean ={
    var flag = false
    val d = (px2 - px1) * (py4 - py3) - (py2 - py1) * (px4 - px3)
    if (d != 0) {
      val r = ((py1 - py3) * (px4 - px3) - (px1 - px3) * (py4 - py3))/ d
      val s = ((py1 - py3) * (px2 - px1) - (px1 - px3) * (py2 - py1)) / d
      if ((r >= 0) && (r <= 1) && (s >= 0) && (s <= 1)) {
        flag = true
      }
    }
    flag
  }

  private def Multiply( px0 : Double,  py0 : Double,
                px1 : Double, py1 : Double,
                px2 : Double, py2 : Double) : Double ={
    ((px1 - px0) * (py2 - py0) - (px2 - px0) * (py1 - py0))
  }

  /**
    * 目标点是否在目标边上边上
    *
    * @param px0
    *            目标点的经度坐标
    * @param py0
    *            目标点的纬度坐标
    * @param px1
    *            目标线的起点(终点)经度坐标
    * @param py1
    *            目标线的起点(终点)纬度坐标
    * @param px2
    *            目标线的终点(起点)经度坐标
    * @param py2
    *            目标线的终点(起点)纬度坐标
    * @return
    */
  private def isPointOnLine( px0 : Double, py0 : Double,
                     px1 : Double, py1 : Double,
                     px2 : Double, py2 : Double) : Boolean ={
    var flag = false
    val ESP = 1e-9;// 无限小的正数
    if ((Math.abs(Multiply(px0, py0, px1, py1, px2, py2)) < ESP)
      && ((px0 - px1) * (px0 - px2) <= 0)
      && ((py0 - py1) * (py0 - py2) <= 0)) {
      flag = true
    }
    flag
  }


  def isPointInPolygon( px : Double, py : Double, polygonXA : List[Double], polygonYA : List[Double]) : Boolean ={
    var isInside = false
    val ESP : Double = 1e-9
    var count : Int = 0
    var linePoint1x : Double = 0.0
    var linePoint1y : Double = 0.0
    val linePoint2x : Double = 180
    var linePoint2y : Double = 0.0

    linePoint1x = px
    linePoint1y = py
    linePoint2y = py

    for(i <- 0 to polygonXA.size - 2){
      val cx1 = polygonXA(i)
      val cy1 = polygonYA(i)
      val cx2 = polygonXA(i + 1)
      val cy2 = polygonYA(i + 1)
      // 如果目标点在任何一条线上
      if (isPointOnLine(px, py, cx1, cy1, cx2, cy2)) {
        count = -1
      }
      // 如果线段的长度无限小(趋于零)那么这两点实际是重合的，不足以构成一条线段
      if (count != -1 && Math.abs(cy2 - cy1) < ESP) {
      }
      // 第一个点是否在以目标点为基础衍生的平行纬度线
      if (count != -1 && isPointOnLine(cx1, cy1, linePoint1x, linePoint1y, linePoint2x,
        linePoint2y)) {
        // 第二个点在第一个的下方,靠近赤道纬度为零(最小纬度)
        if (cy1 > cy2)
          count = count + 1
      }
      // 第二个点是否在以目标点为基础衍生的平行纬度线
      else if (count != -1 && isPointOnLine(cx2, cy2, linePoint1x, linePoint1y,
        linePoint2x, linePoint2y)) {
        // 第二个点在第一个的上方,靠近极点(南极或北极)纬度为90(最大纬度)
        if (cy2 > cy1)
          count = count + 1
      }
      // 由两点组成的线段是否和以目标点为基础衍生的平行纬度线相交
      else if (count != -1 &&isIntersect(cx1, cy1, cx2, cy2, linePoint1x, linePoint1y,
        linePoint2x, linePoint2y)) {
        count = count + 1
      }
    }

    if (count % 2 == 1) {
      isInside = true
    }

    isInside
  }




def main(args: Array[String]): Unit = {
//    println(LonLatUtils.getPointByDistance(106.486654D,29.490295D,0.35355D,135D))

    val distance : Double = 500
    //114.067431,22.553988
    val min_point_lng : Double = 114.065825D
    val max_point_lng : Double = 114.225364D

    val min_point_lat : Double = 22.538635D
    val max_point_lat : Double = 22.617657D

//    val list = LonLatUtils.getCenterPointsByMinMaxLngLat(min_point_lng,max_point_lng,min_point_lat,max_point_lat,distance)
//
//    println(list.size)
//    list.foreach(println)

    var initMoney : Double= 110000
    var initDay : Int = 5

    var totalMoney : Double = 0.0

    for( tag  <- 1 to  initDay){
      initMoney = initMoney + (initMoney * 0.10)
    }

    println(initMoney)

  }
}
