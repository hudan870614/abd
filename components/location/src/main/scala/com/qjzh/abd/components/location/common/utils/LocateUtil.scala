package com.qjzh.abd.components.location.common

import com.qjzh.abd.components.location.common.caseview.CaseClass.{APCircle, Coordinate, GPSCoordinate}
import com.qjzh.abd.components.location.common.conf.CommonConf
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap

/**
  * Created by yufeiwang on 20/12/2016.
  * New location utility class. Used for structured location rules.
  */
object LocateUtil {

  def calculateLoc(points: Array[APCircle]): Coordinate = {

    //区域规则判断
    val area = areaClassify(points)
//        val area = CommonConf.DEFAULT

    var zk: Coordinate = null

    if (points.length >= 3) {
      zk = tclWithThreePoints(points(0), points(1), points(2))
    } else if (points.length == 2) {
      zk = tclWithTwoPoints(points(0), points(1))
    } else {
      zk = Coordinate(points.head.x - Math.random() * 150 + 75, points.head.y - Math.random() * 150 + 75)
    }

    zk = adjustLoc(zk, area)
    zk = Coordinate("%.2f".format(zk.x).toDouble, "%.2f".format(zk.y).toDouble)

    zk
  }

  def calculateOutdoorLoc(points: Array[APCircle]): Coordinate = {

    var zk: Coordinate = null

    if (points.length >= 3) {
      zk = tclWithThreePoints(points(0), points(1), points(2))
    } else if (points.length == 2) {
      zk = tclWithTwoPoints(points(0), points(1))
    } else {
      zk = tclWithOnePoint(points(0))
    }

    zk = Coordinate("%.2f".format(zk.x).toDouble, "%.2f".format(zk.y).toDouble,false,points(0).mac)

    zk
  }

  private def tclWithOnePoint(circle: APCircle) : Coordinate ={

    var latDegree : Double = 0
    var lonDegree : Double = 0

    if(circle.activeAngleList == Nil){
      val latSign = if(Math.random()>0.5) 1 else -1
      val lonSign = if(Math.random()>0.5) 1 else -1
      val latLength = latSign * Math.random()*circle.r
      val lonLength = lonSign * Math.sqrt(circle.r*circle.r - latLength*latLength)

      latDegree = circle.y+latLength
      lonDegree = circle.x+lonLength

    }else{//有特定的取值角度范围

    }
    Coordinate(lonDegree,latDegree)
  }

  /**
    * 判断定位点是否在预判的区域内，如果不是，进行调整
    * 将计算到场地外的点调整进场内（需要修改）
    *
    * @param c
    * @param area
    * @return
    */
  def adjustLoc(c: Coordinate, area: String): Coordinate = {
    var result: Coordinate = null


    if (area != CommonConf.DEFAULT) {
      //被逻辑划分了区位
      val coors = CommonConf.areaCoors.get(area)
      var isIn: Boolean = false
      for (elem <- coors) {
        if (elem(0) <= c.x && c.x <= elem(1) && elem(2) <= c.y && c.y <= elem(3)) {
          isIn = true
        }
      }
      if (!isIn) {
        //计算点位和逻辑区位不重叠
        val nx = if (c.x < coors(0)(0) || c.x > coors(0)(1)) Math.random() * (coors(0)(1) - coors(0)(0)) + coors(0)(0) else c.x
        val ny = if (c.y < coors(0)(2) || c.y > coors(0)(3)) Math.random() * (coors(0)(3) - coors(0)(2)) + coors(0)(2) else c.y
        result = Coordinate(nx, ny, true)
      } else {
        //计算点位和逻辑区位重叠
        result = c
      }
    } else {
      /**
        * 检查没有被归类的定位，是否在场地外
        * 被计算到场地范围外的点 归类到三个大区域内
        */
      val dTcoors = CommonConf.areaCoors.get(CommonConf.DONGTONG)
      val cYcoors = CommonConf.areaCoors.get(CommonConf.CHAYAN)
      if (c.x >= dTcoors(0)(0) && c.x <= dTcoors(0)(1) && c.y >= dTcoors(0)(2) && c.y <= dTcoors(0)(3)) {
        val coors = CommonConf.areaCoors.get(CommonConf.XITONG)
        val nx = if (c.x < coors(1)(0) || c.x > coors(1)(1)) Math.random() * (coors(1)(1) - coors(1)(0)) + coors(1)(0) else c.x
        val ny = if (c.y < coors(1)(2) || c.y > coors(1)(3)) Math.random() * (coors(1)(3) - coors(1)(2)) + coors(1)(2) else c.y
        result = Coordinate(nx, ny, true)
      } else if (c.x >= cYcoors(0)(0) && c.x <= cYcoors(0)(1) && c.y >= cYcoors(0)(2) && c.y <= cYcoors(0)(3)) {
        val coors = CommonConf.areaCoors.get(CommonConf.XITONG)
        val nx = if (c.x < coors(0)(0) || c.x > coors(0)(1)) Math.random() * (coors(0)(1) - coors(0)(0)) + coors(0)(0) else c.x
        val ny = if (c.y < coors(0)(2) || c.y > coors(0)(3)) Math.random() * (coors(0)(3) - coors(0)(2)) + coors(0)(2) else c.y
        result = Coordinate(nx, ny, true)
      } else if (c.y < 32 || (c.x < 26 && c.y >= 32 && c.y <= 352) ||
        (c.x > 360 && c.y >= 32 && c.y <= 352)) {
        val coors = CommonConf.areaCoors.get(CommonConf.BIANJIAN)
        val nx = if (c.x < coors(0)(0) || c.x > coors(0)(1)) Math.random() * (coors(0)(1) - coors(0)(0)) + coors(0)(0) else c.x
        val ny = if (c.y < coors(0)(2) || c.y > coors(0)(3)) Math.random() * (coors(0)(3) - coors(0)(2)) + coors(0)(2) else c.y
        result = Coordinate(nx, ny, true)
      } else if ((c.x < 26 && c.y >= 352 && c.y <= 386) ||
        (c.x > 360 && c.y >= 352 && c.y <= 386)) {
        val coors = CommonConf.areaCoors.get(CommonConf.HUANCHONG)
        val nx = if (c.x < coors(0)(0) || c.x > coors(0)(1)) Math.random() * (coors(0)(1) - coors(0)(0)) + coors(0)(0) else c.x
        val ny = if (c.y < coors(0)(2) || c.y > coors(0)(3)) Math.random() * (coors(0)(3) - coors(0)(2)) + coors(0)(2) else c.y
        result = Coordinate(nx, ny, true)
      } else if (c.y > 721 || (c.x < 92 && c.y > 637) || (c.x > 293 && c.y > 637)) {
        val coors = CommonConf.areaCoors.get(CommonConf.DATING)
        val nx = if (c.x < coors(0)(0) || c.x > coors(0)(1)) Math.random() * (coors(0)(1) - coors(0)(0)) + coors(0)(0) else c.x
        val ny = if (c.y < coors(0)(2) || c.y > coors(0)(3)) Math.random() * (coors(0)(3) - coors(0)(2)) + coors(0)(2) else c.y
        result = Coordinate(nx, ny, true)
      } else if (c.x < 92 && c.y > 386) {
        val coors = CommonConf.areaCoors.get(CommonConf.HUANCHONG)
        val nx = Math.random() * (coors(0)(1) - coors(0)(0)) + coors(0)(0)
        val ny = Math.random() * (coors(0)(3) - coors(0)(2)) + coors(0)(2)
        result = Coordinate(nx, ny, true)
      } else if (c.x > 293 && c.y > 386) {
        val coors = CommonConf.areaCoors.get(CommonConf.XITONG)
        val nx = Math.random() * (coors(1)(1) - coors(1)(0)) + coors(1)(0)
        val ny = Math.random() * (coors(1)(3) - coors(1)(2)) + coors(1)(2)
        result = Coordinate(nx, ny, true)
      } else {
        result = c
      }
    }
    result
  }


  /**
    * 根据探针位置进行定位区域归类
    *
    * @param points
    * @return
    */
  def areaClassify(points: Array[APCircle]): String = {
    var area: String = CommonConf.DEFAULT

    val numOfA = numOfPointsOfGroup("A", points)
    val numOfB = numOfPointsOfGroup("B", points)
    val numOfC = numOfPointsOfGroup("C", points)

    val onlyA = onlyExistInGroup("A", points)
    val onlyB = onlyExistInGroup("B", points)
    val onlyC = onlyExistInGroup("C", points)

    var biggestRssi: Double = points.sortBy(_.r).head.r

    if (points.length >= 3) {
      //      if(canCompose(CommonConf.apMacGroupSpecialA,points,3)){
      //        area = CommonConf.CHAYAN
      //      }else if(canCompose(CommonConf.apMacGroupSpecialB, points,3)){
      //        area = CommonConf.DONGTONG
      //      }else if(canCompose(CommonConf.apMacGroupB, points,3)){
      //        area = CommonConf.GUOJIAN
      //      }else{
      //        area = CommonConf.DEFAULT
      //      }
    } else if (points.length == 2) {
      if (canAllFit(CommonConf.apMacGroupC, points)) {
        area = CommonConf.DATING
      } else if (canAllFit(CommonConf.apMacGroupA, points)) {
        area = CommonConf.BIANJIAN
      }
    } else {
      //points.length == 1
      if (canAllFit(CommonConf.apMacGroupA, points) && points(0).r >= 70 * 5.6F) {
        area = CommonConf.SHENGANG
      } else if (canAllFit(CommonConf.apMacGroupC, points)) {
        area = CommonConf.DATING
      }
    }

    if (!area.equals(CommonConf.DEFAULT)) {
      //      if (numOfA > 1 && numOfA <= 2 && numOfB == 1) {
      //        area = CommonConf.BIANJIAN
      //      } else if (numOfA > 2 && numOfB == 1) {
      //        area = CommonConf.XITONG
      //      } else if (points.length >= 6 && onlyA && biggestRssi > 70 && biggestRssi <= 80) {
      //        area = CommonConf.HUANCHONG
      //      } else if (points.length >= 6 && onlyA && biggestRssi <= 70 && biggestRssi > 0) {
      //        area = CommonConf.XITONG
      //      } else if (points.length <= 3 && onlyA) {
      if (points.length <= 3 && onlyA) {
        area = CommonConf.BIANJIAN
      }
    }
    area
  }

  /**
    *
    * @param group
    * @param points
    * @return
    */
  def canAllFit(group: Object2ObjectOpenHashMap[Int, String], points: Array[APCircle]): Boolean = {
    canAllFit(group, points, points.length)
  }

  /**
    *
    * @param group
    * @param points
    * @param numOfPoints
    * @return
    */
  def canAllFit(group: Object2ObjectOpenHashMap[Int, String], points: Array[APCircle], numOfPoints: Int): Boolean = {
    var r = false
    val temp = points.take(numOfPoints)
    for (elem <- temp) {
      if (!group.containsValue(elem.mac)) {
        r = false
      }
    }
    r
  }

  def numOfPointsOfGroup(group: String, points: Array[APCircle]): Int = {
    var num: Int = 0
    group match {
      case "A" => {
        points.foreach(point => {
          if (CommonConf.apMacGroupA.containsValue(point.mac)) {
            num += 1
          }
        })
      }
      case "B" => {
        points.foreach(point => {
          if (CommonConf.apMacGroupB.containsValue(point.mac)) {
            num += 1
          }
        })
      }
      case "C" => {
        points.foreach(point => {
          if (CommonConf.apMacGroupC.containsValue(point.mac)) {
            num += 1
          }
        })
      }
    }
    num
  }

  def onlyExistInGroup(group: String, points: Array[APCircle]): Boolean = {
    var only: Boolean = true
    group match {
      case "A" => {
        points.foreach(point => {
          if (CommonConf.apMacGroupA.containsValue(point.mac)) {
            only = false
          }
        })
      }
      case "B" => {
        points.foreach(point => {
          if (CommonConf.apMacGroupB.containsValue(point.mac)) {
            only = false
          }
        })
      }
      case "C" => {
        points.foreach(point => {
          if (CommonConf.apMacGroupC.containsValue(point.mac)) {
            only = false
          }
        })
      }
    }
    only
  }

  def tclWithThreePoints(r1: APCircle, r2: APCircle, r3: APCircle): Coordinate = {
    var zk: Coordinate = null

    val cis1 = circle_intersection(r1, r2)
    val cis2 = circle_intersection(r1, r3)
    val cis3 = circle_intersection(r2, r3)
    //    println("两两相交点个数： "+cis1.length+"   "+cis2.length+"     "+cis3.length)
    if (cis1 != null && !cis1.isEmpty && cis2 != null && !cis2.isEmpty && cis3 != null && !cis3.isEmpty) {
      val points = getTriangle(cis1 :: cis2 :: cis3 :: Nil)
      //一个找最小边长三角形求中心，一个找离三圆新最近交点求中心
      //zk=tcl(r1,r2,r3)
      zk = new Coordinate((points(0).x + points(1).x + points(2).x) / 3, (points(0).y + points(1).y + points(2).y) / 3)
    } //两组两圆相交，找交点平均值
    else if (cis1.isEmpty && !cis2.isEmpty && !cis3.isEmpty) {
      val r = getResults(cis2 :: cis3 :: Nil)
      zk = new Coordinate(r(0), r(1))
    } else if (!cis1.isEmpty && cis2.isEmpty && !cis3.isEmpty) {
      val r = getResults(cis1 :: cis3 :: Nil)
      zk = new Coordinate(r(0), r(1))
    } else if (!cis1.isEmpty && !cis2.isEmpty && cis3.isEmpty) {
      val r = getResults(cis1 :: cis2 :: Nil)
      zk = new Coordinate(r(0), r(1))
    } //一组两圆相交，找两点中点
    else if (!cis1.isEmpty && cis2.isEmpty && cis3.isEmpty) {
      val r = getResults(cis1 :: Nil)
      zk = new Coordinate((r(0) + r3.x) / 2, (r(1) + r3.y) / 2)
    } else if (cis1.isEmpty && !cis2.isEmpty && cis3.isEmpty) {
      val r = getResults(cis2 :: Nil)
      zk = new Coordinate((r(0) + r2.x) / 2, (r(1) + r2.y) / 2)
    } else if (cis1.isEmpty && cis2.isEmpty && !cis3.isEmpty) {
      val r = getResults(cis3 :: Nil)
      zk = new Coordinate((r(0) + r1.x) / 2, (r(1) + r1.y) / 2)
    } //无相交，返回信号最强AP坐标
    else {
      zk = new Coordinate((r1.x + r2.x + r3.x) / 3, (r1.y + r2.y + r3.y) / 3)
    }
    zk
  }

  /**
    * 求两圆交点
    *
    * @param r1 ：APCircle类
    * @param r2
    * @return List[Coordinate] 可能有两个点，可能有一给点，可能没有交点
    */
  def circle_intersection(r1: APCircle, r2: APCircle): List[Coordinate] = {
    var points: List[Coordinate] = Nil

    val dx = r2.x - r1.x
    val dy = r2.y - r1.y
    val d = Math.sqrt(dx * dx + dy * dy)

    if (d > (r1.r + r2.r)) {
      //圆心距离大于两半径之和，无解
    } else if (d < Math.abs(r1.r - r2.r)) {
      //内含，无解
    } else if (d == 0 && r1.r == r2.r) {
      //重合，无解
    } else {
      //相交或外切情况
      val a = (r1.r * r1.r - r2.r * r2.r + d * d) / (2 * d)
      val h = Math.sqrt(r1.r * r1.r - a * a)
      val xm = r1.x + a * dx / d
      val ym = r1.y + a * dy / d
      val xs1 = xm + h * dy / d
      val xs2 = xm - h * dy / d
      val ys1 = ym - h * dx / d
      val ys2 = ym + h * dx / d
      if (xs1 == xs2 && ys1 == ys2) {
        //一组解
        points = new Coordinate(xs1, ys1) :: Nil
      } else {
        //两组解
        points = new Coordinate(xs1, ys1) :: new Coordinate(xs2, ys2) :: Nil
      }
    }
    points
  }

  def getResults(ciss: List[List[Coordinate]]): List[Double] = {
    var counter, x, y: Double = 0.0
    ciss.foreach(cis => {
      cis.foreach(ci => {
        x += ci.x
        y += ci.y
        counter += 1
      })
    })
    x / counter :: y / counter :: Nil
  }

  def getTriangle(ciss: List[List[Coordinate]]): List[Coordinate] = {
    var r: List[Coordinate] = Nil
    var result: List[List[Coordinate]] = Nil

    for (elem1 <- ciss(0)) {
      for (elem2 <- ciss(1)) {
        for (elem3 <- ciss(2)) {
          result = (elem1 :: elem2 :: elem3 :: Nil) :: result
        }
      }
    }
    r = result.map(c => {
      val total = getDist(c(0), c(1)) + getDist(c(0), c(2)) + getDist(c(1), c(2))
      (total, c)
    }).sortBy(_._1).head._2
    r
  }

  def getDist(p1: Coordinate, p2: Coordinate): Double = {
    Math.sqrt(Math.pow(p1.x - p2.x, 2) + Math.pow(p1.y - p2.y, 2))
  }

  def tclWithTwoPoints(r1: APCircle, r2: APCircle): Coordinate = {
    var zk: Coordinate = null
    val cis = circle_intersection(r1, r2)
    if (!cis.isEmpty) {
      val r = getResults(cis :: Nil)
      zk = new Coordinate(r(0), r(1))
    } else {
      zk = new Coordinate((r1.x + r2.x) / 2, (r1.y + r2.y) / 2)
    }
    zk
  }

  def canCompose(group: Object2ObjectOpenHashMap[Int, String], points: Array[APCircle], sample: Int): Boolean = {
    var count: Int = 0
    //    val temp = points.take(group.size())
    val temp = points.take(sample)
    for (elem <- temp) {
      if (group.containsValue(elem.mac)) {
        count += 1
      }
    }
    count == group.size()
  }

  def isPointsHavePointOfGroup(group: String, points: Array[APCircle]): Boolean = {
    var has: Boolean = false
    group match {
      case "A" => {
        points.foreach(point => {
          if (CommonConf.apMacGroupA.containsValue(point.mac)) {
            has = true
          }
        })
      }
      case "B" => {
        points.foreach(point => {
          if (CommonConf.apMacGroupB.containsValue(point.mac)) {
            has = true
          }
        })
      }
      case "C" => {
        points.foreach(point => {
          if (CommonConf.apMacGroupC.containsValue(point.mac)) {
            has = true
          }
        })
      }
    }
    has
  }


  def main(args: Array[String]): Unit = {

//    val r1: APCircle = new APCircle(100, 100, 50, CommonConf.apMacGroupA.get(12))
//    val r2: APCircle = new APCircle(200, 100, 50, CommonConf.apMacGroupA.get(11))
//    val r3: APCircle = new APCircle(150, 110, 50, CommonConf.apMacGroupA.get(11))
//    val r4: APCircle = new APCircle(-110, -119, 34, CommonConf.apMacGroupB.get(5))
//    val r5: APCircle = new APCircle(-110, -119, 34, CommonConf.apMacGroupB.get(5))
//    val r6: APCircle = new APCircle(-110, -119, 34, CommonConf.apMacGroupB.get(5))
    //println(new Gson().toJson(tcl2(Array(r1, r2, r3))))
    //    println(new Gson().toJson(Array[Int](1).take(3)))
    //    println(new Gson().toJson(CommonConf.apMacToInt.get("asdfasdfasdf")))
    //    println(new Gson().toJson(adjustLoc(Coordinate(0, 999), CommonConf.DEFAULT)))
    //    println(List(1, 4, 3, 2, 5, 1, 23,, 77, 8).sortBy())
    //println(new Gson().toJson(areaClassify(Array(r4,r5,r1))))


    //    val coors = CommonConf.areaCoors.get(CommonConf.XITONG)
    //    val nx = Math.random() * (coors(1)(1) - coors(1)(0)) + coors(1)(0)
    //    val ny = Math.random() * (coors(1)(3) - coors(1)(2)) + coors(1)(2)
    //    println(new Gson().toJson(Coordinate(nx, ny)))
    println("")
  }
}
