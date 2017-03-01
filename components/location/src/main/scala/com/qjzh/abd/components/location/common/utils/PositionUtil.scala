package com.qjzh.abd.components.location.common.utils

import com.qjzh.abd.components.location.common.conf.CommonConf
import com.qjzh.abd.components.location.common.caseview.CaseClass.{APCircle, Coordinate}

/**
  * Created by yufeiwang on 20/12/2016.
  * Old location util, the prototype on HG project
  * DO NOT TOUCH
  * update: dongtong -> xitong
  */
object PositionUtil {
  def tcl2(points: Array[APCircle]): (Coordinate,Coordinate) = {

    val area = areaClassify(points)

    var zk: Coordinate = null
    if (points.length >= 3) {
      zk = tclWithThreePoints(points(0), points(1), points(2))
    } else if (points.length == 2) {
      zk = tclWithTwoPoints(points(0), points(1))
    } else {
      zk = Coordinate(points.head.x - Math.random() * 150 + 75, points.head.y - Math.random() * 150 + 75)
    }

    //未规则的副本
    val rawZk = zk

    zk = adjustLoc(zk, area)

    zk = Coordinate("%.2f".format(zk.x).toDouble, "%.2f".format(zk.y).toDouble)

    (zk,rawZk)
  }

  def tcl2Test(points: Array[APCircle]): Coordinate = {

//    val area = areaClassify(points)

    var zk: Coordinate = null
    if (points.length >= 3) {
      zk = tclWithThreePoints(points(0), points(1), points(2))
    } else if (points.length == 2) {
      zk = tclWithTwoPoints(points(0), points(1))
    } else {
      zk = Coordinate(points.head.x - Math.random() * 150 + 75, points.head.y - Math.random() * 150 + 75)
    }

//    zk = adjustLoc(zk, area)

    zk = Coordinate("%.2f".format(zk.x).toDouble, "%.2f".format(zk.y).toDouble)

    zk
  }

  def adjustLoc(c: Coordinate, area: String): Coordinate = {
    var result: Coordinate = null

    // 如果被划分到某区域
    if (area != CommonConf.DEFAULT) {
      val coors = CommonConf.areaCoors.get(area)
      var isIn: Boolean = false
      for (elem <- coors) {
        if (elem(0) <= c.x && c.x <= elem(1) && elem(2) <= c.y && c.y <= elem(3)) {
          isIn = true
        }
      }

      // 没有初定位到划分区域里
      if (!isIn) {
        val nx = if (c.x < coors(0)(0) || c.x > coors(0)(1)) Math.random() * (coors(0)(1) - coors(0)(0)) + coors(0)(0) else c.x
        val ny = if (c.y < coors(0)(2) || c.y > coors(0)(3)) Math.random() * (coors(0)(3) - coors(0)(2)) + coors(0)(2) else c.y
        result = Coordinate(nx, ny)
      } else {
        // 初定位到划分区域里
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
        var coors : List[List[Double]] = List(List(0d))
        if(CommonConf.westNotEast){
          coors = CommonConf.areaCoors.get(CommonConf.XITONG)
        }else{
          coors = CommonConf.areaCoors.get(CommonConf.DONGTONG)
        }
        val nx = if (c.x < coors(0)(0) || c.x > coors(0)(1)) Math.random() * (coors(0)(1) - coors(0)(0)) + coors(0)(0) else c.x
        val ny = if (c.y < coors(0)(2) || c.y > coors(0)(3)) Math.random() * (coors(0)(3) - coors(0)(2)) + coors(0)(2) else c.y
        result = Coordinate(nx, ny)
      } else if (c.x >= cYcoors(0)(0) && c.x <= cYcoors(0)(1) && c.y >= cYcoors(0)(2) && c.y <= cYcoors(0)(3)) {
        var coors : List[List[Double]] = List(List(0d))
        if(CommonConf.westNotEast){
          coors = CommonConf.areaCoors.get(CommonConf.XITONG)
        }else{
          coors = CommonConf.areaCoors.get(CommonConf.DONGTONG)
        }
        val nx = if (c.x < coors(0)(0) || c.x > coors(0)(1)) Math.random() * (coors(0)(1) - coors(0)(0)) + coors(0)(0) else c.x
        val ny = if (c.y < coors(0)(2) || c.y > coors(0)(3)) Math.random() * (coors(0)(3) - coors(0)(2)) + coors(0)(2) else c.y
        result = Coordinate(nx, ny)
      } else if (c.y < 32 || (c.x < 26 && c.y >= 32 && c.y <= 352) ||
        (c.x > 360 && c.y >= 32 && c.y <= 352)) {
        val coors = CommonConf.areaCoors.get(CommonConf.BIANJIAN)
        val nx = if (c.x < coors(0)(0) || c.x > coors(0)(1)) Math.random() * (coors(0)(1) - coors(0)(0)) + coors(0)(0) else c.x
        val ny = if (c.y < coors(0)(2) || c.y > coors(0)(3)) Math.random() * (coors(0)(3) - coors(0)(2)) + coors(0)(2) else c.y
        result = Coordinate(nx, ny)
      } else if ((c.x < 26 && c.y >= 352 && c.y <= 386) ||
        (c.x > 360 && c.y >= 352 && c.y <= 386)) {
        val coors = CommonConf.areaCoors.get(CommonConf.HUANCHONG)
        val nx = if (c.x < coors(0)(0) || c.x > coors(0)(1)) Math.random() * (coors(0)(1) - coors(0)(0)) + coors(0)(0) else c.x
        val ny = if (c.y < coors(0)(2) || c.y > coors(0)(3)) Math.random() * (coors(0)(3) - coors(0)(2)) + coors(0)(2) else c.y
        result = Coordinate(nx, ny)
      } else if (c.y > 721 || (c.x < 92 && c.y > 637) || (c.x > 293 && c.y > 637)) {
        val coors = CommonConf.areaCoors.get(CommonConf.DATING)
        val nx = if (c.x < coors(0)(0) || c.x > coors(0)(1)) Math.random() * (coors(0)(1) - coors(0)(0)) + coors(0)(0) else c.x
        val ny = if (c.y < coors(0)(2) || c.y > coors(0)(3)) Math.random() * (coors(0)(3) - coors(0)(2)) + coors(0)(2) else c.y
        result = Coordinate(nx, ny)
      } else if (c.x < 92 && c.y > 386) {
        val coors = CommonConf.areaCoors.get(CommonConf.HUANCHONG)
        val nx = Math.random() * (coors(0)(1) - coors(0)(0)) + coors(0)(0)
        val ny = Math.random() * (coors(0)(3) - coors(0)(2)) + coors(0)(2)
        result = Coordinate(nx, ny)
      } else if (c.x > 293 && c.y > 386) {
        var coors : List[List[Double]] = List(List(0d))
        if(CommonConf.westNotEast){
          coors = CommonConf.areaCoors.get(CommonConf.XITONG)
        }else{
          coors = CommonConf.areaCoors.get(CommonConf.DONGTONG)
        }
        val nx = Math.random() * (coors(0)(1) - coors(0)(0)) + coors(0)(0)
        val ny = Math.random() * (coors(0)(3) - coors(0)(2)) + coors(0)(2)
        result = Coordinate(nx, ny)
      } else {
        result = c
      }
    }
    result
  }


  /** 根据探针位置进行定位区域归类 */
  def areaClassify(points: Array[APCircle]): String = {
    var area: String = CommonConf.DEFAULT

    var numOfA: Int = 0
    points.foreach(point => {
      if (CommonConf.apMacGroupA.containsValue(point.mac)) {
        numOfA += 1
      }
    })
    var numOfB: Int = 0
    points.foreach(point => {
      if (CommonConf.apMacGroupB.containsValue(point.mac)) {
        numOfB += 1
      }
    })

    var onlyA: Boolean = true
    points.foreach(point => {
      if (CommonConf.apMacGroupA.containsValue(point.mac)) {
        onlyA = false
      }
    })

    var biggestRssi: Double = points.sortBy(_.r).head.r


    //先调整东通道和查验区
    if (numOfA > 1 && numOfA <= 2 && numOfB == 1) {
      area = CommonConf.BIANJIAN
    } else if (numOfA > 2 && numOfB == 1) {
      if(CommonConf.westNotEast){
        area = CommonConf.XITONG
      }else{
        area = CommonConf.DONGTONG
      }
    } else if (points.length >= 6 && onlyA && biggestRssi > 70 && biggestRssi <= 80) {
      area = CommonConf.HUANCHONG
    } else if (points.length >= 6 && onlyA && biggestRssi <= 70 && biggestRssi > 0) {
      if(CommonConf.westNotEast){
        area = CommonConf.XITONG
      }else{
        area = CommonConf.DONGTONG
      }
    } else if (points.length <= 5 && onlyA) {
      area = CommonConf.BIANJIAN
    } else {

      if (points.length >= 3) {

        val p1 = points(0).mac
        val p2 = points(1).mac
        val p3 = points(2).mac


        var hasC: Boolean = false
        points.foreach(c => {
          if (CommonConf.apMacGroupC.containsValue(c.mac)) {
            hasC = true
          }
        })

        if (CommonConf.apMacGroupSpecialC.containsValue(p1) &&
          CommonConf.apMacGroupSpecialC.containsValue(p2) &&
          CommonConf.apMacGroupSpecialC.containsValue(p3)) {

          if(CommonConf.westNotEast){
            area = CommonConf.XITONG
          }else{
            area = CommonConf.DEFAULT
          }

        } else if (CommonConf.apMacGroupSpecialA.containsValue(p1) &&
          CommonConf.apMacGroupSpecialA.containsValue(p2) &&
          CommonConf.apMacGroupSpecialA.containsValue(p3)) {

          area = CommonConf.CHAYAN

        } else if (CommonConf.apMacGroupB.containsValue(p1) &&
          CommonConf.apMacGroupB.containsValue(p2) &&
          CommonConf.apMacGroupB.containsValue(p3)) {

          area = CommonConf.GUOJIAN

        } else if ((CommonConf.apMacGroupB.containsValue(p1) && CommonConf.apMacGroupB.containsValue(p2) && !CommonConf.apMacGroupB.containsValue(p3)) ||
          (CommonConf.apMacGroupB.containsValue(p1) && !CommonConf.apMacGroupB.containsValue(p2) && CommonConf.apMacGroupB.containsValue(p3)) ||
          (!CommonConf.apMacGroupB.containsValue(p1) && CommonConf.apMacGroupB.containsValue(p2) && CommonConf.apMacGroupB.containsValue(p3))) {

          area = CommonConf.DATING

        } else {

          area = CommonConf.DEFAULT

        }
      } else if (points.length == 2) {
        val p1 = points(0).mac
        val p2 = points(1).mac

        if (CommonConf.apMacGroupC.containsValue(p1) &&
          CommonConf.apMacGroupC.containsValue(p2)) {

          area = CommonConf.DATING

        }
        if (CommonConf.apMacGroupA.containsValue(p1) &&
          CommonConf.apMacGroupA.containsValue(p2)) {

          area = CommonConf.BIANJIAN

        }
      } else {
        val p1 = points(0).mac
        if (CommonConf.apMacGroupA.containsValue(p1) && points(0).r >= 70 * CommonConf.pointMile) {

          area = CommonConf.SHENGANG

        } else if (CommonConf.apMacGroupC.containsValue(p1)) {

          area = CommonConf.DATING

        }
      }
    }
    area
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

  /**
    * 新.求两圆交点
    *
    * @param r1 ：APCircle类
    * @param r2
    * @return
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

  /**
    * 三角形质心定位算法实现
    * Triangle centroid location
    *
    * @param r1 坐标1为圆心,距离为半径
    * @param r2
    * @param r3
    * @return
    */
  def tcl(r1: APCircle, r2: APCircle, r3: APCircle): Coordinate = {
    var p1: Coordinate = null
    // 有效交叉点1
    var p2: Coordinate = null
    // 有效交叉点2
    var p3: Coordinate = null
    // 有效交叉点3
    var zk: Coordinate = null

    val jds1 = jd(r1.x, r1.y, r1.r, r2.x, r2.y, r2.r)
    // r1,r2交点
    val jds2 = jd(r1.x, r1.y, r1.r, r3.x, r3.y, r3.r)
    // r1,r2交点
    val jds3 = jd(r2.x, r2.y, r2.r, r3.x, r3.y, r3.r) // r1,r2交点


    if (jds1 != null && !jds1.isEmpty && jds2 != null && !jds2.isEmpty && jds3 != null && !jds3.isEmpty) {

      jds1.foreach(jd => {
        if (p1 == null && Math.pow((jd.x - r3.x), 2) + Math.pow(jd.y - r3.y, 2) <= Math.pow(r3.r, 2)) {
          p1 = jd
        } else if (p1 != null) {
          if (Math.pow(jd.x - r3.x, 2) + Math.pow(jd.y - r3.y, 2) <= Math.pow(r3.r, 2)) {
            if (Math.sqrt(Math.pow(jd.x - r3.x, 2) + Math.pow(jd.y - r3.y, 2)) <= Math.sqrt(Math.pow(p1.x - r3.x, 2) + Math.pow(p1.y - r3.y, 2))) {
              p1 = jd
            }
          }
        }
      })

      jds2.foreach(jd => {
        if (p2 == null && Math.pow(jd.x - r2.x, 2) + Math.pow(jd.y - r2.y, 2) <= Math.pow(r2.r, 2)) {
          p2 = jd
        } else if (p2 != null) {
          if (Math.pow(jd.x - r2.x, 2) + Math.pow(jd.y - r2.y, 2) <= Math.pow(r2.r, 2)) {
            if (Math.pow(jd.x - r2.x, 2) + Math.pow(jd.y - r2.y, 2) <= Math.sqrt(Math.pow(p2.x - r2.x, 2) + Math.pow(p2.y - r2.y, 2))) {
              p1 = jd
            }
          }
        }
      })

      jds3.foreach(jd => {
        if (Math.pow(jd.x - r1.x, 2) + Math.pow(jd.y - r1.y, 2) <= Math.pow(r1.r, 2)) {
          p3 = jd
        } else if (p3 != null) {
          if (Math.pow(jd.x - r1.x, 2) + Math.pow(jd.y - r1.y, 2) <= Math.pow(r1.r, 2)) {
            if (Math.pow(jd.x - r1.x, 2) + Math.pow(jd.y - r1.y, 2) <= Math.sqrt(Math.pow(p3.x - r1.x, 2) + Math.pow(p3.y - r1.y, 2))) {
              p3 = jd
            }
          }
        }
      })

    }

    //for debugging use
    //print("P1.x: "+p1.x+"   |   P2.x: "+p2.x+" |   P3.x: "+p3.x)
    //print("\nP1.y: "+p1.y+"   |   P2.y: "+p2.y+" |   P3.y: "+p3.y)
    //质心
    if (p1 != null && p2 != null && p3 != null) {
      //计算三点质心
      val x = (p1.x + p2.x + p3.x) / 3
      val y = (p1.y + p2.y + p3.y) / 3
      zk = new Coordinate(x, y)
    }

    zk

  }

  /**
    * 求两个圆的交点
    *
    * @param x1 ,y1 圆心1坐标
    * @param y1
    * @param r1 半径
    * @param x2
    * @param y2
    * @param r2
    * @return
    */
  def jd(x1: Double, y1: Double, r1: Double, x2: Double, y2: Double, r2: Double): List[Coordinate] = {
    // 两圆心距离
    val d = Math.sqrt(Math.pow(x1 - x2, 2) + Math.pow(y1 - y2, 2))

    var points: List[Coordinate] = Nil
    //    if (d > r1 + r2 || d < Math.abs(r1 - r2)) {//相离或内含
    //      return points
    //    } else if (x1 == x2 && y1 == y2) {//同心圆
    //      return points
    //    }else
    if (y1 == y2 && x1 != x2) {
      val a: Double = ((r1 * r1 - r2 * r2) - (x1 * x1 - x2 * x2)) / (2 * x2 - 2 * x1)
      if (d == Math.abs(r1 - r2) || d == r1 + r2) {
        // 只有一个交点时
        val co = Coordinate(a, y1)
        points = co :: Nil
      } else {
        // 两个交点
        val t = r1 * r1 - (a - x1) * (a - x1)
        val co_1 = Coordinate(a, y1 + Math.sqrt(t))
        val co_2 = Coordinate(a, y1 + y1 - Math.sqrt(t))
        points = co_1 :: co_2 :: Nil
      }
    } else if (y1 != y2) {
      val k = (2 * x1 - 2 * x2) / (2 * y2 - 2 * y1)
      val disp = ((r1 * r1 - r2 * r2) - (x1 * x1 - x2 * x2) - (y1 * y1 - y2 * y2)) / (2 * y2 - 2 * y1) // 直线偏移量

      val a = (k * k + 1)
      val b = (2 * (disp - y1) * k - 2 * x1)
      val c = (disp - y1) * (disp - y1) - r1 * r1 + x1 * x1

      val disc = b * b - 4 * a * c // 一元二次方程判别式
      if (d == Math.abs(r1 - r2) || d == r1 + r2) {
        val coor_x = ((-b) / (2 * a))
        val coor_y = k * coor_x + disp
        val coor = Coordinate(coor_x, coor_y)
        points = coor :: Nil
      } else {
        val coor_x = ((-b) + Math.sqrt(disc)) / (2 * a)
        val coor_y = k * coor_x + disp
        val co_1 = Coordinate(coor_x, coor_y)

        val coor_2_x = ((-b) - Math.sqrt(disc)) / (2 * a)
        val coor_2_y = k * coor_2_x + disp
        val co_2 = Coordinate(coor_2_x, coor_2_y)

        points = co_1 :: co_2 :: Nil
      }
    }
    points
  }

  def main(args: Array[String]): Unit = {
    /**
      * List<Coordinate> xy = jd(0, 3, 4, 4, 0, 3);
      *System.out.println(new Gson().toJson(xy));
      * //[{"x":4.0,"y":2.9999999999999996},{"x":1.1200000000000006,"y":-0.8399999999999994}]
      */

    //    val xy  = jd(0, 3, 4, 4, 0, 3)
    //    println(xy)


    /**
      * //{"x":11.648804333884328,"y":-2.7924742779097174}
      * //      * val r1: APCircle = new APCircle(0, 0, 29.5)
      * //      * val r2: APCircle = new APCircle(11, 0, 28)
      * //      * val r3: APCircle = new APCircle(0, 9, 34)
      * //      *System.out.println(new Gson().toJson(tcl(r1, r2, r3)))
      */
    //{"x":11.648804333884328,"y":-2.7924742779097174}
    val r1: APCircle = new APCircle(-110, -110, 100, CommonConf.apMacGroupA.get(12))
    val r2: APCircle = new APCircle(-111, -110, 28, CommonConf.apMacGroupA.get(11))
    val r3: APCircle = new APCircle(-111, -110, 28, CommonConf.apMacGroupA.get(11))
    val r4: APCircle = new APCircle(-110, -119, 34, CommonConf.apMacGroupB.get(5))
    val r5: APCircle = new APCircle(-110, -119, 34, CommonConf.apMacGroupB.get(5))
    val r6: APCircle = new APCircle(-110, -119, 34, CommonConf.apMacGroupB.get(5))
    //println(new Gson().toJson(tcl2(Array(r1,r2,r3))))

  }
}
