package com.qjzh.abd.components.location.common.utils

import com.qjzh.abd.components.location.common.conf.CommonConf

import scala.io.Source

/**
  * Created by yufeiwang on 25/12/2016.
  */
object MobileUtils {


  def getDistWithBrand(brand: String, rssi: Float): Float = {

    var dist: Double = 0

    //P0 is the rssi with distance of 1 meter, n is the param between rssi and distance of each brand.
//    val paramPair = getDistCalInitials(brand)
//    val p0 = paramPair._1
//    val n = paramPair._2


    val p0 = 40
    val n = 0.65

    //Solve Pd = P0 - 10*n*lg(d) for d
    val pow = (p0 - rssi) / (10 * n)
    dist = Math.pow(10, pow)

    dist.toFloat*0.000009F
  }

  /**
    * Get mobile's P0 and n with specific brand
    *
    * @param brand
    * @return (0,0) if brand is not contained
    */
  private def getDistCalInitials(brand: String): (Double, Double) = {
    var r: (Double, Double) = (0, 0)
    if (isBrandInList(brand)) {
      r = CommonConf.MobileParams.get(brand)
    } else {
      r = (CommonConf.DEFAULT_P0, CommonConf.DIST_RATIO)
    }
    r
  }

  /**
    * Check if the given brand is contained in our brand list
    *
    * @param brand
    * @return
    */
  private def isBrandInList(brand: String): Boolean = {
    CommonConf.MobileParams.containsKey(brand)
  }

  private def readFromFile(path: String): List[Double] = {
    var r: List[Double] = Nil
    val basePath: String = "/Users/yufeiwang/work/loc_test_data/indoor/"
    val file = Source.fromFile(basePath + path)
    for (line <- file.getLines) {
      r = r:+line.replace(" ","").toDouble
    }
    file.close
    r
  }

  def main(args: Array[String]): Unit = {
    println(getDistWithBrand("aaa",-60))

  }
}
