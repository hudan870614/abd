package com.qjzh.abd.components.location.common.utils

import java.nio.file.{Files, Paths}

import scala.collection.mutable
import scala.io.Source

/**
  * Created by yufeiwang on 10/02/2017.
  */
object SimpleEuclideanUtils {

  val basePath: String = LocationPropertiesUtils.getProValue("FingerprintsDBBasePath")
  val targetBasePath: String = LocationPropertiesUtils.getProValue("FPRawBasePath")
  val suffix: String = LocationPropertiesUtils.getProValue("FingerprintSuffix")
  val targetSuffix: String = LocationPropertiesUtils.getProValue("FPRawTargetSuffix")
  val brandName: String = LocationPropertiesUtils.getProValue("BrandName") + "_"
  val positionCount: Int = LocationPropertiesUtils.getProValue("FingerprintPositionCount").toInt
  val apCount: Int = LocationPropertiesUtils.getProValue("ApCount").toInt
  val numberOfCandidate: Int = LocationPropertiesUtils.getProValue("NumOfCandidates").toInt

  def predictPosition(data: List[List[String]]): List[Int] = {
    val sum = new Array[Int](positionCount)
    for (i <- 0 until sum.length) {
      sum.update(i, 0)
    }
    for (elem <- data) {
      val result: Int = takeOneGuess(elem)
      sum.update(result, sum(result) + 1)
    }
    println("\nFingerprint index     Hit count")
    for (i <- 0 until sum.length) {
      println(i + "                      " + sum(i))
    }
    val guesses = findMaxIndex(sum)
    print("Most likely positions are: ")
    for (elem <- guesses) {
      print(elem + " ")
    }
    println("with probability of: "+(sum(guesses.head).toDouble/data.length.toDouble).toString.take(4))
    guesses
  }


  /**
    * Given one set of RSSI, predict the most probable position
    *
    * @param rssies
    * @return
    */
  private def takeOneGuess(rssies: List[String]): Int = {
    val distMap: mutable.HashMap[(Double, Int), Int] = new mutable.HashMap[(Double, Int), Int]
    var count = 0
    for (i <- 0 until positionCount) {
      val path = basePath + brandName + i + suffix
      if (Files.exists(Paths.get(path))) {
        Source.fromFile(path).getLines().foreach { x => {
          val dist = getEuclideanMetric(x.split(",").toList, rssies)
          distMap.put((dist, count), i)
          count += 1
        }
        }
      }
    }
    val result = distMap.toList.sortBy(_._1._1)
    getMostProbableClass(result)
  }

  /**
    * Helper function for calculating the Euclidean distance between two points
    *
    * @param fps
    * @param target
    * @return
    */
  private def getEuclideanMetric(fps: List[String], target: List[String]): Double = {
    var sumTotal = 0d
    for (i <- 0 until apCount) {
      sumTotal = sumTotal + Math.pow(fps(i).toDouble - target(i).toDouble, 2)
    }
    Math.sqrt(sumTotal)
  }

  /**
    * Helper function for getting the most probable position
    *
    * @param result
    * @return
    */
  private def getMostProbableClass(result: List[((Double, Int), Int)]): Int = {
    val distMap: mutable.HashMap[Int, Double] = new mutable.HashMap[Int, Double]
    val tmp = result.take(numberOfCandidate)

    //分类累加，累加值为欧氏距离的倒数和
    for (i <- 0 until numberOfCandidate) {
      val dist: Double = tmp(i)._1._1
      val classIndex: Int = tmp(i)._2
      val value: Double = 1 / dist
      if (!distMap.contains(classIndex)) {
        distMap.put(classIndex, value)
      } else {
        distMap.update(classIndex, distMap.get(classIndex).get + value)
      }
    }
    distMap.toList.sortBy(_._2).reverse.head._1
  }

  private def findMaxIndex(result: Array[Int]): List[Int] = {
    var r: List[Int] = Nil;
    var tmp = Int.MinValue

    var counter = 0
    for (elem <- result) {
      if (elem > tmp) {
        r =  counter :: Nil
        tmp = elem
      } else if (elem == tmp) {
        r = r :+ counter
      }
      counter+=1
    }
    r
  }

  /**
    * For testing purpose only
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    FingerprintsUtils.makeAllFingerprints()
    for(testIndex <- 0 until 100){
      val path = targetBasePath + "_" + testIndex + targetSuffix
      if(Files.exists(Paths.get(path))){
        println("-------------------------\nTest point index: "+testIndex)
        val file = Source.fromFile(path)
        var data: List[List[String]] = Nil
        for (line <- file.getLines) {
          val tmp = line.split(",").toList
          data = tmp :: data
        }
        predictPosition(data)
      }
    }
  }
}
