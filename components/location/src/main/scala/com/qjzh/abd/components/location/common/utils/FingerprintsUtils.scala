package com.qjzh.abd.components.location.common.utils

import java.io.{File, PrintWriter}
import java.lang.NullPointerException
import java.nio.file.{Files, Paths}

import com.google.gson.JsonParser
import org.json4s.jackson.Serialization
import org.json4s.{JValue, ShortTypeHints}

import scala.collection.mutable
import scala.io.Source
import scala.util.parsing.json.JSON

/**
  * Created by yufeiwang on 10/02/2017.
  */
object FingerprintsUtils {

  val baseName : String = LocationPropertiesUtils.getProValue("FingerprintsDBBasePath")
  val trainBaseName : String = LocationPropertiesUtils.getProValue("FPRawBasePath")
  val sourceSuffix : String = LocationPropertiesUtils.getProValue("FPRawSourceSuffix")
  val targetSuffix : String = LocationPropertiesUtils.getProValue("FPRawTargetSuffix")
  val apCount = LocationPropertiesUtils.getProValue("ApCount").toInt


  //初始化AP MACs链表
  val apIndice : mutable.HashMap[String,Int] = new mutable.HashMap[String,Int]
  val apMacs = LocationPropertiesUtils.getAPMACs()
  for (elem <- apMacs) {
    apIndice.put(elem,apMacs.indexOf(elem))
  }

  val parser : JsonParser =new JsonParser()
  val apMac = "apMac"
  val rssi = "rssi"
  val uMac = "userMac"
  val aMac = "apMac"
  val ts = "timeStamp"


  def writeOutFingerPrints(formatted : Array[List[Int]], sourceName : String) : Unit ={
    val writer = new PrintWriter(new File(sourceName+targetSuffix))
    for(i <- 0 to findMinLength(formatted)-1){
      var singleFingerprint :String = formatted(0)(i).toString
      for(j <- 1 to formatted.length-1){
        singleFingerprint = singleFingerprint + "," + formatted(j)(i)
      }
      singleFingerprint = singleFingerprint +"\n"
      writer.write(singleFingerprint)
    }
    writer.close()
  }

  def makeFingerprints(data : List[(Int, Int)]) : Array[List[Int]] ={
    val rssies : Array[List[Int]] = new Array[List[Int]](apCount)
    for (elem <- data) {
      if(rssies(elem._1) == null){
        rssies(elem._1) = Nil
      }
      rssies(elem._1) = elem._2 :: rssies(elem._1)
    }
    rssies
  }

  def readRawData(sourceName : String) : List[(Int, Int)] ={
    var result : List[(Int, Int)] = Nil
    //收集
    val file=Source.fromFile(sourceName+sourceSuffix)
    for(line <- file.getLines){
      val b = JSON.parseFull(line)
      b match {
        case Some(map: Map[String, Any]) =>
          val r : Int= map.get(rssi).get.toString.toInt
          val apMac = map.get(aMac).get.toString
          if(apIndice.contains(apMac)){
            result = (apIndice.get(apMac).get, r) :: result
          }
        case None => println("Parsing failed")
        case other => println("Unknown data structure: " + other)
      }
    }
    file.close
    result
  }

  def findMinLength(formatted : Array[List[Int]]) : Int ={
    var min : Int = Int.MaxValue
    for (elem <- formatted) {
      try{
        if(elem.length<min){
          min = elem.length
        }
      }catch{
        case e: NullPointerException => println("Maybe some AP is offline, please check")
      }

    }
    min
  }

  def makeAllFingerprints():Unit ={
    for(i <- 0 to LocationPropertiesUtils.getProValue("FingerprintPositionCount").toInt){
      if(Files.exists(Paths.get(baseName+"_"+i+sourceSuffix)) && !Files.exists(Paths.get(baseName+"_"+i+targetSuffix))){
        val tmpPath = baseName+"_"+i
        writeOutFingerPrints(makeFingerprints(readRawData(tmpPath)),tmpPath)
      }
    }

    for(i <- 0 until 100) {
      if(Files.exists(Paths.get(trainBaseName+"_"+i+sourceSuffix)) && !Files.exists(Paths.get(trainBaseName+"_"+i+targetSuffix))){
        val tmpPath = trainBaseName+"_"+i
        writeOutFingerPrints(makeFingerprints(readRawData(tmpPath)),tmpPath)
      }
    }

  }

  def main(args: Array[String]): Unit = {
    makeAllFingerprints()
  }

}
