package com.qjzh.abd.function.common

import java.io.FileInputStream
import java.util.Properties


import scala.reflect.ClassTag

/**
  * Created by 92025 on 2016/12/15.
  */
object PropertiesUtils {

  val properties = new Properties()

  def init(propertyFileName : String = "funcation"): Properties ={
    var fileName = propertyFileName
    if(fileName.indexOf(".") == -1){
      fileName =  fileName+".properties"
    }
    val path: String = getPropertyFile(fileName)
    properties.load(new FileInputStream(path))
    properties
  }

  private def getPropertyFile(propertyFileName : String ): String ={
    val curDicPath = System.getProperty("user.dir")
    var filePath = curDicPath
    if(curDicPath.indexOf("hadoop") != -1){
      filePath = curDicPath.substring(0,curDicPath.indexOf("hadoop"))+"/sw"
    }
    filePath = filePath+"/resources/"+propertyFileName
    filePath
  }

  def main(args: Array[String]): Unit = {
    PropertiesUtils.init()
    println(properties)
  }
}
