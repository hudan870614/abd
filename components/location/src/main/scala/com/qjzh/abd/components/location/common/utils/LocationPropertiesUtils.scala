package com.qjzh.abd.components.location.common.utils

import java.io.FileInputStream
import java.util.Properties

import com.qjzh.abd.function.common.PropertiesUtils

/**
  * Created by yufeiwang on 12/02/2017.
  */
object LocationPropertiesUtils {

  val properties = new Properties()
  val propertiesFileName="setting.properties"


  def getProValue(key: String): String = {
    var filePath = System.getProperty("user.dir")
    filePath = filePath + "/components/location/src/main/resources/"+propertiesFileName
    properties.load(new FileInputStream(filePath))
    properties.getProperty(key)
  }

  def getSelectedProjectNames():List[String] ={
    val properties = PropertiesUtils.init("project_settings.properties")
    val projectNames = properties.getProperty("location_active_projects")
    projectNames.replace(" ","").split(",").toList
  }

  def getClassNameByProjName(projectName : String): String ={
    getProValue(projectName+"_className")
  }

  def getAPMACs():List[String] ={
    getProValue("APMACs").replace(" ","").split(",").toList
  }
}
