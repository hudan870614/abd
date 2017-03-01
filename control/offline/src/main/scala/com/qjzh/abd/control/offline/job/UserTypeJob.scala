package com.qjzh.abd.control.offline.job


import com.qjzh.abd.components.label.exe.UserTypeExe
import com.qjzh.abd.control.common.utils.SparkConfig
import com.qjzh.function.log.utils.Syslog

/**
  * Created by damon on 2016/12/26.
  */
object UserTypeJob {
  def main(args:Array[String]):Unit={
    Syslog.info("开始创建server sparkcontent")
    val sparkcontent = SparkConfig.createofflineSparkMain("Forecast")

    Syslog.info("开始对旅客进行汇总...")
    UserTypeExe.init(args)
    UserTypeExe.exe(sparkcontent)
    Syslog.info("汇总完成!!!")

  }
 }
