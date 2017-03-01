package com.qjzh.abd.control.offline.job

import com.qjzh.abd.control.common.utils.SparkConfig
import com.qjzh.function.log.utils.Syslog

/**
  * 按小时离线汇总逻辑
  */
object HourJob {



  def main(args: Array[String]): Unit = {
    val sparkContent = SparkConfig.createofflineSparkMain("HourJob")
//    ImportCrowdExe.init(args)
//    ImportCrowdExe.exe(sparkContent)

  }

}
