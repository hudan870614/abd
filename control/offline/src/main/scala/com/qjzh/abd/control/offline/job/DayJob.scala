package com.qjzh.abd.control.offline.job

import com.qjzh.abd.components.label.exe.LabelDigExe
import com.qjzh.abd.control.common.utils.SparkConfig

/**
  * 按天离线汇总逻辑
  */
object DayJob {



  def main(args: Array[String]): Unit = {

    val sc = SparkConfig.createofflineSparkMain("DayJob")
    LabelDigExe.init(args)
    LabelDigExe.exe(sc)

  }

}
