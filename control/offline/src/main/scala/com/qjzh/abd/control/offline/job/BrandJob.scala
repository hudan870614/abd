package com.qjzh.abd.control.offline.job

import com.qjzh.abd.components.brand.exe.BrandDigExe
import com.qjzh.abd.control.common.utils.SparkConfig

/**
  * 按天离线汇总逻辑
  */
object BrandJob {



  def main(args: Array[String]): Unit = {

    val sc = SparkConfig.createofflineSparkMain("BrandJob")
    BrandDigExe.init(args)
    BrandDigExe.exe(sc)

  }

}
