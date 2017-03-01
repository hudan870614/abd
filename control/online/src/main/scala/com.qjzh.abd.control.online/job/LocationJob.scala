package com.qjzh.abd.control.online.job

import com.qjzh.abd.components.comp_common.conf.CommonConf
import com.qjzh.abd.components.location.exe.OutdoorLocationExe
import com.qjzh.abd.control.common.utils.SparkConfig

/**
  * Created by yufeiwang on 20/12/2016.
  */
object LocationJob {
  def main(args: Array[String]): Unit = {

    val dStream = SparkConfig.createOnlineSparkMain(CommonConf.project_no +"_LocationJob",5)


    OutdoorLocationExe.init()
    OutdoorLocationExe.exe(dStream)

    SparkConfig.dStreamStart()


  }

}
