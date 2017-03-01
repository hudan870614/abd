package com.qjzh.abd.control.online.job

//import com.qjzh.abd.components.warn_water.exe.WarnWaterExe
import com.qjzh.abd.control.common.utils.SparkConfig

/**
  * Created by 92025 on 2016/12/21.
  */
object WarnWaterJob {
  def main(args: Array[String]): Unit = {
    val dStream = SparkConfig.createOnlineSparkMain("WarnWaterJob",60)

    //加载一个
//    val exeClass = SparkConfig.loadOneFileByExeClassName("WarnWaterExe")

//    WarnWaterExe.init(Array())
//    WarnWaterExe.exe(dStream)
    SparkConfig.dStreamStart()
  }
}
