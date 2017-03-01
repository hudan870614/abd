package com.qjzh.abd.control.online.window.job

import com.qjzh.abd.components.diff.exe.DiffExe
import com.qjzh.abd.control.common.utils.SparkConfig

/**
  * Created by 92025 on 2016/12/21.
  */
object DiffJob {
  def main(args: Array[String]): Unit = {
    val dStream = SparkConfig.createOnlineSparkMain("DiffJob", 60)

//    //加载一个//加载一个
//    val exeClass = SparkConfig.loadOneFileByExeClassName("DiffExe")

    DiffExe.init(Array())
    DiffExe.exe(dStream)
    SparkConfig.dStreamStart()
  }
}
