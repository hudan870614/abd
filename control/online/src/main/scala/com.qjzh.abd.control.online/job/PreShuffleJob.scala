package com.qjzh.abd.control.online.job

import com.qjzh.abd.components.comp_common.conf.CommonConf
import com.qjzh.abd.components.pre_shuffle.exe.PreShuffleExe
import com.qjzh.abd.control.common.utils.SparkConfig

/**
  * Created by hushuai on 16/12/14.
  */
object PreShuffleJob {

  def main(args: Array[String]): Unit ={

//    val ru = scala.reflect.runtime.universe
//    val m = ru.runtimeMirror(getClass.getClassLoader)
//    val pack = m.staticClass("com.qjzh.abd.components.pre_shuffle.exe.PreShuffleExe")

    val dStream = SparkConfig.createOnlineSparkMain(CommonConf.project_no+"_PreShuffleJob",60)

    PreShuffleExe.init(args)

    PreShuffleExe.exe(dStream)

    SparkConfig.dStreamStart()


  }





}
