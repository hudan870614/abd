package com.qjzh.abd.control.online.job

import com.qjzh.abd.components.comp_common.conf.CommonConf
import com.qjzh.abd.components.zl.exe.ZlExe
import com.qjzh.abd.control.common.utils.SparkConfig

/**
  * Created by 92025 on 2016/12/23.
  */
object ZlJob {
  def main(args: Array[String]): Unit = {
    val dStream = SparkConfig.createOnlineSparkMain(CommonConf.project_no+"_ZlJob",60)

    //加载一个
    //    val exeClass = SparkConfig.loadOneFileByExeClassName("WarnWaterExe")

    ZlExe.init(Array())
    ZlExe.exe(dStream)
    SparkConfig.dStreamStart()
  }
}
