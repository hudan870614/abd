package com.qjzh.abd.control.online.job

import com.qjzh.abd.components.comp_common.conf.CommonConf
import com.qjzh.abd.components.in_out_move.exe.New7gUserTraceExe
import com.qjzh.abd.control.common.utils.SparkConfig

/**
  * Created by 92025 on 2016/12/27.
  */
object InOutMoveJob {
  def main(args: Array[String]): Unit = {
    val dStream = SparkConfig.createOnlineSparkMain(CommonConf.project_no+"_InOutMoveJob",60)

    //加载一个
    //    val exeClass = SparkConfig.loadOneFileByExeClassName("WarnWaterExe")

    New7gUserTraceExe.init(Array())
    New7gUserTraceExe.exe(dStream)
    SparkConfig.dStreamStart()
  }
}
