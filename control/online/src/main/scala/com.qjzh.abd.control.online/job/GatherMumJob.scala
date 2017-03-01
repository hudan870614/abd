package com.qjzh.abd.control.online.job

import com.qjzh.abd.components.comp_common.conf.CommonConf
import com.qjzh.abd.components.gather_mum.exe.GatherMumExe
import com.qjzh.abd.control.common.utils.SparkConfig


/**
  * Created by 92025 on 2016/12/20.
  */
object GatherMumJob {
  def main(args: Array[String]): Unit = {
    val dStream = SparkConfig.createOnlineSparkMain(CommonConf.project_no+"_GatherMumJob",60)

    GatherMumExe.init(args)
    GatherMumExe.exe(dStream)

    SparkConfig.dStreamStart()
  }
}
