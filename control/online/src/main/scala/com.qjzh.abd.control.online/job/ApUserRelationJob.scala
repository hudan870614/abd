package com.qjzh.abd.control.online.job

import com.qjzh.abd.components.gather_mum.exe.ApUserRelationExe
import com.qjzh.abd.control.common.utils.SparkConfig

/**
  * Created by 92025 on 2017/1/9.
  */
object ApUserRelationJob {
  def main(args: Array[String]): Unit = {
    val dStream = SparkConfig.createOnlineSparkMain("New7gApAndUserRelationJob",60)

    ApUserRelationExe.init(args)
    ApUserRelationExe.exe(dStream)

    SparkConfig.dStreamStart()

  }
}
