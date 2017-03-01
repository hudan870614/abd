package com.qjzh.abd.control.online.job

import com.qjzh.abd.components.comp_common.conf.CommonConf
import com.qjzh.abd.components.user_relation.exe.UserRelationExe
import com.qjzh.abd.control.common.utils.SparkConfig

/**
  * Created by 92025 on 2017/1/12.
  */
object UserRelationJob {
  def main(args: Array[String]): Unit = {
    val dStream = SparkConfig.createOnlineSparkMain(CommonConf.project_no+"_UserRelation",60)
    UserRelationExe.init(Array())
    UserRelationExe.exe(dStream)
    SparkConfig.dStreamStart()
  }
}
