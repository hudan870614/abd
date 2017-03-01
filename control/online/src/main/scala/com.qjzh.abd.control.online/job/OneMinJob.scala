package com.qjzh.abd.control.online.batch.job

import com.qjzh.abd.components.comp_common.conf.CommonConf
import com.qjzh.abd.components.diff.exe.DiffExe
import com.qjzh.abd.components.gather_mum.exe.GatherMumExe
import com.qjzh.abd.components.in_out_move.exe.New7gUserTraceExe
//import com.qjzh.abd.components.warn_water.exe.WarnWaterExe
import com.qjzh.abd.components.zl.exe.ZlExe
import com.qjzh.abd.control.common.utils.{ExeType, SparkConfig}
/**
  * Created by hushuai on 16/12/14.
  */
object OneMinJob {

  def main(args: Array[String]): Unit = {

    val dStream = SparkConfig.createOnlineSparkMain(CommonConf.project_no+"_OneMinJob",60)

    //1.区域汇总业务
    GatherMumExe.init(args)
    GatherMumExe.exe(dStream)

    //2.区域滞留业务
    ZlExe.init(args)
    ZlExe.exe(dStream)

    //3.骤增业务
    DiffExe.init(args)
    DiffExe.exe(dStream)

    //4.警戒水位业务
//    WarnWaterExe.init(args)
//    WarnWaterExe.exe(dStream)

    //5.单人查询业务
    New7gUserTraceExe.init(args)
    New7gUserTraceExe.exe(dStream)


    SparkConfig.dStreamStart()


  }





}
