package com.qjzh.abd.components.forecast.exe

import com.qjzh.abd.components.comp_common.common.{HbaseBusUtils}
import com.qjzh.abd.components.forecast.service.ForecastAnalyseService
import com.qjzh.abd.control.exe.CommonExe
import com.qjzh.abd.function.hbase.utils.HbaseUtils
import com.qjzh.function.log.utils.Syslog
import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.DStream

/**
  * Created by damon on 2017/1/16.
  */
object ForecastAnalyseExe extends CommonExe{

  var _args: Array[String] = _
  //执行前预处理
  override def init(args: Array[String]): Unit = {
    _args = args
    Syslog.info("初始化Hbase表")
    //建表
    HbaseUtils.createTable(HbaseBusUtils.forecast_umac_nber_analyse)
  }

  //执行(实时)
  override def exe[T: ClassManifest](line: DStream[T]): Unit = {

  }

  //执行(离线)
  override def exe(sparkContext: SparkContext): Unit = {
    ForecastAnalyseService.Init(sparkContext,_args)
    ForecastAnalyseService.InitForecastSourceData
    ForecastAnalyseService.InitRealSourceData
    ForecastAnalyseService.doAnalyse
  }
}
