package com.qjzh.abd.components.forecast.exe


import com.qjzh.abd.components.comp_common.common.HbaseBusUtils
import com.qjzh.abd.components.comp_common.conf.CommonConf
import com.qjzh.abd.components.forecast.conf.ForecastCommonConf
import com.qjzh.abd.components.forecast.service.ForecastHourService
import com.qjzh.abd.control.common.utils.ExeType
import com.qjzh.abd.control.exe.CommonExe
import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.DStream
import com.qjzh.abd.function.hbase.utils.HbaseUtils
import com.qjzh.function.log.utils.Syslog

import scala.reflect.ClassTag
/**
  * Created by damon on 2016/12/19.
  */
object ForecastHourExe extends CommonExe{

  // 执行类型
  override def exeType: String      = ExeType.OF_FORECAST
  //执行前预处理
  override def init(args: Array[String] = null) = {
    Syslog.info("初始化Hbase表")
    //建表
    HbaseUtils.createTable(HbaseBusUtils.forecast_umac_nber)
  }
  //执行离线计算
  override def exe(sparkcontent:SparkContext):Unit={
    Syslog.info("开始执行离线计算...")

    Syslog.info("初始化离线数据")
    ForecastHourService.InitSourceData(sparkcontent)
    Syslog.info("开始执行数据预测")
    ForecastHourService.DealForecast()
  }
  override def exe[T: ClassTag](line : DStream[T])={}


}
