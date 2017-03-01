package com.qjzh.abd.components.forecast.exe

import com.qjzh.abd.components.comp_common.common.HbaseBusUtils
import com.qjzh.abd.components.comp_common.conf.CommonConf
import com.qjzh.abd.components.forecast.service.{ForecastDayService, ForecastHourService}
import com.qjzh.abd.control.common.utils.ExeType
import com.qjzh.abd.control.exe.CommonExe
import com.qjzh.abd.function.hbase.utils.HbaseUtils
import com.qjzh.function.log.utils.Syslog
import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

/**
  * Created by damon on 2017/1/18.
  */
object ForecastDayExe extends CommonExe{

  // 执行类型
  override def exeType: String      = ExeType.OF_FORECAST
  //执行前预处理
  override def init(args: Array[String] = null) = {
    Syslog.info("初始化Hbase表")
    //建表
    HbaseUtils.createTable(HbaseBusUtils.forecast_umac_nber)
    ForecastDayService.Init(args)
  }
  //执行离线计算
  override def exe(sparkcontent:SparkContext):Unit={
    Syslog.info("开始执行离线计算...")

    Syslog.info("初始化离线数据")

    ForecastDayService.InitSourceData( sparkcontent )
    Syslog.info("开始执行数据预测")
    ForecastDayService.DealForecast()
  }
  override def exe[T: ClassTag](line : DStream[T])={}


}
