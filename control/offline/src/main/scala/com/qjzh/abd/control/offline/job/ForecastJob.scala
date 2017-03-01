package com.qjzh.abd.control.offline.job

import com.qjzh.abd.components.forecast.exe.{ForecastAnalyseExe, ForecastDayExe, ForecastHourExe, ForecastMinExe}
import com.qjzh.abd.control.common.utils.{ExeType, SparkConfig}
import com.qjzh.function.log.utils.Syslog

/**
  * Created by damon on 2016/12/20.
  */
object ForecastJob {
  def main(args:Array[String]):Unit={

    val sparkcontent = SparkConfig.createofflineSparkMain("Forecast")
    Syslog.Init(sparkcontent,"Forecast")
    Syslog.info("开始进行预测")
    Syslog.info("开始创建server sparkcontent")

    Syslog.info("ForecastJob---Forecast Begin!!!")
    /**
      * 小时预测任务
      */
    try{
      Syslog.info("ForecastJob---Forecast_Hour")
      ForecastHourExe.init(args)
      ForecastHourExe.exe(sparkcontent)
    }catch{
      case ex:Exception => Syslog.error("小时预测异常",ex)
    }

    /**
      * 预测评估参数任务
      */
    try
    {
      Syslog.info("ForecastJob---ForecastAnalyseExe")
      ForecastAnalyseExe.init(args)
      ForecastAnalyseExe.exe(sparkcontent)
    }
    catch{
      case ex:Exception => Syslog.error("预测评估异常",ex)
    }

    /**
      *过滤重叠和伪码数据预测
      */
    try
    {
      Syslog.info("ForecastJob---ForecastDayExe")
      ForecastDayExe.init(args)
      ForecastDayExe.exe(sparkcontent)
    }
    catch{
      case ex:Exception => Syslog.error("天预测任务异常",ex)
    }

    Syslog.info("ForecastJob---Forecast_Min")
    ForecastMinExe.init(args)
    ForecastMinExe.exe(sparkcontent)
    Syslog.info("ForecastJob---Forecast Over!!!")
    Syslog.Close()
  }

}
