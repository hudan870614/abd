package com.qjzh.abd.components.forecast.exe
import com.qjzh.abd.components.forecast.service.ForecastMin_OnlineService
import com.qjzh.abd.control.common.utils.ExeType
import com.qjzh.abd.control.exe.CommonExe
import com.qjzh.abd.function.hbase.utils.HbaseUtils
import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

/**
  * Created by damon on 2017/2/12.
  */
object ForecastMin_OnlineExe extends CommonExe{
  //执行类型
  override def exeType: String = ExeType.ON_FORECAST

  //优先级
  override def priority: Int = super.priority
  //执行前预处理
  override def init(args: Array[String]): Unit = {
    HbaseUtils.createTable("OnlineForecast")
  }

  //执行(实时)
  override def exe[T: ClassTag](line: DStream[T]): Unit =
  {
    ForecastMin_OnlineService.DealDStream(line)
  }
  //执行
  override def exe(sparkContext: SparkContext): Unit = {}
}
