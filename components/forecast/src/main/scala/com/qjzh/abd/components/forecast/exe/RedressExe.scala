package com.qjzh.abd.components.forecast.exe

import com.qjzh.abd.control.common.utils.ExeType
import com.qjzh.abd.control.exe.CommonExe
import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.DStream

/**
  * Created by damon on 2017/1/9.
  */
object RedressExe extends CommonExe{
  //执行前预处理
  override def init(args: Array[String]): Unit = ExeType.OF_FORECAST

  //执行(实时)
  override def exe[T: ClassManifest](line: DStream[T]): Unit = {}

  //执行(离线)
  override def exe(sparkContext: SparkContext): Unit = {

  }

}
