package com.qjzh.abd.control.exe

import com.qjzh.abd.control.common.utils.ExeType
import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

/**
  * Created by hushuai on 16/12/14.
  */
trait CommonExe {

  // 执行类型
  def exeType : String = ExeType.ON_ONE_MIN

  //优先级
  def priority : Int = 0

  //执行前预处理
  def init(args: Array[String] = null)

  //执行(实时)
  def exe[T: ClassTag](line : DStream[T])

  //执行(离线)
  def exe(sparkContext:SparkContext)
}
