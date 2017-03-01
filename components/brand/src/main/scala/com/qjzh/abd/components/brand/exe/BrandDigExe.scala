package com.qjzh.abd.components.brand.exe


import com.qjzh.abd.components.brand.service.BrandService
import com.qjzh.abd.components.comp_common.utils.ComUtils
import com.qjzh.abd.control.exe.CommonExe
import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

/**
  * 品牌执行器
  * Created by 92025 on 2016/10/24.
  */
object BrandDigExe extends CommonExe{

  //要执行的日期
  var _exe_before_day : String  = null


  //执行前预处理
  override def init(args: Array[String]): Unit = {

    //得到执行日期
    _exe_before_day = ComUtils.getBeforeDayByPrams(args)

  }

  //执行(实时)
  override def exe[T: ClassTag](line: DStream[T]): Unit = {}

  //执行(离线)
  override def exe(sparkContext: SparkContext): Unit = {


    BrandService.doExe(_exe_before_day,sparkContext)


  }

}
