package com.qjzh.abd.components.label.exe

import com.qjzh.abd.components.comp_common.common.{HbaseBusUtils, RedisBusUtils}
import com.qjzh.abd.components.comp_common.utils.ComUtils
import com.qjzh.abd.components.label.service.{LabelDayService}
import com.qjzh.abd.control.exe.CommonExe
import com.qjzh.abd.function.common.DateUtils
import com.qjzh.abd.function.hbase.utils.HbaseUtils
import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

/**
  * 标签执行器
  * Created by 92025 on 2016/10/24.
  */
object LabelDigExe extends CommonExe{

  //要执行的日期
  var _exe_before_day : String  = null


  //执行前预处理
  override def init(args: Array[String]): Unit = {

    HbaseUtils.createTableInTTL(HbaseBusUtils.da_umac_behaviour, HbaseBusUtils.LIMIT_ONE_DAY_TTL_TIME * 90)
    HbaseUtils.createTable(HbaseBusUtils.da_umac_brand)

    //得到执行日期
    _exe_before_day = ComUtils.getBeforeDayByPrams(args)

//    _exe_before_day = getRedisDayByPrams()
  }

  //执行(实时)
  override def exe[T: ClassTag](line: DStream[T]): Unit = {}

  //执行(离线)
  override def exe(sparkContext: SparkContext): Unit = {

    if(_exe_before_day != null){
      LabelDayService.dealDay(_exe_before_day,sparkContext)
    }


  }

  def getRedisDayByPrams(): String ={
    var exeDay : String = null
    val index = RedisBusUtils.incrRowKey("Spark:ExeData:Index",0,true,RedisBusUtils.second_one_day * 30).toInt
    if(index <= 60){
      exeDay = DateUtils.getBeforeDayByTagDay("20170230",(-1) *index,"yyyy/MM/dd")
    }
    exeDay

  }
}
