package com.qjzh.abd.components.forecast.service

import com.qjzh.abd.components.comp_common.common.{HbaseBusUtils, HdfsUtils}
import com.qjzh.abd.components.comp_common.conf.CommonConf
import com.qjzh.abd.components.comp_common.utils.GsonTools
import com.qjzh.abd.components.forecast.Common.{Algorithm, ForecastUtil}
import com.qjzh.abd.components.forecast.view.{ForecastCass, ForecastHbaseCass}
import com.qjzh.abd.function.common.DateUtils
import com.qjzh.abd.function.hbase.utils.HbaseUtils
import com.qjzh.abd.function.hdfs.utils.HdfsBase
import com.qjzh.function.log.utils.Syslog
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

/**
  * Created by damon on 2017/1/9.
  */
object ForecastMinService {
  var _hisMemRdd: RDD[ForecastCass]      = _


  def InitSourceData(sparkcontent:SparkContext):Unit= {
    Syslog.info("初始化按分钟预测数据")
    val startday = DateUtils.getBeforeDay("yyyyMMdd",-90)
    val curday = DateUtils.getCurTime("yyyyMMdd")
    val minhdfs = HdfsBase.FUN_HDFS_URL + CommonConf.hdfsDic + HdfsUtils.umac_nber_hdfs_file_path+"/*/*/*/*"
    _hisMemRdd = sparkcontent.textFile(minhdfs).map(_.split(","))
      .filter(x => x.length >= 5 && x(0).length >= 12)
      .map(x => {
        val ymd = x(0).substring(0, 8)
        val week    = ForecastUtil.getWeek(ymd)
        val hourmin = x(0).substring(8, 12)
        val hour = x(0).substring(8, 10)
        val minute = x(0).substring(10, 12)
        ForecastCass(x(0), ymd, week,hourmin, hour, minute, x(1), x(2).toInt, x(3).toInt, x(4).toInt)
      }
      ).filter(x => x.ymd >= startday && x.ymd < curday)
    _hisMemRdd.cache()
  }
  def DealForecast():Unit={
    Syslog.info("开始进行预测...")

    //判断历史数据week 是否足够 有的话考虑周期性
    var flag: Boolean = false;
    val curday = DateUtils.getCurTime("yyyyMMdd")
    val week = ForecastUtil.getWeek(curday)
    var forecastMemRdd: RDD[ForecastCass]      = null
    val daycount = _hisMemRdd.filter(_.week == week).map(_.ymd).distinct().collect().size
    if (daycount >= 3) {
      forecastMemRdd = _hisMemRdd.filter(_.week == week)
      flag = true
    }else {
      forecastMemRdd = _hisMemRdd
    }

    val para_keyValues  = new Object2ObjectOpenHashMap[String,ListBuffer[Object2ObjectOpenHashMap[String,String]]]

    forecastMemRdd.map( x => ((x.hourmin,x.stype),x))
      .groupByKey().collect()
      .sortBy(_._1._1)
      .foreach(x =>
      {
        val minnumlist    = x._2.toList.sortBy(_.time).map(_.minutenum)
        //箱型图剔除异常值
        //箱型图剔除异常值
        var fliterlist:List[Int] = null
        if(minnumlist.size >= 4)
          fliterlist  = Algorithm.BoxPlot(minnumlist)
        else
          fliterlist  = minnumlist
        var forecastdata  = Algorithm.PreData(fliterlist,0.5f,0.5f)
        val week = ForecastUtil.getWeek(curday)
        //如果数据没有周期性且是周末 就引入波动阈值
        if(false == flag && (week == 1 || week == 7))
          forecastdata = forecastdata+(forecastdata*0.2).toInt
        val rowkey    = curday+x._1._1.substring(0,2)+"_"+x._1._2+"_MIN"
        val col_value = new Object2ObjectOpenHashMap[String,String]
        val col       = x._1._1.substring(2,4)

        col_value.put(col,GsonTools.gson.toJson(ForecastHbaseCass(forecastdata)))
        val valuelist:ListBuffer[Object2ObjectOpenHashMap[String,String]] = para_keyValues.get(rowkey)
        if(valuelist != null)
        {
          valuelist.append(col_value)
        }else {
          val tempvaluelist = new ListBuffer[Object2ObjectOpenHashMap[String,String]]
          tempvaluelist.append(col_value)
          para_keyValues.put(rowkey,tempvaluelist)
        }

      })
    Syslog.info("预测结束写入hbase表...")
    HbaseUtils.writeTableBatch(HbaseBusUtils.forecast_umac_nber, para_keyValues)
  }
}
