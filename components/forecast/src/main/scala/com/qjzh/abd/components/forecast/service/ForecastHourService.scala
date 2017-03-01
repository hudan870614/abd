package com.qjzh.abd.components.forecast.service

import com.qjzh.abd.components.comp_common.caseciew.MessageKafka
import com.qjzh.abd.components.comp_common.common.{HbaseBusUtils, HdfsUtils, KafkaUtils}
import com.qjzh.abd.components.comp_common.conf.CommonConf
import com.qjzh.abd.components.comp_common.utils.GsonTools
import com.qjzh.abd.components.forecast.Common.{Algorithm, ForecastUtil}
import com.qjzh.abd.components.forecast.conf.ForecastCommonConf
import com.qjzh.abd.components.forecast.view.{ForecastCass, ForecastHbaseCass, ForecastRddCass, SendForecastCass}
import com.qjzh.abd.function.common.DateUtils
import com.qjzh.abd.function.hbase.utils.HbaseUtils
import com.qjzh.abd.function.hdfs.utils.HdfsBase
import com.qjzh.function.log.utils.Syslog
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

/**
  * Created by damon on 2017/1/6.
  */
object ForecastHourService {
  var _hisMemRdd: RDD[ForecastCass]      = _
  var _sc:SparkContext              = _
  var logString:String              = _


  def InitSourceData(sparkcontent:SparkContext):Unit={
    Syslog.info("初始化预测数据...")
    //获取最近90天 三个月的数据
    val startday = DateUtils.getBeforeDay("yyyyMMdd",-90)
    val curday = DateUtils.getCurTime("yyyyMMdd")
    val minhdfs = HdfsBase.FUN_HDFS_URL + CommonConf.hdfsDic + "/"+HdfsUtils.umac_nber_hdfs_file_path+"/*/*/*/*"
    _hisMemRdd  = sparkcontent.textFile(minhdfs).map(_.split(","))
      .filter(x => x.length >=5 && x(0).length>=12)
      .map(x => {
        val ymd     = x(0).substring(0,8)
        val week    = ForecastUtil.getWeek(ymd)
        val hourmin = x(0).substring(8,12)
        val hour    = x(0).substring(8,10)
        val minute  = x(0).substring(10,12)
        ForecastCass(x(0),ymd,week,hourmin,hour,minute,x(1),x(2).toInt,x(3).toInt,x(4).toInt)
      }
      ).filter(x => x.ymd >= startday && x.ymd < curday && (x.stype.equals("1_ALL") || x.stype.equals("IMP_ALL")))
    _hisMemRdd.cache()
    _sc = sparkcontent
  }


  def DealForecast():Unit= {
    Syslog.info("开始按小时进行预测...")
    val curday = DateUtils.getCurTime("yyyyMMdd")
    Syslog.info("计算日期"+curday)
    //预测当天人流
    forecast(curday,0.2f,0.8f,true)
    val nextday   = DateUtils.getBeforeDayByTagDay(curday,1)
    Syslog.info("计算日期"+nextday)
    //预测隔天人流
    forecast(nextday,0.5f,0.5f,false)
  }

  def forecast(forecastday:String,weight_cur:Float,weight_his:Float,isSave:Boolean):Unit={

    Syslog.info("预测日期"+forecastday)
    //判断历史数据week 是否足够 有的话考虑周期性
    var flag: Boolean = false;
    var forecastMemRdd: RDD[ForecastCass]      = null
    val week = ForecastUtil.getWeek(forecastday)

    val out = _hisMemRdd.filter(_.week == week).map(_.ymd)
    var alldate:String=""
    out.distinct().collect().toList.foreach(x => {alldate += ("日期:"+x)})
    Syslog.info(alldate)
    val daycount = out.distinct().collect().size
    if (daycount >= 3) {
      forecastMemRdd = _hisMemRdd.filter(_.week == week)
      flag = true
      Syslog.info("周期性:"+daycount)
    }else {
      forecastMemRdd = _hisMemRdd
      Syslog.info("无周期性:"+daycount)
    }
    val listforecast    = new ListBuffer[(String,Int)]
    val para_keyValues  = new Object2ObjectOpenHashMap[String,Object2ObjectOpenHashMap[String,String]]
    val flowList        = new ListBuffer[ForecastRddCass]
    forecastMemRdd.filter(_.minute.equals("59"))
      .map( x => ((x.hour,x.stype),x)).groupByKey().collect().foreach(x => {
      val hourlist      = x._2.toList.sortBy(_.time).map(_.hournum)
      //箱型图剔除异常值
      var fliterlist:List[Int] = null
      if(hourlist.size >= 4)
        fliterlist  = Algorithm.BoxPlot(hourlist)
      else
        fliterlist  = hourlist
      if( fliterlist.length > 0 )
      {
        var forecastday_data   = Algorithm.PreData(fliterlist,weight_cur,weight_his)
        val week = ForecastUtil.getWeek(forecastday)
        //如果数据没有周期性且日期是周末就引入波动阈值
        if(false == flag && (week == 1 || week == 7)) {
          forecastday_data = forecastday_data + (forecastday_data * 0.2).toInt
          Syslog.info("加入波动阈值")
        }
        flowList.append( ForecastRddCass(forecastday,x._1._1,x._1._2,forecastday_data) )
        val forecastday_rowkey  = forecastday+x._1._1+"_"+x._1._2
        val forecastday_value   = new Object2ObjectOpenHashMap[String,String]

        forecastday_value.put("data",GsonTools.gson.toJson(ForecastHbaseCass(forecastday_data)))
        para_keyValues.put(forecastday_rowkey,forecastday_value)
        if( x._1._2.equals("1_ALL") )
          listforecast.append((x._1._1,forecastday_data))
        if( x._1._2.equals("1_ALL") || x._1._2.equals("IMP_ALL") )
          {
            KafkaUtils.sendMsgToKafka(KafkaUtils.off_line_one_day_mub,
              MessageKafka[SendForecastCass](CommonConf.FORECAST_DELAY_NUM,
              DateUtils.getCurTime("yyyyMMddHHmm").toLong,SendForecastCass(forecastday_rowkey,forecastday_data)))
          }
      }
    })
    Syslog.info("预测数据写入Hbase 表...")
    HbaseUtils.writeTable(HbaseBusUtils.forecast_umac_nber, para_keyValues)

    /**
      * 保存预测结果 用于按小时的矫正程序
      */
    if(isSave)
    {
      val year    = forecastday.substring(0,4)
      val month   = forecastday.substring(4,6)
      val day     = forecastday.substring(6,8)
      val format  = year+"/"+month+"/"+day
      val path    = CommonConf.hdfsDic+"/temp/forecast/hour/"+format
      HdfsBase.delFile(path)
      _sc.parallelize(flowList.toList,1).saveAsObjectFile(path)
    }
  }
}
