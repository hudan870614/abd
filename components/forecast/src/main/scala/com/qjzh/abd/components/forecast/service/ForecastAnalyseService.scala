package com.qjzh.abd.components.forecast.service

import com.qjzh.abd.components.comp_common.common.{HbaseBusUtils, HdfsUtils}
import com.qjzh.abd.components.comp_common.utils.GsonTools
import com.qjzh.abd.components.forecast.Common.{Algorithm, ForecastUtil}
import com.qjzh.abd.components.forecast.view.{AnalyseHbaseCass, ForecastCass, ForecastRddCass, HourFlowCass}
import com.qjzh.abd.function.common.DateUtils
import com.qjzh.abd.function.hbase.utils.HbaseUtils
import com.qjzh.function.log.utils.Syslog
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

/**
  * Created by damon on 2017/1/16.
  */
object ForecastAnalyseService {
  var _forecastMemRdd: RDD[ForecastRddCass]       = _
  var _realMemRdd: RDD[ForecastCass]              = _

  var _sc:SparkContext                            = _

  val forecast_keyValues = new Object2ObjectOpenHashMap[String,Object2ObjectOpenHashMap[String,Int]]
  val real_keyValues = new Object2ObjectOpenHashMap[String,Object2ObjectOpenHashMap[String,Int]]

  var _args: Array[String]      = _
  var _formatanalyseday:String  = ""
  var _analyseday:String        = ""

  def Init(sparkContext:SparkContext,args: Array[String]):Unit= {
    _sc = sparkContext
    _args = args

    if (_args.length > 0)
    {
      _analyseday = _args(0)
      val year  = _analyseday.substring(0,4)
      val month = _analyseday.substring(4,6)
      val day   = _analyseday.substring(6,8)
      _formatanalyseday = year+"/"+month+"/"+day
    }
    else
    {
      _analyseday         = DateUtils.getBeforeDay("yyyyMMdd", -1)
      _formatanalyseday   = DateUtils.getBeforeDay("yyyy/MM/dd", -1)
    }
    Syslog.info("日期"+_analyseday)
  }

  def InitForecastSourceData():Unit={
    val filePath = HdfsUtils.getPreHourSaveFilePath( HdfsUtils.forecast_hour_hdfs_analyse_path,_formatanalyseday+"/part*")
    Syslog.info("读取hdfs 位置"+filePath)
    _forecastMemRdd = _sc.objectFile(filePath,3)
    _forecastMemRdd.cache()

    _forecastMemRdd.map(x => (x.stype,x)).distinct()
      .groupByKey.collect().foreach(data =>{
      val hourlist = new Object2ObjectOpenHashMap[String,Int]
      data._2.toList.sortBy(_.hour).foreach(h =>
      {
        //Syslog.info("预测数据 date:"+h.date+" hour:"+h.hour+" stype:"+h.stype+" flownum:"+h.flownum)
        hourlist.put(h.hour,h.flownum.toInt)
      })
      forecast_keyValues.put(data._1,hourlist)
    })
  }

  def InitRealSourceData():Unit={
    Syslog.info("初始化预测数据...")

    val filePath = HdfsUtils.getPreHourSaveFilePath(HdfsUtils.umac_nber_hdfs_file_path,_formatanalyseday+"/*")
    _realMemRdd  = _sc.textFile(filePath).map(_.split(","))
      .filter(x => x.length >=5 && x(0).length>=12)
      .map(x => {
        val ymd     = x(0).substring(0,8)
        val week    = ForecastUtil.getWeek(ymd)
        val hourmin = x(0).substring(8,12)
        val hour    = x(0).substring(8,10)
        val minute  = x(0).substring(10,12)
        ForecastCass(x(0),ymd,week,hourmin,hour,minute,x(1),x(2).toInt,x(3).toInt,x(4).toInt)
      }
      )
    _realMemRdd.cache()

    _realMemRdd.filter(x => x.minute.equals("59") ).map(x => (x.stype,x)).distinct()
      .groupByKey.collect().foreach(data =>{
      val hourlist = new Object2ObjectOpenHashMap[String,Int]
      data._2.toList.sortBy(_.hour).foreach(h => {
        //Syslog.info("实际数据 date:"+h.time+" hour:"+h.hour+" stype:"+h.stype+" flownum:"+h.hournum)
        hourlist.put( h.hour,h.hournum )
      })
      real_keyValues.put(data._1,hourlist)
    })

  }

  /**
    * 校验补全对于丢失数据少于4组的 认为等于预测值
    */
  def Check(stype:String,forecast:Object2ObjectOpenHashMap[String,Int],real:Object2ObjectOpenHashMap[String,Int]):(ListBuffer[HourFlowCass],ListBuffer[HourFlowCass])={
    val RealList      = new ListBuffer[HourFlowCass]
    val ForecastList  = new ListBuffer[HourFlowCass]
    for(hourIndex  <- 0 to 23 )
        {
          val forecasthournum:Int = forecast.get(hourIndex.toString)
          val forecasthourflow = HourFlowCass(hourIndex.toString,forecasthournum)
          ForecastList.append( forecasthourflow )
          Syslog.info("区域"+stype+" 预测数据 时间点"+hourIndex+" 数据:"+forecasthournum)
          val realhournum:Int = real.get(hourIndex.toString)
          if(realhournum == null || realhournum == 0)
            {
                RealList.append( forecasthourflow )
              Syslog.info("区域"+stype+"实际数据为空 填补 时间点"+hourIndex+" 数据:"+forecasthournum)
            }else{
                RealList.append( HourFlowCass(hourIndex.toString,realhournum) )
            Syslog.info("区域"+stype+"实际 时间点"+hourIndex+" 数据:"+realhournum)
          }
        }
    (ForecastList,RealList)
  }

  def doAnalyse():Unit=
  {
    val stypelist = _forecastMemRdd.map(_.stype).intersection( _realMemRdd.map(_.stype) ).distinct().collect()
    Syslog.info("stypelist"+stypelist.size)
    stypelist.foreach(stype => {
      val forecast:Object2ObjectOpenHashMap[String,Int] = forecast_keyValues.get( stype )
      val real:Object2ObjectOpenHashMap[String,Int]     = real_keyValues.get( stype )
      Syslog.info("stype:"+stype)
      if(forecast != null && real != null)
      {
        val rowkey = _analyseday+"_"+stype
        if(forecast.size == 24 && (forecast.size - real.size<=4))
        {
          val checklist = Check(stype,forecast,real)
          val forecastlist  = checklist._1.map(_.flownum).toList
          val reallist      = checklist._2.map(_.flownum).toList

          Syslog.info("reallist:"+reallist.size+" forecast:"+forecast.size)

          //计算欧式距离相似度
          val euDis    = Algorithm.EuclideanDistance(forecastlist,reallist)
          //计算皮尔森相关系数
          val pearson  = Algorithm.Pearson(forecastlist,reallist)
          //计算残差和
          val residual = Algorithm.Residual(forecastlist,reallist)
          //计算均方误差
          val rmse     = Algorithm.Meanavg(forecastlist,reallist)
          Syslog.info("euDis:"+euDis+" pearson:"+pearson+" residual:"+residual+" rmse"+rmse)
          HbaseUtils.writeTable(HbaseBusUtils.forecast_umac_nber_analyse,
            rowkey, "data",
            GsonTools.gson.toJson( AnalyseHbaseCass(euDis,pearson,residual,rmse) ))

        }else
        {
          HbaseUtils.writeTable(HbaseBusUtils.forecast_umac_nber_analyse,
            rowkey, "data",
            GsonTools.gson.toJson( AnalyseHbaseCass(-1,-1,-1,-1) ))
        }
      }

    })
  }
}
