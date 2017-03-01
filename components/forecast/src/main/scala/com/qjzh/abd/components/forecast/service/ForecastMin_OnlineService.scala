package com.qjzh.abd.components.forecast.service

import com.qjzh.abd.components.comp_common.caseciew.MessageKafka
import com.qjzh.abd.components.comp_common.conf.CommonConf
import com.qjzh.abd.components.comp_common.utils.GsonTools
import com.qjzh.abd.components.forecast.Common.Algorithm
import com.qjzh.abd.components.forecast.view.ForecastHbaseCass
import com.qjzh.abd.components.gather_mum.caseview.StaMemberCaseInfo
import com.qjzh.abd.function.common.DateUtils
import com.qjzh.abd.function.hbase.utils.HbaseUtils
import com.qjzh.function.log.utils.Syslog
import org.apache.parquet.it.unimi.dsi.fastutil.objects.ObjectArrayList
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

/**
  * Created by damon on 2017/2/12.
  */
object ForecastMin_OnlineService {
   def DealDStream[T: ClassTag](line: DStream[T]): Unit =
   {
     Syslog.info("分钟预测流计算")
     if (line.isInstanceOf[DStream[MessageKafka[ObjectArrayList[StaMemberCaseInfo]]]]) {
       Syslog.info("类型转化正确")
       val data = line.asInstanceOf[DStream[MessageKafka[ObjectArrayList[StaMemberCaseInfo]]]]
         .filter(staMem => (staMem._type == CommonConf.WARN_AREA_NUM) && staMem.value != null)
         .flatMap(_.value.toArray)

       val dValue = data.map(_.asInstanceOf[StaMemberCaseInfo]).filter(_.apMac.equals("1_ALL")).map(x => (x.apMac, (x.curMinue, x.mSum)))
       val windowData = dValue.groupByKeyAndWindow(Seconds(60 * 5), Seconds(60))
       windowData.repartition(1).foreachRDD(r => {
         if (!r.isEmpty()) {
           r.collect().foreach(x => {
             val area = x._1
             val listdata: List[(String, Long)] = x._2.toList
             //val listbuffer:ListBuffer[Int] = new ListBuffer[Int]
             val curmin = DateUtils.getCurTime("yyyyMMddHHmm")
             val outlist = listdata.map(_._2.toInt)
             if (outlist.length == 5) {
               val outbox = Algorithm.BoxPlot(outlist)
               val predictData = Algorithm.PreData(outbox, 0.8f, 0.2f)
               Syslog.info("预测数据:" + predictData)
               val rowkey = curmin + "_" + area
               HbaseUtils.writeTable("OnlineForecast", rowkey, "data", GsonTools.gson.toJson(ForecastHbaseCass(predictData)))
             }

           })
         }
       })
     }
   }
}
