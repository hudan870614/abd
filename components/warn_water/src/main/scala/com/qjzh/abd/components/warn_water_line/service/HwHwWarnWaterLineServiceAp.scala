package com.qjzh.abd.components.warn_water_line.service

import com.qjzh.abd.components.comp_common.utils.{GsonTools, SparkWriteToHbaseHelpler}
import com.qjzh.abd.components.warn_water_line.caseview.{ApCase, WarnWaterLineApCacheInfoToRedis, WarnWaterLineApCaseToHbase}
import com.qjzh.abd.components.warn_water_line.conf.{HwWarnWaterConf, HwWarnWaterContext}
import com.qjzh.abd.components.warn_water_line.dao.RedisClientAp
import com.qjzh.abd.components.warn_water_line.utils.ApUtils
import com.qjzh.abd.control.common.view.Report
import com.qjzh.abd.function.common.DateUtils
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable.ListBuffer


/**
  * Ap警戒水位的业务代码层
  * Created by 92025 on 2017/2/17.
  */
object HwHwWarnWaterLineServiceAp {
  /**
    * 保存Ap历史告警或者预警信息到hbase
    * @param apHistoryAwOrEw
    */
  def saveApHisoryAwOrEwLogDataToHbase(apHistoryAwOrEw: DStream[WarnWaterLineApCaseToHbase]): Unit ={
    //定义数据在hbase中的schmes
    def batchWirteAwOrEwLogDataToHbaseDataConvertFun(data: WarnWaterLineApCaseToHbase): Put ={
      val put = new Put(Bytes.toBytes(DateUtils.getTime(data.serverTimeStamp,DateUtils.DATE_MINUTE) +
        HwWarnWaterConf.PREFIX + data.apMac + HwWarnWaterConf.HBASE_TABLE_AP_LOG_DETAIL_ROWKEY_PREFIX))
      put.add(Bytes.toBytes(HwWarnWaterConf.ColumnFamilyName), Bytes.toBytes(HwWarnWaterConf.ColumnName),
        Bytes.toBytes(GsonTools.gson.toJson(data)))
      put
    }
    //写入ap点位人数的详细记录
    SparkWriteToHbaseHelpler.saveDStreamToHbase(apHistoryAwOrEw,
      HwWarnWaterConf.HBASE_TABLE_AP_LOG_DETAIL,batchWirteAwOrEwLogDataToHbaseDataConvertFun)
  }
  /**
    * 写入ap点位人数的详细记录
    * @param computerApWarnWaterLineBusiness
    */
  def saveApHisoryDetailDataToHbase(computerApWarnWaterLineBusiness: DStream[WarnWaterLineApCaseToHbase]): Unit ={
    //定义数据在hbase中的schmes
    def batchWirteToHbaseApDataConvertFun(data: WarnWaterLineApCaseToHbase): Put ={
      val put = new Put(Bytes.toBytes(DateUtils.getTime(data.serverTimeStamp,DateUtils.DATE_MINUTE) +
        HwWarnWaterConf.PREFIX + data.apMac + HwWarnWaterConf.HBASE_TABLE_AP_HISTORY_DETAIL_ROWKEY_PREFIX))
      put.add(Bytes.toBytes(HwWarnWaterConf.ColumnFamilyName), Bytes.toBytes(HwWarnWaterConf.ColumnName),
        Bytes.toBytes(GsonTools.gson.toJson(data)))
      put
    }
    //写入ap点位人数的详细记录
    SparkWriteToHbaseHelpler.saveDStreamToHbase(computerApWarnWaterLineBusiness,
      HwWarnWaterConf.HBASE_TABLE_AP_HISTORY_DETAIL,batchWirteToHbaseApDataConvertFun)
  }
  /**
    * 计算Ap警戒水位的主业务函数
    * @param recordDStream
    * @return
    */
  def computerApWarnWaterLineBusiness(recordDStream: DStream[(String, (ApCase, Option[Iterable[Report]]))]): DStream[WarnWaterLineApCaseToHbase] ={
    val timeMillis: Long = System.currentTimeMillis()
    val warnWaterLineApCaseToHbaseRdd: DStream[WarnWaterLineApCaseToHbase] = recordDStream.map(xx => {
      val apMac: String = xx._1
      val apCase: ApCase = xx._2._1
      val reports: Option[Iterable[Report]] = xx._2._2
      val warnWaterLineApCaseToHbase: WarnWaterLineApCaseToHbase = reports match {
        case Some(listReport) => {
          val cacheInfoToRedis: WarnWaterLineApCacheInfoToRedis = WarnWaterLineApCacheInfoToRedis(listReport.size, timeMillis, apMac)
          //查询或更新该AP的历史人数峰值信息
          val queryOrUpdateRedisApCacheHisMaxSum: WarnWaterLineApCacheInfoToRedis = HwHwWarnWaterLineServiceAp
            .queryOrUpdateRedisApCacheHisMaxSum(cacheInfoToRedis)
          //判断是否告警或预警
          val awOrEw: (Boolean, Boolean) = HwHwWarnWaterLineServiceAp.isAwOrEw(apCase, listReport.size)
          WarnWaterLineApCaseToHbase(timeMillis, apMac, listReport.size,
            queryOrUpdateRedisApCacheHisMaxSum.hisMaxSum,apCase.awValue,apCase.ewValue, awOrEw._1, awOrEw._2)
        }
        case None => {
          val cacheInfoToRedis: WarnWaterLineApCacheInfoToRedis = WarnWaterLineApCacheInfoToRedis(0, timeMillis, apMac)
          val queryOrUpdateRedisApCacheHisMaxSum: WarnWaterLineApCacheInfoToRedis = HwHwWarnWaterLineServiceAp
            .queryOrUpdateRedisApCacheHisMaxSum(cacheInfoToRedis)
          //判断是否告警或预警
          val awOrEw: (Boolean, Boolean) = HwHwWarnWaterLineServiceAp.isAwOrEw(apCase, 0)
          WarnWaterLineApCaseToHbase(timeMillis, apMac, 0, queryOrUpdateRedisApCacheHisMaxSum.hisMaxSum,apCase.awValue,apCase.ewValue, awOrEw._1, awOrEw._2)
        }
      }
      warnWaterLineApCaseToHbase
    })

    warnWaterLineApCaseToHbaseRdd
  }

  /**
    * 获取Ap配置信息Rdd
    * @return RDD[(String, ApCase)]  K:APMAC   V:AP对象
    */
  def getApCaseList(): RDD[(String, ApCase)] ={
    val apCases: ListBuffer[ApCase] = ApUtils.getApCase(20,10,20,30,40)
    val tuples: ListBuffer[(String, ApCase)] = apCases.map(ap => {(ap.apMac, ap)})
//    val context: SparkContext = KafkaEtl.sparkContext
    val context: SparkContext = HwWarnWaterContext.sparkContext
    val parallelize: RDD[(String, ApCase)] = context.parallelize(tuples)
    val reduceByKey: RDD[(String, ApCase)] = parallelize.reduceByKey((x: ApCase, y: ApCase) => {
      x
    })
    reduceByKey
  }

  /**
    * 判断AP是否预警是否告警
    * @param apCase  ap的配置信息
    * @param cruSum  这个ap当前连接的人数
    * @return   tuple(告警,y预警)
    */
  def isAwOrEw(apCase: ApCase,cruSum: Long):(Boolean,Boolean) = {
    var tuple: (Boolean, Boolean) = (false,false)
    if(cruSum >= apCase.awValue){
      tuple = (true,false)
    }
    if(cruSum >= apCase.ewValue && cruSum < apCase.awValue){
      tuple = (false,true)
    }
    tuple
  }

  /**
    * 查询或更新该AP的历史人数峰值信息
    * @param cacheInfoToRedis  缓存数据  查询或者更新历史峰值
    * @return   redis中缓存的历史最大值
    */
  def queryOrUpdateRedisApCacheHisMaxSum(cacheInfoToRedis: WarnWaterLineApCacheInfoToRedis): WarnWaterLineApCacheInfoToRedis ={
    val queryOrUpdateApHisMaxSumByApMac: WarnWaterLineApCacheInfoToRedis = RedisClientAp.
      queryOrUpdateApHisMaxSumByApMac(HwWarnWaterConf.REDIS_HW_HW_SPARK_WARNWATERLINE,cacheInfoToRedis)
    queryOrUpdateApHisMaxSumByApMac
  }

}
