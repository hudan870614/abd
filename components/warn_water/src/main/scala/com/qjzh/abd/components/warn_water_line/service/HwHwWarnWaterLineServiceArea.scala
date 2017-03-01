package com.qjzh.abd.components.warn_water_line.service

import com.qjzh.abd.components.comp_common.caseciew.SynAreaBean
import com.qjzh.abd.components.comp_common.utils.{GsonTools, SparkWriteToHbaseHelpler}
import com.qjzh.abd.components.warn_water_line.caseview.{WarnWaterLineAreaCacheInfoToRedis, WarnWaterLineAreaCaseToHbase}
import com.qjzh.abd.components.warn_water_line.conf.{HwWarnWaterConf, HwWarnWaterContext}
import com.qjzh.abd.components.warn_water_line.dao.RedisClientArea
import com.qjzh.abd.control.common.view.Report
import com.qjzh.abd.function.common.DateUtils
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream


/**
  * Created by 92025 on 2017/2/20.
  */
object HwHwWarnWaterLineServiceArea {
  private val redisClientArea: RedisClientArea.type = RedisClientArea



  /**
    * 保存区块历史告警或者预警信息到hbase
    * @param areaHistoryAwOrEw
    */
  def saveAreaHisoryAwOrEwLogDataToHbase(areaHistoryAwOrEw: DStream[WarnWaterLineAreaCaseToHbase]): Unit ={
    //定义数据在hbase中的schmes
    def batchWirteToHbaseAreaDataConvertFun(data: WarnWaterLineAreaCaseToHbase): Put ={
      val put = new Put(Bytes.toBytes(DateUtils.getTime(data.serverTimeStamp,DateUtils.DATE_MINUTE) +
        HwWarnWaterConf.PREFIX + data.areaNo + HwWarnWaterConf.HBASE_TABLE_AREA_LOG_DETAIL_ROWKEY_PREFIX))
      put.add(Bytes.toBytes(HwWarnWaterConf.ColumnFamilyName), Bytes.toBytes(HwWarnWaterConf.ColumnName),
        Bytes.toBytes(GsonTools.gson.toJson(data)))
      put
    }
    //写入区块人数的详细记录
    SparkWriteToHbaseHelpler.saveDStreamToHbase(areaHistoryAwOrEw,
      HwWarnWaterConf.HBASE_TABLE_AREA_LOG_DETAIL,batchWirteToHbaseAreaDataConvertFun)
  }

  /**
    * 写入区块人数的详细记录
    * @param computerAreaWarnWaterLineBusiness
    */
  def saveAreaHisoryDetailDataToHbase(computerAreaWarnWaterLineBusiness: DStream[WarnWaterLineAreaCaseToHbase]): Unit ={
    //定义数据在hbase中的schmes
    def batchWirteToHbaseAreaDataConvertFun(data: WarnWaterLineAreaCaseToHbase): Put ={
      val put = new Put(Bytes.toBytes(DateUtils.getTime(data.serverTimeStamp,DateUtils.DATE_MINUTE) +
        HwWarnWaterConf.PREFIX + data.areaNo + HwWarnWaterConf.HBASE_TABLE_AREA_HISTORY_DETAIL_ROWKEY_PREFIX))
      put.add(Bytes.toBytes(HwWarnWaterConf.ColumnFamilyName), Bytes.toBytes(HwWarnWaterConf.ColumnName),
        Bytes.toBytes(GsonTools.gson.toJson(data)))
      put
    }
    //写入区块人数的详细记录
    SparkWriteToHbaseHelpler.saveDStreamToHbase(computerAreaWarnWaterLineBusiness,
      HwWarnWaterConf.HBASE_TABLE_AREA_HISTORY_DETAIL,batchWirteToHbaseAreaDataConvertFun)
  }
  /**
    * 判断区域是否预警是否告警
    * @param synAreaBean  区域的配置信息
    * @param cruSum  这个区域当前的人数
    * @return   tuple(告警,预警)
    */
  def isAwOrEw(synAreaBean: SynAreaBean, cruSum: Long): (Boolean, Boolean) = {
    var tuple: (Boolean, Boolean) = (false,false)
    if(cruSum >= synAreaBean.waMinStage){
      tuple = (true,false)
    }
    if(cruSum >= synAreaBean.paMinStage && cruSum < synAreaBean.waMinStage){
      tuple = (false,true)
    }
    tuple
  }

  /**
    * 查询或更新该区域的历史人数峰值信息
    *
    * @param cacheInfoToRedis  缓存数据  查询或者更新历史峰值
    * @return   redis中缓存的历史最大值
    */
  def queryOrUpdateRedisAreaCacheHisMaxSum(cacheInfoToRedis: WarnWaterLineAreaCacheInfoToRedis): WarnWaterLineAreaCacheInfoToRedis = {
    val queryOrUpdateAreaHisMaxSumByApMac: WarnWaterLineAreaCacheInfoToRedis = RedisClientArea.
      queryOrUpdateAreaHisMaxSumByAreaMac(HwWarnWaterConf.REDIS_HW_HW_SPARK_WARNWATERLINE,cacheInfoToRedis)
    queryOrUpdateAreaHisMaxSumByApMac
  }




  /**
    * 计算区域警戒水位的主业务函数
    * @param recordDStream
    * @return
    */
  def computerAreaWarnWaterLineBusiness(recordDStream: DStream[(String, (SynAreaBean, Option[Iterable[Report]]))]):DStream[WarnWaterLineAreaCaseToHbase]={
    val timeMillis: Long = System.currentTimeMillis()
    val warnWaterLineAreaCaseToHbaseRdd: DStream[WarnWaterLineAreaCaseToHbase] = recordDStream.map(xx => {
      val areaNo: String = xx._1
      val synAreaBean: SynAreaBean = xx._2._1
      val reports: Option[Iterable[Report]] = xx._2._2
      val warnWaterLineAreaCaseToHbase: WarnWaterLineAreaCaseToHbase = reports match {
        case Some(listReport) => {
          val cacheInfoToRedis: WarnWaterLineAreaCacheInfoToRedis = WarnWaterLineAreaCacheInfoToRedis(listReport.size, timeMillis, areaNo)
          //查询或更新该AP的历史人数峰值信息
          val queryOrUpdateRedisAreaCacheHisMaxSum: WarnWaterLineAreaCacheInfoToRedis = HwHwWarnWaterLineServiceArea
            .queryOrUpdateRedisAreaCacheHisMaxSum(cacheInfoToRedis)
          //判断是否告警或预警
          val awOrEw: (Boolean, Boolean) = HwHwWarnWaterLineServiceArea.isAwOrEw(synAreaBean, listReport.size)
          WarnWaterLineAreaCaseToHbase(timeMillis, areaNo, listReport.size,
            queryOrUpdateRedisAreaCacheHisMaxSum.hisMaxSum,synAreaBean.waMinStage.toLong,synAreaBean.paMinStage,awOrEw._1, awOrEw._2)
        }
        case None => {
          val cacheInfoToRedis: WarnWaterLineAreaCacheInfoToRedis = WarnWaterLineAreaCacheInfoToRedis(0, timeMillis, areaNo)
          val queryOrUpdateRedisAreaCacheHisMaxSum: WarnWaterLineAreaCacheInfoToRedis = HwHwWarnWaterLineServiceArea
            .queryOrUpdateRedisAreaCacheHisMaxSum(cacheInfoToRedis)
          //判断是否告警或预警
          val awOrEw: (Boolean, Boolean) = HwHwWarnWaterLineServiceArea.isAwOrEw(synAreaBean, 0)
          WarnWaterLineAreaCaseToHbase(timeMillis, areaNo, 0, queryOrUpdateRedisAreaCacheHisMaxSum.hisMaxSum,
            synAreaBean.waMinStage.toLong,synAreaBean.paMinStage, awOrEw._1, awOrEw._2)
        }
      }
      warnWaterLineAreaCaseToHbase
    })
    warnWaterLineAreaCaseToHbaseRdd
  }


  /**
    * 判断当前点位是否在所属区域下
    * @param point 目标点位
    * @param area 关联区域
    * @return (1:属于区域内;0:在区域边线上;-1:在区域之外)
    */
  def isContainInArea(point : Report, area: SynAreaBean) : Int = {
    redisClientArea.isContainInArea(point,area)
  }

  /**
    * 获取Ap配置信息Rdd
    * @return RDD[(String, ApCase)]  K:APMAC   V:AP对象
    */
  def getSynAreaBeanRdd(): RDD[(String, SynAreaBean)] ={
    val areaList: List[SynAreaBean] = getSynAreaBean()
    val areaListTuple: List[(String, SynAreaBean)] = areaList.map(area => {(area.id.toString,area)})
    val context: SparkContext = HwWarnWaterContext.sparkContext
    val parallelize: RDD[(String, SynAreaBean)] = context.parallelize(areaListTuple)
    val reduceByKey: RDD[(String, SynAreaBean)] = parallelize.reduceByKey((x: SynAreaBean, y: SynAreaBean) => {
      x
    })
    reduceByKey
  }

  /**
    * 获取所有的区域
    * @return
    */
  def getSynAreaBean(): List[SynAreaBean] ={
    redisClientArea.getAreaList()
  }


}
