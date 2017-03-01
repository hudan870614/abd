package com.qjzh.abd.components.warn_water_line.exe

import com.qjzh.abd.components.comp_common.caseciew.SynAreaBean
import com.qjzh.abd.components.warn_water_line.caseview.{ApCase, WarnWaterLineApCaseToHbase, WarnWaterLineAreaCaseToHbase}
import com.qjzh.abd.components.warn_water_line.conf.{HwWarnWaterConf, HwWarnWaterContext}
import com.qjzh.abd.components.warn_water_line.etl.HwHwWarnWaterLineEtl
import com.qjzh.abd.components.warn_water_line.service.{HwHwWarnWaterLineServiceAp, HwHwWarnWaterLineServiceArea}
import com.qjzh.abd.control.common.view.Report
import com.qjzh.abd.control.exe.CommonExe
import com.qjzh.abd.function.hbase.utils.HbaseUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
/**
  * Created by 92025 on 2017/2/17.
  */
object HwHwWarnWaterLineExe extends CommonExe{
  //执行前预处理
  def init(args: Array[String]): Unit = {
    HbaseUtils.createTable(HwWarnWaterConf.HBASE_TABLE_AREA_HISTORY_DETAIL)   //用于保存区块人数和时间的每分钟详细记录
    HbaseUtils.createTable(HwWarnWaterConf.HBASE_TABLE_AREA_LOG_DETAIL)   //用于保存每分钟区块的告警预警信息

    HbaseUtils.createTable(HwWarnWaterConf.HBASE_TABLE_AP_HISTORY_DETAIL)   //用于保存AP人数和时间的每分钟详细记录
    HbaseUtils.createTable(HwWarnWaterConf.HBASE_TABLE_AP_LOG_DETAIL)   //用于保存每分钟AP的告警预警信息
  }
  def initModul[T](line: DStream[T]): Unit ={
    HwWarnWaterContext.sparkContext = line.context.sparkContext
  }
  //执行(实时)
   def exe[T: ClassManifest](line: DStream[T]): Unit = {
     initModul(line)   //初始化模块环境

     if(line.isInstanceOf[DStream[Report]]){
       val dStream: DStream[Report] = line.asInstanceOf[DStream[Report]]
       val distinctReportByUserMac: DStream[Report] = HwHwWarnWaterLineEtl.distinctReportByUserMac(dStream)//对想同的用户去重
       //--------Ap警戒水位计算逻辑
       //1.Ap历史人数记录
       val caseList: RDD[(String, ApCase)] = HwHwWarnWaterLineServiceAp.getApCaseList()//获取ap配置信息
       val groupReportByAp: DStream[(String, Iterable[Report])] = HwHwWarnWaterLineEtl.groupReportByAp(distinctReportByUserMac)//通过ap给用户分组
       val transformAp: DStream[(String, (ApCase, Option[Iterable[Report]]))] = groupReportByAp.transform(rdd => {caseList.leftOuterJoin(rdd)})//用ap关联上报的数据
       val computerApWarnWaterLineBusiness: DStream[WarnWaterLineApCaseToHbase] = HwHwWarnWaterLineServiceAp.computerApWarnWaterLineBusiness(transformAp)
//       HwHwWarnWaterLineServiceAp.saveApHisoryDetailDataToHbase(computerApWarnWaterLineBusiness) //写入ap点位人数的详细记录
//       computerApWarnWaterLineBusiness.print()
      //2.AP历史告警记录
       val apHistoryAwOrEw: DStream[WarnWaterLineApCaseToHbase] = computerApWarnWaterLineBusiness.filter(xx =>{xx.isAw || xx.isEw}) //ap预警或者告警过的信息
//      HwHwWarnWaterLineServiceAp.saveApHisoryAwOrEwLogDataToHbase(apHistoryAwOrEw)  //保存Ap历史告警或者预警信息到hbase
//       apHistoryAwOrEw.print()

       //--------区块警戒水位计算逻辑
       //1.区块历史人数记录
       val areaBeans: List[SynAreaBean] = HwHwWarnWaterLineServiceArea.getSynAreaBean()  //所有的区域信息
       val filter: DStream[Report] = dStream.filter(xx => "000000000000".equals(xx.apMac)).filter(_.isFake == 0) //过滤定位数据和去伪码
       val addAreaNoToReport: DStream[Report] = HwHwWarnWaterLineEtl.addAreaNoToReport(filter,areaBeans)//给所有的数据添加区域编号
       val groupReportByAreaId: DStream[(String, Iterable[Report])] = HwHwWarnWaterLineEtl.groupReportByAreaId(addAreaNoToReport)//根据区域ID分好组的report数据
       val areaBeanRdd: RDD[(String, SynAreaBean)] = HwHwWarnWaterLineServiceArea.getSynAreaBeanRdd() //获取区块的所有配置信息
       val transformArea: DStream[(String, (SynAreaBean, Option[Iterable[Report]]))] = groupReportByAreaId.transform(rdd => {areaBeanRdd.leftOuterJoin(rdd)})//用区域ID分关联区域信息的数据
       val computerAreaWarnWaterLineBusiness: DStream[WarnWaterLineAreaCaseToHbase] = HwHwWarnWaterLineServiceArea.computerAreaWarnWaterLineBusiness(transformArea)
//       HwHwWarnWaterLineServiceArea.saveAreaHisoryDetailDataToHbase(computerAreaWarnWaterLineBusiness) //写入区块人数的详细记录
       computerAreaWarnWaterLineBusiness.print()
//       computerAreaWarnWaterLineBusiness.print()
       //2.区块历史告警记录
       val areaHistoryAwOrEw: DStream[WarnWaterLineAreaCaseToHbase] = computerAreaWarnWaterLineBusiness.filter(xx => {xx.isAw || xx.isEw})//区块预警或者告警过的信息
//      HwHwWarnWaterLineServiceArea.saveAreaHisoryAwOrEwLogDataToHbase(areaHistoryAwOrEw) //保存区块历史告警或者预警信息到hbase
     }
  }

  //执行(离线)
  def exe(sparkContext: SparkContext): Unit = {

  }

//  def main(args: Array[String]): Unit = {
//
//    MyLog.setStreamingLogLevels()
//
//    //sparkContext
//    val sparkContext: SparkContext = KafkaEtl.createSparkContext(this.getClass.getSimpleName,"local")
//
//    val createDStreaming: (DStream[String], StreamingContext) = KafkaEtl.createDStreaming(sparkContext,10,KafkaEtl.brokerList,KafkaEtl.TOPIC,KafkaEtl.kafkaOffset)
//
//    val report: DStream[Report] = KafkaEtl.convertData2Report(createDStreaming._1)
//
////    init(args)
//    exe(report)    //主业务进程启动
//
//
//    createDStreaming._2.start()
//    createDStreaming._2.awaitTermination()
//
//  }
}
