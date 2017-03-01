package com.qjzh.abd.components.hw.in_out_move.exe

import com.qjzh.abd.components.comp_common.caseciew.SynAreaBean
import com.qjzh.abd.components.hw.in_out_move.caseview._
import com.qjzh.abd.components.hw.in_out_move.conf.{HwHwInOutMoveConf, HwHwInOutMoveContext}
import com.qjzh.abd.components.hw.in_out_move.etl.HwHwWarnWaterLineEtl
import com.qjzh.abd.components.hw.in_out_move.service.AreaService
import com.qjzh.abd.control.common.view.Report
import com.qjzh.abd.control.exe.CommonExe
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

/**
  * Created by 92025 on 2017/2/17.
  */
object HwHwInOutMoveExe extends CommonExe{
  //执行前预处理
  def init(args: Array[String]): Unit = {

  }
  def initModul[T](line: DStream[T]): Unit ={
    HwHwInOutMoveContext.sparkContext = line.context.sparkContext
  }
  //执行(实时)
   def exe[T: ClassManifest](line: DStream[T]): Unit = {
     initModul(line)   //初始化模块环境

     if(line.isInstanceOf[DStream[Report]]){
       val dStream: DStream[Report] = line.asInstanceOf[DStream[Report]]
       val areaBeans: List[SynAreaBean] = AreaService.getSynAreaBean()  //所有的区域信息

       val distinctReportByUserMac: DStream[Report] = HwHwWarnWaterLineEtl.distinctReportByUserMac(dStream)//对想同的用户去重
       val filter: DStream[Report] = distinctReportByUserMac.filter(xx => "000000000000".equals(xx.apMac)).filter(_.isFake == 0) //过滤定位数据和去伪码
       val addAreaNoToReport: DStream[Report] = HwHwWarnWaterLineEtl.addAreaNoToReport(filter,areaBeans)//给所有的数据添加区域编号

       val parallelize: RDD[(String, AreaInfoJoin)] = HwHwInOutMoveContext.sparkContext.parallelize(AreaService.queryAreaInfoList().map(xx => {(xx.areaId.toString,xx)})) //获取区域维表RDD
       val addAreaNoToReportTuple: DStream[(String, Report)] = addAreaNoToReport.map(xx => {(xx.areaNo,xx)})

       val transform: DStream[(String, (Report, AreaInfoJoin))] = addAreaNoToReportTuple.transform(rdd => {rdd.join(parallelize)}).filter(_._2._2.isMigrateArea != -1).cache() //给数据源加上详细纬度  过滤掉不做迁徙的数据  然后cache住数据

       //发散每个基数据到父级去
       val addParentAreaDataByChildData: DStream[(Report, AreaInfoJoin)] = AreaService.addParentAreaDataByChildData(transform)

       val groupByKey: DStream[(String, Iterable[(Report, AreaInfoJoin)])] =  addParentAreaDataByChildData.map(xx => {(xx._2.parentAreaId + HwHwInOutMoveConf.SYS_PREFIX + xx._2.areaLevel, xx)}).groupByKey()//根据视角和级别分完组的数据
       val areaNoLevelCombinationRdd: RDD[(String, Null)] = HwHwInOutMoveContext.sparkContext.parallelize(AreaService.queryAreaAndLevelCombinationList()).map((_,null))  //构建<K,V>
       val transformareaNoLevelCombinationLeftJoinReport: DStream[(String, (Null, Option[Iterable[(Report, AreaInfoJoin)]]))] = groupByKey.transform(rdd => {areaNoLevelCombinationRdd.leftOuterJoin(rdd)})//用全部级别列表和当前按级别分组过的数据做左关联
//       将每个视角和级别组合的数据  一条一条到redis验证  迁入迁出
       val cache: DStream[ReportInOutMove] = AreaService.queryInOutMoveByAreaNoLevelAndUserMac(transformareaNoLevelCombinationLeftJoinReport)
         .cache() //已经按视图级别校验过的迁入迁出数据
       //迁入分组
       val viewAndLevelAndAreaNoDStreamInGroup: DStream[(String, Iterable[ReportInOutMove])] = cache.map(xx => {(xx.areaNoAndLevel + HwHwInOutMoveConf.SYS_PREFIX + xx.inAreaNo,xx)}).groupByKey()
       val querySimpleAreaIn: DStream[WriteInSimple] = AreaService.querySimpleAreaIn(viewAndLevelAndAreaNoDStreamInGroup)//单点迁入查询
       querySimpleAreaIn.print()
       AreaService.writeInSingleAreaInfoToRedis(querySimpleAreaIn)  //单点迁入数据入库redis
       //迁出分组
       val viewAndLevelAndAreaNoDStreamOutGroup: DStream[(String, Iterable[ReportInOutMove])] = cache.map(xx => {(xx.areaNoAndLevel + HwHwInOutMoveConf.SYS_PREFIX + xx.outAreaNo,xx)}).groupByKey()
       val querySimpleAreaOut: DStream[WriteOutSimple] = AreaService.querySimpleAreaOut(viewAndLevelAndAreaNoDStreamOutGroup)//单点迁出查询
       querySimpleAreaOut.print()
        AreaService.writeOutSingleAreaInfoToRedis(querySimpleAreaOut) //单点迁出数据入库redis

       val groupByAreaAndLevelReportInOutMove: DStream[(String, Iterable[ReportInOutMove])] = cache.map(xx => {(xx.areaNoAndLevel,xx)}).groupByKey()
       val business: DStream[(String, (Map[Long, Int], Int), (Map[Long, Int], Int), Map[String, Int])] = AreaService.computerHeatBusiness(groupByAreaAndLevelReportInOutMove)
       val heatInData: DStream[WriteHeatInToHbase] = AreaService.getHeatInData(business)//最热迁入
       heatInData.print()
       AreaService.writeHeatInDataToRedis(heatInData)//最热迁入入库redis
       val heatOutData: DStream[WriteHeatOutToHbase] = AreaService.getHeatOutData(business)//最热迁出
       heatOutData.print()
       AreaService.writeHeatOutDataToRedis(heatOutData) //最热迁出入库redis
       val heatInOutData: DStream[WriteHeatInOutToHbase] = AreaService.getHeatInOutData(business)//最热路线
       heatInOutData.print()
       AreaService.writeHeatInOutDataToRedis(heatInOutData)//最热路线入库redis
     }
  }

  //执行(离线)
  def exe(sparkContext: SparkContext): Unit = {

  }

//  def main(args: Array[String]): Unit = {
//
//    MyLog.setStreamingLogLevels()
//
//    val sparkContext: SparkContext = KafkaEtl.createSparkContext(this.getClass.getSimpleName,"local")
//
//    val createDStreaming: (DStream[String], StreamingContext) = KafkaEtl.createDStreaming(sparkContext,10,KafkaEtl.brokerList,KafkaEtl.TOPIC,KafkaEtl.kafkaOffset)
//
//    val report: DStream[Report] = KafkaEtl.convertData2Report(createDStreaming._1)
//
//    exe(report)    //主业务进程启动
//
//
//    createDStreaming._2.start()
//    createDStreaming._2.awaitTermination()
//  }
}
