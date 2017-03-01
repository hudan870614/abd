package com.qjzh.abd.components.hw.in_out_move.etl

import com.qjzh.abd.components.comp_common.caseciew.SynAreaBean
import com.qjzh.abd.components.hw.in_out_move.service.AreaService
import com.qjzh.abd.control.common.view.Report
import org.apache.spark.streaming.dstream.DStream

/**
  * 室外华为的数据ETL层
  * Created by 92025 on 2017/2/17.
  */
object HwHwWarnWaterLineEtl {
  val etl_prefix = "_"


  /**
    * 判断给RDD类的所有数据加上区域编号
    * @param lines
    * @param areaList
    * @return
    */
  def addAreaNoToReport(lines: DStream[Report],areaList: List[SynAreaBean]): DStream[Report] ={
    lines.map(report => {

      val synAreaBeans: List[SynAreaBean] = areaList.filter(x => {
//        (x.floor == report.floor) && (HwHwWarnWaterLineServiceArea.isContainInArea(report, x) != -1)
        (AreaService.isContainInArea(report, x) != -1)
      }).take(1)

      if(!synAreaBeans.isEmpty){
        report.areaNo = synAreaBeans.head.id + ""
      }else{
        report.areaNo = "-1"
      }
      report
    })
  }
  /**
    * 根据区域编号进行分组
    * @param linesData    分组后的数据    (areaNo,Iterable[Report])
    * @return
    */
  def groupReportByAreaId(linesData: DStream[Report]): DStream[(String, Iterable[Report])] ={
    val dStream: DStream[(String, Report)] = linesData.map(report => {
      (report.areaNo, report)
    })
    val groupByKey: DStream[(String, Iterable[Report])] = dStream.groupByKey()
    groupByKey
  }

  /**
    * 根据ApMac进行分组
    * @param linesData    分组后的数据    (ApMac,Iterable[Report])
    * @return
    */
  def groupReportByAp(linesData: DStream[Report]): DStream[(String, Iterable[Report])] ={
    val dStream: DStream[(String, Report)] = linesData.map(report => {
      (report.apMac, report)
    })
    val groupByKey: DStream[(String, Iterable[Report])] = dStream.groupByKey()
    groupByKey
  }

  /**
    * 根据userMac去重这个批次的所有数据
    * @param linesFromKafka  根据userMac去重后的一分钟全量数据
    */
  def distinctReportByUserMac(linesFromKafka: DStream[Report]): DStream[Report] ={
    val dStream: DStream[(String, Report)] = linesFromKafka.map(report => {
      (report.userMac, report)
    })
    val distinctReport: DStream[Report] = dStream.reduceByKey((lasterReport: Report, cruReport: Report) => {
      cruReport
    }).map(xx => {
      xx._2
    })
    distinctReport
  }

  /**
    * 根据区域ID和UserMac联合去重
    * @param linesDataReport   去重后的数据
    * @return
    */
  def distinctReportByAreaAndUserMac(linesDataReport:DStream[Report]): DStream[Report] ={
    linesDataReport.map(x => {
      (x.areaNo+x.userMac,x)
    }).reduceByKey((laster:Report,cru:Report) => {
      cru
    }).map(_._2)
  }

  /**
    * 根据设备ID和UserMac联合去重
    * @param linesDataReport
    * @return
    */
  def distinctReportByApAndUserMac(linesDataReport:DStream[Report]): DStream[Report] ={
    linesDataReport.map(x => {
      (x.apMac+x.userMac,x)
    }).reduceByKey((laster:Report,cru:Report) => {
      cru
    }).map(_._2)
  }
}
