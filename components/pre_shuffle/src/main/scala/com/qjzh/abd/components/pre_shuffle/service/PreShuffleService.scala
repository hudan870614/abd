package com.qjzh.abd.components.pre_shuffle.service

import com.qjzh.abd.control.common.view.Report
import com.qjzh.abd.function.common.DateUtils


/**
  * Created by hushuai on 16/12/15.
  */
object PreShuffleService {

  /**
    * 在批量中得到场强的平均值
    * @param batchReport
    * @return
    */
  def getAvgRssiByBatchReport(batchReport : ((String,String),Iterable[Report])) : Report = {
    val report = batchReport._2.head
    val sum_rssi = batchReport._2.iterator.map(_.rssi.toLong).reduce(_+_)
    val avg_rs = sum_rssi / batchReport._2.size
    report.rssi = avg_rs.toString
    //统一为服务处理时间
    report.timeStamp = DateUtils.getCurTime("yyyyMMddHHmm").toLong
    report
  }

}
