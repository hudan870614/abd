package com.qjzh.abd.components.location.service

import com.qjzh.abd.components.location.common.caseview.CaseClass.Coordinate
import com.qjzh.abd.control.common.view.Report
import org.apache.spark.streaming.dstream.DStream

/**
  * Created by yufeiwang on 15/02/2017.
  */
object ShowCaseService {

  /**
    * calculate the user's position using prototype algorithm
    *
    * @param userRssiList
    * @return
    */
  def calculatePos(userRssiList: DStream[(String, Iterable[Report])]): DStream[(String, String,String, (Coordinate,Coordinate))] = {
    val result : DStream[(String, String,String, (Coordinate,Coordinate))] = null

    //先过滤出这个项目的数据
    val userLoc = userRssiList.map(x => {
      val pointMap = x._2.toList.filter(report => report.projectNo.equals("这里修改为此业务的项目名")).sortBy(_.rssi).map(x => {
        //计算。。。。
        x
      })
    })

    result
  }
}
