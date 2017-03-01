package com.qjzh.abd.components.in_out_move.exe

import com.qjzh.abd.control.common.utils.Tool
import com.qjzh.abd.control.common.view.Report

/**
  * Created by 92025 on 2016/12/27.
  */
object Test {
  def main(args: Array[String]): Unit = {
    val json = "{\"userMac\":\"68DFDD636A53\",\"apMac\":\"00:00:00:00:00:00\",\"timeStamp\":1482834406091,\"brand\":\"\",\"projectNo\":\"sk\",\"areaNo\":\"\",\"isFake\":0,\"pointX\":0.0,\"pointY\":0.0,\"pointZ\":0.0,\"mapID\":1,\"floor\":1,\"serverTimeStamp\":201612271826,\"coordinateX\":245.57,\"coordinateY\":473.53,\"coordinateZ\":0.0}"
    val data: Report = Tool.getReportByJsonStr(json)
    println(data)
  }
}
