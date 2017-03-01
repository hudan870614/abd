package com.qjzh.abd.control.common.view

import com.qjzh.abd.control.common.CaseReport
import it.unimi.dsi.fastutil.objects._
import org.apache.parquet.it.unimi.dsi.fastutil.objects.ObjectArrayList
/**
  * Created by hushuai on 16/3/5.
  */
class BatchReportCollect  extends Serializable{

  var report : Report = new Report()
  /**
    * 是否是重点人
    */
  var isImp : Boolean = false
  var aimReport : Report = new Report()

  var reportList = new ObjectArrayList[Report]()
  val reportMap = new Object2ObjectOpenHashMap[String,Report]()
  val reportCaseMap = new Object2ObjectOpenHashMap[String,CaseReport]()


  var maxCount : Long = 0
  var minCount : Long = 0
  var curCount : Long = 0

}
