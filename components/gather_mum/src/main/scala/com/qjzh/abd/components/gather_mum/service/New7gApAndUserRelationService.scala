package com.qjzh.abd.components.gather_mum.service

import com.qjzh.abd.components.comp_common.common.HbaseBusUtils
import com.qjzh.abd.components.comp_common.utils.GsonTools
import com.qjzh.abd.components.gather_mum.caseview.{HbaseBeanApMacDetail, HbaseBeanUserMacDetailMinuteAndHour}
import com.qjzh.abd.control.common.view.Report
import com.qjzh.abd.function.common.DateUtils
import com.qjzh.abd.function.hbase.utils.HbaseUtils
import org.apache.spark.streaming.dstream.DStream

/**
  * Created by 92025 on 2017/1/9.
  */
object New7gApAndUserRelationService {
  /**
    * 按分钟  小时  汇总单个ap扫描到的usermac
    * 指定userMac找出被扫描到的ap清单
    *
    * @param ditinctH
    */
  def preGatherToHbase(ditinctH: DStream[(String, Report)]) = {
    ditinctH.foreachRDD(xx => {
      xx.foreach(yy => {
        val cruTimeMinute: Long = DateUtils.getCurTime("yyyyMMddHHmm").toLong
        val cruTimeHour: Long = DateUtils.getCurTime("yyyyMMddHH").toLong
        val serverTimeStamp: Long = System.currentTimeMillis()
        val split: Array[String] = yy._1.split("_")
        val userMac: String = split(0)
        val apMac: String = split(1)
        val _2: Report = yy._2
        //按分钟  小时  汇总单个ap扫描到的usermac
        HbaseUtils.writeTable(HbaseBusUtils.HBASE_TABLE_DETAIL_USER_MAC, cruTimeHour + "_" + apMac, userMac,
          GsonTools.gson.toJson(HbaseBeanUserMacDetailMinuteAndHour(userMac, serverTimeStamp, _2.timeStamp, _2.isFake.toString, _2.apMac, cruTimeMinute)))
        //指定userMac找出被扫描到的ap清单
        HbaseUtils.writeTable(HbaseBusUtils.HBASE_TABLE_DETAIL_USER_MAC, userMac, cruTimeHour + "_" + apMac,
          GsonTools.gson.toJson(HbaseBeanApMacDetail(userMac, serverTimeStamp, _2.timeStamp, _2.isFake.toString, _2.apMac, cruTimeMinute)))
      })
    })
  }
}
