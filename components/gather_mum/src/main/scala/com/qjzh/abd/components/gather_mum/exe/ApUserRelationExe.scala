package com.qjzh.abd.components.gather_mum.exe

import com.qjzh.abd.components.comp_common.common.HbaseBusUtils
import com.qjzh.abd.components.gather_mum.service.New7gApAndUserRelationService
import com.qjzh.abd.control.common.view.Report
import com.qjzh.abd.control.exe.CommonExe
import com.qjzh.abd.function.hbase.utils.HbaseUtils
import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.DStream

/**
  * Created by 92025 on 2017/1/9.
  * 按分钟  小时  汇总单个ap扫描到的usermac
  * 指定userMac找出被扫描到的ap清单
  */
object ApUserRelationExe extends CommonExe{
  //执行前预处理
  override def init(args: Array[String]): Unit = {
    HbaseUtils.createTable(HbaseBusUtils.HBASE_TABLE_DETAIL_USER_MAC)
  }

  //执行(实时)
  override def exe[T: ClassManifest](line: DStream[T]): Unit = {
    val data = line.asInstanceOf[DStream[Report]]

    val ditinctH: DStream[(String, Report)] = data.map(report => {
      (report.userMac + "_" + report.apMac, report)
    }).reduceByKey((pre, aft) => {
      pre
    })
    //按分钟  小时  汇总单个ap扫描到的usermac
    //指定userMac找出被扫描到的ap清单
    New7gApAndUserRelationService.preGatherToHbase(ditinctH)
  }

  //执行(离线)
  override def exe(sparkContext: SparkContext): Unit = {

  }
}
