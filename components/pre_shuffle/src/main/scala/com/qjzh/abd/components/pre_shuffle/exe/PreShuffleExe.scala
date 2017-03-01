package com.qjzh.abd.components.pre_shuffle.exe

import com.qjzh.abd.components.comp_common.common.{HdfsUtils, RddUtils}
import com.qjzh.abd.components.pre_shuffle.service.PreShuffleService
import com.qjzh.abd.control.common.utils.ExeType
import com.qjzh.abd.control.common.view.Report
import com.qjzh.abd.control.exe.CommonExe
import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

/**
  * Created by hushuai on 16/12/15.
  */
object PreShuffleExe extends CommonExe{

  // 执行类型
  override def exeType: String = ExeType.ON_PRE_SHUFFLE

  //优先级
  override def priority: Int = super.priority

  //执行前预处理
  override def init(args: Array[String]) = ()

  //执行
  override def exe(sparkcontent: SparkContext) = ()

  //执行
  override def exe[T: ClassTag](line: DStream[T]) = {

    if(line.isInstanceOf[DStream[Report]]){

      val result = line.asInstanceOf[DStream[Report]].map(report => {

        ((report.userMac,report.apMac),report)

      }).groupByKey().map(gr =>{
        //求算场强平均值
        PreShuffleService.getAvgRssiByBatchReport(gr)
      })

      //探针明细数据入hdfs
      result.map(_.toRecodeStr).repartition(1).foreachRDD(rdd =>{

        RddUtils.saveToHdfs(rdd,HdfsUtils.getSaveFilePath(HdfsUtils.recode_load_hdfs_file_path))

      })

      //点位信息入hdfs
      result.filter(f => {

        (f != null && f.pointX > 0)

      }).map(_.toPointStr).repartition(1).foreachRDD(rdd => {

        RddUtils.saveToHdfs(rdd,HdfsUtils.getSaveFilePath(HdfsUtils.point_load_hdfs_file_path))

      })


    }

  }

}
