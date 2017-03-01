package com.qjzh.abd.components.gather_mum.exe

import com.qjzh.abd.components.comp_common.common.{HbaseBusUtils, RedisBusUtils}
import com.qjzh.abd.components.comp_common.conf.CommonConf
import com.qjzh.abd.components.gather_mum.service.GatherMumService
import com.qjzh.abd.control.common.utils.ExeType
import com.qjzh.abd.control.common.view.Report
import com.qjzh.abd.control.exe.CommonExe
import com.qjzh.abd.function.common.DateUtils
import com.qjzh.abd.function.hbase.utils.HbaseUtils
import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

/**
  *
  * add by hushuai
  *
  * 汇总实时人数＼最大值＼及驻留时间，主要包括为：
  * 1＼接收kafka中的数据
  * 2＼汇总每分钟＼小时＼天的人数
  * 3＼存储到hbase表中
  *
  * nohup spark-submit --master yarn-cluster --driver-memory 1G --executor-memory 1G --executor-cores 1
  * --class com.qiji.new7g.main.New7gStaMemMain --conf spark.executor.extraClassPath=/data/7g_lib/*     */
  * --conf spark.driver.extraClassPath=/data/7g_lib/* /data/7g_spark/7g_calculate/lib/7g_calculate.jar &    */
  *
  *
  * /* 50 */1 * * * /data/7g_spark/7g_calculate/bin/
  * /* 55 */1 * * * /home/7g/lib/7g_calculate/bin/new7gImportCrowdMain.sh
  * 区块预警告警接口
  * 警戒水位接口
  * 滞留警告消息接口
  */
object GatherMumExe extends CommonExe {

  // 执行类型
  override def exeType: String = ExeType.ON_ONE_MIN

  //优先级
  override def priority: Int = super.priority


  //执行(离线)
  override def exe(sparkcontent: SparkContext): Unit = {}

  //执行
  override def exe[T: ClassTag](line: DStream[T]) = {


    //点位汇总
    val curMin = DateUtils.getCurTime("yyyyMMddHHmm")


    val data = line.asInstanceOf[DStream[Report]]

    //过滤＼去重
    val disReportRdd: DStream[Report] =
      data.filter(_.projectNo.equalsIgnoreCase(CommonConf.project_no))
        .map(report => {
        ((report.userMac,report.apMac), report)
      }).reduceByKey((pre, aft) => {
        pre
      }).map(_._2)

    // ＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝  汇总逻辑
    GatherMumService.dealMemSta(disReportRdd,curMin)

    // ＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝  热力图逻辑
    GatherMumService.dealHotPoint(disReportRdd,curMin)


  }

  //执行前预处理
  override def init(args: Array[String]): Unit = {

    HbaseUtils.createTable(HbaseBusUtils.sta_umac_nber)

    HbaseUtils.createTable(HbaseBusUtils.HBASE_7gG_STA_HEAT_AREA_HIS)

    //清空一些历史变量
    RedisBusUtils.delRowKey(RedisBusUtils.sparkStreaming_heatArea_lData_his)
    RedisBusUtils.delRowKey(RedisBusUtils.sparkStreaming_heatImportCrowd_lData_his)
  }
}
