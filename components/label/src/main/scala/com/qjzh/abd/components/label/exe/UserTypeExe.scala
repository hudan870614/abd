package com.qjzh.abd.components.label.exe

import com.qjzh.abd.components.comp_common.caseciew.MessageKafka
import com.qjzh.abd.components.comp_common.common.{HbaseBusUtils, HdfsUtils, KafkaUtils}
import com.qjzh.abd.components.comp_common.conf.CommonConf
import com.qjzh.abd.components.comp_common.utils.{ComUtils, GsonTools}
import com.qjzh.abd.components.label.view.{UserTypeClass, UserTypeHbaseCass}
import com.qjzh.abd.control.common.utils.ExeType
import com.qjzh.abd.control.exe.CommonExe
import com.qjzh.abd.function.common.DateUtils
import com.qjzh.abd.function.hdfs.utils.HdfsBase
import com.qjzh.function.log.utils.Syslog
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import com.qjzh.abd.function.hbase.utils.HbaseUtils

import scala.reflect.ClassTag
/**
  * Created by damon on 2016/12/26.
  */
object UserTypeExe extends CommonExe{

  var _hisMemRdd: RDD[UserTypeClass]      = _
  // 执行类型
  override def exeType: String      = ExeType.OF_ONE_HOUR

  //执行前预处理
  override def init(args: Array[String] = null): Unit = {
    Syslog.info("初始化Hbase表**7g_sta_user_type**")
    //建表
    HbaseUtils.createTable(HbaseBusUtils.sta_user_type)
  }

  //执行(实时)
  def exe[T: ClassTag](line : DStream[T]){
  }

  //执行(离线)
  override def exe(sparkcontent: SparkContext): Unit = {
    InitSourceData(sparkcontent)
    doSummaryHour()
  }

  def InitSourceData(sparkcontent:SparkContext):Unit={
    Syslog.info("初始化汇总...")
    val filePath =  HdfsUtils.getPreHourSaveFilePath(HdfsUtils.UserType_hdfs_file_path,DateUtils.getBeforeHour("yyyy/MM/dd",-1)+"/*/*/*")
    Syslog.info("读取hdfs 位置"+filePath)
    _hisMemRdd  = sparkcontent.textFile(filePath).map(x => {
      val userTypeView = HdfsUtils.usertype_hdfs_parser(x.split(","))
      val ymd     = userTypeView.time.substring(0,8)
      val hour    = userTypeView.time.substring(8,10)
      val minute  = userTypeView.time.substring(10,12)
      UserTypeClass(userTypeView.time,ymd,hour,minute,userTypeView.usermac,userTypeView.usertype)
    }).filter( x => ComUtils.isFakeMac(x.usermac) == 0 && x.usertype.trim.length>0)
    _hisMemRdd.cache()
  }



  def doSummaryHour():Unit = {
    val curDay = DateUtils.getBeforeHour("yyyyMMdd", -1)
    val curHour = DateUtils.getBeforeHour("HH", -1)

    for (hour_x <- 0 to curHour.toInt) {
      Syslog.info("处理时间: startHour:"+hour_x+" endHour:"+curHour)
      doSummary(curDay,hour_x,curHour.toInt)
    }
  }
  /**
    * 执行合并计算
    */
  def doSummary(curDay: String,startHour:Int, endHour:Int): Unit ={
    val typeCountMap = new Object2ObjectOpenHashMap[String, Long]
    //排序取最后一个时间点的标签
    val baseMap = _hisMemRdd.filter(x => {
      (x.hour.toInt >= startHour) && (x.hour.toInt <= endHour)
    }) .map( x => (x.usermac,(x.timestamp,x.usertype,x))).groupByKey().map(x => {
      val list = x._2.toList.sortBy(y => y._1)
      val outclass: UserTypeClass = list.apply(list.length-1)._3
      outclass
    })
    //总值
    val total = baseMap.map(_.usermac).distinct().collect().size
    //标签分值
    baseMap.map(x => (x.usermac, x.usertype))
      .distinct().map(x => (x._2, 1)).reduceByKey(_ + _).collect().foreach(x => {
      val usertype = x._1
      val count = x._2.toLong
      typeCountMap.put(usertype, count)
    })
    val keyPass         = typeCountMap.get("keyPass")
    val firstPass       = typeCountMap.get("firstPass")
    val occasionalPass  = typeCountMap.get("occasionalPass")
    val activePass      = typeCountMap.get("activePass")
    val staff           = typeCountMap.get("staff")

    Syslog.info("keypass" + keyPass + "firstPass" + firstPass + "occasionalPass" + occasionalPass +
      "activePass" + activePass + "staff" + staff)
    //汇总0点到上小时
    val rowkey = curDay + "_" + dealDay(startHour) + "_" + dealDay(endHour.toInt)
    val hbaseclass = UserTypeHbaseCass(total, keyPass, firstPass, occasionalPass, activePass, staff)
    HbaseUtils.writeTable(HbaseBusUtils.sta_user_type, rowkey, "data", GsonTools.gson.toJson(hbaseclass))

    //若开始时间为 00，结束时间为23，刚补上当前 日期yyyyddmm日期的汇总值
    if(startHour == 0 && endHour == 23){
      //发送kafka add by hudan
      val offLineParamKafaMap = new Object2ObjectOpenHashMap[String,String]
      offLineParamKafaMap.put("keyPass",keyPass.toString)
      offLineParamKafaMap.put("firstPass",firstPass.toString)
      offLineParamKafaMap.put("occasionalPass",occasionalPass.toString)
      offLineParamKafaMap.put("activePass",activePass.toString)
      offLineParamKafaMap.put("staff",staff.toString)
      offLineParamKafaMap.put("total",total.toString)
      offLineParamKafaMap.put("rowKey",curDay)
      KafkaUtils.sendMsgToKafka(KafkaUtils.off_line_one_day_mub,
        MessageKafka(CommonConf.OFFLINE_STA_MEM_BY_DAY,
          DateUtils.getTime(System.currentTimeMillis(),"yyyyMMddHHmm").toLong,offLineParamKafaMap))
    }
  }

  /**
    * 处理格式
    * @param tag
    * @return
    */
  def dealDay(tag : Int) : String = {
    var result = tag.toString
    if(tag < 10){
      result = "0"+tag.toString
    }
    result
  }

  def main(args: Array[String]): Unit = {
    doSummaryHour()
  }

}
