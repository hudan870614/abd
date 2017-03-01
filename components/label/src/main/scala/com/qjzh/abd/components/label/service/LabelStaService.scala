//package com.qjzh.abd.components.label.service
//
//import com.qjzh.abd.components.comp_common.caseciew.{hdfs_view_duration, hdfs_view_label}
//import com.qjzh.abd.components.comp_common.common.{HbaseBusUtils, HdfsUtils, RddUtils, RedisBusUtils}
//import com.qjzh.abd.components.comp_common.conf.CommonConf
//import com.qjzh.abd.components.comp_common.utils.{CompRedisClientUtils, GsonTools}
//import com.qjzh.abd.components.label.view._
//import com.qjzh.abd.function.common.DateUtils
//import com.qjzh.abd.function.hbase.utils.HbaseUtils
//import com.qjzh.abd.function.hdfs.utils.HdfsBase
//import org.apache.parquet.it.unimi.dsi.fastutil.objects.ObjectArrayList
//import org.apache.spark.SparkContext
//import org.apache.spark.rdd.RDD
//
///**
//  * 标签人数汇总逻辑
//  * Created by 92025 on 2016/10/24.
//  */
//object LabelStaService {
//
//  //重点旅客全量汇总统计
//  var sparkStreaming_importMac_staALLType = "sparkStreaming:importMac:staALLType"
//  //全量历史中的key值
//  var had_save_sta_user_type_hash_redis_key = "had:save:sta:user:type:hash:redis:key"
//
//
//  /**
//    * 标签统计模块
//    * @param exeDayStr
//    * @param sc
//    */
//  def toStaLabel(exeDayStr : String,sc : SparkContext): Unit = {
//    val exeDaySimp = exeDayStr.replaceAll("\\/","")
//    //统计最近30天
//    val recently30DaysRDD =  loadLabelHdfsFileByDay(-30 ,exeDaySimp, sc)
//    doExe(recently30DaysRDD,sc,"YY30")
//
//    //统计当前一月]
//    val curMonthRDD = loadLabelHdfsFileByMonth(exeDayStr,sc)
//    val exeMonth = exeDayStr.substring(0,exeDayStr.lastIndexOf("/"))
//    val exeMonthSimp = exeMonth.replaceAll("\\/","")
//    doExe(curMonthRDD,sc,exeMonthSimp)
//
//    //统计全量
//    doExe(loadLabelHdfsFile(sc),sc, "ALL")
//
//  }
//
//
//  /**
//    *
//    * @param rdd
//    * @param sc
//    * @param prefix
//    */
//  def doExe(rdd : RDD[hdfs_view_label],sc : SparkContext,prefix : String): Unit ={
//
//    //清理数据
//    HbaseUtils.dealByRowKey(HbaseBusUtils.sta_umac_brand_nber, (prefix+"_key").toUpperCase())
//    HbaseUtils.dealByRowKey(HbaseBusUtils.sta_umac_brand_nber, (prefix+"_first").toUpperCase())
//    HbaseUtils.dealByRowKey(HbaseBusUtils.sta_umac_brand_nber, (prefix+"_act").toUpperCase())
//    HbaseUtils.dealByRowKey(HbaseBusUtils.sta_umac_brand_nber, (prefix+"_ALL").toUpperCase())
//    HbaseUtils.dealByRowKey(HbaseBusUtils.sta_umac_brand_nber, (prefix+"_occ").toUpperCase())
//    RedisBusUtils.delRowKey(had_save_sta_user_type_hash_redis_key)
//
//    /**
//      * 手机型号分析
//      */
//    val AllHisMacMapRDD = rdd.filter(_ != null).groupBy(_.userMac).map(x => {
//      val corePerson : hdfs_view_label = x._2.toList.sortBy(_.curDay).takeRight(1).head
//      corePerson
//    })
//
//    val allBrandMapRDD = AllHisMacMapRDD.map(x => {
//      var label_key : String = null
//      if(x.label.indexOf(CommonConf.LABLE_CORE_ALL_DATA) != -1){
//        label_key = "key"
//      }else if(x.label.indexOf(CommonConf.LABLE_GUEST_FIR_DATA) != -1){
//        label_key = "first"
//      }else if(x.label.indexOf(CommonConf.LABLE_GUEST_ACT_DATA) != -1){
//        label_key = "act"
//      }else if(x.label.indexOf(CommonConf.LABLE_GUEST_ONALLY_DATA) != -1){
//        label_key = "occ"
//      }
//      (label_key,x)
//    }).filter(_._1 != null).filter(!_._2.brand.isEmpty)
//
//    val allTypeCountMap = allBrandMapRDD.map(x => {
//      (x._1,1)
//    }).reduceByKey(_+_).toLocalIterator.toMap
//
//    val totalType = allTypeCountMap.map(_._2).reduce(_+_)
//
//    allBrandMapRDD.filter(_._1 != null).map(x => {
//      ((x._1,x._2.brand),1)
//    }).reduceByKey(_+_).map(x => {
//      val typeKey = x._1._1
//      val brand = x._1._2
//      val bSum = x._2
//      val tSum = allTypeCountMap.filter(_._1.equals(typeKey)).head._2
//      val columnName = (Long.MaxValue - bSum)+"_"+brand
//      HbaseUtils.writeTable(HbaseBusUtils.sta_umac_brand_nber, (prefix+"_"+typeKey).toUpperCase(), columnName, GsonTools.gson.toJson(HbaseBrandCountCase(tSum,bSum,brand)))
//      (brand,bSum)
//    }).reduceByKey(_+_).foreachPartition(p => {
//      p.foreach(x => {
//        val brand = x._1
//        val bSum = x._2
//        val columnName = (Long.MaxValue - bSum)+"_"+brand
//        HbaseUtils.writeTable(HbaseBusUtils.sta_umac_brand_nber, (prefix+"_ALL").toUpperCase(), columnName, GsonTools.gson.toJson(HbaseBrandCountCase(totalType,bSum,brand)))
//      })
//    })
//
//
//
//
//    /**
//      * 所有旅客类型分析
//      * 将各种维度的值汇总后暂存到redis中
//      */
//    AllHisMacMapRDD.filter(_ != null).flatMap(x => {
//      val staList = new ObjectArrayList[((String,String),hdfs_view_label)]
//      staList.add((("total",x.userMac),x))
//      if(x.label.indexOf(CommonConf.LABLE_CORE_ALL_DATA) != -1){
//        staList.add((("keyPass",x.userMac),x))
//        staList.add((("impTotal",x.userMac),x))
//      }
//      if(x.label.indexOf(CommonConf.LABLE_CORE_NEW_DATA) != -1){
//        staList.add((("keyNew",x.userMac),x))
//      }
//      if(x.label.indexOf(CommonConf.LABLE_CORE_ACT_DATA) != -1){
//        staList.add((("keyAct",x.userMac),x))
//      }
//      if(x.label.indexOf(CommonConf.LABLE_CORE_ACC_DATA) != -1){
//        staList.add((("keyAcc",x.userMac),x))
//      }
//      if(x.label.indexOf(CommonConf.LABLE_CORE_RULE_DATA) != -1){
//        staList.add((("keyRule",x.userMac),x))
//        //如果当前规则包含规则主键，则将规则主键解析出来一并汇总
//        val labels = x.label.split("\\;").filter(_.indexOf(CommonConf.LABLE_CORE_RULE_DATA) != -1).takeRight(1).head
//        if("ALL".equalsIgnoreCase(prefix) && labels.split("_").size == 3){
//          val ruleId = labels.split("_").takeRight(1).head
//          staList.add((("tagRule_"+ruleId,x.userMac),x))
//        }
//      }
//      if(x.label.indexOf(CommonConf.LABLE_FIR_DATA) != -1){
//        staList.add((("firstPass",x.userMac),x))
//      }
//      if(x.label.indexOf(CommonConf.LABLE_ONALLY_DATA) != -1){
//        staList.add((("occasionalPass",x.userMac),x))
//      }
//      if(x.label.indexOf(CommonConf.LABLE_GUEST_ACT_DATA) != -1){
//        staList.add((("activePass",x.userMac),x))
//      }
//      if(x.label.indexOf(CommonConf.LABLE_NORMAL_DATA) != -1){
//        staList.add((("staff",x.userMac),x))
//      }
//      staList.elements().filter(_ != null)
//    }).reduceByKey((x,y) => x).map(x => {
//      (x._1._1,1)
//    }).reduceByKey(_+_).foreachPartition(p => {
//      p.foreach(s => {
//        //规则的全量汇总单独进行填充
//        if(s._1.indexOf("tagRule") != -1){
//          val ruleId = s._1.split("_").takeRight(1).head
//          RedisBusUtils.hset(sparkStreaming_importMac_staALLType,ruleId,GsonTools.gson.toJson(RedisCountCase(ruleId,s._2)))
//          HbaseUtils.writeTable(HbaseBusUtils.sta_impr_day_nber, (prefix+"_"+ruleId).toUpperCase(), "ALL".toUpperCase(), GsonTools.gson.toJson(HbaseImpStaCountCase(s._2)))
//        }
//        RedisBusUtils.hset(had_save_sta_user_type_hash_redis_key,s._1, s._2.toString,RedisBusUtils.second_one_day)
//      })
//    })
//    //从redis中取到各个统计值
//    val redisTypeValuesMap = RedisBusUtils.hgetall(had_save_sta_user_type_hash_redis_key)
//
//    val totalSize = redisTypeValuesMap.getOrElse("total",0).toString.toLong
//    val impTotalSize = redisTypeValuesMap.getOrElse("impTotal",0).toString.toLong
//    val keyPassSize = redisTypeValuesMap.getOrElse("keyPass",0).toString.toLong
//    val keyNewSize = redisTypeValuesMap.getOrElse("keyNew",0).toString.toLong
//    val keyActSize = redisTypeValuesMap.getOrElse("keyAct",0).toString.toLong
//    val keyAccSize = redisTypeValuesMap.getOrElse("keyAcc",0).toString.toLong
//    val keyRuleSize = redisTypeValuesMap.getOrElse("keyRule",0).toString.toLong
//
//    val monthSta = HbaseAllPassDetailCase(totalSize,keyPassSize,
//      redisTypeValuesMap.getOrElse("firstPass",0).toString.toLong,redisTypeValuesMap.getOrElse("occasionalPass",0).toString.toLong,
//      redisTypeValuesMap.getOrElse("activePass",0).toString.toLong,redisTypeValuesMap.getOrElse("staff",0).toString.toLong)
//    HbaseUtils.writeTable(HbaseBusUtils.sta_user_type, (prefix+"_ALL").toUpperCase(), "data", GsonTools.gson.toJson(monthSta))
//
//    //全量历史总数
//    RedisBusUtils.hset(sparkStreaming_importMac_staALLType,prefix.toUpperCase(), GsonTools.gson.toJson(RedisCountCase(prefix,impTotalSize)))
//    HbaseUtils.writeTable(HbaseBusUtils.sta_impr_day_nber, prefix.toUpperCase(), "ALL".toUpperCase(), GsonTools.gson.toJson(HbaseImpStaCountCase(impTotalSize)))
//    HbaseUtils.writeTable(HbaseBusUtils.sta_impr_day_nber, prefix.toUpperCase(), ("new_ALL").toUpperCase(), GsonTools.gson.toJson(HbaseImpStaCountCase(keyNewSize)))
//    HbaseUtils.writeTable(HbaseBusUtils.sta_impr_day_nber, prefix.toUpperCase(), ("act_ALL").toUpperCase(), GsonTools.gson.toJson(HbaseImpStaCountCase(keyActSize)))
//    HbaseUtils.writeTable(HbaseBusUtils.sta_impr_day_nber, prefix.toUpperCase(), ("occ_ALL").toUpperCase(), GsonTools.gson.toJson(HbaseImpStaCountCase(keyAccSize)))
//    HbaseUtils.writeTable(HbaseBusUtils.sta_impr_day_nber, prefix.toUpperCase(), ("rule_ALL").toUpperCase(), GsonTools.gson.toJson(HbaseImpStaCountCase(keyRuleSize)))
//
//
//  }
//
//
//  /**
//    * 加载指定日期数据
//    * @param exeDay 执行日
//    * @param sc
//    * @return
//    */
//  def loadLabelHdfsFileByDay(preDayNum : Int, exeDay : String, sc : SparkContext): RDD[hdfs_view_label] = {
//    //最近指定天的日期
//    val dayPathDates = HdfsUtils.getPreDayFilePath(exeDay, preDayNum)
//    var sumCaseRdd: RDD[hdfs_view_label] = null
//    dayPathDates.elements().filter(_ != null).foreach(x => {
//      val caseRdd: RDD[hdfs_view_label] = loadLabelBase(x,sc)
//      sumCaseRdd = RddUtils.unionRdd(sumCaseRdd,caseRdd)
//    })
//    sumCaseRdd.filter(_ != null)
//  }
//
//  /**
//    * 加载指定月份数据
//    * @param exeDay 执行日
//    * @param sc
//    * @return
//    */
//  def loadLabelHdfsFileByMonth(exeDay : String, sc : SparkContext): RDD[hdfs_view_label] = {
//    val exeMonth = exeDay.substring(0,exeDay.lastIndexOf("/"))
//    loadLabelBase(exeMonth+"/*",sc)
//  }
//
//  /**
//    * 加载全量数据
//    * @param sc
//    * @return
//    */
//  def loadLabelHdfsFile(sc : SparkContext): RDD[hdfs_view_label] = {
//    loadLabelBase("*/*/*",sc)
//  }
//
//  /**
//    * 根据目录进行解析
//    * @param fileDic
//    * @param sc
//    * @return
//    */
//  def loadLabelBase(fileDic : String, sc : SparkContext) : RDD[hdfs_view_label] = {
//    var caseRdd: RDD[hdfs_view_label] = null
//    //当前目录
//    val dictionary = HdfsUtils.getHdfsFilePath(HdfsUtils.label_hdfs_file_path, fileDic)
//    //当前目录是否已经存在
//    if (HdfsBase.exitsFile(dictionary) || dictionary.indexOf("*") != -1) {
//      //生成当前的文件路径
//      val filePath = HdfsUtils.getHdfsFilePath(HdfsUtils.label_hdfs_file_path, fileDic + "/part*")
//      //路径
//      caseRdd = sc.textFile(filePath).map(lines => {
//        HdfsUtils.label_hdfs_parser(lines.split(","))
//      })
//    }
//    caseRdd
//  }
//
//
//
//
//  def main(args: Array[String]): Unit = {
//    val exeMonth = "2017/01/03".substring(0,"2017/01/03".lastIndexOf("/"))
//    println(exeMonth)
//
////    HdfsUtils.getPreDayFilePath("20161226", cal_pre_days).elements().foreach(println(_))
//  }
//
//}
