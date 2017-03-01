//package com.qjzh.abd.components.label.service
//
//import com.qjzh.abd.components.comp_common.caseciew.{MessageKafka, RedisImpDetailCase, hdfs_view_duration, hdfs_view_label}
//import com.qjzh.abd.components.comp_common.common._
//import com.qjzh.abd.components.comp_common.conf.CommonConf
//import com.qjzh.abd.components.comp_common.utils.{CompRedisClientUtils, GsonTools, LabelTools}
//import com.qjzh.abd.components.label.utils.LabelDigUtils
//import com.qjzh.abd.components.label.view._
//import com.qjzh.abd.function.common.DateUtils
//import com.qjzh.abd.function.hbase.utils.HbaseUtils
//import com.qjzh.abd.function.hdfs.utils.HdfsBase
//import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
//import org.apache.parquet.it.unimi.dsi.fastutil.objects.ObjectArrayList
//import org.apache.spark.SparkContext
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql.{DataFrame, SQLContext}
//
///**
//  * 标签人数挖掘逻辑
//  * Created by hdshuai on 2016/10/24.
//  */
//object LabelDigService {
//
//  //存储分区
//  val COALESCE_SIZE : Int = 50
//
//  //重点旅客明细推送
//  var sparkStreaming_importMac_staDetail = "sparkStreaming:importMac:staDetail"
//
//
//  //加载天数
//  val cal_pre_days : Int  =  -90
//
//  //手机mac
//  val user_mac_propery_name = "userMac"
//  //天次
//  val day_num_propery_name = "dayNum"
//  //累计次数
//  val times_propery_name = "times"
//  //累计时长
//  val duration_propery_name = "duration"
//  //品牌
//  val brand_propery_name = "brand"
//  //标签
//  val label_propery_name = "label"
//
//  /**
//    * 删除当天已经生成的历史记录
//    * （如果已经存在的话）
//    * @param day
//    * @return
//    */
//  def dealHisHdfsFileByDay(day : String) = {
//    val behaviourFilePath = HdfsUtils.getHdfsFilePath(HdfsUtils.label_hdfs_file_path,day)
//    HdfsBase.delFile(behaviourFilePath)
//  }
//
//
//  /**
//    * 加载最近90天的时长数据
//    * @param exeDay 执行日
//    * @param sc
//    * @return
//    */
//  def loadDurationHdfsFileByDay(exeDay : String,sc : SparkContext): RDD[hdfs_view_duration] = {
//    //最近90天的日期
////    val dayPathDates = HdfsUtils.getPreDayFilePath(exeDay, cal_pre_days)
////    var sumCaseRdd: RDD[hdfs_view_duration] = null
////    dayPathDates.elements().filter(_ != null).foreach(x => {
////      //当前目录
////      val dictionary = HdfsUtils.getHdfsFilePath(HdfsUtils.duration_hdfs_file_path, x)
////      //当前目录是否已经存在
////      if (HdfsBase.exitsFile(dictionary)) {
////        //生成当前的文件路径
////        val filePath = HdfsUtils.getHdfsFilePath(HdfsUtils.duration_hdfs_file_path, x + "/*/*/part*")
////        //路径
////        val caseRdd: RDD[hdfs_view_duration] = sc.textFile(filePath).map(lines => {
////          if(lines != null && lines.split(",").size == 7) {
////            HdfsUtils.duration_hdfs_parser(lines.split(","))
////          }else{
////            null
////          }
////        })
////        //聚合rdd
////        if (sumCaseRdd == null) {
////          sumCaseRdd = caseRdd
////        } else {
////          sumCaseRdd = sumCaseRdd.union(caseRdd)
////        }
////      }
////    })
////    sumCaseRdd.filter(_ != null)
//
//    //生成当前的文件路径
//    val filePath = HdfsUtils.getHdfsFilePath(HdfsUtils.duration_hdfs_file_path,"/*/*/*/*/*/part*")
//    //路径
//    val caseRdd: RDD[hdfs_view_duration] = sc.textFile(filePath).map(lines => {
//      if(lines != null && lines.split(",").size == 7) {
//        HdfsUtils.duration_hdfs_parser(lines.split(","))
//      }else{
//        null
//      }
//    })
//    caseRdd.filter(_ != null)
//  }
//
//
//  /**
//    * 根据指定执行日挖掘出相关的标签
//    * @param exeDayStr
//    * @param sc
//    */
//  def toDigLabel(exeDayStr : String,sc : SparkContext): Unit = {
//
//    //删除当前的历史数据
//    LabelDigService.dealHisHdfsFileByDay(exeDayStr)
//
//    //当前服务器要执行的日期
//    val exeDaySimp : String = exeDayStr.replaceAll("\\/","")
//
//    val sqlContext = new SQLContext(sc)
//    import sqlContext.implicits._
//
//    //加载90天的数据
//    val allDurDayRdd = loadDurationHdfsFileByDay(exeDaySimp,sc)
//
//    allDurDayRdd.cache()
//
//    //90天内 按手机mac汇总
//    val sta90DayByUserMacRDD = allDurDayRdd.groupBy(_.userMac).map(dur => {
//      val duration = getDuration(dur._2)
//      val times = dur._2.map(_.isNewTime).reduce(_+_)
//      val dayNum = dur._2.map(_.curDay).toList.distinct.size
//      val brand = dur._2.head.brand
//      label_view_duration(null,dayNum,dur._1,times,duration,0,brand)
//    })
//    val sta90DayByUserMacDF =  sta90DayByUserMacRDD.toDF()
//
//    //90天内 按手机mac,手机日期汇总
//    val sta90DayByUserMacAndDay = allDurDayRddByCurDayAndUserMac(allDurDayRdd,exeDaySimp)
//    val sta90DayByUserMacAndDayDF = sta90DayByUserMacAndDay.toDF()
//
//    //60天内 按手机mac汇总
//    val sta60DayByUserMacDF = LabelDigUtils.groupByUserMac(sta90DayByUserMacAndDay.filter(_.differDay <= 60)).toDF()
//
//    //当天 按手机mac汇总
//    val staExeDayByUserMacRDD = LabelDigUtils.groupByUserMac(sta90DayByUserMacAndDay.filter(_.differDay == 0))
//    staExeDayByUserMacRDD.cache()
//
//    val staExeDayByUserMacDF = staExeDayByUserMacRDD.toDF()
//
//    val staUserMacByExeDayUserMacDF = staExeDayByUserMacDF.select(user_mac_propery_name)
//
//    //当天的品牌
////    val staUserMacBrandByExeDayUserMacRDD= staExeDayByUserMacRDD.map(x => {
////      (x.userMac,x.brand)
////    }).distinct()
//
//    //加载redis的手工配置mac信息
//    /**
//      * mac: MAC地址
//      * type: 人员类型  1:重点旅客，2:工作人员 3:普通旅客
//      * source: 来源  1:手工添加，2:规则判断
//      */
//    val redisWorkUserMac = CompRedisClientUtils.getRedisImpUserMacs().toDF().filter("rpType = 2").select("mac").withColumnRenamed("mac",user_mac_propery_name)
//    val redisImpUserMac = CompRedisClientUtils.getRedisImpUserMacs().toDF().filter("rpType = 1").select("mac").withColumnRenamed("mac",user_mac_propery_name)
//    val redisALLUserMac = CompRedisClientUtils.getRedisImpUserMacs().toDF().select("mac").withColumnRenamed("mac",user_mac_propery_name)
//
//
//
//    //工作人员规则
//    //工作人员：90天内，每天在线时长超过3小时或者出现频率超过5次 且出现的总天数比例大于60%，小于等于100%的人
//    var workMacs = sta90DayByUserMacAndDayDF.where(" duration >= 180 or times >= 5")
//      .groupBy(user_mac_propery_name).agg(count("curDay").alias("totalDay")).where("totalDay >= " + cal_pre_days * (-1) * 0.6 )
//      .select(user_mac_propery_name)
//    workMacs = RddUtils.unionDataFrame(workMacs,redisWorkUserMac)
//    workMacs = workMacs.intersect(staUserMacByExeDayUserMacDF)
//
//
//    //加载重点人规则
//    /**
//      * 重点旅客：90天内，出现天数大于12天的人（平均每15天通关记录超过2次记录的人）
//      * 新增重点旅客，每日通过规则或手动添加最新生成的重点旅客人群
//      * 活跃重点旅客，最近60天内，出现次数大于等于8天的重点旅客
//      * 沉默重点旅客，最近60天内，出现次数小于8天的重点旅客
//      */
//    //所有重点旅客
//    val allBaseCoreMacs = sta90DayByUserMacDF.where("dayNum >= 12").select(user_mac_propery_name).except(workMacs).intersect(staUserMacByExeDayUserMacDF)
//    //活跃重点旅客
//    val allCoreActMacs = sta60DayByUserMacDF.where("dayNum >= 8").select(user_mac_propery_name).except(workMacs).intersect(allBaseCoreMacs)
//    //沉默重点旅客
//    val allCoreOccMacs = sta60DayByUserMacDF.where("dayNum < 8").select(user_mac_propery_name).except(workMacs).intersect(allBaseCoreMacs)
//
//
//    //加载并解析重点自定义规则
////    val handleAllRuleDataframe = loadHadleRule(sc,allDurDayRdd, staExeDayByUserMacDF,exeDaySimp)
//
//    val offLineParamKafaMap = new Object2ObjectOpenHashMap[String,Any]
//
//    //全量手工重点规则旅客
//    var handleAllRuleMacsWithLabelRDD : RDD[(String,String)] = null
////    handleAllRuleDataframe.foreach(rule => {
////      //当前userMac
////      val curRuleUserMacDF = rule._2.select(user_mac_propery_name).intersect(staUserMacByExeDayUserMacDF).except(redisWorkUserMac)
////
////      if(curRuleUserMacDF != null && curRuleUserMacDF.count() > 0){
////        //当前userMac 关联 所属标签
////        val curRuleWithLabel = LabelDigUtils.withLabel(curRuleUserMacDF,user_mac_propery_name,CommonConf.LABLE_CORE_RULE_DATA+"_"+rule._1)
////
////        handleAllRuleMacsWithLabelRDD = RddUtils.unionRdd(handleAllRuleMacsWithLabelRDD,curRuleWithLabel)
////
////        //当前规则当天首次新增
////        val newRuleKeyPassUserMacs = LabelDigUtils.getFristUserMacs(curRuleUserMacDF,user_mac_propery_name).toDF().withColumnRenamed("_1",user_mac_propery_name)
////        //当前规则当天活跃
////        val actRuleKeyPassUserMacs = curRuleUserMacDF.except(newRuleKeyPassUserMacs).intersect(allCoreActMacs)
////        //当前规则当天沉默
////        val accRuleKeyPassUserMacs = curRuleUserMacDF.except(newRuleKeyPassUserMacs).except(actRuleKeyPassUserMacs).intersect(allCoreOccMacs)
////
////        val ruleId = rule._1.toString
////
////        val all_count = curRuleUserMacDF.distinct().count()
////        val new_count = newRuleKeyPassUserMacs.distinct().count()
////        val act_count = actRuleKeyPassUserMacs.distinct().count()
////        val acc_count = accRuleKeyPassUserMacs.distinct().count()
////        //      val other_count = all_count - (new_count+act_count+acc_count)
////
////        offLineParamKafaMap.put(ruleId+"_ALL",all_count)
////        offLineParamKafaMap.put(ruleId+"_NEW",new_count)
////        offLineParamKafaMap.put(ruleId+"_ACT",act_count)
////        offLineParamKafaMap.put(ruleId+"_OCC",acc_count)
////
////        //按天－规则汇总到数据库
////        HbaseUtils.writeTable(HbaseBusUtils.sta_impr_day_nber, exeDaySimp, ruleId+"_ALL", GsonTools.gson.toJson(HbaseImpStaCountCase(all_count)))
////        HbaseUtils.writeTable(HbaseBusUtils.sta_impr_day_nber, exeDaySimp, ruleId+"_NEW", GsonTools.gson.toJson(HbaseImpStaCountCase(new_count)))
////        HbaseUtils.writeTable(HbaseBusUtils.sta_impr_day_nber, exeDaySimp, ruleId+"_ACT", GsonTools.gson.toJson(HbaseImpStaCountCase(act_count)))
////        HbaseUtils.writeTable(HbaseBusUtils.sta_impr_day_nber, exeDaySimp, ruleId+"_OCC", GsonTools.gson.toJson(HbaseImpStaCountCase(acc_count)))
////        //      HbaseUtils.writeTable(HbaseBusUtils.sta_impr_day_nber, exeDaySimp, ruleId+"_OTHER", GsonTools.gson.toJson(HbaseImpStaCountCase(other_count)))
////      }
////
////    })
//
//    var handleAllRuleMacs : DataFrame = null
//    if(handleAllRuleMacsWithLabelRDD != null && !handleAllRuleMacsWithLabelRDD.isEmpty()){
//      handleAllRuleMacs = handleAllRuleMacsWithLabelRDD.map(_._1).toDF().withColumnRenamed("_1",user_mac_propery_name).distinct()
//    }
//    //全部重点人
//    var allKeyPassMacs : DataFrame =  null
//    allKeyPassMacs = RddUtils.unionDataFrame(allKeyPassMacs,allCoreActMacs)
//    allKeyPassMacs = RddUtils.unionDataFrame(allKeyPassMacs,allCoreOccMacs)
//    allKeyPassMacs = RddUtils.unionDataFrame(allKeyPassMacs,handleAllRuleMacs)
//    allKeyPassMacs = RddUtils.unionDataFrame(allKeyPassMacs,redisImpUserMac)
//
//
//    //新增重点旅客
//    var newKeyPassUserMacs : DataFrame = null
//    if(allKeyPassMacs != null){
//      newKeyPassUserMacs = LabelDigUtils.getFristUserMacs(allKeyPassMacs,user_mac_propery_name).toDF().withColumnRenamed("_1",user_mac_propery_name)
//    }
//
//    //按天汇总
//    var allKeyPassMacsCount : Long = 0
//    if(allKeyPassMacs != null) {
////      allKeyPassMacsCount = allKeyPassMacs.distinct().count()
//    }
//    var newKeyPassUserMacsCount : Long = 0
//    if(newKeyPassUserMacs != null) {
////      newKeyPassUserMacsCount = newKeyPassUserMacs.distinct().count()
//    }
//    var allCoreActMacsCount : Long = 0
//    if(allCoreActMacs != null){
////      allCoreActMacsCount = RddUtils.exceptFrame(allCoreActMacs,List(newKeyPassUserMacs)).distinct().count()
//    }
//    var allCoreOccMacsCount : Long = 0
//    if(allCoreOccMacs != null){
////      allCoreOccMacsCount = RddUtils.exceptFrame(allCoreOccMacs,List(newKeyPassUserMacs)).distinct().count()
//    }
//    var handleAllRuleMacsCount : Long = 0
//    if(handleAllRuleMacs != null){
////      handleAllRuleMacsCount = handleAllRuleMacs.distinct().count()
//    }
//
//
//    //======================================================================================按天汇总存储到hbase
////    HbaseUtils.writeTable(HbaseBusUtils.sta_impr_day_nber, exeDaySimp, "ALL",
////      GsonTools.gson.toJson(HbaseImpStaCountCase(allKeyPassMacsCount)))
////    HbaseUtils.writeTable(HbaseBusUtils.sta_impr_day_nber, exeDaySimp, "NEW_ALL",
////      GsonTools.gson.toJson(HbaseImpStaCountCase(newKeyPassUserMacsCount)))
////    HbaseUtils.writeTable(HbaseBusUtils.sta_impr_day_nber, exeDaySimp, "ACT_ALL",
////      GsonTools.gson.toJson(HbaseImpStaCountCase(allCoreActMacsCount)))
////    HbaseUtils.writeTable(HbaseBusUtils.sta_impr_day_nber, exeDaySimp, "OCC_ALL",
////      GsonTools.gson.toJson(HbaseImpStaCountCase(allCoreOccMacsCount)))
////    HbaseUtils.writeTable(HbaseBusUtils.sta_impr_day_nber, exeDaySimp, "RULE_ALL",
////      GsonTools.gson.toJson(HbaseImpStaCountCase(handleAllRuleMacsCount)))
//
//    //======================================================================================按天汇总发送kafka
//
//    offLineParamKafaMap.put("ALL",allKeyPassMacsCount)
//    offLineParamKafaMap.put("NEW_ALL",newKeyPassUserMacsCount)
//    offLineParamKafaMap.put("ACT_ALL",allCoreActMacsCount)
//    offLineParamKafaMap.put("OCC_ALL",allCoreOccMacsCount)
//    offLineParamKafaMap.put("RULE_ALL",handleAllRuleMacsCount)
//    offLineParamKafaMap.put("rowKey",exeDaySimp.toString)
//
////    KafkaUtils.sendMsgToKafka(KafkaUtils.off_line_one_day_mub,
////      MessageKafka(CommonConf.OFFLINE_IMP_STA_BY_DAY,
////        DateUtils.getTime(System.currentTimeMillis(),"yyyyMMddHHmm").toLong,offLineParamKafaMap))
//
//
//
//    //加载普通旅客（首次＼活跃＼偶而）
//    /**
//      * 首次旅客：90天内，首次出现且当天没有出现超过两次通关记录的人。
//      偶尔旅客：90天内，出现天数大于一天，小于等于三天且单天没有出现超过两次通关记录的人
//      活跃旅客：90天内，出现天数大于3天，小于等于12天且单天没有出现超过两次通关记录的人
//      */
//
//    // 首次旅客
//    val firGenPassUserMacs = staUserMacByExeDayUserMacDF.except(allKeyPassMacs).except(redisALLUserMac).map(x =>{
//      x.getAs[String](user_mac_propery_name)
//    }).filter(!HbaseUtils.containRowKey(HbaseBusUtils.user_label_info,_))
//      .toDF().withColumnRenamed("_1",user_mac_propery_name)
//
//    // 活跃旅客
//    val actGenPassUserMacs = sta90DayByUserMacDF.filter(sta90DayByUserMacDF(day_num_propery_name).between(4,12))
//      .select(user_mac_propery_name).except(redisALLUserMac).except(allKeyPassMacs).except(firGenPassUserMacs).intersect(staUserMacByExeDayUserMacDF)
//
//    // 偶尔旅客
//    val onllGenPassUserMacs = staUserMacByExeDayUserMacDF.except(allKeyPassMacs).except(firGenPassUserMacs).except(actGenPassUserMacs).except(redisALLUserMac)
//
//
//    ////******************************************************************* 将执行日内所有标签汇总后合并后入库
//    // 将每种类型关联标签
//    //普通旅客—活跃
////    val actGenPassUserMacsLabel = LabelDigUtils.withLabel(actGenPassUserMacs,user_mac_propery_name,CommonConf.LABLE_GUEST_ACT_DATA)
////    val actGenPassUserMacsLabel = LabelDigUtils.withLabelAndSaveHbase(exeDaySimp,actGenPassUserMacs,CommonConf.LABLE_GUEST_ACT_DATA,staUserMacBrandByExeDayUserMacRDD)
//    //普通旅客—偶然
////    val onllGenPassUserMacsLabel = LabelDigUtils.withLabelAndSaveHbase(exeDaySimp,onllGenPassUserMacs,CommonConf.LABLE_GUEST_ONALLY_DATA,staUserMacBrandByExeDayUserMacRDD)
//    //普通旅客—首次
////    val firGenPassUserMacsLabel = LabelDigUtils.withLabelAndSaveHbase(exeDaySimp,firGenPassUserMacs,CommonConf.LABLE_GUEST_FIR_DATA,staUserMacBrandByExeDayUserMacRDD)
////    //重点人员—沉默
////    val accTimesUserMacsLabel = LabelDigUtils.withLabelAndSaveHbase(exeDaySimp,allCoreOccMacs,CommonConf.LABLE_CORE_ACC_DATA,staUserMacBrandByExeDayUserMacRDD)
////    //重点人员—活跃
////    val actKeyPassUserMacsLabel = LabelDigUtils.withLabelAndSaveHbase(exeDaySimp,allCoreActMacs,CommonConf.LABLE_CORE_ACT_DATA,staUserMacBrandByExeDayUserMacRDD)
////    //重点人员-新增
////    val newKeyPassUserMacsLabel = LabelDigUtils.withLabelAndSaveHbase(exeDaySimp,newKeyPassUserMacs,CommonConf.LABLE_CORE_NEW_DATA,staUserMacBrandByExeDayUserMacRDD)
////    //工作人员－常规
////    val workerDFLabel = LabelDigUtils.withLabelAndSaveHbase(exeDaySimp,workMacs,CommonConf.LABLE_WORRKER_NORMAL_DATA,staUserMacBrandByExeDayUserMacRDD)
//
//
//
//    actGenPassUserMacs.registerTempTable("actGenPassUserMacsTab")
//    val actGenPassUserMacsLabel  = sqlContext.sql(" select t."+user_mac_propery_name+", '"+CommonConf.LABLE_GUEST_ACT_DATA+"' "+label_propery_name+" from actGenPassUserMacsTab t ")
//
//    onllGenPassUserMacs.registerTempTable("onllGenPassUserMacsTab")
//    val onllGenPassUserMacsLabel  = sqlContext.sql(" select t."+user_mac_propery_name+", '"+CommonConf.LABLE_GUEST_ONALLY_DATA+"' "+label_propery_name+" from onllGenPassUserMacsTab t ")
//
//    firGenPassUserMacs.registerTempTable("firGenPassUserMacsTab")
//    val firGenPassUserMacsLabel  = sqlContext.sql(" select t."+user_mac_propery_name+", '"+CommonConf.LABLE_GUEST_FIR_DATA+"' "+label_propery_name+" from firGenPassUserMacsTab t ")
//
//    allCoreOccMacs.registerTempTable("allCoreOccMacsTab")
//    val allCoreOccMacsLabel  = sqlContext.sql(" select t."+user_mac_propery_name+", '"+CommonConf.LABLE_CORE_ACC_DATA+"' "+label_propery_name+" from allCoreOccMacsTab t ")
//
//    allCoreActMacs.registerTempTable("allCoreActMacsTab")
//    val allCoreActMacsLabel  = sqlContext.sql(" select t."+user_mac_propery_name+", '"+CommonConf.LABLE_CORE_ACT_DATA+"' "+label_propery_name+" from allCoreActMacsTab t ")
//
//    newKeyPassUserMacs.registerTempTable("newKeyPassUserMacsTab")
//    val newKeyPassUserMacsLabel  = sqlContext.sql(" select t."+user_mac_propery_name+", '"+CommonConf.LABLE_CORE_NEW_DATA+"' "+label_propery_name+" from newKeyPassUserMacsTab t ")
//
//    workMacs.registerTempTable("workMacsTab")
//    val workMacsLabel  = sqlContext.sql(" select t."+user_mac_propery_name+", '"+CommonConf.LABLE_WORRKER_NORMAL_DATA+"' "+label_propery_name+" from workMacsTab t ")
//
//
//    //全量汇总
//
//    var totalLabel : DataFrame = null
//    totalLabel = RddUtils.unionDataFrame(totalLabel,actGenPassUserMacsLabel)
//    totalLabel = RddUtils.unionDataFrame(totalLabel,onllGenPassUserMacsLabel)
//    totalLabel = RddUtils.unionDataFrame(totalLabel,firGenPassUserMacsLabel)
//    totalLabel = RddUtils.unionDataFrame(totalLabel,allCoreOccMacsLabel)
//    totalLabel = RddUtils.unionDataFrame(totalLabel,allCoreActMacsLabel)
//    totalLabel = RddUtils.unionDataFrame(totalLabel,newKeyPassUserMacsLabel)
//    totalLabel = RddUtils.unionDataFrame(totalLabel,workMacsLabel)
//
//    totalLabel.map(x => {
//      val userMac = x.getAs[String](LabelDigService.user_mac_propery_name)
//      val label = x.getAs[String](LabelDigService.label_propery_name)
//      val view = hdfs_view_label(exeDaySimp,userMac,label,"")
//      HbaseUtils.writeTable(HbaseBusUtils.user_label_info, userMac, "data", GsonTools.gson.toJson(view))
//      exeDaySimp+","+userMac+","+label+","+""
//    }).coalesce(COALESCE_SIZE).saveAsTextFile(HdfsUtils.getPreHourSaveFilePath(HdfsUtils.label_hdfs_file_path,exeDayStr))
////
////    totalLabel.filter(_ != null).map(x => {
////      x.userMac+","+x.userMac+","+x.label+","+x.brand
////    }).coalesce(COALESCE_SIZE).saveAsTextFile(HdfsUtils.getPreHourSaveFilePath(HdfsUtils.label_hdfs_file_path,exeDayStr))
//
//
//    ////********************************************** 当前新增的重点人返回到 kafka
////    if(newKeyPassUserMacsLabel != null && newKeyPassUserMacsLabel.count() > 0){
////      //本地化当前新增的重点人
////      val newKeyPassLabelList = newKeyPassUserMacsLabel.toLocalIterator.toList
////      //关联当前重点人的时长＼次数等基本信息 转为本地集合
////      val userDescList: ObjectArrayList[RedisImpDetailCase] = new ObjectArrayList[RedisImpDetailCase]()
////      sta90DayByUserMacRDD.filter(x => {
////        newKeyPassLabelList.contains(x.userMac)
////      }).map(x => {
////        val userLabels = newKeyPassLabelList.filter(_._1.equalsIgnoreCase(x.userMac)).head._2
////        RedisImpDetailCase(x.userMac,
////          DateUtils.getTime(System.currentTimeMillis(), "yyyyMMddHHmm")
////          ,x.times.toInt,x.duration.toDouble,0,LabelTools.getLabelRoleId(userLabels))
////      }).toLocalIterator.toList.foreach(userDescList.add(_))
////      //发送kafka
////      com.qjzh.abd.components.comp_common.common.KafkaUtils.sendMsgToKafka(
////        com.qjzh.abd.components.comp_common.common.KafkaUtils.oline_one_minute_mub,
////        MessageKafka[ObjectArrayList[RedisImpDetailCase]](CommonConf.WARN_AREA_CORE_DETAIL,
////          DateUtils.getTime(System.currentTimeMillis(),"yyyyMMddHHmm").toLong,userDescList))
////    }
//
//
//    ////**********************查询已经当前历史中已经失效的所有重点旅客
////    val forOldResultRedis = redisALLUserMac.map(_.getAs[String](user_mac_propery_name)).filter(!HbaseUtils.containRowKey(HbaseBusUtils.user_label_info,_)).map(x => {
////      RedisImpDetailCase(x,"",0,0,1)
////    })
////    //将失效的重点人合并后推送到redis
////    if(forOldResultRedis != null  && forOldResultRedis.count() > 0){
////      RedisBusUtils.hset(sparkStreaming_importMac_staDetail,exeDaySimp+"_OLD",GsonTools.gson.toJson(forOldResultRedis.toLocalIterator.toList.toArray))
////    }
//
//  }
//
//  /**
//    * 加载自定义规则
//    * @param sc
//    * @param allDurDayRdd
//    * @param staExeDayByUserMacDF
//    * @return
//    */
//  def loadHadleRule(sc : SparkContext, allDurDayRdd : RDD[hdfs_view_duration], staExeDayByUserMacDF:DataFrame, exeDaySimp : String): List[(Int,DataFrame)] ={
//    val sqlContext = new SQLContext(sc)
//    import sqlContext.implicits._
//
//    // 最的90天的数据
//    var baseAllDurDayRDD = allDurDayRdd
//
//    //加载重点规则
//    val baseRoleDF = CompRedisClientUtils.getRedisRuleMacs.filter(x => {
//    (x.ruleType == 1 && x.apStatus.equals("0"))
//  }).map(x => {
//    var ruleDF : DataFrame = null
//    val isUseFi = x.isUseFi //是否启用规则一(0:启用 1:不启用)
//    val dateType = x.dateType//日期类型(1:所有日期{默认},2:工作日,3:周末)
//    var ruleLimitHoursList: List[Int] = List()
//    //解析多个时段
//    //将允许的所有上时打散成一个个小时，如 12:00 到 15:00 打散成（12，13，14，15）
//    if(x.timeSegs != null && x.timeSegs.size() > 0){
//    x.timeSegs.elements().filter( _ != null).foreach(time => {
//    val beginHour = time.begin.split("\\:").head.toInt
//    val endHour = time.end.split("\\:").head.toInt
//    if(beginHour <= endHour){
//    val allHour = beginHour to endHour
//    ruleLimitHoursList = ruleLimitHoursList.:::(allHour.toList).distinct
//  }
//  })
//  }
//    if(isUseFi == 0){
//    val fiLaterDay = x.fiLaterDay // 规则一最近天数
//    val fiEnDayBegin = x.fiEnDayBegin//规则一入境天数开始
//    val fiEnDayEnd = x.fiEnDayEnd //规则一入境天数结束
//    val fiCheckedSet = x.fiCheckedSet //规则一设置在线时长/频次(0:选中 1:不选中)
//
//    val onlDHourBegin =x.onlDHourBegin // 每天在线时长开始
//    val onlDHourEnd = x.onlDHourEnd // 每天在线时长结束
//    val onlDAppearBegin = x.onlDAppearBegin // 每天出现频次开始
//    val onlDAppearEnd = x.onlDAppearEnd // 每天出现频次结束
//
//    //填充时间期限
//    if(ruleLimitHoursList.length > 0){
//    baseAllDurDayRDD = baseAllDurDayRDD.filter(_.endTime.toLong > 0).filter(dur => {
//    val startHour = DateUtils.getHour(dur.startTime.toLong)
//    val endHour = DateUtils.getHour(dur.endTime.toLong)
//    //开始小时与结束小时都在设置的范围内，并且 在指定的日期类型内
//    ruleLimitHoursList.contains(startHour) && ruleLimitHoursList.contains(endHour) && DateUtils.isInWorkOrHoliday(dur.startTime.toLong,dateType)
//  })
//  }
//
//    val sta90DayByUserMacAndDay:RDD[label_view_duration] = allDurDayRddByCurDayAndUserMac(baseAllDurDayRDD,exeDaySimp)
//
//    //规则一 限制最近天数
//    var ruleOneDF = LabelDigUtils.groupByUserMac(sta90DayByUserMacAndDay.filter(_.differDay <= fiLaterDay)).toDF()
//    //规则一 加上天数限制
//    ruleOneDF = ruleOneDF.filter(ruleOneDF(day_num_propery_name).between(fiEnDayBegin,fiEnDayEnd))
//
//    if(fiCheckedSet == 0){
//    ruleOneDF = ruleOneDF.filter(ruleOneDF(duration_propery_name).between(onlDHourBegin * 60,onlDHourEnd * 60))//在线时长
//    ruleOneDF = ruleOneDF.filter(ruleOneDF(times_propery_name).between(onlDAppearBegin,onlDAppearEnd))//在线次数
//  }
//
//    ruleDF = ruleOneDF.select(user_mac_propery_name)
//
//    val isUseSe = x.isUseSe  // 是否启用规则二(0:启用 1:不启用)
//    val seLaterDay = x.seLaterDay // 规则二最近天数
//    val seSigDayBegin = x.seSigDayBegin // 规则二单日入境次数开始
//    val seSigDayEnd = x.seSigDayBegin // 规则二单日入境次数结束
//    val seCheckedSet = x.seCheckedSet //规则二是否设置在线时长(0:选中 1:不选中)
//
//    if(isUseSe == 0){
//    // 限制最近天数
//    var ruleTwoDF = LabelDigUtils.groupByUserMac(sta90DayByUserMacAndDay.filter(_.differDay <= seLaterDay)).toDF()
//    //限制入境天数
//    ruleTwoDF = ruleTwoDF.filter(ruleTwoDF(day_num_propery_name).between(seSigDayBegin,seSigDayEnd))
//
//    if(seCheckedSet == 0){
//    ruleTwoDF = ruleTwoDF.filter(ruleTwoDF(duration_propery_name).between(onlDHourBegin,onlDHourEnd))//限制在线时长
//  }
//
//    ruleDF = ruleDF.intersect(ruleTwoDF.select(user_mac_propery_name)).distinct()
//  }
//
//    val isUseTh = x.isUseTh // 是否启用规则三(0:启用 1:不启用)
//    val thLaterDay = x.thLaterDay // 规则三今日入境次数
//    val thCheckedSet = x.thCheckedSet // 规则三是否设置在线时长(0:选中 1:不选中)
//
//    if(isUseTh == 0){
//
//    var ruleThreeDF = staExeDayByUserMacDF.filter(staExeDayByUserMacDF(times_propery_name).>=(thLaterDay))
//
//    if(thCheckedSet == 0){
//    ruleThreeDF = ruleThreeDF.filter(ruleThreeDF(duration_propery_name).between(onlDHourBegin,onlDHourEnd))//限制在线时长
//  }
//
//    ruleDF = ruleDF.intersect(ruleThreeDF.select(user_mac_propery_name)).distinct()
//
//  }
//
//  }
//    (x.id,ruleDF)
//  }).filter(_._2 != null)
//
//    baseRoleDF
//  }
//
//    def main(args: Array[String]): Unit = {
//      // 2017-01-10 07:05:42   2017-01-10 12:53:07
//      //
////    val view = null
//    println(DateUtils.getTime(1484023987590L,"yyyy-MM-dd HH:mm:ss"))
//
//
//      val reportMap = new Object2ObjectOpenHashMap[String,Any]
//
//      reportMap.put("ALL",1)
//      reportMap.put("NEW_ALL",1)
//      reportMap.put("ACT_ALL",1)
//
//      val exeDaySimp : String = "2017/01/14".replaceAll("\\/","")
//      reportMap.put("OCC_ALL",exeDaySimp.toString)
//
//      println(GsonTools.gson.toJson(reportMap))
//  }
//
//
//    /**
//      * 得到总时长
//      * @param it
//      * @return
//      */
//    def getDuration(it: Iterable[hdfs_view_duration]): Long ={
//    var duration : Long = 0
//    val durationMap = it.map(l => {
//    l.endTime.toLong - l.startTime.toLong
//  }).filter(_ > 0)
//    if(durationMap.size > 0) {
//    duration = durationMap.reduce(_+_)/1000/60
//  }
//    duration
//  }
//
//    /**
//      * 将90天的数据按天＼用户ＭＡＣ分组
//      * @param allDurDayRdd
//      * @return
//      */
//    def allDurDayRddByCurDayAndUserMac(allDurDayRdd : RDD[hdfs_view_duration], exeDaySimp : String): RDD[label_view_duration] ={
//      allDurDayRdd.groupBy(d => {
//        (d.curDay,d.userMac)
//      }).map(dur => {
//        val duration = getDuration(dur._2)
//        val times = dur._2.map(_.isNewTime).reduce(_+_)
//        val diffDays = DateUtils.getDiffDay(dur._1._1,exeDaySimp)
//        val brand = dur._2.head.brand
//        label_view_duration(dur._1._1,0,dur._1._2,times,duration,diffDays.toInt,brand)
//      })
//  }
//
//
//}
