package com.qjzh.abd.components.in_out_move.service

import java.io.Serializable
import java.util
import java.util.ArrayList

import com.qjzh.abd.components.comp_common.caseciew.{MessageKafka, Point, RedisImpDetailCase, hdfs_view_duration}
import com.qjzh.abd.components.comp_common.common.RedisBusUtils
import com.qjzh.abd.components.comp_common.conf.CommonConf
import com.qjzh.abd.components.comp_common.utils.{ComUtils, CompRedisClientUtils, GsonTools, LabelTools}
import com.qjzh.abd.components.in_out_move.caseview._
import com.qjzh.abd.components.in_out_move.conf.{HbaseIncrTableConf, New7gUserTraceConf}
import com.qjzh.abd.components.in_out_move.dao.{HbaseDao, RedisDao}
import com.qjzh.abd.control.common.view.Report
import com.qjzh.abd.function.common.DateUtils
import com.qjzh.abd.function.hbase.utils.HbaseUtils
import com.qjzh.abd.function.redis.utils.RedisUtils
import it.unimi.dsi.fastutil.objects.Object2ObjectMap.FastEntrySet
import it.unimi.dsi.fastutil.objects.{Object2ObjectOpenHashMap, ObjectSet}
import kafka.serializer.StringDecoder
import org.apache.parquet.it.unimi.dsi.fastutil.objects.ObjectArrayList
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable


/**
  * Created by 92025 on 2016/12/24.
  */
object New7gUserTraceService {

  def updateUserArea(yy: UserMacPointTrajectory): Unit = {

    val traceHbaseKey: String = getHbaseKeyByUserTrace(yy)
    val table: Object2ObjectOpenHashMap[String, String] = HbaseDao.readTable(New7gUserTraceConf.Hbase_Table_Name_Trace_IN_OUT, traceHbaseKey)
    val remove: String = table.remove("rowKey")
    val keySet: ObjectSet[String] = table.keySet()
    keySet.toArray().toList
//    val objectEntrySet: FastEntrySet[String, String] = table.object2ObjectEntrySet()
    /*val reverse: List[String] = filter1.keySet.toList.sorted.reverse
    val head1: String = reverse.head
    val detail: String = hgetall.get(head1).get
    val inAndOutHbase: InAndOutHbase = GsonTools.gson.fromJson(detail,classOf[InAndOutHbase])
    //更新记录
    New7gUserTraceService.updateUserDetail(report,inAndOutHbase,New7gUserTraceConf.Hbase_Table_Name_Trace_IN_OUT)*/
  }


  def createUserArea(yy: UserMacPointTrajectory): Unit = {
    val hbaseKey = getHbaseKeyByUserTrace(yy)

    val timeHourMinuteOut: String = DateUtils.getTime(yy.startTime,"HHmm")
    val timeHourMinuteIn: String = DateUtils.getTime(yy.startTime - (1000 * 60),"HHmm")
    //该用户今天没有被ap扫描到过,往hbase和redis中添加新记录
    val hbase: InAndOutHbase = InAndOutHbase(yy.startTime,yy.startTime,yy.areaNo.toLong)
    //封装一个用户今天第一次被ap扫描到的数据
    val stringToString: mutable.HashMap[String, String] = mutable.HashMap((timeHourMinuteOut -> GsonTools.gson.toJson(hbase)),
      (timeHourMinuteIn + "_" + "I" -> GsonTools.gson.toJson(hbase)),
      (timeHourMinuteOut + "_" + "O" -> GsonTools.gson.toJson(hbase)))
    //存储到hbase
    HbaseDao.setMapByRowKey(New7gUserTraceConf.Hbase_Table_Name_Trace_IN_OUT,hbaseKey,stringToString)
  }
  /**
    * 得到hbase_key
    * @return
    */
  def getHbaseKeyByUserTrace(userMacPointTrajectory : UserMacPointTrajectory): String ={
    //当前终端手机mac
    val userMac = userMacPointTrajectory.userMac
    //当前redisKey
    val key =  userMac + "_" + DateUtils.getCurTime("yyyyMMdd")
    key
  }

  def getRedisKeyByUserMacPointTrajectory(userMacPointTrajectory : UserMacPointTrajectory): String ={
    //当前终端手机mac
    val userMac = userMacPointTrajectory.userMac
    //当前redisKey
    val curUserMacRedisKey = "abd:in_out_move:trace:"+DateUtils.getCurTime("yyyyMMdd")+":"+userMac
    curUserMacRedisKey
  }
  /**
    * 创建天的记录
    * @param yy
    */
  def createUserDay(yy: UserMacPointTrajectory) = {
    val mac: String = yy.userMac
    val yyMM: String = DateUtils.getTime(yy.startTime,"yyyyMM")
    val day: String = DateUtils.getTime(yy.startTime,"dd")
    val strings: util.ArrayList[String] = new ArrayList[String]
    strings.add(day)
    HbaseDao.insertHbaseList(New7gUserTraceConf.Hbase_Table_Name_Trace_gather,mac + "_IN_OUT",yyMM,strings)
  }


  /**
    * 用户当天首次
    *
    * @param yy  用户视图
    */
  def createUserDayAndArea(yy: UserMacPointTrajectory): Unit = {
    New7gUserTraceService.createUserDay(yy)
    New7gUserTraceService.createUserArea(yy)
  }




  val firstRowKey = "001"
  val secondRowKey = "002"
  /**
    * 创建一个sparkContext
    * @param appName
    * @param master
    * @return
    */
  def createSparkContext(appName: String,master: String = null): SparkContext ={
    val sparkConf = new SparkConf().setAppName(appName)
    if(master != null){
      sparkConf.setMaster(master)
    }
    new SparkContext(sparkConf)
  }

  /**
    * 创建一个Dtreaming
    * @param sparkContext   spark上下文对象
    * @param batchTime       批次的时间
    * @param kafkaList       kafka的broker列表
    * @param kafkaTopicSet  kafkatopic列表
    * @param offsetAuto      kafka偏移量自动重置策略
    * @return      Dtreaming
    */
  def createDStreaming(sparkContext: SparkContext,batchTime: Long = 60,kafkaList:String,kafkaTopicSet:String,offsetAuto: String): (DStream[String],StreamingContext) ={
    val ssc: StreamingContext = new StreamingContext(sparkContext, Seconds(batchTime))
    val topicsSet = kafkaTopicSet.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> kafkaList, "auto.offset.reset" -> "largest")
    val stream: DStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet) map (_._2)
    (stream,ssc)
  }


  /**
    * 用户今天第n次通关  新建通关记录
    * @param report
    * @param hbaseTableName  移动终端迁入迁出明细表
    */
  def createUserDetailArea(report: Report,hbaseTableName: String): Unit ={
//    val time: String = DateUtils.getTime(report.timeStamp,"yyyyMMdd")
//    val mac: String = report.userMac
//    val redisKey = mac + "_" + time

    val curUserMacRedisKey = getRedisKeyByUserTrace(report)
    val hbaseKey = getHbaseKeyByUserTrace(report)

    val timeHourMinuteOut: String = DateUtils.getTime(report.timeStamp,"HHmm")

    val timeHourMinuteIn: String = DateUtils.getTime(report.timeStamp - (1000 * 60),"HHmm")
    //该用户今天没有被ap扫描到过,往hbase和redis中添加新记录
    val hbase: InAndOutHbase = InAndOutHbase(report.timeStamp,report.timeStamp,report.areaNo.toLong)
    //封装一个用户今天第一次被ap扫描到的数据
    val stringToString: mutable.HashMap[String, String] = mutable.HashMap((timeHourMinuteOut -> GsonTools.gson.toJson(hbase)),
      (timeHourMinuteIn + "_" + "I" -> GsonTools.gson.toJson(hbase)),
      (timeHourMinuteOut + "_" + "O" -> GsonTools.gson.toJson(hbase)))
    //存储到redis
    RedisDao.hsetMap(curUserMacRedisKey,stringToString,RedisDao.one_day)
    //存储到hbase
    HbaseDao.setMapByRowKey(hbaseTableName,hbaseKey,stringToString)
  }

  /**
    * 用户今天第一次被扫描  新建记录
    * @param report
    * @param hbaseTableName  移动终端迁入迁出明细表
    * @param tableName   存储用户出入关记录表
    */

  def createUserDetail(report: Report,hbaseTableName: String,tableName: String): Unit ={
    createUserDetailArea(report,hbaseTableName)
    createUserDayDetail(tableName,report)
  }

  /**
    * 添加一个用某月份那几天有过关记录
    * @param tableName
    * @param report
    */
  def createUserDayDetail(tableName: String,report: Report): Unit ={
    val mac: String = report.userMac
    val yyMM: String = DateUtils.getTime(report.timeStamp,"yyyyMM")
    val day: String = DateUtils.getTime(report.timeStamp,"dd")
    val strings: util.ArrayList[String] = new ArrayList[String]
    strings.add(day)
    HbaseDao.insertHbaseList(tableName,mac + "_IN_OUT",yyMM,strings)
  }

  /**
    *
    * @param report
    * @param lastInAndOutHbase
    * @param hbaseTableName
    */
  def updateUserDetail(report: Report,lastInAndOutHbase: InAndOutHbase,hbaseTableName: String): Unit ={
//    val time: String = DateUtils.getTime(report.timeStamp,"yyyyMMdd")
//    val mac: String = report.userMac
//    val redisKey = mac + "_" + time

    val curUserMacRedisKey = getRedisKeyByUserTrace(report)
    val hbaseKey = getHbaseKeyByUserTrace(report)

    val timeHourMinute: String = DateUtils.getTime(report.timeStamp,"HHmm")
    val lasterHourMinute: String = DateUtils.getTime(lastInAndOutHbase.outTime,"HHmm")

    val hbase: InAndOutHbase = InAndOutHbase(lastInAndOutHbase.outTime,report.timeStamp,report.areaNo.toLong)

    //先判断是否和上一个区域的id相同
    if(isSameArea(report,lastInAndOutHbase)){//相同  需要合并两个区域
      val hbaseUpdateMerge: InAndOutHbase = InAndOutHbase(lastInAndOutHbase.inTime,report.timeStamp,report.areaNo.toLong)
      RedisDao.hdel(curUserMacRedisKey,lasterHourMinute)
      RedisDao.hset(curUserMacRedisKey,timeHourMinute,GsonTools.gson.toJson(hbaseUpdateMerge),RedisDao.one_day)

      HbaseDao.setRowKeyAndValue(hbaseTableName,hbaseKey,timeHourMinute,GsonTools.gson.toJson(hbaseUpdateMerge))
      HbaseDao.detCol(hbaseTableName,hbaseKey,lasterHourMinute)

    }else{
      RedisDao.hset(curUserMacRedisKey,timeHourMinute,GsonTools.gson.toJson(hbase),RedisDao.one_day)
      HbaseDao.setRowKeyAndValue(hbaseTableName,hbaseKey,timeHourMinute,GsonTools.gson.toJson(hbase))
    }


    RedisDao.hdel(curUserMacRedisKey,lasterHourMinute + "_O")
    RedisDao.hset(curUserMacRedisKey,timeHourMinute + "_O",GsonTools.gson.toJson(InAndOutHbase(lastInAndOutHbase.inTime,report.timeStamp,report.areaNo.toLong)),RedisDao.one_day)

    HbaseDao.detCol(hbaseTableName,hbaseKey,lasterHourMinute + "_O")
    HbaseDao.setRowKeyAndValue(hbaseTableName,hbaseKey,timeHourMinute + "_O",GsonTools.gson.toJson(InAndOutHbase(lastInAndOutHbase.inTime,report.timeStamp,report.areaNo.toLong)))
  }


  /**
    * 在redis中通过单个点的定义，构造出 UserMacPointTrajectory轨迹对象
    * @param report
    * @return
    */
  def dealUserMacPointTrajectory(report : Report) : List[UserMacPointTrajectory] ={
    var resultList : List[UserMacPointTrajectory] = null
    //当前终端手机mac
    val userMac = report.userMac
    //当前分钟
    val curMin = DateUtils.getCurTime("HHmm")
    //当前redisKey
    val curUserMacRedisKey = "abd:in_out_move:point:traj:"+DateUtils.getCurTime("yyyyMMdd")+":"+userMac
    //当前redis中的缓存值
    val curUserPointTras: Map[String, String] = RedisBusUtils.hgetall(curUserMacRedisKey)  //获取当前用户今天是否被探针扫描过

    if(curUserPointTras == null || curUserPointTras.size == 0) {
      //该用户今天没有被ap扫描到过
      val pointTrject = UserMacPointTrajectory(curMin,null,1,1,userMac,report.areaNo,System.currentTimeMillis(),0,report.brand)
      RedisBusUtils.hset(curUserMacRedisKey,firstRowKey,GsonTools.gson.toJson(pointTrject),RedisDao.one_day)
    }else{
      //当前最晚的那个区域数据
      val lastView = getUserPointTraLastRowKey(curUserPointTras)
      val pointTrject: UserMacPointTrajectory = GsonTools.gson.fromJson(lastView._2,classOf[UserMacPointTrajectory])
      val spMin: Long = DateUtils.getDiffMinute(pointTrject.curSTMin,curMin,"HHmm")
      //前后两个点在一个小时内
      if(spMin < 60){
        if(pointTrject.areaNo.equalsIgnoreCase(report.areaNo)){
          //1个小时之内区域相同，刚更新当前区域结束时间与当次的结束时间
          pointTrject.endTime = System.currentTimeMillis()
          pointTrject.curETMin = curMin
          RedisBusUtils.hset(curUserMacRedisKey,lastView._1,GsonTools.gson.toJson(pointTrject),RedisDao.one_day)
        }else{
          //1个小时之内区域不同，移出最早的那个区域，并且新增一个新的区域
          resultList = getRemoveUserPointTra(curUserMacRedisKey)
          val newAreaTrject = UserMacPointTrajectory(pointTrject.curSTMin,null,0,0,userMac,report.areaNo,System.currentTimeMillis(),0,report.brand)
          RedisBusUtils.hset(curUserMacRedisKey,secondRowKey,GsonTools.gson.toJson(newAreaTrject),RedisDao.one_day)
        }
      }else{
        //如果大于一个小时，则把上一批次所有区域全部移除，并且新增一个新的次数与区域
        resultList = getRemoveUserPointTra(curUserMacRedisKey,1)
        val pointTrject = UserMacPointTrajectory(curMin,null,0,1,userMac,report.areaNo,System.currentTimeMillis(),0,report.brand)
        RedisBusUtils.hset(curUserMacRedisKey,firstRowKey,GsonTools.gson.toJson(pointTrject),RedisDao.one_day)
      }
    }
    resultList

  }

  /**
    * 得到当前晚一条redisKey
    * @param redisMap
    * @return
    */
  def getUserPointTraLastRowKey(redisMap : Map[String, String]):(String,String) = {
    var result : (String,String) = null
    if(redisMap.size == 1){
      result = (firstRowKey,redisMap.getOrElse(firstRowKey,null))
    }else{
      result = (secondRowKey,redisMap.getOrElse(secondRowKey,null))
    }
    result
  }

  /**
    * 如果当前轨迹中即将超过2个区域，则将最早的那个区域移出，第二个区域前移
    * @param redisKey
    * @return
    */
  def getRemoveUserPointTra(redisKey :  String, isRemoveAll : Int  = 0):List[UserMacPointTrajectory] = {
    var resultList : List[UserMacPointTrajectory] = null

    RedisUtils.getRedisClient().withClient(redis => {

      val curRedisSaveLen = redis.hlen(redisKey)
      //如果设备为移除全部
      if(isRemoveAll == 1){
          var firJson : UserMacPointTrajectory = null
          var secJson : UserMacPointTrajectory = null
          if(redis.hexists(redisKey,firstRowKey)) {
            firJson = GsonTools.gson.fromJson(redis.hget[String](redisKey, firstRowKey).get, classOf[UserMacPointTrajectory])
            redis.hdel(redisKey,firstRowKey)//清空数据 注意不能直接del redis_key(防止后续以为这个人又为当天首次)
          }
          if(redis.hexists(redisKey,secondRowKey)) {
            secJson = GsonTools.gson.fromJson(redis.hget[String](redisKey, secondRowKey).get, classOf[UserMacPointTrajectory])
            redis.hdel(redisKey,secondRowKey)//清空数据 注意不能直接del redis_key(防止后续以为这个人又为当天首次)
          }
          //所有数据移出，支援空数据
          resultList = List(firJson,secJson)
      }
      //如果当前值超过2个区域
      if(curRedisSaveLen.size >= 2){
          //当前两个key值已经存在
          if(redis.hexists(redisKey,firstRowKey) && redis.hexists(redisKey,secondRowKey)){
            //取出第一个值
            val firJson =  GsonTools.gson.fromJson(redis.hget[String](redisKey,firstRowKey).get,classOf[UserMacPointTrajectory])
            //移除
            redis.hdel(redisKey,firstRowKey)
            //取出第二个值
            val secondValue = redis.hget[String](redisKey,secondRowKey).get
            //将第二值移到第第一位
            redis.hset(redisKey,firstRowKey,secondValue)
            //删除第二个值
            redis.hdel(redisKey,secondRowKey)
            resultList = List(firJson)
          }else{
            //清空数据 注意不能直接del redis_key(防止后续以为这个人又为当天首次)
            redis.hgetall().get.foreach(x => {
              redis.hdel(redisKey,x._1)
            })
          }
        }
    })
    resultList
  }

//  /**
//    * 计算用户通关的详细记录方法的补丁
//    * @param report
//    */
//  def computerUserDetailFix(report: Report): List[InAndOutFix] ={
//    val time: String = DateUtils.getTime(report.timeStamp,"yyyyMMdd")
//    val mac: String = report.userMac
//    val redisKey = time + "_" + mac + "_" + "fix"
//    val timeMinuteHour = DateUtils.getTime(report.timeStamp,"HHmm")
//
//    val hgetall: Map[String, String] = RedisBusUtils.hgetall(redisKey)  //获取当前用户今天是否被探针扫描过
//
//    if( hgetall == null || hgetall.size == 0){   //该用户今天没有被ap扫描到过
//      val inAndOutFix: InAndOutFix = InAndOutFix(report.timeStamp,"1","1",report.userMac,report.areaNo.toLong,report.timeStamp,0)
//      RedisBusUtils.hset(redisKey,timeMinuteHour,GsonTools.gson.toJson(inAndOutFix),RedisDao.one_day)
//      null
//    }else{//该用户今天被ap扫描到过，更新redis,和hbase的值
//      val sorted: List[String] = hgetall.keySet.toList.sorted
//      val reverse: List[String] = sorted.reverse
//
//      val inAndOutFix: InAndOutFix = GsonTools.gson.fromJson(hgetall.get(reverse.head).get,classOf[InAndOutFix])//获取最后一次用户记录
//
//      val lastTime: Long = inAndOutFix.endTime match { //拿到上次用户最后一条的时间
//        case 0 => inAndOutFix.startTime
//        case _ => inAndOutFix.endTime
//      }
//      val lastOutTime: String = DateUtils.getTime(lastTime,"HHmm")
//      val minute: Long = DateUtils.getDiffMinute(timeMinuteHour,lastOutTime,"HHmm")
//
//      if(minute >= 60){//本次被扫描与上次被扫描的时差大于一小时,需要新建一次通关记录,将前面已有的记录清空到hbase
//        val toList: List[InAndOutFix] = hgetall.map(xx => {
//          GsonTools.gson.fromJson(xx._2, classOf[InAndOutFix])
//        }).filter(_.endTime != 0).toList
//        toList
//      }else{//本次被扫描与上次被扫描的时差小于一小时,需要新建一次通关记录
//
//        if(sorted.size < 3){//如果该用户记录小于三条
//          if(report.areaNo.toLong == inAndOutFix.areaNo){ //与上一条记录区域相同  修改记录的结束时间
//            val outFix: InAndOutFix = InAndOutFix(inAndOutFix.simpleTime,inAndOutFix.isCruDay,
//              inAndOutFix.isSimple,inAndOutFix.userMac,inAndOutFix.areaNo,inAndOutFix.startTime,report.timeStamp)
//            val stringToString: Map[String, String] = hgetall ++ Map(reverse.head -> GsonTools.gson.toJson(outFix))
//            RedisDao.hsetMuMap(redisKey,stringToString,RedisDao.one_day)
//            null
//          }else{//与上一条记录不同,直接保存redis中
//            RedisDao.hset(redisKey,timeMinuteHour,GsonTools.gson.toJson(InAndOutFix(report.timeStamp,"0","0",report.userMac,report.areaNo.toLong,report.timeStamp,0)))
//            null
//          }
//        }else{//如果用户的过关记录大于等于三条,移除第一条  ,整合当前一条数据
//          if(report.areaNo.toLong == inAndOutFix.areaNo){//相同区域做整合
//            val outFix: InAndOutFix = InAndOutFix(inAndOutFix.simpleTime,inAndOutFix.isCruDay,
//              inAndOutFix.isSimple,inAndOutFix.userMac,inAndOutFix.areaNo,inAndOutFix.startTime,report.timeStamp)
//            val stringToString: Map[String, String] = hgetall ++ Map(reverse.head -> GsonTools.gson.toJson(outFix))
//            RedisDao.hsetMuMap(redisKey,stringToString,RedisDao.one_day)
//            null
//          }else{//不同区域需要排挤掉第一条数据,然后添加一条数据
//            val outFix: InAndOutFix = InAndOutFix(report.timeStamp,"0","0",report.userMac,report.areaNo.toLong,report.timeStamp,0)
//            val first: String = hgetall.get(sorted.head).get
//            val stringToSerializable: Map[String, String] = hgetall.-(sorted.head) ++ Map(timeMinuteHour -> GsonTools.gson.toJson(outFix))//排挤出第一条  添加了一条的集合
//            RedisDao.hsetMuMap(redisKey,stringToSerializable,RedisDao.one_day) //重新存入
//            val userFirstBean: InAndOutFix = GsonTools.gson.fromJson(first,classOf[InAndOutFix])
//            if(userFirstBean.endTime > 0){//最上面的一个区域结束时间不为0   就需要保存
//              List(userFirstBean)
//            }else{
//              null
//            }
//          }
//        }
//      }
//    }
//  }


  /**
    * 计算用湖通关详细记录方法
    */
  def computerUserDetail(report: Report): Unit ={

//    val time: String = DateUtils.getTime(report.timeStamp,"yyyyMMdd")
//    val mac: String = report.userMac
//    val redisKey = mac + "_" + time

    val curUserMacRedisKey = getRedisKeyByUserTrace(report)

    val hgetall: Map[String, String] = RedisBusUtils.hgetall(curUserMacRedisKey)  //获取当前用户今天是否被探针扫描过

    if( hgetall == null || hgetall.size == 0){   //该用户今天没有被ap扫描到过
      New7gUserTraceService.createUserDetail(report,New7gUserTraceConf.Hbase_Table_Name_Trace_IN_OUT,New7gUserTraceConf.Hbase_Table_Name_Trace_gather)//穿件一条新的通关记录
    }else{//该用户今天被ap扫描到过，更新redis,和hbase的值
    val timeHourMinute: String = DateUtils.getTime(report.timeStamp,"HHmm")
      val filter: Set[String] = hgetall.keySet.filter(xx => {
        val split: Array[String] = xx.split("_")
        val split1: String = split(split.length - 1)
        "O".equals(split1)
      })
      val head: String = filter.toList.sorted.reverse.head
      val outBean: String = hgetall.get(head).get  //最后一次记录的时间
      val outBeanJson: InAndOutHbase = GsonTools.gson.fromJson(outBean,classOf[InAndOutHbase])

      val outBeanJsonOutTime: String = DateUtils.getTime(outBeanJson.outTime,"HHmm")
      val minute: Long = DateUtils.getDiffMinute(outBeanJsonOutTime,timeHourMinute,"HHmm")

      if(minute >= 60){//本次被扫描与上次被扫描的时差大于一小时,需要新建一次通关记录
        New7gUserTraceService.createUserDetailArea(report,New7gUserTraceConf.Hbase_Table_Name_Trace_IN_OUT)//穿件一条新的通关记录
      }else{//本次被扫描与上次被扫描的时差小于一小时,追加通关记录信息
      val filter1: Map[String, String] = hgetall.filter(map => {//筛选出通关记录信息
        /*!map._1.contains("I") && !*/map._1.contains("O")
      })
        val reverse: List[String] = filter1.keySet.toList.sorted.reverse
        val head1: String = reverse.head
        val detail: String = hgetall.get(head1).get

        val inAndOutHbase: InAndOutHbase = GsonTools.gson.fromJson(detail,classOf[InAndOutHbase])
        //更新记录
        New7gUserTraceService.updateUserDetail(report,inAndOutHbase,New7gUserTraceConf.Hbase_Table_Name_Trace_IN_OUT)
//        New7gUserTraceService.getUserAreaStartAndEndDetail(report,inAndOutHbase)
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val minue: Long = DateUtils.getDiffMinute("0708","1248","HHmm")
    println(minue)
  }
  /**
    * 得到redis_key
    * @param report
    * @return
    */
  def getRedisKeyByUserTrace(report : Report): String ={
    //当前终端手机mac
    val userMac = report.userMac
    //当前redisKey
    val curUserMacRedisKey = "abd:in_out_move:trace:"+DateUtils.getCurTime("yyyyMMdd")+":"+userMac
    curUserMacRedisKey
  }


  /**
    * 得到hbase_key
    * @param report
    * @return
    */
  def getHbaseKeyByUserTrace(report : Report): String ={
    //当前终端手机mac
    val userMac = report.userMac
    //当前redisKey
    val key =  userMac + "_" + DateUtils.getCurTime("yyyyMMdd")
    key
  }


//  /**
//    * 获取用户在每个区域的起始时间  结束时间  详细记录
//    * @param report
//    * @param lastInAndOutHbase
//    */
//    def getUserAreaStartAndEndDetail(report: Report,lastInAndOutHbase: InAndOutHbase): hdfs_view_duration ={
//      if(!isSameArea(report,lastInAndOutHbase)){
//        val lastOutTimeDay: String = DateUtils.getTime(lastInAndOutHbase.outTime,"yyyyMMdd")
//        hdfs_view_duration(lastOutTimeDay,report.userMac,lastInAndOutHbase.inTime.toString,lastInAndOutHbase.outTime.toString,lastInAndOutHbase.areaId.toString,report.userMac)
//      }else{
//        null
//      }
//    }

  /**
    * 判断该用户 当前所在区域和上一次记录 是否在同于区域
    * @param report
    * @param lastInAndOutHbase
    * @return
    */
  def isSameArea(report: Report,lastInAndOutHbase: InAndOutHbase): Boolean ={
    report.areaNo.toLong == lastInAndOutHbase.areaId
  }



  /**
    * 格式化数据源的数据
    * @param report
    */
  def parseSrc(report: Report): Report ={
//      report.serverTimeStamp = DateUtils.getCurTime("yyyyMMddHHmm").toLong

      val areaList = CompRedisClientUtils.getRedisAreaMap().filter(x => {
//        x.floor == report.floor && ComUtils.isContainInArea(report,x) != -1
        ComUtils.isContainInArea(report,x) != -1
      }).take(1)

      if (!areaList.isEmpty) {
        report.areaNo = areaList(0).id + ""
      } else {
        report.areaNo = "-1"
      }

      report
   }

  def updateUserDesc(s : UserStatics) :Unit ={
    val userMac = s.userMac
    val labels = LabelTools.getUserLabel(userMac)

    val desc : UserStaticsJson = UserStaticsJson(labels._1,"-1",s.mostFreqTSlot,s.dayCountTotal,s.minuteTotal,s.lastApprDate,s.firstApprDate,s.countAvgByWeek,s.dayCountAvgByWeek,
      s.maxByDay,s.avgDay,System.currentTimeMillis().toString)
    HbaseDao.insertHbase(HbaseIncrTableConf.descShowTable, userMac, "data",GsonTools.gson.toJson(desc))

    if("1".equals(s.isFirst) && "keyPass".equals(labels._1)){
      val roleId = LabelTools.getLabelRoleId(labels._2)
      val impDetail = RedisImpDetailCase(userMac,
        DateUtils.getTime(System.currentTimeMillis(), "yyyyMMddHHmm")
        ,s.countTotal.toInt,s.minuteTotal.toDouble,0,roleId)

      val userDescList: ObjectArrayList[RedisImpDetailCase] = new ObjectArrayList[RedisImpDetailCase]()
      userDescList.add(impDetail)

      //发送kafka
      com.qjzh.abd.components.comp_common.common.KafkaUtils.sendMsgToKafka(
        com.qjzh.abd.components.comp_common.common.KafkaUtils.off_line_one_day_mub,
        MessageKafka[ObjectArrayList[RedisImpDetailCase]](CommonConf.WARN_AREA_CORE_DETAIL,
        DateUtils.getTime(System.currentTimeMillis(),"yyyyMMddHHmm").toLong,userDescList))
    }

  }


}
