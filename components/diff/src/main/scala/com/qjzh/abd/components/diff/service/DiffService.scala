package com.qjzh.abd.components.diff.service

import java.util
import java.util.{Date, UUID}

import com.qjzh.abd.components.comp_common.caseciew.{MessageKafka, SynAreaBean}
import com.qjzh.abd.components.comp_common.common.RedisBusUtils
import com.qjzh.abd.components.comp_common.conf.CommonConf
import com.qjzh.abd.components.comp_common.utils.{ComUtils, CompRedisClientUtils, GsonTools}
import com.qjzh.abd.components.diff.caseview.{EwAndAwRedisData, HbaseDiffCruBean, RedisDiffLasterBean}
import com.qjzh.abd.components.diff.conf.DiffConf
import com.qjzh.abd.components.diff.dao.redis.RedisClientUtils
import com.qjzh.abd.control.common.view.Report
import com.qjzh.abd.function.common.DateUtils
import com.qjzh.abd.function.hbase.utils.HbaseUtils
import com.qjzh.abd.function.kafka.utils.KafkaProUtils
/**
  * Created by 92025 on 2016/12/20.
  */
object DiffService {
  def readDiffCruData(floorArea: String, head: MessageKafka[HbaseDiffCruBean]): MessageKafka[HbaseDiffCruBean] = {
    val hget: String = RedisBusUtils.hget(DiffConf.REDIS_CRU_DIFF_KEY,floorArea)
    val split: Array[String] = floorArea.split("_")
    if(hget.isEmpty){
      MessageKafka[HbaseDiffCruBean](CommonConf.WARN_AREA_DIFF_NUM,head.timeStamp,HbaseDiffCruBean(split(1).toLong,
        head.timeStamp,
        0,
        split(0).toLong,
        0,
        0,
        0.0,
        false,
        false))
    }else{
      val json: HbaseDiffCruBean = GsonTools.gson.fromJson(hget,classOf[HbaseDiffCruBean])
      MessageKafka[HbaseDiffCruBean](CommonConf.WARN_AREA_DIFF_NUM,head.timeStamp,HbaseDiffCruBean(
        json.areaId,
        head.timeStamp,
        0,json.floor,0,0,0.0,false,false
      ))
    }
  }

  def compuerLossAreaData(xx: (Long, Iterable[MessageKafka[HbaseDiffCruBean]])): List[MessageKafka[HbaseDiffCruBean]] = {

    val head: MessageKafka[HbaseDiffCruBean] = xx._2.head

    val toList = xx._2.toList

    val floorAreaIdAll: Set[String] = toList.map(xx => {
      xx.value.floor + "_" +xx.value.areaId
    }).toSet

    val floorAreatoSet: Set[String] = CompRedisClientUtils.getRedisAreaMap().map(xx => {
      xx.id.toString
    }).toSet
    val floorAreaQueryData: Set[String] = floorAreatoSet -- floorAreaIdAll

    val lossAreaData: Set[MessageKafka[HbaseDiffCruBean]] = floorAreaQueryData.map(floorArea => {
      DiffService.readDiffCruData(floorArea, head)
    })
    val toListLoss: List[MessageKafka[HbaseDiffCruBean]] = lossAreaData.toList
    val value: List[MessageKafka[HbaseDiffCruBean]] = toList ++: toListLoss
    value
  }


  /**
    * 骤增核心业务计算
    *
    * @param xx
    * @return
    */
  def computerDiff(xx:(String, (Report, Int))): MessageKafka[HbaseDiffCruBean] ={

    //        println(xx)
    val curTimeMin = DateUtils.getCurTime("yyyyMMddHHmm").toLong
    val split: Array[String] = xx._1.split("_")
    val floor = split(0)
    val areaId = split(1)
    val sum: Int = xx._2._2 //人数
    val bean: RedisDiffLasterBean = RedisDiffLasterBean(areaId.toLong,curTimeMin,sum,floor.toLong)

    //获取上一十分钟的数据
    val mapKey:String = bean.floor + "_" + bean.areaId

    val lasterBean: RedisDiffLasterBean = GsonTools.gson.fromJson(RedisBusUtils.hget(DiffConf.REDIS_LASTER_DIFF_KEY,mapKey),classOf[RedisDiffLasterBean])

    val hbaseBean: HbaseDiffCruBean = DiffService.computeHbaseBean(lasterBean,bean)//计算要存储的hbase记录信息

    //存入hbase
    HbaseUtils.writeTable(DiffConf.HBASE_DIFF_TABLE,curTimeMin + "_" + DiffConf.OUTER + "_" + DiffConf.DIFF_MODULE_NAME + "_" + bean.areaId,DiffConf.COLUMNE,GsonTools.gson.toJson(hbaseBean))
    //更新redis  上十分钟数据
    RedisBusUtils.hset(DiffConf.REDIS_LASTER_DIFF_KEY,mapKey,GsonTools.gson.toJson(bean))
    //更新redis  当前值
    RedisBusUtils.hset(DiffConf.REDIS_CRU_DIFF_KEY,mapKey,GsonTools.gson.toJson(hbaseBean))

    //骤增预警告警消息通知模块
    /*SendAlarmMsgToKafkaServiceImpl.sendAlarmMsgToKafka(CommonConf.oline_one_minute_mub,
      GsonTools.gson.toJson(
        EaWaMessageToKafka(CommonConf.WARN_AREA_DIFF_NUM,DateUtils.getTime(xx._2._1.serverTimeStamp,"yyyyMMddHHmm").toLong,hbaseBean)))*/

    /*val infoes: ObjectArrayList[EaWaMessageInfo] = new ObjectArrayList[EaWaMessageInfo]()
    infoes.add(hbaseBean)*/

    MessageKafka[HbaseDiffCruBean](CommonConf.WARN_AREA_DIFF_NUM,DateUtils.getTime(xx._2._1.serverTimeStamp,"yyyyMMddHHmm").toLong,hbaseBean)
  }

  /**
    * 发送告警相关人数统计信息
    * @param topic
    * @param msg
    */
  def sendMessageToKafka(topic: String,msg: String): Unit ={
    KafkaProUtils.sendMessages(topic,msg)
  }
  /**
    * 发送预警告警消息通知
    * @param hbaseBean
    */
  def sendEwAndAwWarnMsg(hbaseBean: HbaseDiffCruBean): Unit = {
    val areaBean: SynAreaBean = RedisClientUtils.getAreaInfoByFloorAndAreaNo(hbaseBean.floor,hbaseBean.areaId)
    if(areaBean != null){
      if( hbaseBean.isSurgeThresholdWarn){//是够发送告警消息
      val data: EwAndAwRedisData = EwAndAwRedisData(hbaseBean.areaId,new Date().getTime,hbaseBean.diff,CommonConf.WARN_AREA_DIFF.toLong,CommonConf.WARN_LEVEL_EW,areaBean.surgeThreshold.toLong)
        val datas: util.ArrayList[EwAndAwRedisData] = new util.ArrayList[EwAndAwRedisData]()
        datas.add(data)
        RedisBusUtils.hset(RedisBusUtils.EW_AW_REDIS_KEY,UUID.randomUUID().toString,GsonTools.gson.toJson(datas))
      }else if( hbaseBean.isEwWarn){//是否发送预警消息
      val ewAndAwRedisData: EwAndAwRedisData = EwAndAwRedisData(hbaseBean.areaId,new Date().getTime,hbaseBean.diff,CommonConf.WARN_AREA_DIFF.toLong,CommonConf.WARN_LEVEL_AW,areaBean.preSurgeThreshold.toLong)
        val datas: util.ArrayList[EwAndAwRedisData] = new util.ArrayList[EwAndAwRedisData]()
        datas.add(ewAndAwRedisData)
        RedisBusUtils.hset(RedisBusUtils.EW_AW_REDIS_KEY,UUID.randomUUID().toString,GsonTools.gson.toJson(datas))
      }
    }
  }

  //==============================骤增用到的函数
  /**
    * 计算是否预警
    * @param redisLasterDiff
    * @param redisCruDiff
    * @return
    */
  def computeEwWarn(redisLasterDiff:RedisDiffLasterBean,redisCruDiff:RedisDiffLasterBean): Boolean ={
    val id: Long = redisCruDiff.areaId
    val floor: Long = redisCruDiff.floor

    val bean: SynAreaBean = RedisClientUtils.getAreaInfoByFloorAndAreaNo(floor,id)

    if(bean != null){
      return redisCruDiff.sum - redisLasterDiff.sum > bean.preSurgeThreshold
    }
    false
  }
  /**
    * 计算是否告警
    * @param redisLasterDiff
    * @param redisCruDiff
    * @return
    */
  def computeSurgeThresholdWarn(redisLasterDiff:RedisDiffLasterBean,redisCruDiff:RedisDiffLasterBean): Boolean ={
    val id: Long = redisCruDiff.areaId
    val floor: Long = redisCruDiff.floor
    val bean: SynAreaBean = RedisClientUtils.getAreaInfoByFloorAndAreaNo(floor,id)

    if(bean != null){
      return redisCruDiff.sum - redisLasterDiff.sum > bean.surgeThreshold
    }
    false
  }
  /**
    * 根据当前十分中值和上一十分钟值算Hbase该保存的值
    * @param redisLasterDiff
    * @param redisCruDiff
    * @return
    */
  def computeHbaseBean(redisLasterDiff:RedisDiffLasterBean,redisCruDiff:RedisDiffLasterBean): HbaseDiffCruBean ={
    var bean: HbaseDiffCruBean = null
    if(redisLasterDiff == null){//上个十分钟数据为空,将当前十分钟数据存入redis和hbase
      bean = HbaseDiffCruBean(redisCruDiff.areaId,redisCruDiff.dateTime,redisCruDiff.sum,redisCruDiff.floor)
    }else{//上个十分钟数据不为空,将当前十分与上个十分钟做计算,求增值,和增幅,是否告警,是否预警
    val cruSum: Long = redisCruDiff.sum
      val lasterSum: Long = redisLasterDiff.sum

      bean = HbaseDiffCruBean(redisCruDiff.areaId,redisCruDiff.dateTime,cruSum,redisCruDiff.floor,lasterSum,cruSum - lasterSum,
        (cruSum - lasterSum).toDouble/lasterSum,computeEwWarn(redisLasterDiff,redisCruDiff),computeSurgeThresholdWarn(redisLasterDiff,redisCruDiff)
      )
      //如果人数同时满足预警和告警  只告警不预警
      /*if(bean.isEwWarn && bean.isSurgeThresholdWarn){
        bean.isEwWarn = false
      }*/

    }
    bean
  }
}
