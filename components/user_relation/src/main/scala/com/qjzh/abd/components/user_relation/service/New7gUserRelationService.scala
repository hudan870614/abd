package com.qjzh.abd.components.user_relation.service

import com.qjzh.abd.components.comp_common.utils.{ComUtils, CompRedisClientUtils, GsonTools, LabelTools}
import com.qjzh.abd.components.user_relation.caseview.{G7UserRelateInfo, G7UserRelateInfoDetail, G7UserRelateInfoToKafka, RaletionMacInfo}
import com.qjzh.abd.components.user_relation.conf.New7gUserRelationConf
import com.qjzh.abd.components.user_relation.dao.New7gUserRelationHbaseDao
import com.qjzh.abd.control.common.view.Report
import com.qjzh.abd.function.common.DateUtils

import scala.collection.{JavaConverters, mutable}
import scala.collection.mutable.ListBuffer
import java.util.ArrayList

import com.qjzh.abd.components.comp_common.caseciew.MessageKafka
import com.qjzh.abd.components.comp_common.conf.CommonConf
import com.qjzh.abd.components.user_relation.Utils
import org.apache.spark.streaming.dstream.DStream
/**
  * Created by 92025 on 2017/1/10.
  */
object New7gUserRelationService {

  /**
    * 判断是不是重点人
    * @param report
    * @return
    */
  def isCorePerson(report: Report): Boolean ={

    /*if("C4F1D102E016".equals(report.userMac)){
      true
    }else{
      false
    }*/
//    true
    LabelTools.getUserLabelType(report.userMac)._2
  }

  /**
    * 判断关联元祖中有没有重点人
    * @param tuple
    * @return
    */
  def haveCorePerson(tuple: (Report, Report)): Boolean ={  //必须都是重点人的组合才要
//    true
    New7gUserRelationService.isCorePerson(tuple._1) && New7gUserRelationService.isCorePerson(tuple._2)
  }

  /**
    * 查询重点人的信息
    * @param report
    */
  def queryCorePersonInfo(report: Report): (Long,Long) ={
    val reTimes: Long = New7gUserRelationHbaseDao.queryCorePersonReTimes(report)
    val sinTimes: Long = New7gUserRelationHbaseDao.queryCorePersonSinTimes(report)
    (reTimes,sinTimes)
  }

  /**
    * 查询两个userMac关联次数
    * @param tuple
    * @return
    */
  def queryCorePersonRalTimes(tuple: (Report, Report)): String ={
    var times = "0"
    val compareToTuple: (Report, Report) = New7gUserRelationService.compareTo(tuple)
    val userMacAndUserMac: String = compareToTuple._1.userMac + New7gUserRelationConf.prefix_xiahuaxian + compareToTuple._2.userMac
    val table: mutable.Map[String, String] = New7gUserRelationHbaseDao.readTable(New7gUserRelationConf.g7_user_relate_info_detail,userMacAndUserMac)
    if(!table.isEmpty && table.remove(New7gUserRelationConf.rowKey).size > 0){ //有关联的数据
      times = table.get(New7gUserRelationConf.col_name) match {
        case Some(data) =>{
           GsonTools.gson.fromJson(data,classOf[G7UserRelateInfoDetail]).times.toString
        }
        case None => "0"
      }
    }
    times
  }

  /**
    * 创建重点人发送实时推送消息
    * @param tuple
    * @return
    */
  def createG7UserRelateInfoToKafka(tuple: (Report, Report)): G7UserRelateInfoToKafka ={

    val raletionMacInfos = new ArrayList[RaletionMacInfo]()
    raletionMacInfos.add(RaletionMacInfo(tuple._2.userMac,New7gUserRelationService.queryCorePersonRalTimes(tuple).toInt))

    val info: (Long, Long) = New7gUserRelationService.queryCorePersonInfo(tuple._1)
    G7UserRelateInfoToKafka(tuple._1.userMac,
      raletionMacInfos,
      tuple._1.pointX.toFloat,
      tuple._1.pointY.toFloat,
      tuple._1.areaNo.toInt,
      info._1.toInt,
      info._2.toInt)
  }

  /**
    * 保存重点人关系推送日志
    * @param dStream
    */
  def insertG7UserRelateInfoToKafkaLog(dStream: DStream[MessageKafka[G7UserRelateInfoToKafka]]): Unit ={
    dStream.foreachRDD(rdd => {
      rdd.foreach(data => {
        New7gUserRelationHbaseDao.writeTable(New7gUserRelationConf.g7_user_relate_info_log_table,
          DateUtils.getCurTime("yyyyMMddHHmm") + New7gUserRelationConf.prefix_xiahuaxian + CommonConf.WARN_AREA_CORE_NUM + New7gUserRelationConf.prefix_xiahuaxian + data.value.mac,
          New7gUserRelationConf.col_name,
          GsonTools.gson.toJson(data))
      })
    })
  }
  /**
    * 将关联的人按区与和重点人分组
    * @param tuple
    */
  def relationCorePersonAndAreaGroup(tuple: (Report, Report)): ListBuffer[(String, G7UserRelateInfoToKafka)] ={

    val listBufferRelation: ListBuffer[(String, G7UserRelateInfoToKafka)] = mutable.ListBuffer[(String,G7UserRelateInfoToKafka)]()

    (New7gUserRelationService.isCorePerson(tuple._1),New7gUserRelationService.isCorePerson(tuple._2)) match {
      case (true,true) => {
        val createG7UserRelateInfoToKafka1: G7UserRelateInfoToKafka = createG7UserRelateInfoToKafka(tuple)
        listBufferRelation += ((tuple._1.areaNo + New7gUserRelationConf.prefix_xiahuaxian + tuple._1.userMac,createG7UserRelateInfoToKafka1))

        val createG7UserRelateInfoToKafka2: G7UserRelateInfoToKafka = createG7UserRelateInfoToKafka((tuple._2,tuple._1))
        listBufferRelation += ((tuple._2.areaNo + New7gUserRelationConf.prefix_xiahuaxian + tuple._2.userMac,createG7UserRelateInfoToKafka2))

      }
      case (true,false) => {
        /*val createG7UserRelateInfoToKafka1: G7UserRelateInfoToKafka = createG7UserRelateInfoToKafka(tuple)
        listBufferRelation += ((tuple._1.areaNo + New7gUserRelationConf.prefix_xiahuaxian + tuple._1.userMac,createG7UserRelateInfoToKafka1))*/
      }
      case (false,true) => {
        /*val createG7UserRelateInfoToKafka2: G7UserRelateInfoToKafka = createG7UserRelateInfoToKafka((tuple._2,tuple._1))
        listBufferRelation += ((tuple._2.areaNo + New7gUserRelationConf.prefix_xiahuaxian + tuple._2.userMac,createG7UserRelateInfoToKafka2))*/

      }
      case (false,false) => {}
    }

    listBufferRelation
  }

  /**
    * 格式化数据源的数据
    * @param report
    */
  def parseSrc(report: Report): Report ={
    val areaList = CompRedisClientUtils.getRedisAreaMap().filter(x => {
       ComUtils.isContainInArea(report,x) != -1
    }).take(1)
    if (!areaList.isEmpty) {
      report.areaNo = areaList(0).id + ""
    } else {
      report.areaNo = "-1"
    }
    report
  }

  /**
    * 计算重点人实时推送到kafka的数据
    * @param tuple
    */
  def computerG7UserRelateInfoToKafkaData(tuple: (Report, Report)): ListBuffer[G7UserRelateInfoDetail] = {

    val details: ListBuffer[G7UserRelateInfoDetail] = ListBuffer[G7UserRelateInfoDetail]()

    val compareToTuple: (Report, Report) = New7gUserRelationService.compareTo(tuple)
    val userMacAndUserMac: String = compareToTuple._1.userMac + New7gUserRelationConf.prefix_xiahuaxian + compareToTuple._2.userMac
    val table: mutable.Map[String, String] = New7gUserRelationHbaseDao.readTable(New7gUserRelationConf.g7_user_relate_info_detail,userMacAndUserMac)

    if(!table.isEmpty && table.remove(New7gUserRelationConf.rowKey).size > 0){ //有关联的数据
      table.get(New7gUserRelationConf.col_name) match {
        case Some(data) =>{
          details += GsonTools.gson.fromJson(data,classOf[G7UserRelateInfoDetail])
        }
        case None =>
      }
    }
    details
  }

  def main(args: Array[String]): Unit = {
    val macInfo1: RaletionMacInfo = RaletionMacInfo("1",1)
    val macInfo2: RaletionMacInfo = RaletionMacInfo("2",2)
    val list1 = new ArrayList[RaletionMacInfo]()
    val list2 = new ArrayList[RaletionMacInfo]()
    list1.add(macInfo1)
    list2.add(macInfo2)

    val kafka: G7UserRelateInfoToKafka = G7UserRelateInfoToKafka("11",list1,1.0f,1.0f,1,1,1)
    val kafka1: G7UserRelateInfoToKafka = G7UserRelateInfoToKafka("22",list2,2.0f,2.0f,2,2,2)

    println(kafka)

    kafka.reMacs.addAll(kafka1.reMacs)

    println(kafka)
  }
  /**
    * 关联关系计算方法
    *
    * @param tuple
    */
  def userRelationComputer(tuple: (Report, Report)): Unit = {
    val compareToTuple: (Report, Report) = New7gUserRelationService.compareTo(tuple)
    val userMacAndUserMac: String = compareToTuple._1.userMac + New7gUserRelationConf.prefix_xiahuaxian + compareToTuple._2.userMac

    val table: mutable.Map[String, String] = New7gUserRelationHbaseDao.readTable(New7gUserRelationConf.g7_user_relate_info_detail,userMacAndUserMac)

    if(table.isEmpty){//数据不存在
      New7gUserRelationService.createUserRelateInfoDetail(compareToTuple)   //新建一条关联数据  内部表用  索引表
      New7gUserRelationService.createRelateInfo(compareToTuple)   //新建一条关联数据  web查询表  业务查询记录
    }else{
      val userInfoDetail: Option[G7UserRelateInfoDetail] = New7gUserRelationService.updateUserRelateInfoDetail(table, compareToTuple) //更新一条关联数据  内部表用  索引表
      New7gUserRelationService.updateRelateInfo(userInfoDetail,compareToTuple) //更新一条关联数据  web查询表  业务查询记录
    }
  }

  /**
    * 更新web查询表
    * @param userInfoDetail
    */
  def updateRelateInfo(userInfoDetail: Option[G7UserRelateInfoDetail],tuple: (Report, Report)): Unit = {
    userInfoDetail match {
      case Some(infoDetail) => {
        val l: Long = infoDetail.times.toLong - 1
        New7gUserRelationHbaseDao.detCol(New7gUserRelationConf.g7_user_relate_info,tuple._1.userMac,l.toString + New7gUserRelationConf.prefix_xiahuaxian + tuple._2)
        New7gUserRelationHbaseDao.detCol(New7gUserRelationConf.g7_user_relate_info,tuple._2.userMac,l.toString + New7gUserRelationConf.prefix_xiahuaxian + tuple._1)

        New7gUserRelationHbaseDao.writeTable(New7gUserRelationConf.g7_user_relate_info,tuple._1.userMac,infoDetail.times + New7gUserRelationConf.prefix_xiahuaxian + tuple._2.userMac,
          GsonTools.gson.toJson(G7UserRelateInfo(infoDetail.times,infoDetail.lastUpdate,infoDetail.firstTime,tuple._2.userMac)))

        New7gUserRelationHbaseDao.writeTable(New7gUserRelationConf.g7_user_relate_info,tuple._2.userMac,infoDetail.times + New7gUserRelationConf.prefix_xiahuaxian + tuple._1.userMac,
          GsonTools.gson.toJson(G7UserRelateInfo(infoDetail.times,infoDetail.lastUpdate,infoDetail.firstTime,tuple._1.userMac)))
      }
      case None => {
//        println("是同一天更新过的关联数据  不用更新数据")
      }
    }
  }

  /**
    * 更新一条详细记录   A-B    times   索引表
    * @param table    需要更新的数据
    */
  def updateUserRelateInfoDetail(table: mutable.Map[String, String],tuple: (Report, Report)): Option[G7UserRelateInfoDetail] = {
    table.get(New7gUserRelationConf.col_name) match {
      case Some(data) => {   //map中有该列数据
        val json: G7UserRelateInfoDetail = GsonTools.gson.fromJson(data,classOf[G7UserRelateInfoDetail])

        if(!DateUtils.isSomneDay(json.lastUpdate,tuple._1.serverTimeStamp)){ //判断是不是相同的一天
          val l: Long = json.times.toLong + 1
          val infoDetail: G7UserRelateInfoDetail = G7UserRelateInfoDetail(l.toString,tuple._1.serverTimeStamp,json.firstTime)

          New7gUserRelationHbaseDao.writeTable(New7gUserRelationConf.g7_user_relate_info_detail,
            tuple._1.userMac + New7gUserRelationConf.prefix_xiahuaxian + tuple._2.userMac,
            New7gUserRelationConf.col_name,
            GsonTools.gson.toJson(infoDetail))

          Some(infoDetail)
        }else{//是同一天   不用更新
          None
        }

      }
      case None => {//map中m没有该列数据
        New7gUserRelationService.createUserRelateInfoDetail(tuple)
        Some(G7UserRelateInfoDetail("1",tuple._1.serverTimeStamp,tuple._1.serverTimeStamp))
      }
    }
  }



  /**
    * 创建一条关联数据   -----  查询表
    * @param tuple
    */
  def createRelateInfo(tuple: (Report, Report)): Unit ={
    New7gUserRelationHbaseDao.writeTable(New7gUserRelationConf.g7_user_relate_info,
      tuple._1.userMac,"1" + New7gUserRelationConf.prefix_xiahuaxian + tuple._2.userMac,
      GsonTools.gson.toJson(G7UserRelateInfo("1",tuple._1.serverTimeStamp,tuple._1.serverTimeStamp,tuple._2.userMac))) //A关联B

    New7gUserRelationHbaseDao.writeTable(New7gUserRelationConf.g7_user_relate_info,
      tuple._2.userMac,"1" + New7gUserRelationConf.prefix_xiahuaxian + tuple._1.userMac,
      GsonTools.gson.toJson(G7UserRelateInfo("1",tuple._2.serverTimeStamp,tuple._2.serverTimeStamp,tuple._1.userMac))) //B关联A
  }
  /**
    * 新建一条详细记录   A-B    times   索引表
    * @param tuple
    */
  def createUserRelateInfoDetail(tuple: (Report, Report)): Unit ={
    val g7UserRelateInfoDetail: G7UserRelateInfoDetail = G7UserRelateInfoDetail("1",tuple._1.serverTimeStamp,tuple._2.serverTimeStamp)
    New7gUserRelationHbaseDao.writeTable(New7gUserRelationConf.g7_user_relate_info_detail,
      tuple._1.userMac + New7gUserRelationConf.prefix_xiahuaxian + tuple._2.userMac
      ,New7gUserRelationConf.col_name,
      GsonTools.gson.toJson(g7UserRelateInfoDetail))
  }

  /**
    * 元素的比较方法
    * @param tuple
    * @return
    */
  def compareTo(tuple: (Report, Report)): (Report, Report) ={
    tuple._1.userMac.compare(tuple._2.userMac) < 0 match {
      case true => (tuple._1,tuple._2)
      case _ => (tuple._2,tuple._1)
    }
  }


  /**
    * 两个元素结合的组合算法
    * @param array
    * @return
    */
  def combination2Element[T](array: Array[T]): ListBuffer[(T, T)] ={
    val tuples: ListBuffer[(T, T)] = ListBuffer[(T, T)]()
    if ( array.length < 2){
      return tuples
    }
    for(i <- 0 until (array.length - 1) ){
      for(j <- 0 until array.length){
        if(i < j){
          val tuple: (T, T) = (array(i),array(j))
          tuples += tuple
        }
      }
    }
    tuples
  }

  /**
    * 业务初始化方法
    */
  def init(): Unit ={
    New7gUserRelationHbaseDao.createTable(New7gUserRelationConf.g7_user_relate_info)
    New7gUserRelationHbaseDao.createTable(New7gUserRelationConf.g7_user_relate_info_detail)
//    New7gUserRelationHbaseDao.createTableInTTL(New7gUserRelationConf.g7_user_relate_info_ten_five_data,New7gUserRelationConf.hbaseTableColTime)
  }




}
