package com.qjzh.abd.components.user_relation.dao

import com.qjzh.abd.components.comp_common.common.HbaseBusUtils
import com.qjzh.abd.components.in_out_move.utils.IncrTableUtils
import com.qjzh.abd.control.common.view.Report
import com.qjzh.abd.function.hbase.utils.HbaseUtils
import org.apache.commons.lang3.StringUtils

import scala.collection.{JavaConverters, mutable}
import scala.util.Random
/**
  * Created by 92025 on 2017/1/10.
  */
object New7gUserRelationHbaseDao {
  val HbaseSession = HbaseBusUtils

  val HbaseSessionUtils = HbaseUtils

  /**
    * 创建一张表
    * @param tableName
    */
  def createTable(tableName: String): Unit = {
    HbaseSessionUtils.createTable(tableName)
  }

  /**
    * 创建一张列上有TTL的表
    * @param tableName
    * @param ttl
    */
  def createTableInTTL(tableName:String, ttl:Int): Unit = {
    HbaseSessionUtils.createTableInTTL(tableName, ttl)
  }
  /**
    * 查询rowKey下的数据
    * @param tableName
    * @param rowKey
    * @return
    */
  def readTable(tableName:String, rowKey:String) : mutable.Map[String,String] = {
    JavaConverters.mapAsScalaMapConverter(HbaseSessionUtils.readTable(tableName, rowKey)).asScala
  }

  /**
    * 写入一条记录
    * @param tableName
    * @param rowKey
    * @param columnName
    * @param value
    */
  def writeTable(tableName:String, rowKey:String, columnName:String, value:String) : Unit = {
    HbaseSessionUtils.writeTable(tableName, rowKey, columnName, value)
  }

  def detCol(tableName: String,rowKey: String,colName: String): Unit ={
    HbaseSessionUtils.deleByRowCol(tableName,rowKey,colName)
  }

  /**
    * 查询重点人近期次数
    * @param report
    * @return
    */
  def queryCorePersonReTimes(report: Report): Long ={
//    new Random().nextInt(205).toLong
//    IncrTableUtils.getHalfMonthDayCount(report.userMac).toLong
    val count: String = IncrTableUtils.getHalfMonthDayCount(report.userMac)
    StringUtils.isEmpty(count) match {
      case true => 0l
      case false => count.toLong
    }
  }
  /**
    * 查询重点人单日次数
    * @param report
    * @return
    */
  def queryCorePersonSinTimes(report: Report): Long ={
//    new Random().nextInt(205).toLong
//    IncrTableUtils.getHistoryMaxCount(report.userMac).toLong
    val count: String = IncrTableUtils.getHistoryMaxCount(report.userMac)
    StringUtils.isEmpty(count) match {
      case true => 0l
      case false => count.toLong
    }
  }

  def main(args: Array[String]): Unit = {
//    val times: Long = queryCorePersonSinTimes(new Report)
//    println(times)
  }
}
