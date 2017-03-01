package com.qjzh.abd.components.in_out_move.dao

import java.util
import java.util.ArrayList

import com.qjzh.abd.components.comp_common.common.HbaseBusUtils
import com.qjzh.abd.components.comp_common.utils.GsonTools
import com.qjzh.abd.components.in_out_move.caseview.Days
import com.qjzh.abd.function.hbase.utils.HbaseUtils
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap

import scala.collection.mutable

/**
  * Created by 92025 on 2016/12/26.
  */
object HbaseDao {
  val HbaseSession = HbaseBusUtils

  val HbaseSessionUtils = HbaseUtils


  def createTable(tableName:String) : Unit = {
    HbaseSessionUtils.createTable(tableName)
  }

  def setMapByRowKey(tableName: String,rowKey: String,colAndValue: mutable.HashMap[String,String]): Unit ={
    colAndValue.foreach(xx => {
      HbaseUtils.writeTable(tableName,rowKey,xx._1,xx._2)
    })
  }

  def setRowKeyAndValue(tableName: String,rowKey: String,colName: String,value: String): Unit ={
    HbaseUtils.writeTable(tableName,rowKey,colName,value)
  }

  def updateHbaseList(tableName: String,rowKey: String,colName: String,day: String): Unit ={
    val table: Object2ObjectOpenHashMap[String, String] = HbaseSessionUtils.readTable(tableName,rowKey)
    val value: String = table.get(colName)
    val json: util.ArrayList[String] = GsonTools.gson.fromJson(value,classOf[util.ArrayList[String]])
    json.add(day)
    HbaseSessionUtils.writeTable(tableName,rowKey,colName,GsonTools.gson.toJson(json))
  }

  def insertHbaseList(tableName: String,rowKey: String,colName: String,value: ArrayList[String]): Unit ={
    HbaseSessionUtils.writeTable(tableName,rowKey,colName,GsonTools.gson.toJson(Days(value)))
  }

  def insertHbase(tableName: String,rowKey: String,colName: String,value: String): Unit ={
    HbaseSessionUtils.writeTable(tableName,rowKey,colName,value)
  }

  def detCol(tableName: String,rowKey: String,colName: String): Unit ={
    HbaseSessionUtils.deleByRowCol(tableName,rowKey,colName)
  }

  def readTable(tableName: String, rowKey: String): Object2ObjectOpenHashMap[String, String] = {
    HbaseSessionUtils.readTable(tableName, rowKey)
  }
}
