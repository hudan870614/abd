package com.qjzh.abd.function.hbase.utils

import java.math.BigInteger
import java.util

import com.qjzh.abd.function.hbase.conf.{HbaseConfigContext, HbaseConfigProperties}
import it.unimi.dsi.fastutil.objects.{Object2ObjectOpenHashMap, ObjectArrayList}
import org.apache.commons.lang.ObjectUtils.Null
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.{Result, _}
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

/** connection
  * Created by hudan
  */
class HbaseUtils private(hbaseZkHosts: String) extends Serializable {

  private val conf: Configuration = new Configuration()
  private val DEFAULT_COLUMN_FAMILIES = "data"
  conf.set("hbase.zookeeper.quorum", hbaseZkHosts)
  private val connection = ConnectionFactory.createConnection(conf)

  def getConnection(): Connection = {
    connection
  }

  def createTable(tableName: String): Unit = {
    val tb = TableName.valueOf(tableName)
    val admin = connection.getAdmin
    val tableDes = new HTableDescriptor(tb)

    if (!admin.tableExists(tb)) {
      tableDes.addFamily(new HColumnDescriptor("data")) //.setCompressionType(Algorithm.GZ))
      admin.createTable(tableDes)
    }
  }


  def readTable(tableName: String, rowKey: String): Object2ObjectOpenHashMap[String, String] = {

    val tableInterface = connection.getTable(TableName.valueOf(tableName))
    try {

      val resultList = new Object2ObjectOpenHashMap[String, String]()
      val get = new Get(Bytes.toBytes(rowKey))
      val result = tableInterface.get(get)

      val lisetCells = result.listCells()
      if (lisetCells != null) {
        for (i <- 0 until lisetCells.size()) {
          val cell = lisetCells.get(i)
          val rowKey = new String(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(), "UTF-8")
          val qualifier = new String(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength(), "UTF-8")
          val value = new String(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength(), "UTF-8")
          resultList.put(qualifier, value)
          resultList.put("rowKey", rowKey)
        }
      }

      resultList

    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        null
      }
    }
  }


  def findByRowKey(tableName: String, rowKey: String): Map[String, String] = {
    var resultMap:Map[String, String] = Map()
    val tableInterface = connection.getTable(TableName.valueOf(tableName))
    val get = new Get(Bytes.toBytes(rowKey))
    val result = tableInterface.get(get)

    val lisetCells = result.listCells()
    if (lisetCells != null) {
      for (i <- 0 until lisetCells.size()) {
        val cell = lisetCells.get(i)
//        val rowKey = new String(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(), "UTF-8")
        val qualifier = new String(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength(), "UTF-8")
        val value = new String(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength(), "UTF-8")
        resultMap += ((qualifier,value))
//        resultMap += (("rowKey",rowKey))
      }
    }

    resultMap
  }

  /**
    * Get value using specific rowkey, family name and qualifier
    *
    * @param tableName
    * @param rowKey
    * @param fName
    * @param qName
    * @return
    */
  def readTable(tableName: String, rowKey: String, fName: String, qName: String): String = {

    var r: String = ""
    val tableInterface = connection.getTable(TableName.valueOf(tableName))
    try {

      val get = new Get(Bytes.toBytes(rowKey))
      val result = tableInterface.get(get)

      val lisetCells = result.listCells()
      if (lisetCells != null) {
        for (i <- 0 until lisetCells.size()) {
          val cell = lisetCells.get(i)
          val qualifier = new String(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength(), "UTF-8")
          val family = new String(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(), "UTF-8")
          val value = new String(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength(), "UTF-8")

          if (family.equalsIgnoreCase(fName) && qualifier.equalsIgnoreCase(qName)) {
            r = value
          }
        }
      }

      r

    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        null
      }
    }
  }

  def getClumnSize(tableName: String, rowKey: String): Long = {
    val tableInterface = connection.getTable(TableName.valueOf(tableName))
    val get = new Get(Bytes.toBytes(rowKey))
    val result = tableInterface.get(get)
    result.listCells().size()
  }


  def containRowKey(tableName: String, rowKey: String): Boolean = {
    val tableInterface = connection.getTable(TableName.valueOf(tableName))
    try {
      val get = new Get(Bytes.toBytes(rowKey))
      tableInterface.exists(get)
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        false
      }
    }
  }


  def searchRegexRowKey(tableName: String, likeStr: String, line: Int = 1): Object2ObjectOpenHashMap[String, Object2ObjectOpenHashMap[String, String]] = {
    val result = new Object2ObjectOpenHashMap[String, Object2ObjectOpenHashMap[String, String]]()
    try {
      val tableInterface = connection.getTable(TableName.valueOf(tableName))
      val scan = new Scan()
      val regexFilter: Filter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator(likeStr))
      val limitFilter: Filter = new PageFilter(line)

      val filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL)
      filterList.addFilter(regexFilter)
      filterList.addFilter(limitFilter)

      scan.setFilter(filterList)

      val resultsScanner = tableInterface.getScanner(scan)
      if (resultsScanner != null) {
        val resultIt = resultsScanner.iterator()
        while (resultIt.hasNext) {
          val resultList = new Object2ObjectOpenHashMap[String, String]()
          val lisetCells = resultIt.next().listCells()
          for (i <- 0 until lisetCells.size()) {
            val cell = lisetCells.get(i)
            val rowKey = new String(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(), "UTF-8")
            val qualifier = new String(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength(), "UTF-8")
            val value = new String(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength(), "UTF-8")
            resultList.put(qualifier, value)
            resultList.put("rowKey", rowKey)
          }
          result.put(resultList.get("rowKey"), resultList)
        }
      }

      result
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        null
      }
    }
  }


  def readTable(tableName: String, keyColumns: Object2ObjectOpenHashMap[String, ObjectArrayList[String]]): Object2ObjectOpenHashMap[String, Object2ObjectOpenHashMap[String, String]] = {
    val readResult = new Object2ObjectOpenHashMap[String, Object2ObjectOpenHashMap[String, String]]()
    try {
      val tableInterface = connection.getTable(TableName.valueOf(tableName))
      val iter = keyColumns.object2ObjectEntrySet().fastIterator()
      val gets = new util.ArrayList[Get]()
      while (iter.hasNext) {
        val keyColumn = iter.next()
        val rowkey = keyColumn.getKey
        val columns = keyColumn.getValue
        val get = new Get(Bytes.toBytes(rowkey));
        for (i <- 0 until columns.size()) {
          val column = columns.get(i)
          get.addColumn(Bytes.toBytes(DEFAULT_COLUMN_FAMILIES), Bytes.toBytes(column))
        }
        gets.add(get)
      }
      val results = tableInterface.get(gets)
      for (i <- 0 until (results.size)) {
        val result = results(i)
        val rowkey = Bytes.toString(result.getRow)
        val cells = result.listCells()
        if (cells != null && !cells.isEmpty) {
          var columnValue = readResult.get(rowkey);
          if (columnValue == null) {
            columnValue = new Object2ObjectOpenHashMap[String, String]()
            readResult.put(rowkey, columnValue)
          }
          for (j <- 0 until (cells.size())) {
            val cell = cells.get(j)
            val column = Bytes.toString(cell.getQualifierArray, cell.getQualifierOffset, cell.getQualifierLength)
            val value = Bytes.toString(cell.getValueArray, cell.getValueOffset, cell.getValueLength)
            columnValue.put(column, value)
          }
        }
      }
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    }
    readResult
  }

  def writeTable(tableName: String, rowKey: String, columnName: String, value: String): Unit = {
    val tableInterface = connection.getTable(TableName.valueOf(tableName))
    val put = new Put(Bytes.toBytes(rowKey))
    put.addColumn(Bytes.toBytes(DEFAULT_COLUMN_FAMILIES), Bytes.toBytes(columnName), Bytes.toBytes(value))
    tableInterface.put(put)
  }

  def writeTable(tableName: String, rowKey: String, familyName : String, columnName: String, value: String): Unit = {
    val tableInterface = connection.getTable(TableName.valueOf(tableName))
    val put = new Put(Bytes.toBytes(rowKey))
    put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName), Bytes.toBytes(value))
    tableInterface.put(put)
  }

  def dealByRowKey(tableName: String, rowKey: String): Unit = {
    try {
      val tableInterface = connection.getTable(TableName.valueOf(tableName))
      val d1 = new Delete(rowKey.getBytes())
      tableInterface.delete(d1)
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    }
  }

  def writeTable(tableName: String, keyValues: Object2ObjectOpenHashMap[String, Object2ObjectOpenHashMap[String, String]]): Unit = {
    val tableInterface = connection.getTable(TableName.valueOf(tableName))
    val iterator = keyValues.object2ObjectEntrySet().fastIterator()
    val puts = new util.ArrayList[Put]()
    while (iterator.hasNext) {
      val keyValue = iterator.next()
      val rowKey = keyValue.getKey
      val columnValues = keyValue.getValue
      val iteraotrColumn = columnValues.object2ObjectEntrySet().fastIterator()
      val put = new Put(Bytes.toBytes(rowKey))
      while (iteraotrColumn.hasNext) {
        val columnValue = iteraotrColumn.next()
        val columnName = columnValue.getKey
        val value = columnValue.getValue
        put.addColumn(Bytes.toBytes(DEFAULT_COLUMN_FAMILIES), Bytes.toBytes(columnName), Bytes.toBytes(value))

      }
      puts.add(put)

    }
    tableInterface.put(puts)
  }


  def writeTableBatch(tableName: String, keyValues: Object2ObjectOpenHashMap[String, ListBuffer[Object2ObjectOpenHashMap[String, String]]]): Unit = {
    val tableInterface = connection.getTable(TableName.valueOf(tableName))
    val iterator = keyValues.object2ObjectEntrySet().fastIterator()
    val puts = new util.ArrayList[Put]()
    while (iterator.hasNext) {
      val keyValue = iterator.next()
      val rowKey = keyValue.getKey
      val listvalue: ListBuffer[Object2ObjectOpenHashMap[String, String]] = keyValue.getValue
      listvalue.foreach(data => {
        val iteraotrColumn = data.object2ObjectEntrySet().fastIterator()
        val put = new Put(Bytes.toBytes(rowKey))
        while (iteraotrColumn.hasNext) {
          val columnValue = iteraotrColumn.next()
          val columnName = columnValue.getKey
          //Syslog.info("colname:"+columnName)
          val value = columnValue.getValue
          put.addColumn(Bytes.toBytes(DEFAULT_COLUMN_FAMILIES), Bytes.toBytes(columnName), Bytes.toBytes(value))
        }
        puts.add(put)
      })
    }
    tableInterface.put(puts)
  }

  def writeToIncrTable(record: List[(String, String, String, String, String)]) = {
    val tableInterface = connection.getTable(TableName.valueOf(record(0)._1))
    val puts = new util.ArrayList[Put]()
    for (elem <- record) {
      val rowKey = elem._2
      val fName = elem._3
      val qualifier = elem._4
      val value = elem._5
      val put = new Put(Bytes.toBytes(rowKey))
      put.addColumn(Bytes.toBytes(fName), Bytes.toBytes(qualifier), Bytes.toBytes(value))
      puts.add(put)
    }
    tableInterface.put(puts)
  }

  def writeToIncrTable(record: (String, String, String, String, String)) = {
    val tableInterface = connection.getTable(TableName.valueOf(record._1))
    val puts = new util.ArrayList[Put]()
    val rowKey = record._2
    val fName = record._3
    val qualifier = record._4
    val value = record._5
    val put = new Put(Bytes.toBytes(rowKey))
    put.addColumn(Bytes.toBytes(fName), Bytes.toBytes(qualifier), Bytes.toBytes(value))
    puts.add(put)
    tableInterface.put(put)
  }


  def createTableInTTL(tableName: String, ttl: Int): Unit = {
    val tb = TableName.valueOf(tableName)
    val admin = connection.getAdmin
    val table = new HTableDescriptor(TableName.valueOf(tableName))

    if (!admin.tableExists(tb)) {
      val column = new HColumnDescriptor("data")
      column.setTimeToLive(ttl)
      table.addFamily(column)
      admin.createTable(table)
    }

  }

  def delTableRow(tableName: String): Unit = {
    val tb = TableName.valueOf(tableName)
    val admin = connection.getAdmin
    try {
      HbaseUtils.createTable(tableName)
      admin.disableTable(tb)
      admin.deleteTable(tb)
      HbaseUtils.createTable(tableName)
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    }
  }

  def truncateTable(tableName: String): Unit = {
    val tb = TableName.valueOf(tableName)
    val admin = connection.getAdmin
    try {
      admin.truncateTable(tb, false)
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    }
  }

  def increTable(tableName: String, increment: Increment): Unit = {
    val tableInterface = connection.getTable(TableName.valueOf(tableName))
    tableInterface.increment(increment)
  }

  /**
    * Performe increment on incr_table with specific rowKey.
    * Family and qualifier are fixed
    *
    * @param rowKey
    * @param value
    */
  def incrTable(rowKey: String, value: Long): Unit = {
    incrToTable(rowKey, value)
  }

  /**
    * Increment the incr_table with specific rowKey, family name and qualifier
    *
    * @param rowKey
    * @param familyName
    * @param qualifierName
    * @param value
    */
  def incrToTableincrTable(tableName: String, rowKey: String, familyName: String, qualifierName: String, value: Long): Unit = {
    val tableInterface = connection.getTable(TableName.valueOf(tableName))

    tableInterface.incrementColumnValue(Bytes.toBytes(rowKey), Bytes.toBytes(familyName), Bytes.toBytes(qualifierName), value)

    tableInterface.close()
  }

  /**
    * Increment the incr_table with specific rowKey, family name and qualifier
    *
    * @param rowKey
    * @param value
    */
  def incrToTable(rowKey: String, value: Long): Unit = {
    val tableInterface = connection.getTable(TableName.valueOf(HbaseConfigProperties.incrTableName))

    tableInterface.incrementColumnValue(Bytes.toBytes(rowKey), Bytes.toBytes(HbaseConfigProperties.defaultFamily)
      , Bytes.toBytes(HbaseConfigProperties.defaultQualifier), value)

    tableInterface.close()
  }

  def createIncrTable(): Unit = {
    val tb = TableName.valueOf(HbaseConfigProperties.incrTableName)
    val admin = connection.getAdmin
    val tableDes = new HTableDescriptor(tb)

    if (!admin.tableExists(tb)) {
      tableDes.addFamily(new HColumnDescriptor(HbaseConfigProperties.defaultFamily))
      admin.createTable(tableDes)
    }
  }

  def readFromIncrTable(tableName: String, rowKey: String): String = {
    readFromIncrTable(tableName, rowKey, HbaseConfigProperties.defaultFamily, HbaseConfigProperties.defaultQualifier)
  }

  /**
    * Get the value from incr_table
    *
    * @param rowKey
    * @param familyName
    * @param qualifierName
    * @return
    */
  def readFromIncrTable(tableName: String, rowKey: String, familyName: String, qualifierName: String): String = {
    var r: String = ""
    val tableInterface = connection.getTable(TableName.valueOf(tableName))
    try {
      val get = new Get(Bytes.toBytes(rowKey))
      val result = tableInterface.get(get)
      val value = result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(qualifierName))
      if(value != null){
        r = new BigInteger(value).longValue().toString
      }else{
        r="0"
      }

    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    }
    tableInterface.close()

    r
  }

  def deleByRowCol(tableName:String, rowKey:String,col: String): Unit = {
    val tableInterface = connection.getTable(TableName.valueOf(tableName))
    val deal = new Delete(rowKey.getBytes())
    val columns: Delete = deal.deleteColumns(Bytes.toBytes("data"),Bytes.toBytes(col))
    tableInterface.delete(columns)
  }

  def deleByRowCol(tableName:String, rowKey:String,fName : String,col: String): Unit = {
    val tableInterface = connection.getTable(TableName.valueOf(tableName))
    val deal = new Delete(rowKey.getBytes())
    val columns: Delete = deal.deleteColumns(Bytes.toBytes(fName),Bytes.toBytes(col))
    tableInterface.delete(columns)
  }

  /**
    * Count the number of records in a family column
    * @param tableName
    * @param userMac
    * @param fName
    * @return
    */
  def countColumnOfFamily(tableName : String, userMac : String, fName : String) : Int ={
    var r : Int = 0
    val tableInterface = connection.getTable(TableName.valueOf(tableName))
    try {
      val get = new Get(Bytes.toBytes(userMac))
      val result = tableInterface.get(get)
      val qvMap = result.getFamilyMap(Bytes.toBytes(fName))
      if(qvMap != null){
        r = qvMap.size()
      }else{
        r=0
      }
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    }
    r
  }

  def getQVMap(tableName : String, userMac : String, fName : String) : util.NavigableMap[Array[Byte], Array[Byte]] ={
    var r : util.NavigableMap[Array[Byte], Array[Byte]] =null
    val tableInterface = connection.getTable(TableName.valueOf(tableName))
    try {
      val get = new Get(Bytes.toBytes(userMac))
      val result = tableInterface.get(get)
      r = result.getFamilyMap(Bytes.toBytes(fName))
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    }
    r
  }

}


object HbaseUtils {


  private val hbaseUtils = new HbaseUtils(HbaseConfigContext.hbase_zookeeper_host)


  def createIncrTable(): Unit = {
    hbaseUtils.createIncrTable
  }

  /**
    * 返回一个列簇下的qualifier - value map
    * @param tableName
    * @param userMac
    * @param fName
    * @return
    */
  def getQVMap(tableName : String, userMac : String, fName : String) : util.NavigableMap[Array[Byte], Array[Byte]] ={
    hbaseUtils.getQVMap(tableName, userMac, fName)
  }

  /**
    * 返回一个列簇中有多少列
    * @param tableName
    * @param userMac
    * @param fName
    * @return
    */
  def countColumnOfFamily(tableName : String, userMac : String, fName : String) : Int ={
    hbaseUtils.countColumnOfFamily(tableName, userMac, fName)
  }

  /**
    * 批量写入 incr table
    * @param record
    */
  def writeToIncrTable(record: List[(String, String, String, String, String)]) = {
    hbaseUtils.writeToIncrTable(record)
  }
  def writeToIncrTable(record: (String, String, String, String, String)) = {
    hbaseUtils.writeToIncrTable(record)
  }

  /**
    * 批量累加方法
    * @param tableName
    * @param increment
    */
  def incrTable(tableName: String, increment: Increment): Unit = {
    hbaseUtils.increTable(tableName, increment)
  }

  /**
    * 单个累加方法
    * @param tableName
    * @param rowKey
    * @param familyName
    * @param qualifierName
    * @param value
    */
  def incrTable(tableName: String, rowKey: String, familyName: String, qualifierName: String, value: Long): Unit = {
    hbaseUtils.incrToTableincrTable(tableName, rowKey, familyName, qualifierName, value)
  }

  /**
    * 读非累加的一般值的方法
    * @param tableName
    * @param rowKey
    * @param familyName
    * @param qualifierName
    * @return
    */
  def readFromTable(tableName: String, rowKey: String, familyName: String, qualifierName: String): String = {
    hbaseUtils.readTable(tableName, rowKey, familyName, qualifierName)
  }

  /**
    * 读累加值的方法
    * @param tableName
    * @param rowKey
    * @param familyName
    * @param qualifierName
    * @return
    */
  def readFromIncrTable(tableName: String, rowKey: String, familyName: String, qualifierName: String): String = {
    hbaseUtils.readFromIncrTable(tableName, rowKey, familyName, qualifierName)
  }

  def readTable(tableName: String, rowKey: String, fName: String, qualifier: String): String = {
    hbaseUtils.readTable(tableName, rowKey, fName, qualifier)
  }

  def getBaseUtils(): HbaseUtils = {
    new HbaseUtils(HbaseConfigContext.hbase_zookeeper_host)
  }

  def createTable(tableName: String): Unit = {
    hbaseUtils.createTable(tableName)
  }

  def createTableInTTL(tableName: String, ttl: Int): Unit = {
    hbaseUtils.createTableInTTL(tableName, ttl)
  }

  def readTable(tableName: String, rowKey: String): Object2ObjectOpenHashMap[String, String] = {
    hbaseUtils.readTable(tableName, rowKey)
  }

  def findByRowKey(tableName: String, rowKey: String): Map[String, String] = {
    hbaseUtils.findByRowKey(tableName, rowKey)
  }

  def readTable(tableName: String, keyColumns: Object2ObjectOpenHashMap[String, ObjectArrayList[String]]): Object2ObjectOpenHashMap[String, Object2ObjectOpenHashMap[String, String]] = {
    hbaseUtils.readTable(tableName, keyColumns)
  }

  def writeTable(tableName: String, rowKey: String, columnName: String, value: String): Unit = {
    hbaseUtils.writeTable(tableName, rowKey, columnName, value)
  }

  def dealByRowKey(tableName: String, rowKey: String): Unit = {
    hbaseUtils.dealByRowKey(tableName, rowKey)
  }

  def writeTable(tableName: String, keyValues: Object2ObjectOpenHashMap[String, Object2ObjectOpenHashMap[String, String]]): Unit = {
    hbaseUtils.writeTable(tableName, keyValues)
  }

  def writeTableBatch(tableName: String, keyValues: Object2ObjectOpenHashMap[String, ListBuffer[Object2ObjectOpenHashMap[String, String]]]): Unit = {
    hbaseUtils.writeTableBatch(tableName, keyValues)
  }

  def getClumnSize(tableName: String, rowKey: String): Long = {
    hbaseUtils.getClumnSize(tableName, rowKey)
  }

  def delTableRow(tableName: String): Unit = {
    hbaseUtils.delTableRow(tableName)
  }

  def truncateTable(tableName: String): Unit = {
    hbaseUtils.truncateTable(tableName)
  }

  def searchRegexRowKey(tableName: String, likeStr: String, line: Int = 1) = {
    hbaseUtils.searchRegexRowKey(tableName, likeStr, line)
  }

  def containRowKey(tableName: String, rowKey: String) = {
    hbaseUtils.containRowKey(tableName, rowKey)
  }

  def getConnection(): Connection = {
    hbaseUtils.getConnection
  }

  def deleByRowCol(tableName:String, rowKey:String,col: String): Unit = {
    hbaseUtils.deleByRowCol(tableName,rowKey,col)
  }

  def deleByRowCol(tableName:String, rowKey:String,family : String, col: String): Unit = {
    hbaseUtils.deleByRowCol(tableName,rowKey,family, col)
  }

  def main(args: Array[String]): Unit = {

    //    createIncrTable()

    //    println(readFromIncrTable("fred_times"))

//    incrToTable("test_ttl", "john_times", 1)
    //    println(readFromIncrTable("john_times"))

    //    incrTable("john_times", 2)
    //    println(readFromIncrTable("john_times"))

    //    println(incrTableCheckExists("fred_times"))

    //    createTableInTTL("test_ttl", 30)
    //    writeTable("test_ttl", "fred",Math.random().toString,"1")
    //    writeTable("test_ttl", "fred",Math.random().toString,"1")
    //    writeTable("test_ttl", "fred",Math.random().toString,"1")

  }
}
