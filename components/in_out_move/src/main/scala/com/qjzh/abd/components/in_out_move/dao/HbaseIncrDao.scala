package com.qjzh.abd.components.in_out_move.dao

import java.math.BigInteger
import java.util

import com.qjzh.abd.components.comp_common.common.HbaseBusUtils
import com.qjzh.abd.components.comp_common.utils.GsonTools
import com.qjzh.abd.components.in_out_move.caseview.{UserMacPointTrajectory, UserStatics}
import com.qjzh.abd.components.in_out_move.conf.HbaseIncrTableConf
import com.qjzh.abd.components.in_out_move.service.New7gUserTraceService
import com.qjzh.abd.function.common.DateUtils
import com.qjzh.abd.function.hbase.utils.HbaseUtils
import org.apache.hadoop.hbase.client.Increment
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}

/**
  * Created by yufeiwang on 29/12/2016.
  */
object HbaseIncrDao {

  val HbaseSessionUtils = HbaseUtils


  /**
    * Update the user's statistics given a InAndOutFix case view
    *
    * @param userInfo
    */
  def updateUserStatistics(userInfo: UserMacPointTrajectory): UserStatics = {


    //-------------------------------------------更新数据前的计算-------------------------------------//

    val hasEnd = userInfo.endTime != 0

    //是否当天新出现
    val isNewApprDay: Boolean = userInfo.isFirst == 1

    var appTimeSlot: List[Int] = Nil
    var duration: Long = 0
    if (hasEnd) {
      //计算出频繁出现时间段
      appTimeSlot = getApprTimeSlot(userInfo.startTime, userInfo.endTime)

      //此条记录持续时长，单位分钟
      duration = (userInfo.endTime - userInfo.startTime) / (1000 * 60)
    }


    //最新探测时间
    //    val firstTime = DateUtils.getTime(userInfo.startTime, "yyyyMMddHHmm")
//    var time: String = ""
//    if (hasEnd) {
//      time = DateUtils.getTime(userInfo.endTime, "yyyyMMddHHmm")
//    } else {
//      time = DateUtils.getTime(System.currentTimeMillis(), "yyyyMMddHHmm")
//    }
    val firstTime = userInfo.startTime.toString
    var time: String = ""
    if (hasEnd) {
      time = userInfo.endTime.toString
    }
//    else {
//      time = System.currentTimeMillis().toString
//    }



    //计算此条是否会添加次数
    val count: Int = userInfo.isNewTime


    //计算此条时候回添加天数
    val dayCount: Int = userInfo.isFirst


    //------------------------------更新Hbase incr_table--------------------------------------------//
    //    HbaseSessionUtils.writeTable(HbaseIncrTableConf.incrTableName,userInfo.userMac,"data",GsonTools.gson.toJson(userInfo))

    updateHbaseIncrTable(userInfo.userMac, appTimeSlot, duration, firstTime, time, count, dayCount, isNewApprDay)

    getStatistics(userInfo.userMac, dayCount.toString)
  }

  /**
    * Method that actually update the details in the incr table
    *
    * @param userMac
    * @param appTimeSlot
    * @param duration
    * @param time
    * @param count
    * @param dayCount
    * @param isNewApprDay
    */
  def updateHbaseIncrTable(userMac: String, appTimeSlot: List[Int], duration: Long, firstTime: String, time: String, count: Int, dayCount: Int, isNewApprDay: Boolean) = {

    val existsFlag = HbaseSessionUtils.containRowKey(HbaseIncrTableConf.incrTableName, userMac)

    if (!existsFlag) {
      initUserIncrTable(userMac, firstTime)
    }

    /** ----------------------------------------更新 incr column -------------------------------------------------------------- */
    val incr: Increment = new Increment(Bytes.toBytes(userMac))

    //频繁出现时间段累加
    if (!appTimeSlot.isEmpty) {
      appTimeSlot.foreach(slot => {
        incr.addColumn(Bytes.toBytes(HbaseIncrTableConf.time_slot_fName), Bytes.toBytes(HbaseIncrTableConf.slot_prefix + slot.toString), 1)
      })
    }


    //累计出现时间
    incr.addColumn(Bytes.toBytes(HbaseIncrTableConf.min_fName), Bytes.toBytes(HbaseIncrTableConf.min_qualifier), duration)

    //累计出现次数累加
    incr.addColumn(Bytes.toBytes(HbaseIncrTableConf.appear_fName), Bytes.toBytes(HbaseIncrTableConf.appear_count_qualifer), count)
    incr.addColumn(Bytes.toBytes(HbaseIncrTableConf.appear_fName), Bytes.toBytes(HbaseIncrTableConf.appear_dayCount_qualifier), dayCount)

    //当天出现次数累计
    if (count == 1) {
      if (!isNewApprDay) {
        incr.addColumn(Bytes.toBytes(HbaseIncrTableConf.max_fName), Bytes.toBytes(HbaseIncrTableConf.max_cur_qualifier), count)
      } else {
        HbaseSessionUtils.deleByRowCol(HbaseIncrTableConf.incrTableName, userMac, HbaseIncrTableConf.max_fName, HbaseIncrTableConf.max_cur_qualifier)

        incr.addColumn(Bytes.toBytes(HbaseIncrTableConf.max_fName), Bytes.toBytes(HbaseIncrTableConf.max_cur_qualifier), count)
      }
    }

    HbaseSessionUtils.incrTable(HbaseIncrTableConf.incrTableName, incr)


    /** ------------------------------------------更新 str column ------------------------------------------------ */
    var record: List[(String, String, String, String, String)] = Nil

    //更新最新出现时间
    if(!time.equalsIgnoreCase("")){
      record = record :+ (HbaseIncrTableConf.incrTableName, userMac, HbaseIncrTableConf.time_stamp_fName, HbaseIncrTableConf.ts_last_qualifier, time)
    }

    //添加90天ttl column
    if (count == 1) {
      //如果是新记录 添加
      record = record :+ (HbaseIncrTableConf.incrTableName, userMac, HbaseIncrTableConf.week_count_fName, Math.random().toString, count.toString)
    }
    if (dayCount == 1) {
      //如果是新天记录 添加
      record = record :+ (HbaseIncrTableConf.incrTableName, userMac, HbaseIncrTableConf.week_dayCount_fName, Math.random().toString, count.toString)
      record = record :+ (HbaseIncrTableConf.incrTableName, userMac, HbaseIncrTableConf.fifteen_dayCount_fName, Math.random().toString, count.toString)
    }


    //如果当前当天次数大于以往，替换之
    if (count == 1) {
      val preTemp = HbaseSessionUtils.readFromTable(HbaseIncrTableConf.incrTableName, userMac, HbaseIncrTableConf.max_fName,
        HbaseIncrTableConf.max_previous_qualifier).toInt
      val curTemp = HbaseSessionUtils.readFromIncrTable(HbaseIncrTableConf.incrTableName, userMac, HbaseIncrTableConf.max_fName,
        HbaseIncrTableConf.max_cur_qualifier)
      val curMax = new BigInteger(curTemp).longValue().toInt
      if (preTemp < curMax) {
        record = record :+ (HbaseIncrTableConf.incrTableName, userMac, HbaseIncrTableConf.max_fName, HbaseIncrTableConf.max_previous_qualifier, curTemp.toString)
      }
    }


    if(record.length > 0){
      HbaseSessionUtils.writeToIncrTable(record)
    }


  }


  /**
    * 初始化一个user
    *
    * @param userMac
    */
  def initUserIncrTable(userMac: String, time: String): Unit = {
    /** ------------------初始化column values------------------------------ **/

    val incr: Increment = new Increment(Bytes.toBytes(userMac))

    incr.addColumn(Bytes.toBytes(HbaseIncrTableConf.appear_fName), Bytes.toBytes(HbaseIncrTableConf.appear_count_qualifer), 0)
    incr.addColumn(Bytes.toBytes(HbaseIncrTableConf.appear_fName), Bytes.toBytes(HbaseIncrTableConf.appear_dayCount_qualifier), 0)
    incr.addColumn(Bytes.toBytes(HbaseIncrTableConf.max_fName), Bytes.toBytes(HbaseIncrTableConf.max_cur_qualifier), 0)
    incr.addColumn(Bytes.toBytes(HbaseIncrTableConf.min_fName), Bytes.toBytes(HbaseIncrTableConf.min_qualifier), 0)
    for (i <- 1 to 24) {
      incr.addColumn(Bytes.toBytes(HbaseIncrTableConf.time_slot_fName), Bytes.toBytes(HbaseIncrTableConf.slot_prefix + i.toString), 0)
    }
    HbaseSessionUtils.incrTable(HbaseIncrTableConf.incrTableName, incr)

    var record: List[(String, String, String, String, String)] = Nil
    record = record :+ (HbaseIncrTableConf.incrTableName, userMac, HbaseIncrTableConf.time_stamp_fName, HbaseIncrTableConf.ts_first_qualifier, time)
    record = record :+ (HbaseIncrTableConf.incrTableName, userMac, HbaseIncrTableConf.time_stamp_fName, HbaseIncrTableConf.ts_last_qualifier, time)
    record = record :+ (HbaseIncrTableConf.incrTableName, userMac, HbaseIncrTableConf.max_fName, HbaseIncrTableConf.max_previous_qualifier, "0")

    HbaseSessionUtils.writeToIncrTable(record)

  }

  /**
    * 创建累加表
    */
  def createIncrTable(): Unit = {
    val tb = TableName.valueOf(HbaseIncrTableConf.incrTableName)
    val admin = HbaseSessionUtils.getConnection().getAdmin
    val tableDes = new HTableDescriptor(tb)

    if (!admin.tableExists(tb)) {
      val count_ttl = new HColumnDescriptor(HbaseIncrTableConf.week_count_fName)
      val dayCount_ttl = new HColumnDescriptor(HbaseIncrTableConf.week_dayCount_fName)
      val appear = new HColumnDescriptor(HbaseIncrTableConf.appear_fName)
      val time_stamp = new HColumnDescriptor(HbaseIncrTableConf.time_stamp_fName)
      val max = new HColumnDescriptor(HbaseIncrTableConf.max_fName)
      val time_slot = new HColumnDescriptor(HbaseIncrTableConf.time_slot_fName)
      val min_total = new HColumnDescriptor(HbaseIncrTableConf.min_fName)
      val ffteenDayCount_ttl = new HColumnDescriptor(HbaseIncrTableConf.fifteen_dayCount_fName)
      //      val data = new HColumnDescriptor("data")

      count_ttl.setTimeToLive(HbaseIncrTableConf.weekRecordTTL)
      dayCount_ttl.setTimeToLive(HbaseIncrTableConf.weekRecordTTL)
      ffteenDayCount_ttl.setTimeToLive(HbaseIncrTableConf.ffteenTTL)

      tableDes.addFamily(count_ttl)
      tableDes.addFamily(dayCount_ttl)
      tableDes.addFamily(appear)
      tableDes.addFamily(time_stamp)
      tableDes.addFamily(max)
      tableDes.addFamily(time_slot)
      tableDes.addFamily(min_total)
      tableDes.addFamily(ffteenDayCount_ttl)
      //      tableDes.addFamily(data)

      admin.createTable(tableDes)
    }
  }


  /**
    * Method that returns all the user statistics
    *
    * @param userMac
    * @return
    */
  def getStatistics(userMac: String, isFirst: String): UserStatics = {
    //90天次数
    val countWeekTotal = HbaseSessionUtils.countColumnOfFamily(HbaseIncrTableConf.incrTableName, userMac, HbaseIncrTableConf.week_count_fName)

    //90天天数
    val dayCountWeekTotal = HbaseSessionUtils.countColumnOfFamily(HbaseIncrTableConf.incrTableName, userMac, HbaseIncrTableConf.week_dayCount_fName)

    //总时长
    val minuteTotal = HbaseSessionUtils.readFromIncrTable(HbaseIncrTableConf.incrTableName, userMac, HbaseIncrTableConf.min_fName, HbaseIncrTableConf.min_qualifier)

    //日均出现次数
    val avgDay: String = formatDigits(countWeekTotal.toFloat / dayCountWeekTotal.toFloat)

    //周平均次数
    val countAvgByWeek: String = formatDigits(countWeekTotal.toFloat / 14.toFloat)

    //周平均天数
    val dayCountAvgByWeek: String = formatDigits(dayCountWeekTotal.toFloat / 14.toFloat)

    //单日最大次数
    val maxByDay = HbaseSessionUtils.readTable(HbaseIncrTableConf.incrTableName, userMac, HbaseIncrTableConf.max_fName, HbaseIncrTableConf.max_previous_qualifier)

    //首次出现
    val firstApprDate = HbaseSessionUtils.readTable(HbaseIncrTableConf.incrTableName, userMac, HbaseIncrTableConf.time_stamp_fName, HbaseIncrTableConf.ts_first_qualifier)

    //最近出现
    val lastApprDate = HbaseSessionUtils.readTable(HbaseIncrTableConf.incrTableName, userMac, HbaseIncrTableConf.time_stamp_fName, HbaseIncrTableConf.ts_last_qualifier)

    //时间段
    val qvMap = HbaseSessionUtils.getQVMap(HbaseIncrTableConf.incrTableName, userMac, HbaseIncrTableConf.time_slot_fName)
    var mostFreqTSlot: String = ""
    if (qvMap != null) {
      mostFreqTSlot = getMostFreqTSlot(qvMap)
    }


    //总次数
    val countTotal = HbaseSessionUtils.readFromIncrTable(HbaseIncrTableConf.incrTableName, userMac, HbaseIncrTableConf.appear_fName, HbaseIncrTableConf.appear_count_qualifer)
    //总天数
    val daycountTotal = HbaseSessionUtils.readFromIncrTable(HbaseIncrTableConf.incrTableName, userMac, HbaseIncrTableConf.appear_fName, HbaseIncrTableConf.appear_dayCount_qualifier)


    UserStatics(userMac, countTotal, daycountTotal, mostFreqTSlot, minuteTotal, lastApprDate, firstApprDate, countAvgByWeek, dayCountAvgByWeek, maxByDay, avgDay, isFirst)
  }

  /**
    * Find out the most frequent appearance time slot and format it.
    *
    * @param map
    * @return
    */
  private def getMostFreqTSlot(map: util.NavigableMap[Array[Byte], Array[Byte]]): String = {
    var tTmp = ""
    var cTmp = 0
    val it = map.entrySet().iterator()
    while (it.hasNext()) {
      val tmp = it.next()
      val key = new String(tmp.getKey)
      val value = tmp.getValue
      if (value.length > 0) {
        val count = new BigInteger(value).longValue().toInt
        if (count >= cTmp) {
          tTmp = key.toString()
          cTmp = count
        }
      }

    }

    //输出格式 XX:00 - YY:00
    var freqTime = ""
    if (tTmp.equalsIgnoreCase("")) {
      //没有最大返回空
    } else {
      var p1, p2 = ""
      if (tTmp.toInt  < 10) {
        p1 = "0" + tTmp + ":00 - "
      } else {
        p1 = tTmp + ":00 - "
      }
      if (tTmp.toInt + 1< 10) {
        p2 = "0" + (tTmp.toInt + 1).toString + ":00"
      } else {
        p2 = (tTmp.toInt + 1).toString + ":00"
      }
      freqTime = p1 + p2
    }
    freqTime

  }

  /**
    * 通过分析旅客出入时间找到对应的所有时间段
    *
    * @param start
    * @param end
    * @return e.g. (6,7) 表示在5：00-6：00， 6：00-7：00出现过
    */
  private def getApprTimeSlot(start: Long, end: Long): List[Int] = {
    var r: List[Int] = Nil

    val startTime: Int = DateUtils.getTime(start, "HH").toInt
    val endTime: Int = DateUtils.getTime(end, "HH").toInt

    val timeLength: Int = endTime - startTime

    //      r =r :+ startTime
    for (i <- 0 to timeLength) {
      r = r :+ startTime + i
    }
    r
  }

  /**
    * 保留两位小数
    *
    * @param arg
    * @return
    */
  private def formatDigits(arg: Float): String = {
    "%.2f".format(arg)
  }

  /**
    * 格式化时间 201612301223 -> 2016-12-30 12:23
    *
    * @param arg
    * @return
    */
  private def formatDate(arg: String): String = {
    arg.substring(0, 4) + "-" + arg.substring(4, 6) + "-" + arg.substring(6, 8) + " " + arg.substring(8, 10) + ":" + arg.substring(10, 12)
  }

  def main(args: Array[String]): Unit = {

        createIncrTable()
    //
    //    val fake: UserMacPointTrajectory = new UserMacPointTrajectory("10000", "10000", 1, 1, "fred", "area_1", System.currentTimeMillis(), System.currentTimeMillis() + 90 * 60 * 1000, "iphone")
    //    val fake1: UserMacPointTrajectory = new UserMacPointTrajectory("10000", "10000", 0, 1, "fred", "area_1", System.currentTimeMillis() + 100 * 60 * 1000, System.currentTimeMillis() + 200 * 60 * 1000,"iphone")
    //    val fake2: UserMacPointTrajectory = new UserMacPointTrajectory("10000", "10000", 0, 1, "fred", "area_1", System.currentTimeMillis() + 210 * 60 * 1000, System.currentTimeMillis() + 300 * 60 * 1000,"iphone")
    //    val fake3: UserMacPointTrajectory = new UserMacPointTrajectory("10000", "10000", 0, 1, "fred", "area_1", System.currentTimeMillis() + 1100 * 60 * 1000, System.currentTimeMillis() + 1200 * 60 * 1000,"iphone")
    //    val fake4: UserMacPointTrajectory = new UserMacPointTrajectory("10000", "10000", 0, 1, "fred", "area_1", System.currentTimeMillis() + 1210 * 60 * 1000, System.currentTimeMillis() + 1300 * 60 * 1000,"iphone")
    //
    //    updateUserStatistics(fake)
    //    updateUserStatistics(fake1)
    //    updateUserStatistics(fake2)
    //    updateUserStatistics(fake3)
    //    updateUserStatistics(fake4)

    var record: List[(String, String, String, String, String)] = Nil

    println(record.length)
  }
}
