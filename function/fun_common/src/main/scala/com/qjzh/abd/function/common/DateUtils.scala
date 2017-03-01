package com.qjzh.abd.function.common

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}


/**
  * Created by 92025 on 2016/12/15.
  */
object DateUtils {
  val dateForMat: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmm")
  val dateForMatDay: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
  val DATE_MINUTE = "yyyyMMddHHmm"
  val holidayArra = List(7,1)

  val workDayArra = List(2,3,4,5,6)


  /**
    * 根据类型自动识别是不是节假日
    * @param timestamp
    * @param stype 2:工作日,3:周末
    * @return
    */
  def isInWorkOrHoliday(timestamp: Long, stype : Int) : Boolean = {
    if(stype == 2){
      isInHoliday(timestamp)
    }else if(stype == 3){
      isInHoliday(timestamp)
    }else{
      true
    }
  }

  def isInHoliday(timestamp: Long) : Boolean = {
    val week  = getWeek(timestamp)
    holidayArra.contains(week)
  }

  def isInWorkday(timestamp: Long) : Boolean = {
    val week  = getWeek(timestamp)
    workDayArra.contains(week)
  }

  def formatTimestamp(timestamp: Timestamp): Long ={
    dateForMat.format(timestamp.getTime()).toLong
  }

  def getCurTime(formatStr:String = "yyyyMMddHHmmss"):String ={
    val timestamp = System.currentTimeMillis()
    DateUtils.getTime(timestamp,formatStr)
  }

  def getTime(timestamp:Long, formatStr:String) : String = {
    val date = new Date(timestamp)
    val format = new SimpleDateFormat(formatStr)
    format.format(date)
  }

  def getDay(timestamp:Long) : Long = {
    val ca = Calendar.getInstance()
    ca.setTime(new Date(timestamp))
    ca.set(Calendar.HOUR, 0)
    ca.set(Calendar.MINUTE, 0)
    ca.set(Calendar.SECOND, 0)
    ca.set(Calendar.MILLISECOND, 0)
    ca.getTimeInMillis
  }

  def getHour(timestamp:Long) : Long = {
    val ca = Calendar.getInstance()
    ca.setTime(new Date(timestamp))
    ca.get(Calendar.HOUR_OF_DAY)
  }

  def getWeek(timestamp:Long) : Long = {
    val ca = Calendar.getInstance()
    ca.setTime(new Date(timestamp))
    ca.get(Calendar.DAY_OF_WEEK)
  }

  def getBeforeMonth(formatStr:String = "yyyyMMdd",month:Int = -1):String  = {
    val ca = Calendar.getInstance()
    ca.add(Calendar.MONTH, month)
    val format = new SimpleDateFormat(formatStr)
    format.format(ca.getTimeInMillis)
  }

  def getBeforeDay(formatStr:String = "yyyyMMddHHmmss",day:Int = -1):String  = {
    val ca = Calendar.getInstance()
    ca.add(Calendar.DAY_OF_MONTH, day)
    val format = new SimpleDateFormat(formatStr)
    format.format(ca.getTimeInMillis)
  }

  def getBeforeHour(formatStr:String = "yyyyMMddHHmmss", hour:Int):String  = {
    val ca = Calendar.getInstance()
    ca.add(Calendar.HOUR_OF_DAY, hour)
    val format = new SimpleDateFormat(formatStr)
    format.format(ca.getTimeInMillis)
  }

  def getBeforeMinue(formatStr:String = "yyyyMMddHHmmss", minu:Int):String  = {
    val ca = Calendar.getInstance()
    ca.add(Calendar.MILLISECOND, minu * 1000 * 60)
    val format = new SimpleDateFormat(formatStr)
    format.format(ca.getTimeInMillis)
  }


  def getLongByTime(timestampStr : String ): Long = {

    val year = timestampStr.substring(0,4).toInt
    val month = timestampStr.substring(4,6).toInt
    val day = timestampStr.substring(6,8).toInt
    var hour = 0
    var minute = 0
    if(timestampStr.length > 8)
      hour = timestampStr.substring(8,10).toInt
    if(timestampStr.length > 10)
      minute = timestampStr.substring(10,12).toInt

    val ca = Calendar.getInstance()
    ca.set(Calendar.HOUR_OF_DAY, hour )
    ca.set(Calendar.YEAR, year)
    ca.set(Calendar.DAY_OF_MONTH, day)
    ca.set(Calendar.MONTH, month - 1 )
    ca.set(Calendar.MINUTE, minute)
    ca.set(Calendar.SECOND, 0)
    ca.set(Calendar.MILLISECOND, 0)
    ca.getTimeInMillis

  }


  def getDateByTime(timestampStr : String ): Calendar = {

    val year = timestampStr.substring(0,4).toInt
    val month = timestampStr.substring(4,6).toInt
    val day = timestampStr.substring(6,8).toInt
    var hour = 0
    var minute = 0
    if(timestampStr.length > 8){
      hour = timestampStr.substring(8,10).toInt
    }
    if(timestampStr.length > 10){
      minute = timestampStr.substring(10,12).toInt
    }


    val ca = Calendar.getInstance()
    ca.set(Calendar.HOUR_OF_DAY, hour )
    ca.set(Calendar.YEAR, year)
    ca.set(Calendar.DAY_OF_MONTH, day)
    ca.set(Calendar.MONTH, month - 1 )
    ca.set(Calendar.MINUTE, minute)
    ca.set(Calendar.SECOND, 0)
    ca.set(Calendar.MILLISECOND, 0)
    ca

  }


  def getBeforeDayByTagDay(tagDay : String,day:Int = -1,formatStr:String = "yyyyMMdd" ): String = {

    val year = tagDay.substring(0,4).toInt
    val month = tagDay.substring(4,6).toInt
    val curDay = tagDay.substring(6,8).toInt


    val ca = Calendar.getInstance()
    ca.set(Calendar.HOUR_OF_DAY, 0 )
    ca.set(Calendar.YEAR, year)
    ca.set(Calendar.DAY_OF_MONTH, curDay)
    ca.set(Calendar.MONTH, month - 1 )
    ca.set(Calendar.MINUTE, 0)
    ca.set(Calendar.SECOND, 0)
    ca.set(Calendar.MILLISECOND, 0)

    ca.add(Calendar.DAY_OF_MONTH, day)
    val format = new SimpleDateFormat(formatStr)
    format.format(ca.getTimeInMillis)

  }

  def getBeforeMinByTagMin(tagMin : String,min:Int = -1,formatStr:String = "yyyyMMddHHmm" ): String = {

    val year = tagMin.substring(0,4).toInt
    val month = tagMin.substring(4,6).toInt
    val curDay = tagMin.substring(6,8).toInt
    val curhour   = tagMin.substring(8,10).toInt
    val curmin    = tagMin.substring(10,12).toInt


    val ca = Calendar.getInstance()
    ca.set(Calendar.HOUR_OF_DAY, curhour )
    ca.set(Calendar.YEAR, year)
    ca.set(Calendar.DAY_OF_MONTH, curDay)
    ca.set(Calendar.MONTH, month - 1 )
    ca.set(Calendar.MINUTE, curmin)
    ca.set(Calendar.SECOND, 0)
    ca.set(Calendar.MILLISECOND, 0)

    //ca.add(Calendar.MINUTE, min)
    ca.add(Calendar.MILLISECOND, min * 1000 * 60)
    val format = new SimpleDateFormat(formatStr)
    format.format(ca.getTimeInMillis)

  }
  def getDiffMinue(start_date : String , end_date : String): Long = {
    val nm = 1000*60
    val diff = getLongByTime(end_date) - getLongByTime(start_date)
    diff/nm
  }

  def getDiffDay(start_date : String , end_date : String): Long = {
    val daylen = 1000*60*60*24
    val diff = getLongByTime(end_date) - getLongByTime(start_date)
    diff/daylen
  }

  def getDiffHour(start_date : String , end_date : String): Long = {
    val daylen = 1000*60*60
    val diff = getLongByTime(end_date) - getLongByTime(start_date)
    diff/daylen
  }

  /**
    * 求两个时间相差时间
    * @param startDate
    * @param endDate
    * @param formatRex
    * @return
    */
  def getDiffMinute(startDate: String,endDate: String,formatRex: String): Long ={

    val simpe = new SimpleDateFormat(formatRex)

    val parse: Date = simpe.parse(startDate)
    val parse1: Date = simpe.parse(endDate)
    (parse1.getTime - parse.getTime)/1000/60
  }

  def isSomneDay(startTime: Long,endTime: Long): Boolean ={
    val startTimeFormat: String = DateUtils.dateForMatDay.format(new Date(startTime))
    val endTimeFormat: String = DateUtils.dateForMatDay.format(new Date(endTime))

    endTimeFormat.equals(startTimeFormat)
  }
  def main(args: Array[String]): Unit = {
//    println(DateUtils.getHour(System.currentTimeMillis()))

    println(DateUtils.getBeforeDayByTagDay("20161028",1,"yyyy/MM/dd"))
  }

}
