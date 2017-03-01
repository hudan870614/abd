package com.qjzh.abd.components.comp_common.common

import java.io.IOException
import java.net.URI

import com.qjzh.abd.components.comp_common.caseciew.{hdfs_user_type, hdfs_view_duration, hdfs_view_label, hdfs_view_umac_nber}
import com.qjzh.abd.components.comp_common.conf.CommonConf
import com.qjzh.abd.components.comp_common.utils.ComUtils
import com.qjzh.abd.control.common.view.Report
import com.qjzh.abd.function.common.DateUtils
import com.qjzh.abd.function.hdfs.utils.HdfsBase
import it.unimi.dsi.fastutil.objects.ObjectArrayList
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}

/**
  * Created by hushuai on 16/3/5.
  */
object HdfsUtils {

  //======================================================================= 天维度

    /**
      * 用户标签表
      * 格式:curDay:当前时间,userMac:用户mac,label:标签,brand:品牌
      * 对应case类:hdfs_view_label
      */
    val label_hdfs_file_path = "result/day/"+CommonConf.project_no+"sta_da_umac_label"
    /**
      * 用户行为表 解析器
      * @param data curDay
      * @return
      */
    def label_hdfs_parser(data : Array[String]):hdfs_view_label = {
      val Array(curDay,userMac,label,brand) = data
      hdfs_view_label(curDay,userMac,label,brand)
    }

    /**
      * 旅客汇总表
      * 格式:分钟汇总时间(yyyyMMDDHHmm),Usermac,标签,区域块
      * 对应case类：hdfs_user_type
      * */
    val UserType_hdfs_file_path = "/result/minute/"+CommonConf.project_no+"sta_user_type"
    /**
    * 用户行为表 解析器
    * @param data curDay
    * @return
    */
    def usertype_hdfs_parser(data : Array[String]):hdfs_user_type = {
      val Array(time,userMac,label,stype) = data
      hdfs_user_type(time,userMac,label,stype)
    }

    val forecast_hour_hdfs_analyse_path = "temp/forecast/hour"

  //======================================================================= 分钟维度
  /**
    * 探针基础表
    格式: 手机mac,设备mac,场强,时间戳,设备类型,手机品牌,所属项目,区域编码,是否伪码
    对应case类:
    */
  val recode_load_hdfs_file_path = "base/minute/"+CommonConf.project_no+"ap_mi_recode_info"
  /**
    * 探针基础表 解析器
    * @param data
    * @return
    */
  def recode_hdfs_parser(data : Array[String]) : Report = {
    val Array(userMac,apMac,rssi,timestamp,apType,brand,projectNo,areaNo,isFake) = data
    val report : Report = new Report()
    report.userMac = userMac
    report.apMac = apMac
    report.rssi = rssi
    report.timeStamp = timestamp.toLong
    report.apType = apType
    report.brand = brand
    report.projectNo = projectNo
    report.areaNo = areaNo
    report.isFake = isFake.toInt
    report
  }

  /**
    * 探针定位表
    * 格式:手机mac,时间戳,x座标,y座标,z座标,所属区域,定位编码
    * 对应case类:
    */
  val point_load_hdfs_file_path = "base/minute/ap_mi_point_info"
  /**
    * 探针定位表 解析器
    * @param data
    * @return
    */
  def point_hdfs_parser(data : Array[String]) : Report = {
    val Array(userMac,timestamp,pointX,pointY,pointZ,areaNo,mapID) = data
    val report : Report = new Report()
    report.userMac = userMac
    report.timeStamp = timestamp.toLong
    report.pointX = pointX.toDouble
    report.pointY = pointY.toDouble
    report.pointZ = pointZ.toDouble
    report.areaNo = areaNo
    report.mapID = mapID
    report.isFake = ComUtils.isFakeMac(userMac)
    report
  }


  /**
    * 用户在线时长表
    * 格式: 日期(yyyyddmm),手机mac,当次区域开始时间戳,当次区域结束时间戳,手机品牌,本次是否新增入关次数,当次所属区域
    * 对应case类:hdfs_view_duration
    */
  val duration_hdfs_file_path = "base/minute/sta_mi_umac_duration"
  /**
    * 用户在线时长表 解析器
    * @param data
    * @return
    */
  def duration_hdfs_parser(data : Array[String]) : hdfs_view_duration= {
    val Array(curDay,userMac,startTime,endTime,brand,isNewTime,areaId) = data
    hdfs_view_duration(curDay,userMac,startTime,endTime,brand,isNewTime.toInt,areaId)
  }

  /**
    * 每分钟维度统计人数表
    * 格式:
    * 当前分钟(格式:yyyyMMddHHmm),当前设备＼区域编码＼楼层＼类型 等,当前分钟值,当前小时值,当前天值,无用,当天历史最大值,当天历史最大值时间分钟(格式:yyyyMMddHHmm)
    * 对应case类:sta_mi_umac_nber
    */
  val umac_nber_hdfs_file_path = "result/minute/sta_mi_umac_nber"

  /**
    * 每分钟维度统计人数表 解析器
    * @param data
    * @return
    */
  def umac_nber_hdfs_parser(data : Array[String]) : hdfs_view_umac_nber= {
    val Array(curTimeMinute,apMac,mSum,hSum,dSum,dSum2,hisMax,hisMaxTime) = data
    hdfs_view_umac_nber(curTimeMinute,apMac,mSum.toLong,hSum.toLong,dSum.toLong,dSum2.toLong,hisMax.toLong,hisMaxTime.toLong)
  }


  /**
    * 根据主题得到存储目录
    * @param fileType
    * @return
    */
  def getSaveFilePath(fileType : String, dataStr : String = "yyyy/MM/dd/HH/mm/") : String = {

    val  dateDic : String = DateUtils.getCurTime(dataStr)

    // 拼装存储目录
    val filePath = HdfsBase.FUN_HDFS_URL+"/new7g/"+fileType+"/"+dateDic

    filePath
  }

  /**
    * 根据主题得到存储目录
    * @param fileType
    * @param data 个性自定义路径
    * @return
    */
  def getSaveFilePathByData(fileType : String, data : String ) : String = {

//    val  dateDic : String = DateUtils.getCurTime(dataStr)

    // 拼装存储目录
    val filePath = HdfsBase.FUN_HDFS_URL+"/new7g/"+fileType+"/"+data

    filePath
  }


  def getHdfsFilePath(fileType : String, dataStr : String ) : String = {

//    val  dateDic : String = DateUtils.getBeforeHour(dataStr, -1)

    // 拼装存储目录
    val filePath = HdfsBase.FUN_HDFS_URL+"/new7g/"+fileType+"/"+dataStr

    filePath
  }


  def getPreHourSaveFilePath(fileType : String, dataStr : String ) : String = {

    //    val  dateDic : String = DateUtils.getBeforeHour(dataStr, -1)

    // 拼装存储目录
    val filePath = HdfsBase.FUN_HDFS_URL+"/new7g/"+fileType+"/"+dataStr

    filePath
  }


  /**
    * 加载前N天的路径
    * @return
    */
  def getPreDayFilePath(tagday: String ,preDays : Int, dateFormat : String = "yyyy/MM/dd") : List[String] = {
    var dayList: List[String] = List()
    for(day <-  preDays to 0){
      val cur_day = DateUtils.getBeforeDayByTagDay(tagday,day,dateFormat)
      dayList = dayList.::(cur_day)
    }
    dayList
  }


  /**
    * 将一个字符串list写入hdfs
    *
    * @param hdfs
    * @param path
    * @param result
    */
  def saveListStringToHdfsFile(hdfs: String, path: String, result: List[String]) = {
    val conf = new Configuration()
    var dfs: FileSystem = null
    var os: FSDataOutputStream = null
    try {
      dfs = FileSystem.get(URI.create(hdfs), conf)
      os = dfs.create(new Path(path), true)
      result.foreach(line => {
        os.write((line + "\n").getBytes)
      })
    } catch {
      case ex: IOException => {
      }
    } finally {
      os.flush()
      os.close()
      dfs.close()
    }


  }




  def main(args: Array[String]): Unit = {
//    val curDay = DateUtils.getBeforeHour("yyyy/MM/dd", -1)
//    println(HdfsUtils.getSaveFilePath(HdfsUtils.umac_nber_hdfs_file_path+"/"+curDay,"yyyyMMddHHmm"))

    val time = "20170210"
    getPreDayFilePath(time, -30,"yyyyMMdd").foreach(println(_))

  }


}
