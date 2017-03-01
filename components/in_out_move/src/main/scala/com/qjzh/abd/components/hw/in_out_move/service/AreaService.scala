package com.qjzh.abd.components.hw.in_out_move.service

import com.qjzh.abd.components.comp_common.caseciew.SynAreaBean
import com.qjzh.abd.components.comp_common.utils.GsonTools
import com.qjzh.abd.components.hw.in_out_move.caseview._
import com.qjzh.abd.components.hw.in_out_move.conf.HwHwInOutMoveConf
import com.qjzh.abd.components.hw.in_out_move.dao.redis.AreaRedisDao
import com.qjzh.abd.control.common.view.Report
import org.apache.commons.lang3.StringUtils
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable.ListBuffer


/**
  * Created by 92025 on 2017/2/22.
  */
object AreaService {

  def isContainInArea(report: Report, x: SynAreaBean) = {
    areaRedisDao.isContainInArea(report,x)
  }

  val areaRedisDao = AreaRedisDao
  /**
    * 获取所有的区域
    * @return
    */
  def getSynAreaBean(): List[SynAreaBean] ={
    areaRedisDao.getAreaList()
  }
  /**
    * 单点迁入数据写入redis
    * @param querySimpleAreaOut
    */
  def writeOutSingleAreaInfoToRedis(querySimpleAreaOut: DStream[WriteOutSimple]): Unit ={
    val currentTimeMillis: Long = System.currentTimeMillis()
    querySimpleAreaOut.foreachRDD(rdd => {
      rdd.foreach(data => {
        areaRedisDao.hset(data.viewAndLevel + HwHwInOutMoveConf.REDIS__KEY_OUT_SINGLE_PREFIX,data.outAreaNo.toString,
          GsonTools.gson.toJson(data),HwHwInOutMoveConf.REDIS_TTL_FOR_REPORT_REDISCACHE)

        //写入趋势数据
        val simpleInOutTrenPoint: SimpleInOutTrenPoint = SimpleInOutTrenPoint(data.viewAndLevel,data.outAreaNo,data.outAllSum,currentTimeMillis)
        areaRedisDao.lpushRpopByLenth(simpleInOutTrenPoint.viewAndLevel + HwHwInOutMoveConf.SYS_PREFIX + simpleInOutTrenPoint.areaNo,
          GsonTools.gson.toJson(simpleInOutTrenPoint),HwHwInOutMoveConf.REDIS_KEY_TREN_LENTH,
          HwHwInOutMoveConf.REDIS_TTL_FOR_REPORT_REDISCACHE)
      })
    })
  }
  /**
    * 单点迁入数据写入redis
    * @param querySimpleAreaIn
    */
  def writeInSingleAreaInfoToRedis(querySimpleAreaIn: DStream[WriteInSimple]): Unit ={
    val currentTimeMillis: Long = System.currentTimeMillis()
    querySimpleAreaIn.foreachRDD(rdd => {
      rdd.foreach(data => {
        areaRedisDao.hset(data.viewAndLevel + HwHwInOutMoveConf.REDIS__KEY_IN_SINGLE_PREFIX,data.inAreaNo.toString,
          GsonTools.gson.toJson(data),HwHwInOutMoveConf.REDIS_TTL_FOR_REPORT_REDISCACHE)
        //写入趋势数据
        val simpleInOutTrenPoint: SimpleInOutTrenPoint = SimpleInOutTrenPoint(data.viewAndLevel,data.inAreaNo,data.inAllSum,currentTimeMillis)
        areaRedisDao.lpushRpopByLenth(simpleInOutTrenPoint.viewAndLevel + HwHwInOutMoveConf.SYS_PREFIX + simpleInOutTrenPoint.areaNo,
          GsonTools.gson.toJson(simpleInOutTrenPoint),HwHwInOutMoveConf.REDIS_KEY_TREN_LENTH,
          HwHwInOutMoveConf.REDIS_TTL_FOR_REPORT_REDISCACHE)
      })
    })
  }


  /**
    * 写入最热迁入数据到redis
    * @param heatInData
    */
  def writeHeatInDataToRedis(heatInData: DStream[WriteHeatInToHbase]): Unit ={
    heatInData.foreachRDD(rdd => {
      rdd.foreach(data => {
        areaRedisDao.hset(data.view + HwHwInOutMoveConf.SYS_PREFIX + data.level + HwHwInOutMoveConf.REDIS__KEY_IN_HEAT_PREFIX,data.inAreaNo.toString,
          GsonTools.gson.toJson(data),HwHwInOutMoveConf.REDIS_TTL_FOR_REPORT_REDISCACHE)
      })
    })
  }
  /**
    * 写入最热迁出数据到redis
    * @param heatOutData
    */
  def writeHeatOutDataToRedis(heatOutData: DStream[WriteHeatOutToHbase]): Unit ={
    heatOutData.foreachRDD(rdd => {
      rdd.foreach(data => {
        areaRedisDao.hset(data.view + HwHwInOutMoveConf.SYS_PREFIX + data.level + HwHwInOutMoveConf.REDIS__KEY_OUT_HEAT_PREFIX,data.outAreaNo.toString,
          GsonTools.gson.toJson(data),HwHwInOutMoveConf.REDIS_TTL_FOR_REPORT_REDISCACHE)
      })
    })
  }
  /**
    * 写入最热迁入迁出数据到redis
    * @param heatInOutData
    */
  def writeHeatInOutDataToRedis(heatInOutData: DStream[WriteHeatInOutToHbase]): Unit ={
    heatInOutData.foreachRDD(rdd => {
      rdd.foreach(data => {
        areaRedisDao.hset(data.view + HwHwInOutMoveConf.SYS_PREFIX + data.level + HwHwInOutMoveConf.REDIS__KEY_IN_OUT_HEAT_PREFIX,
          data.inAreaNo.toString + HwHwInOutMoveConf.SYS_PREFIX +data.outAreaNo.toString,
          GsonTools.gson.toJson(data),HwHwInOutMoveConf.REDIS_TTL_FOR_REPORT_REDISCACHE)
      })
    })
  }

  /**
    * 单点迁出计算
    * @param viewAndLevelAndAreaNoDStreamOutGroup
    * @return
    */
  def querySimpleAreaOut(viewAndLevelAndAreaNoDStreamOutGroup: DStream[(String, Iterable[ReportInOutMove])]): DStream[WriteOutSimple] ={
    viewAndLevelAndAreaNoDStreamOutGroup.map(xx => {
      val outAreaNo = xx._1
      val inAreaItrerableReportInOutMove = xx._2
      val groupByInNo= inAreaItrerableReportInOutMove.groupBy(_.inAreaNo)
      val inAreaNo_sum = groupByInNo.map(inAreaGroup => {
        SimpleInOut(inAreaGroup._1, inAreaGroup._2.size, inAreaGroup._2.size.toDouble / inAreaItrerableReportInOutMove.size.toDouble)
      })
      WriteOutSimple(outAreaNo.substring(0,outAreaNo.lastIndexOf(HwHwInOutMoveConf.SYS_PREFIX)),outAreaNo.split(HwHwInOutMoveConf.SYS_PREFIX).last.toLong,inAreaItrerableReportInOutMove.size,inAreaNo_sum.toList)
    })
  }


  /**
    * 单点迁入计算
    * @param viewAndLevelAndAreaNoDStreamInGroup
    * @return
    */
  def querySimpleAreaIn(viewAndLevelAndAreaNoDStreamInGroup: DStream[(String, Iterable[ReportInOutMove])]): DStream[WriteInSimple] ={
    viewAndLevelAndAreaNoDStreamInGroup.map(xx => {
      val inAreaNo = xx._1
      val outAreaItrerableReportInOutMove = xx._2
      val groupByOutNo = outAreaItrerableReportInOutMove.groupBy(_.outAreaNo)
      val outAreaNo_sum = groupByOutNo.map(outAreaGroup => {
        SimpleInOut(outAreaGroup._1, outAreaGroup._2.size, outAreaGroup._2.size.toDouble / outAreaItrerableReportInOutMove.size.toDouble)
      })
      WriteInSimple(inAreaNo.substring(0, inAreaNo.lastIndexOf(HwHwInOutMoveConf.SYS_PREFIX)), inAreaNo.split(HwHwInOutMoveConf.SYS_PREFIX).last.toLong, outAreaItrerableReportInOutMove.size, outAreaNo_sum.toList)
    })
  }
  /**
    * 获取所有最热迁入迁出数据
    * @param business  (视角_级别,(每个区块的迁入人数,总迁入人数),(每个区块的迁出人数,总迁出人数),(每个区块的迁入迁出人数,总迁入迁出人数))
    */
  def getHeatInOutData(business: DStream[(String, (Map[Long, Int], Int), (Map[Long, Int], Int), Map[String, Int])]): DStream[WriteHeatInOutToHbase] ={
    val map: DStream[WriteHeatInOutToHbase] = business.flatMap(xx => {
      val currentTimeMillis: Long = System.currentTimeMillis()
      val viewAndLevel = xx._1
      val areaSumMap = xx._4

      val writeHeatOutToHbases = areaSumMap.map(areaAndAreaNo => {
        val areaNo = areaAndAreaNo._1
        val sum = areaAndAreaNo._2

        val sorted: List[String] = List(areaNo.split(HwHwInOutMoveConf.SYS_PREFIX).head,areaNo.split(HwHwInOutMoveConf.SYS_PREFIX).last).sorted

        WriteHeatInOutToHbase(currentTimeMillis,sorted.head.toLong ,sorted.last.toLong, sum,
          viewAndLevel.split(HwHwInOutMoveConf.SYS_PREFIX)(0).toLong, viewAndLevel.split(HwHwInOutMoveConf.SYS_PREFIX)(1).toLong)
      })
      writeHeatOutToHbases
    })
    map
  }

  /**
    * 获取所有最热迁出数据
    * @param business  (视角_级别,(每个区块的迁入人数,总迁入人数),(每个区块的迁出人数,总迁出人数),(每个区块的迁入迁出人数,总迁入迁出人数))
    */
  def getHeatOutData(business: DStream[(String, (Map[Long, Int], Int), (Map[Long, Int], Int), Map[String, Int])]): DStream[WriteHeatOutToHbase] ={
    val map: DStream[WriteHeatOutToHbase] = business.flatMap(xx => {
      val currentTimeMillis: Long = System.currentTimeMillis()
      val viewAndLevel = xx._1
      val out: (Map[Long, Int], Int) = xx._3
      val outAllSum = out._2
      val areaSumMap = out._1

      val writeHeatOutToHbases = areaSumMap.map(areaAndSum => {
        val areaNo = areaAndSum._1
        val sum = areaAndSum._2
        WriteHeatOutToHbase(currentTimeMillis, areaNo, sum, sum.toDouble / outAllSum.toDouble,
          viewAndLevel.split(HwHwInOutMoveConf.SYS_PREFIX)(0).toLong, viewAndLevel.split(HwHwInOutMoveConf.SYS_PREFIX)(1).toLong, outAllSum)
      })

      writeHeatOutToHbases
    })
    map
  }


  /**
    * 获取所有最热迁入数据
    * @param business
    */
    def getHeatInData(business: DStream[(String, (Map[Long, Int], Int), (Map[Long, Int], Int), Map[String, Int])]): DStream[WriteHeatInToHbase] ={
      val map: DStream[WriteHeatInToHbase] = business.flatMap(xx => {
        val currentTimeMillis: Long = System.currentTimeMillis()
        val viewAndLevel = xx._1
        val in: (Map[Long, Int], Int) = xx._2
        val inAllSum = in._2
        val areaSumMap = in._1

        val writeHeatInToHbases = areaSumMap.map(areaAndSum => {
          val areaNo = areaAndSum._1
          val sum = areaAndSum._2
          WriteHeatInToHbase(currentTimeMillis, areaNo, sum, sum.toDouble / inAllSum.toDouble,
            viewAndLevel.split(HwHwInOutMoveConf.SYS_PREFIX)(0).toLong, viewAndLevel.split(HwHwInOutMoveConf.SYS_PREFIX)(1).toLong, inAllSum)
        })
        writeHeatInToHbases
      })
      map
    }
  /**
    * 求出每个视角级别  对应的每个区域的人数和占比
    *
    * @param dataStreaming
    * @return (视角_级别,(每个区块的迁入人数,总迁入人数),(每个区块的迁出人数,总迁出人数),(每个区块的迁入迁出人数,总迁入迁出人数))
    */
  def computerHeatBusiness(dataStreaming: DStream[(String, Iterable[ReportInOutMove])]): DStream[(String, (Map[Long, Int], Int), (Map[Long, Int], Int), Map[String, Int])] = {

    val map: DStream[(String, (Map[Long, Int], Int), (Map[Long, Int], Int), Map[String, Int])] = dataStreaming.map(data => {

      val groupByInAreaNo = data._2.groupBy(_.inAreaNo) //迁入区域就是当前区块   按照当前所在区块分组
      val groupByInAreaNoSum = groupByInAreaNo.map(xx => {
        //每个视图级别  -----  子区块下的数据
        (xx._1, xx._2.size)
      }) //每个区域的迁入人数
      val aggregateIn: Int = groupByInAreaNoSum.aggregate(0)(_ + _._2, _ + _) //迁入总人数

      val groupByOutAreaNo = data._2.groupBy(_.outAreaNo) //迁出区域就是当前区块   按照当前所在区块分组
      val groupByOutAreaNoSum = groupByOutAreaNo.map(xx => {
        //每个视图级别  -----  子区块下的数据
        (xx._1, xx._2.size)
      }) //每个区域的迁出人数
      val aggregateOut: Int = groupByOutAreaNoSum.aggregate(0)(_ + _._2, _ + _) //迁出总人数


      val groupByInAreaOutArea = data._2.groupBy(xx => {
        xx.inAreaNo + HwHwInOutMoveConf.SYS_PREFIX + xx.outAreaNo
      }) //按迁入迁出区块组合分组

      val groupByInAreaOutAreaSum = groupByInAreaOutArea.map(xx => {
        //每个视图级别  -----  迁入迁出区块数据
        (xx._1, xx._2.size)
      })
      (data._1, (groupByInAreaNoSum, aggregateIn), (groupByOutAreaNoSum, aggregateOut), (groupByInAreaOutAreaSum))
    })
    map
  }

  /**
    * 根据视图和级别到redis 验证该数据的燕如迁出
    */
  def queryInOutMoveByAreaNoLevelAndUserMac(transformareaNoLevelCombinationLeftJoinReport: DStream[(String, (Null, Option[Iterable[(Report, AreaInfoJoin)]]))]): DStream[ReportInOutMove] = {
//    (String, (Null, Option[Iterable[(Report, AreaInfoJoin)]]))
    val map: DStream[ReportInOutMove] = transformareaNoLevelCombinationLeftJoinReport.flatMap(report => {
      val reportInOutMoves: ListBuffer[ReportInOutMove] = ListBuffer()

      report._2._2 match {
        case Some(data) => {
          data.foreach(xx => {
            val userMac = xx._1.userMac
            val queryRedisKV = areaRedisDao.queryRedisKV(HwHwInOutMoveConf.REDIS_HW_HW_SPARKSTREAM_IN_OUT_MOVE + report._1 + HwHwInOutMoveConf.SYS_PREFIX + userMac)
            if (StringUtils.isNotEmpty(queryRedisKV)) {
              //redis中存在该用户的迁徙信息
              val reportRedisCache: ReportRedisCache = GsonTools.gson.fromJson(queryRedisKV, classOf[ReportRedisCache])
              if (reportRedisCache.lastAreaNo != xx._1.areaNo.toLong) {
                //在地图上位置发生变化
                reportInOutMoves.append(ReportInOutMove(xx._1.serverTimeStamp, xx._1.areaNo.toLong, reportRedisCache.lastAreaNo, userMac, report._1))
              } else {
                //与上一次位置一样  //更新时间
                areaRedisDao.saveOrUpdateKV(HwHwInOutMoveConf.REDIS_HW_HW_SPARKSTREAM_IN_OUT_MOVE + report._1 + HwHwInOutMoveConf.SYS_PREFIX + userMac,
                  GsonTools.gson.toJson(ReportRedisCache(xx._1.serverTimeStamp, xx._1.areaNo.toLong, userMac)), HwHwInOutMoveConf.REDIS_TTL_FOR_REPORT_REDISCACHE)
              }
            }

            if (StringUtils.isEmpty(queryRedisKV)) {
              ////redis中不存在该用户的迁徙信息   构建迁徙信息存储到redis
              areaRedisDao.saveOrUpdateKV(HwHwInOutMoveConf.REDIS_HW_HW_SPARKSTREAM_IN_OUT_MOVE + report._1 + HwHwInOutMoveConf.SYS_PREFIX + userMac,
                GsonTools.gson.toJson(ReportRedisCache(xx._1.serverTimeStamp, xx._1.areaNo.toLong, userMac)), HwHwInOutMoveConf.REDIS_TTL_FOR_REPORT_REDISCACHE)
            }

          })
        }
        case None => {

        }
      }

      reportInOutMoves
    })
    map
  }

  /**
    * 查询区块的详细信息
    * 级别维表信息
    */
  def queryAreaInfoList(): ListBuffer[AreaInfoJoin] = {
    areaRedisDao.queryAreaInfoList()
  }

  /**
    * 获取区域级别列表
    *
    * @return
    */
  def queryAreaLevelList(): ListBuffer[Long] = {
    areaRedisDao.queryAreaInfoList().map(_.areaLevel).distinct.sorted.reverse
  }

  /**
    * 查询所有区域和级别的组合
    */
  def queryAreaAndLevelCombinationList(): ListBuffer[String] = {
    val reverse: ListBuffer[Long] = areaRedisDao.queryAreaInfoList().map(_.areaLevel).distinct.sorted //所有的级别
    val areaList: ListBuffer[AreaInfoJoin] = areaRedisDao.queryAreaInfoList() //所有的区域

    val listBuffer: ListBuffer[String] = areaList.flatMap(xx => {
      val area_level: ListBuffer[String] = ListBuffer()
      reverse.filter(_ > xx.areaLevel).foreach(yy => {
        area_level.append(xx.areaId + HwHwInOutMoveConf.SYS_PREFIX + yy)
      })
      area_level
    })
    listBuffer
  }

  def main(args: Array[String]): Unit = {
    val areaInfoJoin: AreaInfoJoin = AreaInfoJoin(8,3,4,1)
    val buffer: ListBuffer[AreaInfoJoin] = ListBuffer()
    queryAreaInfo(areaInfoJoin,buffer)
    buffer.append(areaInfoJoin)
    println(buffer.distinct)

  }
  /**
    * 查询父级
    *
    * @param areaInfoJoin
    * @return
    */
  def queryReportParentAreaReport(areaInfoJoin: AreaInfoJoin): ListBuffer[AreaInfoJoin] = {
    val filter: ListBuffer[AreaInfoJoin] = ListBuffer()
    if (areaInfoJoin.parentAreaId > 0) {
      val listBuffer: ListBuffer[AreaInfoJoin] = areaRedisDao.queryAreaInfoList.filter(_.areaId == areaInfoJoin.parentAreaId)
      if (listBuffer.size != 0) {
        filter.append(listBuffer.head)
      }
    }
    filter
  }

  /**
    * 查找所有的上级
    * @param areaInfoJoin
    * @param list
    */
  def queryAreaInfo(areaInfoJoin: AreaInfoJoin,list:ListBuffer[AreaInfoJoin]): Unit ={
    if(areaInfoJoin.parentAreaId < 0){
      list.append(areaInfoJoin)
    }else{
      val filter: ListBuffer[AreaInfoJoin] = areaRedisDao.queryAreaInfoList.filter(_.areaId == areaInfoJoin.parentAreaId)
      if(filter.size != 0){
        list.append(filter.head)
        queryAreaInfo(filter.head,list)
      }
    }
  }

  /**
    * 发散每个基数据到父级去
    * @param transform
    * @return
    */
  def addParentAreaDataByChildData(transform: DStream[(String, (Report, AreaInfoJoin))]): DStream[(Report, AreaInfoJoin)] ={
    transform.flatMap(xx => {
      val areaInfoJoin = xx._2._2
      val buffer: ListBuffer[AreaInfoJoin] = ListBuffer()
      AreaService.queryAreaInfo(areaInfoJoin, buffer)
      val distinct: ListBuffer[AreaInfoJoin] = buffer.distinct
      distinct.append(areaInfoJoin)
      val listBuffer = distinct.map(area => {
        val newReport = GsonTools.gson.fromJson(GsonTools.gson.toJson(xx._2._1), classOf[Report])
        newReport.areaNo = area.areaId.toString
        (newReport, area)
      })
      listBuffer
    })
  }


}
