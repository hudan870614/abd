package com.qjzh.abd.components.brand.service

import com.qjzh.abd.components.brand.view.HbaseBrandCountCase
import com.qjzh.abd.components.comp_common.common.{HbaseBusUtils, HdfsUtils}
import com.qjzh.abd.components.comp_common.conf.CommonConf
import com.qjzh.abd.components.comp_common.utils.GsonTools
import com.qjzh.abd.function.hbase.utils.HbaseUtils
import org.apache.spark.SparkContext

/**
  * 标签按天离线汇总挖掘逻辑
  * Created by hdshuai on 2016/10/24.
  */
object BrandService {

  def doExe(exeDay:String,sc:SparkContext): Unit ={
    val curDay = exeDay.replaceAll("\\/","")

    //最近30天
    val cruBefor30DaysList = HdfsUtils.getPreDayFilePath(curDay, -30,"yyyyMMdd")
    dealBrandByType("YY30",cruBefor30DaysList,sc)


    //当前一个月
    val curMonth = curDay.substring(0,curDay.length-2)
    val curMonthDaysList = HdfsUtils.getPreDayFilePath(curDay, -35,"yyyyMMdd").filter(_.indexOf(curMonth) != -1)
    dealBrandByType(curMonth,curMonthDaysList,sc)

  }


  /**
    * 按类型汇总手机品牌
    * @param prefix
    * @param curBeforeList
    * @param sc
    */
  def dealBrandByType(prefix : String,curBeforeList: List[String],sc:SparkContext): Unit ={

    //清理数据
    HbaseUtils.dealByRowKey(HbaseBusUtils.sta_umac_brand_nber, (prefix+"_key").toUpperCase())
    HbaseUtils.dealByRowKey(HbaseBusUtils.sta_umac_brand_nber, (prefix+"_first").toUpperCase())
    HbaseUtils.dealByRowKey(HbaseBusUtils.sta_umac_brand_nber, (prefix+"_act").toUpperCase())
    HbaseUtils.dealByRowKey(HbaseBusUtils.sta_umac_brand_nber, (prefix+"_ALL").toUpperCase())
    HbaseUtils.dealByRowKey(HbaseBusUtils.sta_umac_brand_nber, (prefix+"_occ").toUpperCase())
    HbaseUtils.dealByRowKey(HbaseBusUtils.sta_umac_brand_nber, (prefix+"_other").toUpperCase())

    val brandLabelMap = sc.parallelize(curBeforeList)
      .filter(HbaseUtils.containRowKey(HbaseBusUtils.da_umac_brand,_))
      .flatMap(day => {
      HbaseUtils.findByRowKey(HbaseBusUtils.da_umac_brand,day).flatMap(x => {
        x._2.split("\\;").map(mac => {
          (x._1,mac)
        })
      })
    }).distinct().map(x => {
      val label = HbaseUtils.readTable(HbaseBusUtils.user_label_info,x._2,
        HbaseBusUtils.hbase_common_cloumn_family,HbaseBusUtils.hbase_common_cloumn_family)
      var label_key : String = null
      if(label.indexOf(CommonConf.LABLE_CORE_DATA) != -1){
        label_key = "key"
      }else if(label.indexOf(CommonConf.LABLE_GUEST_FIR_DATA) != -1){
        label_key = "first"
      }else if(label.indexOf(CommonConf.LABLE_GUEST_ACT_DATA) != -1){
        label_key = "act"
      }else{
        label_key = "occ"
      }
      ((label_key,x._1),1)
    })

    val allTypeCountMap = brandLabelMap.map(x => {
      (x._1._1,1)
    }).reduceByKey(_+_).toLocalIterator.toMap

    val allCount = allTypeCountMap.map(_._2).reduce(_+_)

    brandLabelMap.reduceByKey(_+_).map(r => {
      val label = r._1._1
      val brand = r._1._2
      val bSum = r._2
      val tSum = allTypeCountMap.getOrElse(label,0)
      val columnName = (Long.MaxValue - bSum)+"_"+brand
      HbaseUtils.writeTable(HbaseBusUtils.sta_umac_brand_nber, (prefix+"_"+label).toUpperCase(), columnName, GsonTools.gson.toJson(HbaseBrandCountCase(tSum,bSum,brand)))
      (brand,bSum)
    }).reduceByKey(_+_).foreachPartition(p => {
      p.foreach(r => {
        val brand = r._1
        val bSum = r._2
        val columnName = (Long.MaxValue - bSum)+"_"+brand
        HbaseUtils.writeTable(HbaseBusUtils.sta_umac_brand_nber, (prefix+"_ALL").toUpperCase(), columnName, GsonTools.gson.toJson(HbaseBrandCountCase(allCount,bSum,brand)))

      })
    })
  }



  def main(args: Array[String]): Unit = {

    println("CORE_ACT".indexOf(CommonConf.LABLE_CORE_DATA))


  }



}