package com.qjzh.abd.components.label.utils

import com.qjzh.abd.components.comp_common.caseciew.hdfs_view_label
import com.qjzh.abd.components.comp_common.common.HbaseBusUtils
import com.qjzh.abd.components.comp_common.conf.CommonConf
import com.qjzh.abd.components.label.view.label_view_duration
import com.qjzh.abd.function.hbase.utils.HbaseUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
  * Created by hushuai on 16/12/27.
  */
object LabelDigUtils {

  /**
    * 按设备mac分组
    * @param rdd
    * @return
    */
  def groupByUserMac(rdd : RDD[label_view_duration]) : RDD[label_view_duration] = {
    rdd.groupBy(_.userMac)
      .map(dur => {
        val duration = dur._2.map(_.duration).reduce(_+_)
        val times = dur._2.map(_.times).reduce(_+_)
        val dayNum = dur._2.map(_.curDay).toList.distinct.size
        val brand = dur._2.head.brand
        label_view_duration(null,dayNum,dur._1,times,duration,0,brand)
      })
  }

  /**
    * 查询出第一次出现的userMac
    * @param df
    * @param propertyName
    * @return
    */
  def getFristUserMacs(df : DataFrame,propertyName : String ) : RDD[String] = {
    var result : RDD[String] = null
    if(df != null) {
      result = df.filter(df(propertyName).isNotNull).map(x => {
        x.getAs[String](propertyName)
      }).filter(x => {
        var isRun = true
        val userLabelInfoMaps = HbaseUtils.readTable(HbaseBusUtils.user_label_info, x)
        if (userLabelInfoMaps != null && userLabelInfoMaps.containsKey(HbaseBusUtils.hbase_common_cloumn_family)) {
          val macLableJson = userLabelInfoMaps.get(HbaseBusUtils.hbase_common_cloumn_family)
          if (macLableJson.indexOf(CommonConf.LABLE_CORE_DATA) != -1) {
            isRun = false
          }
        }
        isRun
      })
    }
    result
  }

  /**
    * 追加标签属性
    * @param df
    * @param propertyName
    * @param label
    * @return
    */
  def withLabel(df:DataFrame , propertyName : String, label : String ) : RDD[(String,String)] = {
    var result : RDD[(String,String)] =  null
    if(df != null ){
      result = df.filter(df(propertyName).isNotNull).map(x => {
        val mac = x.getAs[String](propertyName)
        (mac,label)
      })
    }
    result
  }


//  /**
//    * 追加标签属性
//    * @param df
//    * @param newPropertyName
//    * @param label
//    * @return
//    */
//  def withLabel(df:DataFrame , newPropertyName : String, label : String ) : DataFrame = {
//    var result : DataFrame =  null
//    if(df != null ){
//      result = df.
//    }
//    result
//  }


  /**
    * 追加标签属性
    * @param df
    * @param label
    * @return
    */
  def withLabelAndSaveHbase(exeDaySimp : String ,df:DataFrame ,label : String, allDurBrandRDD : RDD[(String,String)]) : RDD[hdfs_view_label] = {
//    var result : RDD[hdfs_view_label] =  null
//    if(df != null ){
//      result =

//        df.filter(df(LabelDigService.user_mac_propery_name).isNotNull).map(x => {
//          val userMac = x.getAs[String](LabelDigService.user_mac_propery_name)
//          userMac
//        }).cartesian(allDurBrandRDD).filter(x => {
//          x._1.equalsIgnoreCase(x._2._1)
//        }).map(x => {
//          val userMac = x._2._1
//          val view = hdfs_view_label(exeDaySimp,userMac,label,x._2._2)
//          HbaseUtils.writeTable(HbaseBusUtils.user_label_info, userMac, "data", GsonTools.gson.toJson(view))
//          view
//        })

//      df.map(x => {
//        val userMac = x.getAs[String](LabelDigService.user_mac_propery_name)
//        val view = hdfs_view_label(exeDaySimp,userMac,label,"")
////        HbaseUtils.writeTable(HbaseBusUtils.user_label_info, userMac, "data", GsonTools.gson.toJson(view))
//        view
//      })

//        df.filter(df(LabelDigService.user_mac_propery_name).isNotNull)
//          .join(allDurBrandDF,df(LabelDigService.user_mac_propery_name)
//            .equalTo(allDurBrandDF(LabelDigService.user_mac_propery_name)),"left")
//          .drop(allDurBrandDF(LabelDigService.user_mac_propery_name)).map(x => {
//
//          val userMac = x.getAs[String](LabelDigService.user_mac_propery_name)
//          var brand = x.getAs[String](LabelDigService.brand_propery_name)
//          if(brand ==  null || brand.isEmpty){
//            brand = "other"
//          }
//
//          val view = hdfs_view_label(exeDaySimp,userMac,label,brand)
//          HbaseUtils.writeTable(HbaseBusUtils.user_label_info, userMac, "data", GsonTools.gson.toJson(view))
//
//          view
//      })
//    }
//    result
    null
  }
}
