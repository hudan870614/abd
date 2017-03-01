package com.qjzh.abd.components.comp_common.common

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.reflect.ClassTag
/**
  * Created by hushuai on 16/12/17.
  */
object RddUtils {

  def saveToHdfs(rdd : RDD[String],filePath : String): Unit = {
    if(rdd != null && !rdd.isEmpty() && rdd.count() > 0){
      rdd.saveAsTextFile(filePath)
    }
  }

  /**
    * ＲＤＤ聚合
    * @param resultRdd
    * @param unionRdd
    * @return
    */
  def unionRdd[T: ClassTag](resultRdd : RDD[T], unionRdd: RDD[T]): RDD[T] ={
    var midRdd = resultRdd
    if(resultRdd != null && unionRdd != null){
      midRdd = midRdd.union(unionRdd)
    }
    if(resultRdd == null && unionRdd != null){
      midRdd = unionRdd
    }
    midRdd
  }

  /**
    * Fraem聚合
    * @param resultDF
    * @param unionDF
    * @return
    */
  def unionDataFrame(resultDF : DataFrame, unionDF: DataFrame): DataFrame ={
    var midDF = resultDF
    if(resultDF != null && unionDF != null){
      midDF = midDF.unionAll(unionDF)
    }
    if(resultDF == null && unionDF != null){
      midDF = unionDF
    }
    midDF
  }

  /**
    * 移除
    * @param resultDF
    * @param exceptDFList
    * @return
    */
  def exceptFrame(resultDF : DataFrame, exceptDFList: List[DataFrame]): DataFrame ={
    var midDF = resultDF
    if(resultDF != null && exceptDFList != null && exceptDFList.size > 0){
      exceptDFList.foreach(x => {
        if(x != null && x.count() > 0){
          midDF = midDF.except(x)
        }
      })
    }
    midDF
  }


  def main(args: Array[String]): Unit = {
    println("aaaa")
  }
}
