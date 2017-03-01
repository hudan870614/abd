package com.qjzh.abd.components.hw.in_out_move.utils

import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

/**
  * Created by 92025 on 2017/2/16.
  */
object SparkWriteToHbaseHelpler {

  /**
    * spark高速输出到hbase api
    * @param dStream
    * @param tableName
    * @param sparkContext
    * @param batchWirteToHbaseDataConvertFun
    * @tparam T
    */
  def saveDStreamToHbase[T](dStream:DStream[T],tableName: String,
                            sparkContext: SparkContext,batchWirteToHbaseDataConvertFun:(T) => Put){
    dStream.foreachRDD(rdd => {

      val hbaseJob: Job = SparkWriteToHbaseHelpler.createHbaseJob(tableName,sparkContext)

      val hbaseRdd: RDD[(ImmutableBytesWritable, Put)] = SparkWriteToHbaseHelpler.createBatchWriteHbaseRdd(rdd,batchWirteToHbaseDataConvertFun)

      hbaseRdd.saveAsNewAPIHadoopDataset(hbaseJob.getConfiguration)

    })
  }

  /**
    * 将一个rdd转换成可以批量写入hbase的Rdd
    * @param rdd 目标Rdd
    * @param batchWirteToHbaseDataConvertFun 转换函数  ,将记录转换成可以批量写入hbase的函数
    * @tparam T   数据类型
    * @return
    */
  def createBatchWriteHbaseRdd[T](rdd: RDD[T],batchWirteToHbaseDataConvertFun:(T) => Put): RDD[(ImmutableBytesWritable, Put)] ={
    rdd.map(
      xx => {(new ImmutableBytesWritable(),batchWirteToHbaseDataConvertFun(xx))})
  }

  /**
    * 创建一个批量写入hbase的job描述
    * @param tableName  表名  准备写入的表
    * @return
    */
  def createHbaseJob(tableName: String,sc: SparkContext): Job ={
    sc.hadoopConfiguration.set("hbase.zookeeper.quorum","slave1,slave2,slave3")
    sc.hadoopConfiguration.set("hbase.zookeeper.property.clientPort", "2181")
    sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    val job = new Job(sc.hadoopConfiguration)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    job
  }
}
