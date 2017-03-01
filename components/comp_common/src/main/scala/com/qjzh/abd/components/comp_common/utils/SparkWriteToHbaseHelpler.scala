package com.qjzh.abd.components.comp_common.utils

import com.qjzh.abd.components.comp_common.common.{HbaseBusUtils, RedisBusUtils}
import com.qjzh.abd.components.gather_mum.caseview.PointMember
import com.qjzh.abd.function.hbase.conf.HbaseConfigProperties
import com.qjzh.abd.function.hbase.utils.HbaseUtils
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.spark

/**
  * Created by 92025 on 2017/2/16.
  */
object SparkWriteToHbaseHelpler {


  def main(args: Array[String]): Unit = {

    val _conf = new SparkConf().setAppName("test")
    _conf.setMaster("local")
    val _sparkContent = new SparkContext(_conf)

    HbaseUtils.createTable(HbaseBusUtils.sta_umac_nber)




    def pointMemberBatchWirteToHbaseFun(data: String): Put ={
      val put = new Put(Bytes.toBytes(data))
      put.add(Bytes.toBytes(HbaseBusUtils.hbase_common_cloumn_family), Bytes.toBytes(HbaseBusUtils.hbase_common_cloumn_family),
        Bytes.toBytes(GsonTools.gson.toJson(data)))
      put
    }

    val txt : List[String] = List("aa","bb")

    val reulst = _sparkContent.parallelize(txt)

    val hbaseJob: Job = SparkWriteToHbaseHelpler.createHbaseJob(HbaseBusUtils.sta_umac_nber,_sparkContent)

    val hbaseRdd: RDD[(ImmutableBytesWritable, Put)] = SparkWriteToHbaseHelpler.createBatchWriteHbaseRdd(reulst,pointMemberBatchWirteToHbaseFun)

    hbaseRdd.saveAsNewAPIHadoopDataset(hbaseJob.getConfiguration)
  }

  /**
    * spark高速输出到hbase api
    * @param dStream
    * @param tableName
    * @param batchWirteToHbaseDataConvertFun
    * @tparam T
    */
  def saveDStreamToHbase[T](dStream:DStream[T],tableName: String,batchWirteToHbaseDataConvertFun:(T) => Put){
    dStream.foreachRDD(rdd => {

      val hbaseJob: Job = SparkWriteToHbaseHelpler.createHbaseJob(tableName,dStream.context.sparkContext)

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
    sc.hadoopConfiguration.set("hbase.zookeeper.quorum",HbaseConfigProperties.getProValue(HbaseConfigProperties.hbase_zookeeper_host))
    sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    val job = new Job(sc.hadoopConfiguration)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    job
  }
}
