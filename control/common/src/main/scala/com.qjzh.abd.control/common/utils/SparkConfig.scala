package com.qjzh.abd.control.common.utils

import java.io.File

import com.qjzh.abd.components.comp_common.caseciew.MessageKafka
import com.qjzh.abd.control.common.view.{Report}
import com.qjzh.abd.control.exe.CommonExe
import com.qjzh.abd.function.kafka.conf.KafkaConfigAndProperties
import kafka.serializer.StringDecoder
import org.apache.parquet.it.unimi.dsi.fastutil.objects.ObjectArrayList
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.reflect.ClassTag

/**
  * Created by hushuai on 16/12/15.
  */
object SparkConfig {
  var _sc : StreamingContext = null
  var _sparkContent : SparkContext = _
  var _conf:SparkConf =_
  /**
    * 动态生成任务
    * @param JobName
    *
    * @param seconds
    * @return
    */
  def createOnlineSparkMain(JobName : String, seconds : Int) : DStream[Report] = {
    val topicsSet = KafkaConfigAndProperties.getProValue("kafka.defalt.topic").split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> KafkaConfigAndProperties.getProValue("metadata.broker.list"), "auto.offset.reset" -> "largest")
    println(kafkaParams)
    val _conf = new SparkConf().setAppName(JobName)
    _sc = new StreamingContext(_conf, Seconds(seconds))
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](_sc, kafkaParams, topicsSet).map(x => {
      Tool.getReportByJsonStr(x._2)
    })
  }
  def createOnlineSparkMain[T](JobName : String, topics:Array[String],seconds : Int) : DStream[MessageKafka[ObjectArrayList[T]]] = {
    val topicsSet = topics.toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> KafkaConfigAndProperties.getProValue("metadata.broker.list"), "auto.offset.reset" -> "largest")
    println(kafkaParams)
    _conf = new SparkConf().setAppName(JobName)
    _sc = new StreamingContext(_conf, Seconds(seconds))
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](_sc, kafkaParams, topicsSet).map(x => {
        Tool.getInfoByJsonStr[T](x._2)
    })
  }

  def createofflineSparkMain(JobName : String):SparkContext={
    val _conf = new SparkConf().setAppName(JobName)
    _sparkContent = new SparkContext(_conf)
    _sparkContent
  }

  def createLocalTestofflineSparkMain(JobName : String):SparkContext={
    val _conf = new SparkConf().setAppName(JobName)
    _conf.setMaster("spark://master:7077")
    _sparkContent = new SparkContext(_conf)
    _sparkContent.addJar("/Users/apple/Documents/work/7g/7g-core/trunk/dba/control/offline/target/offline-1.0-SNAPSHOT.jar")
    _sparkContent.addJar("/Users/apple/Documents/work/7g/7g-core/trunk/dba/components/comp_common/target/comp_common-1.0-SNAPSHOT.jar")
    _sparkContent.addJar("/Users/apple/Documents/work/7g/7g-core/trunk/dba/components/label/target/label-1.0-SNAPSHOT.jar")
    _sparkContent.addJar("/Users/apple/Documents/work/7g/7g-core/trunk/dba/function/fun_common/target/fun_common-1.0-SNAPSHOT.jar")
    _sparkContent.addJar("/Users/apple/Documents/work/7g/7g-core/trunk/dba/function/hdfs/target/hdfs-1.0-SNAPSHOT.jar")
    _sparkContent.addJar("/Users/apple/Documents/work/7g/7g-core/trunk/dba/function/redis/target/redis-1.0-SNAPSHOT.jar")
    _sparkContent.addJar("/Users/apple/Documents/work/7g/7g-core/trunk/dba/function/log/target/log-1.0-SNAPSHOT.jar")
    _sparkContent
  }
  /**
    * 开始作业
    */
  def dStreamStart(): Unit ={
    if(_sc != null){
      _sc.start()
      _sc.awaitTermination()
    }
  }

  /**
    * 如果指定执行名称就只加载执行名称
    * @param exeClassName
    * @return
    */
  def loadOneFileByExeClassName(exeClassName : String) : CommonExe = {
    loadExeFileList[CommonExe].filter(_.getClass.getName.indexOf(exeClassName) != -1).head
  }

  /**
    * 加载指定类型
    * @param exeType
    * @return
    */
  def loadListFileByExeType(exeType : String) : List[CommonExe] = {
    loadExeFileList[CommonExe].filter(_.exeType.equalsIgnoreCase(exeType))
  }


  /**
    * 加载工程中已经实现当前执行器的业务实现类
    * @tparam T
    * @return
    */
  def loadExeFileList[T: ClassTag] : List[T] = {
    val cur = Thread.currentThread().getContextClassLoader().getResource("com.qjzh.abd.components")
    val filePathList =  getExeFile[T](cur.getFile).filter(x => {
      val ins = Class.forName(x).newInstance()
      ins.isInstanceOf[T]
    }).map(x => {
      val ins = Class.forName(x).newInstance()
      ins.asInstanceOf[T]
    })
    println(filePathList)
    filePathList
  }


  /**
    * 递归查询各个执行类中实现类
    * @param path
    * @param files
    * @return
    */
  private def getExeFile[T: ClassTag](path : String,files : List[String] = Nil ) : List[String]= {
    var fileList = files
    val file = new File(path)
    file.listFiles().foreach(x => {
      if(x.isDirectory) {
        fileList = getExeFile[T](path + "/" + x.getName, files) ::: fileList
      }
      if(x.isFile && x.isInstanceOf[T]){
        val path = x.getAbsolutePath
        fileList = (path.substring(path.indexOf("com")).replaceAll("\\/",".").replaceAll(".class","")) :: fileList
      }
    })
    println(fileList)
    fileList
  }


  def main(args: Array[String]): Unit = {
    val sc = SparkConfig.createofflineSparkMain("DayJob")
    print(sc.externalBlockStoreFolderName)
  }
}
