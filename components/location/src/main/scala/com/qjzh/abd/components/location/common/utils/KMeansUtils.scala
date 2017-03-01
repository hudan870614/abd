package com.qjzh.abd.components.location.common.utils

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yufeiwang on 16/01/2017.
  */
object KMeansUtils {
  def main(args: Array[String]) {
    //设置环境
    val sparkConconf=new SparkConf().setAppName("KMeansSample").setMaster("local")
    val sparkContext=new SparkContext(sparkConconf)

    //装载数据
    val fileData=sparkContext.textFile("/Users/yufeiwang/Desktop/fpSamples.txt",1)
    val parseData=fileData.map(record=>Vectors.dense(record.split(",").map(_.toDouble)))

    //模型训练
    val dataModelNumber=3
    val dataModelTrainTimes=20
    val model=KMeans.train(parseData,dataModelNumber,dataModelTrainTimes)
//    model.save(sc,"")

    //打印出中心点
    println("Cluster centers:")
    for (c <- model.clusterCenters) {
      println("  " + c.toString)
    }

    println("Vectors -50,-60,-10 belongs to clusters:" + model.predict(Vectors.dense("-50,-60,-10".split(',').map(_.toDouble))))


//    //交叉评估1，只返回结果
//    val testdata = fileData.map(s =>Vectors.dense(s.split(' ').map(_.toDouble)))
//    val result1 = model.predict(testdata)
//    result1.saveAsTextFile("/data/mllib/result1")
//
//    //交叉评估2，返回数据集和结果
//    val result2 = fileData.map {
//      line =>
//        val linevectore = Vectors.dense(line.split(' ').map(_.toDouble))
//        val prediction = model.predict(linevectore)
//        line + " " + prediction
//    }.saveAsTextFile("/data/mllib/result2")

    sparkContext.stop()


  }
}
