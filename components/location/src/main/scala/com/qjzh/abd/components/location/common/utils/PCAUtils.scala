package com.qjzh.abd.components.location.common.utils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.feature.{PCA, PCAModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vector

object PCAUtils {

  def PCAwithDim(dimNum : Int, fullDim : RDD[String]) : RDD[String] ={
    val parseData = fullDim.map{ line =>
      val part = line.split(',')
      Vectors.dense(part.map(_.toDouble))
    }

    val model = new PCA(dimNum).fit(parseData)

    model.transform(parseData).map(x => x.toString)
  }

  def getModel(dimNum : Int, fullDim : RDD[String]) : PCAModel={
    val parseData = fullDim.map{ line =>
      val part = line.split(',')
      Vectors.dense(part.map(_.toDouble))
    }

    val model = new PCA(dimNum).fit(parseData)
    model
  }


  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("PCA example").setMaster("local")
    val sc = new SparkContext(conf)

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.Server").setLevel(Level.OFF)

    val data = sc.textFile("/Users/yufeiwang/Desktop/pca.data")
    //data.foreach(println)

    val parseData = data.map{ line =>
      val part = line.split(',')
      Vectors.dense(part.map(_.toDouble))
    }

    val model = new PCA(3).fit(parseData)



    model.transform(parseData).foreach(println)
    //--------------------------------------------------------------------------
    /**
      * [-198.49935555431662,61.7455925014451,-33.61561582724634]
        [-142.6503762139188,42.83576581230462,-27.723300375043127]
        [-94.48444346449276,37.63137787042039,-18.467916687311757]
        [-93.78770648660057,53.13442729915277,-20.324679585348406]
        [-115.21309309209421,64.72629901491086,-24.068684431501]
        [-141.13717390563068,62.443549430022024,-32.15482042868974]
        [-139.84404002633448,85.49929177772042,-26.90430756804854]
        [-106.34627395862736,57.60589638943985,-23.47345414370614]
        [-254.30867520979697,40.87956572432333,-12.424267061380176]
        [-146.56200808994245,52.842236008590454,-16.703674457958243]
        [-170.42181527333886,63.27229377718993,-21.440842300235158]
        [-139.13974251002367,74.9052975468746,-12.130842693355147]
        [-131.03062483262897,72.29955746812841,-15.20705763790804]
        [-126.21628609915788,71.19600990352119,-11.411808043562743]
        [-120.23904415710874,39.83322827884836,-26.220672650471542]
        [-97.36990893617941,43.377395313806836,-17.568739657112463]
      */
    println("---------------------------------------------------")

    sc.stop()


  }
}