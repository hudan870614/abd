package com.qjzh.abd.components.location.common.utils

import java.io._
import java.nio.file.{Files, Paths}
import java.util

import com.google.gson.Gson
import org.apache.spark.ml.classification.OneVsRestModel
import org.apache.spark.mllib.feature.PCAModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created by yufeiwang on 20/12/2016.
  */
object CommonUtils {
  val gson: Gson = new Gson()


  var basePath: String = _
  var conf: SparkConf = _
  var sc: SparkContext = _
  var sqlContext: SQLContext = _

  def main(args: Array[String]): Unit = {

    basePath = "/home/qjzh/fingerPrint"
    conf = new SparkConf().setAppName("finger print demo").setMaster("local")
    sc= new SparkContext(conf)
    sqlContext = new SQLContext(sc)

    initModels(100,100,7)
    testModels()
  }

  def testModels() = {
    val pcaModel = deserialize[PCAModel](null, "pcaModel.txt")
    val ovrModel = deserialize[OneVsRestModel](null, "ovrModel.txt")
    val seeds = deserialize[util.ArrayList[String]](null, "seeds.txt")

    //生成测试值
    val df: DataFrame = sqlContext.createDataFrame(sc.parallelize(makeTestList(pcaModel, seeds)))

    //计算分类结果，打印计算时间
    val start: Long = System.currentTimeMillis()
    val predictions = getPrediction(ovrModel, df)
    val end: Long = System.currentTimeMillis()

    //检查准确率
    println(calculateCorrectness(predictions))
    println("Time taken: " + (end - start))
  }

  def calculateCorrectness(predictions: DataFrame): String = {
    val allCount = predictions.count()
    val correctCount = predictions.filter("prediction = label").count()
    "Correctness: " + correctCount.toDouble / allCount.toDouble
  }

  /**
    * 生成用来生成特征数组的seeds
    *
    * @return
    */
  def genSeeds(num: Int): util.ArrayList[String] = {
    val seeds: util.ArrayList[String] = new util.ArrayList[String]
    for (i <- 1 to num) {
      var seedCandi: String = ""
      for (i <- 0 to 15) {
        seedCandi = seedCandi + ((Math.ceil(Math.random() * 5) + 4).toInt.toString)
      }
      if (!seeds.contains(seedCandi))
        seeds.add(seedCandi)
      seedCandi = ""
    }
    util.Collections.sort(seeds)
    seeds
  }

  def genTrainingData(seeds: util.ArrayList[String], sampleCount: Int) = {
    val ito = seeds.iterator()
    while (ito.hasNext) {
      val seed = ito.next()
      var source: List[String] = Nil
      for (i <- 1 to sampleCount) {
        source = source :+ genRssiTuple(seed, 10)
      }
      val writer = new PrintWriter(new File(basePath + "point_" + seeds.indexOf(seed) + ".txt"))
      for (elem <- source) {
        writer.write(elem + "\n")
      }
      writer.close()
    }
  }


  def makeTestList(model: PCAModel, seeds: util.ArrayList[String]): List[LabeledPoint] = {
    var r: List[LabeledPoint] = Nil
    val seed = seeds.get(10)
    for (i <- 0 to 999) {
      r = r :+ LabeledPoint(10, model.transform(Vectors.dense(genRssiTuple(seed, 8).split(',').map(_.toDouble))))
    }
    r
  }

  def initModels(seedNum: Int, sampleCount: Int, dimNum: Int) = {
    val seeds = genSeeds(seedNum)
    serialize(seeds, "seeds.txt")
    genTrainingData(seeds, sampleCount)
    val model = generatePCAFiles(dimNum)
    serialize(model, "pcaModel.txt")
    val ovrModel = getOVAModel()
    serialize(ovrModel, "ovrModel.txt")
  }

  def generatePCAFiles(dimNum: Int): PCAModel = {
    var fullDimSource: List[String] = Nil

    //把所有数据汇总用来计算PCA model
    var counter: Int = 0
    while (Files.exists(Paths.get(basePath + "point_" + counter + ".txt"))) {
      for (line <- Source.fromFile(basePath + "point_" + counter + ".txt").getLines) {
        fullDimSource = fullDimSource :+ line
      }
      counter += 1
    }
    counter -= 1

    val fullDimData = sc.parallelize(fullDimSource)

    //得到PCA model
    val model = PCAUtils.getModel(dimNum, fullDimData)

    //Save the PCAed data to seperated files
    for (i <- 0 to counter) {
      var singlePointRssiList: List[String] = Nil
      for (line <- Source.fromFile(basePath + "point_" + i + ".txt").getLines) {
        singlePointRssiList = singlePointRssiList :+ line
      }
      val singlePointData = sc.parallelize(singlePointRssiList)
      val parseSinglePointData = singlePointData.map { line =>
        val part = line.split(',')
        Vectors.dense(part.map(_.toDouble))
      }
      val writer = new PrintWriter(new File(basePath + "point_" + i + "_PCA.txt"))
      for (elem <- model.transform(parseSinglePointData).collect()) {
        var singleOutput = i + " "
        val parts = elem.toArray
        for (j <- 0 to parts.length - 1) {
          singleOutput = singleOutput + " " + (j + 1) + ":" + parts(j)
        }
        writer.write(singleOutput + "\n")
      }
      writer.close()
    }
    model
  }

  def getOVAModel(): OneVsRestModel = {
    val data = sqlContext.read.format("libsvm").load(basePath+"*PCA.txt")
    OVAUtils.getOVRModel(data)
  }

  def getPrediction(model: OneVsRestModel, data: DataFrame): DataFrame = {
    val predictions = model.transform(data)

    //Mock that the results are used
    predictions.foreach(x => {
      val a = x.get(0)
    })
    predictions
  }


  /**
    * generate a rssi tuple with a given seed and amplitude. The length of tuple should be the same as length of seed
    *
    * @param seed
    * @return
    */
  def genRssiTuple(seed: String, amp: Int): String = {
    var rssiTuple: String = ""
    for (i <- 0 to seed.length - 1) {
      val postfix = if (i != seed.length - 1) "," else ""
      val seedDigit = seed.charAt(i).toInt * (-1)
      rssiTuple = rssiTuple + genRssi(seedDigit, amp) + postfix
    }
    rssiTuple
  }

  /**
    * generate rssi with given seed and amplitude
    *
    * @param seed
    * @param amp
    * @return
    */
  def genRssi(seed: Int, amp: Int): Int = {
    (Math.random() * (amp) + seed - amp / 2).round.toInt
  }


  def serialize[T](o: T, fileName: String) {
    val bos = new FileOutputStream(basePath + fileName)
    //基于磁盘文件流的序列化
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(o)
    oos.close()
  }

  def deserialize[T](bytes: Array[Byte], fileName: String): T = {
    val bis = new FileInputStream(basePath + fileName)
    val ois = new ObjectInputStream(bis)
    ois.readObject.asInstanceOf[T]
  }
}
