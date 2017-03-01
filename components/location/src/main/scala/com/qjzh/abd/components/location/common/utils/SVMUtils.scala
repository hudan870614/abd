package com.qjzh.abd.components.location.common.utils

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils
/**
  * Created by yufeiwang on 16/01/2017.
  */
object SVMUtils {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("PCA example").setMaster("local")
    val sc = new SparkContext(conf)


    // Load training data in LIBSVM format.
    val data = MLUtils.loadLibSVMFile(sc, "/Users/yufeiwang/Desktop/sample_libsvm_data.txt")

    // Split data into training (60%) and test (40%).
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    // Run training algorithm to build the model
    val numIterations = 100
    val model = SVMWithSGD.train(training, numIterations)

    // Clear the default threshold.
    model.clearThreshold()

    // Compute raw scores on the test set.
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    // Get evaluation metrics.
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()

    println("Area under ROC = " + auROC)

    // Save and load model
    model.save(sc, "/Users/yufeiwang/Desktop/scalaSVMWithSGDModel")
    val sameModel = SVMModel.load(sc, "/Users/yufeiwang/Desktop/scalaSVMWithSGDModel")


  }
}
