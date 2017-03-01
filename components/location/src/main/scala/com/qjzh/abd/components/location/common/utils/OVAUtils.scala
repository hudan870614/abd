package com.qjzh.abd.components.location.common.utils

import org.apache.spark.ml.classification.{LogisticRegression, OneVsRest, OneVsRestModel}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yufeiwang on 16/01/2017.
  */
object OVAUtils {

  def getOVRModel(data : DataFrame) : OneVsRestModel={
    val classifier = new LogisticRegression()
      .setMaxIter(10)
      .setTol(0.01)
      .setFitIntercept(true);

    // instantiate the One Vs Rest Classifier.
    val ovr = new OneVsRest().setClassifier(classifier)

    // train the multiclass model.
    val ovrModel:OneVsRestModel = ovr.fit(data)
    ovrModel
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("PCA example").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)



    // Load training data in LIBSVM format.
    //  val data = MLUtils.loadLibSVMFile(sc, "/Users/yufeiwang/Desktop/sample_libsvm_data.txt")

    val data = sqlContext.read.format("libsvm").load("/Users/yufeiwang/Desktop/sample_libsvm_data_copy.txt")


//    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
//    val training = splits(0).cache()
//    val test = splits(1)

    // instantiate the base classifier
    val classifier = new LogisticRegression()
      .setMaxIter(30)
      .setTol(0.1)
      .setFitIntercept(true);

    // instantiate the One Vs Rest Classifier.
    val ovr = new OneVsRest().setClassifier(classifier)

    // train the multiclass model.
    val ovrModel:OneVsRestModel = ovr.fit(data)

    // score the model on test data.
//    val pointList = List("","")
//    val test = sqlContext.createDataFrame(List())
    val test = sqlContext.read.format("libsvm").load("/Users/yufeiwang/Desktop/libsvm_test.txt")
    val predictions = ovrModel.transform(test)

    val predictionAndLabels = predictions.select("prediction", "label")
    predictionAndLabels.show()

//    // obtain evaluator.
//    val evaluator = new MulticlassClassificationEvaluator()
//
//    // compute the classification error on test data.
//    val accuracy = evaluator.evaluate(predictions)
//    println(s"Test Error = ${1 - accuracy}")
//    // $example off$

    sc.stop()
  }
}
