package com.qjzh.abd.components.location.common.utils

import java.io.{File, PrintWriter}

import org.apache.commons.math3.filter._
import org.apache.commons.math3.linear.{Array2DRowRealMatrix, ArrayRealVector, RealMatrix, RealVector}
import org.apache.commons.math3.random.{JDKRandomGenerator, RandomGenerator}
import org.apache.spark.ml.feature.PCAModel
import org.apache.spark.mllib.classification.SVMModel

import scala.io.Source

/**
  * Created by yufeiwang on 19/12/2016.
  */
object KalmanFilter {

  /**
    * For debugging and testing purpose only
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {

//    val source = getRssies("/Users/yufeiwang/Desktop/rssi.txt")
//    filter(source,"/Users/yufeiwang/Desktop/rssi_filtered")
    PCAModel
    SVMModel
  }

  def getRssies(fileName : String) : List[Double]={
    var rssies : List[Double] = Nil
    val file=Source.fromFile(fileName)
    for(line <- file.getLines)
    {
      if(line!=""){
        rssies = rssies :+ line.toDouble
      }
    }
    file.close
    rssies
  }

  def filter(facts: List[Double]): Double = {
    filter(facts,"")
  }

  /**
    * 卡尔曼滤波使用过程
    * 滤波精度需要调整measurementNoise, processNoise
    *
    * @param facts
    * @return
    */
  def filter(facts: List[Double],outputPath:String): Double = {
    var est: Double = 0
    val measurementNoise = 30d;
    val processNoise = 0.1d
    val stringBuffer :StringBuffer = new StringBuffer()
    // A = [ 1 ]
    val A: RealMatrix = new Array2DRowRealMatrix(Array[Double](1d))
    // no control input
    val B: RealMatrix = null
    // Q = [ 0 ]
    val Q: RealMatrix = new Array2DRowRealMatrix(Array[Double](processNoise))
    // x = [ 10 ]
    var x: RealVector = new ArrayRealVector(Array[Double](0))

    // H = [ 1 ]
    val H: RealMatrix = new Array2DRowRealMatrix(Array[Double](1d))
    // R = [ 0 ]
    val R: RealMatrix = new Array2DRowRealMatrix(Array[Double](measurementNoise))

    val pm: ProcessModel = new DefaultProcessModel(A, B, Q, x, null);
    val mm: MeasurementModel = new DefaultMeasurementModel(H, R);
    val filter: KalmanFilter = new KalmanFilter(pm, mm);

    val pNoise: RealVector = new ArrayRealVector(1);
    val mNoise: RealVector = new ArrayRealVector(1);

    val rand: RandomGenerator = new JDKRandomGenerator();

    // iterate for all values we have
    for (elem <- facts) {

      filter.predict();

      // simulate the process
      pNoise.setEntry(0, processNoise * rand.nextGaussian());

      // x = A * x + p_noise
      x = A.operate(x).add(pNoise)

      // simulate the measurement
      mNoise.setEntry(0, measurementNoise * rand.nextGaussian());

      // z = H * x + m_noise
      //val z: RealVector = H.operate(x).add(mNoise);

      //用测量出来的数据进行修正
      filter.correct((new ArrayRealVector(Array[Double](elem))).add(mNoise));

      val estimation: Double = filter.getStateEstimation()(0);
      stringBuffer.append(estimation+"\n")
      //      println(estimation)
      est = estimation
    }
    if(!outputPath.equals("")){
      val writer = new PrintWriter(new File(outputPath))
      writer.write(stringBuffer.toString)
      writer.close()
    }
    est
  }

}
