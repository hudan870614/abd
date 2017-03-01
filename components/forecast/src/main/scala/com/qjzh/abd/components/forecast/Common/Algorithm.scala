package com.qjzh.abd.components.forecast.Common

/**
  * Created by damon on 2016/12/20.
  */
object Algorithm {

  /**
    * 异常值检测
    * 方法：基于正态分布的一元离群点检测方法
    * 区域  包含了99.7% 的数据，
    * 如果某个值距离分布的均值 u 超过了 2@(标准差)，那么这个值就可以被简单的标记为一个异常点（outlier）。
    * */
  def TakeOutlier(listdata:List[Int]):List[Int]={
    val num = listdata.size
    var finaldata:List[Int] = null
    if( num > 2 ) {
      val Sum = listdata.sum
      val Avg = Sum / num
      //方差
      val variance      = listdata.map( x => math.pow( (x -Avg),2)).sum / num
      //标准差
      val stddeviation  = math.sqrt( variance )
      //偏差大于2倍标准差或小于1倍标准差，可认为是异常值，进行剔除
       finaldata = listdata.filter( x => {
        val tempDis = x - Avg
        -1*stddeviation < tempDis && tempDis <  2*stddeviation
      })
      }else if( num <= 2) {
          finaldata = listdata
      }
    finaldata
  }

  /**
    *
    * @param listdata   源数据
    * @param quartIndex 四分位索引
    * @return 四分位数值
    */
  def Quartile(listdata:List[Int],quartIndex:Int):Float={
    val Size      = listdata.size
    val floatQ    = (Size + 1) / 4.0 * quartIndex
    val IntQ      = floatQ.toInt
    val weightQ   = floatQ - IntQ
    val Q         = listdata(IntQ-1) + (listdata(IntQ)-listdata(IntQ-1))*weightQ
     Q.toFloat
  }
  /**
    *箱线图剔除异常值
    * @param listdata
    * @return
    */
  def BoxPlot(listdata:List[Int]):List[Int]={
    val sortList = listdata.sorted
    val Size  = sortList.size
    val min   = sortList.min
    val max   = sortList.max

    /**
      * 第一四分位数Q1
      * 等于该样本中所有数值由小到大排列后第25%的数字。
      * */
    val Q1 = Quartile(sortList,1)
    /**
      * 第二四分位数 (Q2)，
      * 又称中位数（Median）将数据排序（从大到小或从小到大）后，位置在最中间的数值
      */
    val Q2 = Quartile(sortList,2)

    /**
      * 第三四分位数Q1
      * 等于该样本中所有数值由小到大排列后第75%的数字。
      * */
    val Q3 = Quartile(sortList,3)

    /**
      * 四分位数间距（IQR，interquartile range），
      * 又称”内距”,是上四分位数与下四分位数之差,用四分位数间距可反映变异程度的大小。
      */
    val IQR = math.abs(Q1 - Q3)

    /**
      * 内限:Q1-0.5*IQR,Q3+0.5*IQR称为内限。
      * 异常点（outliers）：超出内限的值称为异常点。
      */
    val error_down  = Q1 - 0.5 * IQR
    val error_up    = Q3 + 0.5 * IQR

    listdata.filter(x => x >= error_down && x <= error_up)
  }

  /**
    * 功能:加权历史数据
    * @param listdata 数据
    * @param weight_cur 当前数据权重
    * @param weight_his 历史数据权重
    */
  def PreData(listdata:List[Int],weight_cur:Float,weight_his:Float):Int={
    val length = listdata.length
    var predata:Int = 0
    if( length >= 3)
    {
      val initsum = listdata(0) + listdata(1) + listdata(2)
      val avg = initsum / 3
      predata = (listdata(0)*weight_cur + avg * weight_his).toInt
      for(i <- 1 to length-1){
        val temppredata = (listdata(i)*weight_cur +predata* weight_his)
        predata = temppredata.ceil.toInt
      }
    }else if(length > 0){
      predata = (listdata.sum / listdata.length)
    }
    predata
  }

  /**
    * 欧氏距离转相似度
    * @param listdata1
    * @param listdata2
    * @return -1 数据为异常值 相似度 转为0 - 1 之间
    */
  def EuclideanDistance(listdata1:List[Int],listdata2:List[Int]):Double={
    var euDis:Double = -1
    if(listdata1.size >0 && listdata1.size == listdata2.size)
    {
      var sum:Double=0
      //归一化
      var listMax1 = listdata1.max
      var listMax2 = listdata2.max
      var max:Double = 0
      if(listMax1 > listMax2)max = listMax1 else max = listMax2

      val NormList1 = listdata1.map(_/max)
      val NormList2 = listdata2.map(_/max)
      for(i <- 0 to NormList1.size-1)
      {
        sum += math.pow(( NormList1(i) - NormList2(i)),2.0f)
      }
      val sqSum = math.sqrt(sum)
      euDis = 1 / (sqSum + 1)
    }
    euDis
  }

  /**
    * 残差和
    * @param listdata1
    * @param listdata2
    * @return -1 数据异常
    */
  def Residual(listdata1:List[Int],listdata2:List[Int]):Double={
    var residual:Double = -1
    if(listdata1.size > 0 && listdata1.size == listdata2.size)
    {
      residual = 0
      for( i <- 0 to listdata1.size-1)
      {
        residual += math.abs( listdata1(i) -listdata2(i) )
      }
    }
    residual
  }

  /**
    * 标准差
    * @param listdata1
    * @return -1 数据异常
    */
  def StandardDeviation(listdata1:List[Int]):Double={
    var sd:Double = -1f
    if(listdata1.size > 0)
    {
      var avg:Double = listdata1.sum.toDouble / listdata1.size
      var sum:Double = 0.0f;
      listdata1.foreach(x => {
        sum +=  math.pow((x - avg),2.0f)
      })
      sd = math.sqrt(sum / listdata1.size)
    }
    sd
  }

  /**
    * 皮尔森相关系数
    * @param listdata1
    * @param listdata2
    * @return -1:完全负相关 1:完全正相关 0:为不相关 -2:数据异常
    */
  def Pearson(listdata1:List[Int],listdata2:List[Int]):Double={
    var pearson:Double = -2f
    if(listdata1.size>0 && (listdata1.size == listdata2.size) )
    {
      val list1Avg:Double = listdata1.sum.toDouble / listdata1.size
      var sd1:Double       = StandardDeviation(listdata1)
      if(sd1 == 0) sd1=0.1f
      val list2Avg:Double = listdata2.sum.toDouble / listdata2.size
      var sd2:Double       = StandardDeviation(listdata2)
      if(sd2 == 0) sd2 = 0.1f
      var sumPearson:Double = 0.0f
      for(i <- 0 to listdata1.size-1 )
      {
        val d1 = (listdata1(i) - list1Avg) / sd1
        val d2 = (listdata2(i) - list2Avg) / sd2
        sumPearson += d1 * d2
      }
      pearson = sumPearson / (listdata1.size)
    }
    pearson
  }

  /**
    * 余弦相似度
    * @param listdata1
    * @param listdata2
    * @return -1 数据异常
    */
  def Cosine(listdata1:List[Int],listdata2:List[Int]): Double =
  {
    var cosine:Double = -1
    if(listdata1.size > 0 && listdata1.size == listdata2.size)
    {
      var totalmean:Double=0
      var uptotal:Double=0
      var left :Double = 0
      var right :Double = 0
      for(index <- 0 to listdata1.size-1)
      {
        uptotal += listdata1(index) * listdata2(index)
        left    +=  math.pow(listdata1(index),2)
        right   += math.pow(listdata2(index),2)
      }
      val down = (math.sqrt(left) * math.sqrt(right))
      cosine =  uptotal / down
    }
    cosine
  }

  /**
    * 均方根误差
    * @param listdata1
    * @param listdata2
    * @return -1 数据异常
    */
  def Meanavg(listdata1:List[Int],listdata2:List[Int]): Double =
  {
    var Meansquare:Double = -1
    if( listdata1.size > 0 && listdata1.size == listdata2.size)
    {
      var totalmean:Double=0
      for(index <- 0 to listdata1.size-1 )
      {
        val singlemean = math.pow((listdata1(index) - listdata2(index)),2)
        totalmean += singlemean
      }
      Meansquare = math.sqrt( totalmean / listdata1.size )
    }
    Meansquare
  }

}
