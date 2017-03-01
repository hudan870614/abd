package com.qjzh.abd.components.warn_water_line.utils

import com.qjzh.abd.components.warn_water_line.caseview.ApCase

import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
  * Created by 92025 on 2017/2/20.
  */
object ApUtils {
  val apMacPool = Array("AABBCC001122","AABBCC001123","AABBCC001124","AABBCC001125","AABBCC001126"
    ,"AABBCC001127","AABBCC001128","AABBCC001129","AABBCC001130","AABBCC001131")

  /**
    * 获取ap设备的方法
    * @param num
    * @param startEw
    * @param endEw
    * @param startAw
    * @param endAw
    * @return
    */
  def getApCase(num: Int,startEw:Long,endEw: Long,startAw: Long,endAw: Long): ListBuffer[ApCase] ={
    val apCases: ListBuffer[ApCase] = ListBuffer[ApCase]()
    val random: Random = new Random()

    for(i <- 0 to num){
      val int: Int = random.nextInt(apMacPool.length)
      val pool: String = apMacPool(int)
      apCases.append(ApCase(pool,random.nextInt(endAw.toInt - startAw.toInt).toLong + startAw,
        random.nextInt(endEw.toInt - startEw.toInt).toLong + startEw))
    }
    apCases
  }

}
