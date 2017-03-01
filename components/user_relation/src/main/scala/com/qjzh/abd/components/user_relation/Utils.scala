package com.qjzh.abd.components.user_relation

import com.qjzh.abd.components.comp_common.utils.GsonTools

import scala.util.Random

/**
  * Created by 92025 on 2017/1/17.
  */
object Utils {
  private val random: Random = new Random()

  /**
    * 获取指定范围的随机数
    * @param range
    */
  def getRandom(range: Int): Long ={
    random.nextInt(range).toLong
  }

  def bisnuss(f:Student => String,student: Student): Unit ={
    val flag = false
    if(flag){
      f(student)
    }else{
      println("函数没有执行.....")
    }
  }

  def callBack(student: Student): String ={
    println(student)
    GsonTools.gson.toJson(student)
  }

  def main(args: Array[String]): Unit = {
    val bisnuss1: Unit = bisnuss(callBack,Student(18,"连雪峰"))
  }
}

case class Student(val age: Int,val name: String) extends Serializable
