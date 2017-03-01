package com.qjzh.abd.components.hw.in_out_move.dao.redis

import com.qjzh.abd.components.comp_common.caseciew.SynAreaBean
import com.qjzh.abd.components.comp_common.common.RedisBusUtils
import com.qjzh.abd.components.comp_common.utils.ComUtils
import com.qjzh.abd.components.hw.in_out_move.caseview.AreaInfoJoin
import com.qjzh.abd.control.common.view.Report
import com.qjzh.abd.function.redis.utils.RedisUtils

import scala.collection.mutable.ListBuffer

/**
  * 区块信息与redis的交互层
  * Created by 92025 on 2017/2/22.
  */
object AreaRedisDao {

  val utils = ComUtils
  val redisSession = RedisUtils
  val redisBusSession = RedisBusUtils
  /**
    * 判断当前点位是否在所属区域下
    * @param point 目标点位
    * @param area 关联区域
    * @return (1:属于区域内;0:在区域边线上;-1:在区域之外)
    */
  def isContainInArea(point : Report, area: SynAreaBean) : Int = {
    utils.isContainInArea(point,area)
  }



  /**
    * 获取Area配置信息
    * @return RDD[(String, ApCase)]  K:APMAC   V:AP对象
    */
  def getAreaList(): List[SynAreaBean] ={
    redisBusSession.getRedisAreaMap()
  }
  /**
    * 查询区块的详细信息
    * 级别维表信息
    */
  def queryAreaInfoList(): ListBuffer[AreaInfoJoin] ={
    //级别  1    2    3

    //一级   1
    //二级   2 3 4
    //三级   5 6 7 8

    //关系   1 ---->  2 3 4
    //       2 ---->  5
    //       3 ---->  6 7
    //       4 ---->  8
    ListBuffer(AreaInfoJoin(1,1,-1,1),AreaInfoJoin(2,2,1,1),AreaInfoJoin(3,2,1,1),AreaInfoJoin(4,2,1,1)
    ,AreaInfoJoin(5,3,2,1),AreaInfoJoin(6,3,3,1),AreaInfoJoin(7,3,3,1),AreaInfoJoin(8,3,4,1))
  }

  /**
    * hash 设置值
    * @param key
    * @param fild
    * @param value
    * @param ttl
    */
  def hset(key:String,fild:String,value:String,ttl:Int = 0): Unit ={
    redisBusSession.hset(key,fild,value,ttl)
  }

  def lpush(key:String,value: String,ttl:Int = 0): Unit ={
    redisBusSession.lpush(key,value,ttl)
  }

  def llen(key:String): Long ={
    redisBusSession.llen(key)
  }

  def rpop(key:String,ttl:Int = 0): Unit ={
    redisBusSession.rpop(key)
  }

  def lpushRpopByLenth(key:String,value: String,lenth: Long,ttl:Int = 0): Unit ={
    lpush(key,value,0)
    val llen1: Long = llen(key)
    if(llen1 > lenth){rpop(key,ttl)}
  }

  def queryRedisKV(key: String): String ={
    redisBusSession.get(key)
  }
  def saveOrUpdateKV(key: String,value: String,ttl:Int = 0): Unit ={
    redisBusSession.set(key,value,ttl)
  }

  def main(args: Array[String]): Unit = {
    val stringToInt: Map[String, Int] = Map("1" -> 1,"1" -> 3,"2" -> 2)
    println(stringToInt)
  }
}
