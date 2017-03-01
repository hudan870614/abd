package com.qjzh.abd.components.comp_common.common

import com.google.gson.Gson
import com.qjzh.abd.components.comp_common.caseciew.{SettingRedisData, SynApDeviceBean, SynAreaBean}
import com.qjzh.abd.components.comp_common.conf.{CommDataTest, CommonConf}
import com.qjzh.abd.components.comp_common.utils.{ComUtils, GsonTools}
import com.qjzh.abd.control.common.view.Report
import com.qjzh.abd.function.redis.utils.RedisUtils
import com.redis.RedisClient.ASC
import it.unimi.dsi.fastutil.objects.ObjectArrayList

/**
  * Created by hushuai on 16/3/25.
  */
object RedisBusUtils {


  //重点人群热力
  var sparkStreaming_heatImportCrowd_lData_his = CommonConf.project_no+"_sparkStreaming:heatImportCrowd:lData:his"
  //普通热力
  var sparkStreaming_heatArea_lData_cre = CommonConf.project_no+"_sparkStreaming:heatArea:lData:cre"
//  var sparkStreaming_heatArea_lData = "sparkStreaming:heatArea:lData"
  var sparkStreaming_heatArea_lData_his = CommonConf.project_no+"_sparkStreaming:heatArea:lData:his"
  //实时人数
  var sparkStreaming_timesMemCount_data = CommonConf.project_no+"_abd:sparkStreaming:timesMemCount:data"
  var redis_rowkey_apmac_is_avail = CommonConf.project_no+"_sparkStreaming:apmac:lastTime"
  //标签前缀
  var redis_spark_stamen_minu_prefix = CommonConf.project_no+"_spark_stamen_minu_"
  var redis_spark_max_meb_min_full_prefix = CommonConf.project_no+"_spark_max_meb_min_full_"

  //滞留的用户mac前缀   实际===  REDIS_KEY_ZL_USER_MAC + "_" + yyyyMMdd
  val REDIS_KEY_ZL_USER_MAC = CommonConf.project_no+"_sparkStreaming:warnwaterlevel:module:zl:user:mac"


  //============警戒水位配置
  val REDIS_WRAN_DATA_KEY = CommonConf.project_no+"_sparkStreaming:warnwaterlevel:module:minutedata"   //保存警戒水位的值

  val REDIS_WRAN_HOUR_DATA_KEY = CommonConf.project_no+"_sparkStreaming:warnwaterlevel:module:hourdata"  //警戒水位一小时的数据

  val REDIS_WRAN_HOUR_MAX_DATA_KEY = CommonConf.project_no+"_sparkStreaming:warnwaterlevel:module:maxhourdata"    //保存一小时最大的值

  val REDIS_WRAN_HIS_MAX_DATA_KEY = CommonConf.project_no+"_sparkStreaming:warnwaterlevel:module:hismaxdata"     //区块历史人数最大值


  //============预警告警
  val EW_AW_REDIS_KEY = CommonConf.project_no+"_sparkStreaming:info:staDetail"

  var second_one_day = 60 * 60 * 1 * 24

  var second_one_hour = 60 * 60 * 1

  var second_one_half_hour = 60 * 30

  var second_one_five_minue = 60 * 5

  var second_one_minue = 60

  def serviceInit(): Unit ={
  }

  /**
    * 是否存在于指定库中
    * @return
    */
  def hexists(redisKey : String ,fileName : String): Boolean = {
    val result = RedisUtils.getRedisClient().withClient[Boolean](x => {
      x.hexists(redisKey,fileName)
    })
    result
  }

  def saveByRowKey(rowKey:String,value:Any, ttlSecond : Long = 0, onlyIfExists: Boolean = false) : Boolean = {
    RedisUtils.getRedisClient().withClient[Boolean](x => {
      if(ttlSecond == 0 ){
        x.set(rowKey,value)
      }else{
        if(onlyIfExists && !x.exists(rowKey)){
          x.set(rowKey,value,false,com.redis.Seconds(ttlSecond))
        }else{
          x.set(rowKey,value,onlyIfExists,com.redis.Seconds(ttlSecond))
        }
      }
    })
  }


  def exists(rowKey:String):Boolean = {
    val result = RedisUtils.getRedisClient().withClient[Boolean](x => {
      x.exists(rowKey)
    })
    result
  }

  def getByRowKey(rowKey:String, defaultValue : Any = null):Any = {
    val result = RedisUtils.getRedisClient().withClient[Any](x => {
      x.get(rowKey).getOrElse(defaultValue)
    })
    result
  }
  def incrRowKey(rowKey:String,defaultValue : Long, isAddCur : Boolean = true,ttlSecond : Long = -1):Long = {
    val result = RedisUtils.getRedisClient().withClient[Long](x => {
      var tag : Long = 0l
      if(!x.exists(rowKey)){
        x.set(rowKey,defaultValue,false,com.redis.Seconds(ttlSecond))
        tag = defaultValue
      }else{
        if(isAddCur) x.incr(rowKey)
        tag = x.get(rowKey).get.toLong
      }
      tag
    })
    result
  }
  def delMapKey(rowKey:String,filed: String) = {
    RedisUtils.getRedisClient().withClient[Unit](x => {
      x.hdel(rowKey,filed)
    })
  }
  def delRowKey(rowKey:String) = {
    RedisUtils.getRedisClient().withClient[Unit](x => {
      x.del(rowKey)
    })
  }


  def getLValuesByLKey(rowKey:String):ObjectArrayList[String] = {
    val result = RedisUtils.getRedisClient().withClient[ObjectArrayList[String]]( x => {
      val result = new ObjectArrayList[String]()
      val sensSize = x.llen(rowKey).get
      for(i <- 0 until sensSize.toInt - 1){
        val one = x.lindex(rowKey,i).get
        result.add(one)
      }
      result
    })
    result
  }


  def hset(key:String,fild:String,value:String,ttl:Int = 0): Unit ={
     RedisUtils.getRedisClient().withClient[Unit]( x => {
       x.hset(key,fild,value)
      if(ttl > 0) x.expire(key,ttl)
    })
  }

  def main(args: Array[String]): Unit = {
      RedisBusUtils.getApDevicesFromRedis().foreach(println)
  }
  /**
    * K,V 对
    * @param key
    * @param value
    * @param ttl
    */
  def set(key:String,value:String,ttl:Int = 0): Unit ={
     RedisUtils.getRedisClient().withClient[Unit]( x => {
       x.set(key,value)
      if(ttl > 0) x.expire(key,ttl)
    })
  }

  /**
    * 获取map的数据
    * @param key
    * @return
    */
  def get(key:String,dv : String = ""):String = {
    RedisUtils.getRedisClient().withClient[String]( x => {
      val get1: Option[String] = x.get(key)
      get1.getOrElse(dv)
    })
  }

  def hgetall(rowKey:String):Map[String,String] = {
    RedisUtils.getRedisClient().withClient[Map[String,String]]( x => {
      x.hgetall(rowKey).get
    })
  }

  def hlen(rowKey:String):Long= {
    RedisUtils.getRedisClient().withClient[Long]( x => {
      x.hlen(rowKey).get
    })
  }
  /**
    * 获取map的数据
    * @param rowKey
    * @return
    */
  def hget(rowKey:String,mapKey:String):String = {
    RedisUtils.getRedisClient().withClient[String]( x => {
      val hget1: Option[String] = x.hget(rowKey, mapKey)
      hget1.getOrElse("")
    })
  }

  def zadd(rowkey:String, score:Double, score_name:String) = {
    RedisUtils.getRedisClient().withClient(x => {
      x.zadd(rowkey,score,score_name)
    })
  }

  def zrange(rowkey:String) = {
    RedisUtils.getRedisClient().withClient(x => {
      x.zrange(rowkey)
    })
  }

  def zrangeWithScore(rowkey:String,start : Int = 0 , end :Int = -1 ) = {
    RedisUtils.getRedisClient().withClient(x => {
      x.zrangeWithScore(rowkey,start,end,ASC)
    })
  }

  def zremrangebyrank(rowkey:String,start : Int = 0 , end :Int = -1 ) = {
    RedisUtils.getRedisClient().withClient(x => {
      x.zremrangebyrank(rowkey,start,end)
    })
  }

  /**
    * list操作
    * @param rowKey
    * @param value
    * @return
    */
  def lpush(rowKey:String, value:String) ={
    RedisUtils.getRedisClient().withClient(x => {
      x.lpush(rowKey,value)
    })
  }

  def lpop(rowKey:String) : String={
    var result : String = ""
    RedisUtils.getRedisClient().withClient(x => {
      val r:Option[String] = x.lpop(rowKey)
      if(!r.isEmpty){
        result=r.get
      }
    })
    result
  }
  def lpush(key:String,value: String,ttl:Int = 0): Unit ={
    RedisUtils.getRedisClient().withClient[Unit]( x => {
      x.lpush(key,value)
      if(ttl > 0) x.expire(key,ttl)
    })
  }

  def llen(key:String): Long ={
    RedisUtils.getRedisClient().withClient[Long]( x => {
      val llen1: Option[Long] = x.llen(key)
      llen1.getOrElse(0l)
    })
  }

  def rpop(key:String,ttl:Int = 0): Unit ={
    RedisUtils.getRedisClient().withClient[Unit]( x => {
      x.rpop(key)
      if(ttl > 0) x.expire(key,ttl)
    })
  }
  def lrange(rowKey:String, start:Int, end:Int) :List[String]={
    var result : List[String] = Nil
    RedisUtils.getRedisClient().withClient(x => {
      val r:Option[List[Option[String]]] = x.lrange(rowKey,start,end)
      if(!r.isEmpty){
        val listTmp=r.get
        for (elem <- listTmp) {
          if(!elem.isEmpty){
            result = result :+ elem.get
          }
        }
      }
    })
    result
  }

  def ltrim(rowKey:String, start:Int, end:Int)={
    RedisUtils.getRedisClient().withClient(x => {
      x.ltrim(rowKey,start,end)
    })
  }



  /**
    * 根据key的类型在redis中找到历史最大值
    * （没有的话就新增，有的话就比较大小，若比redis中的大，就更新到redis中）
    * 注：历史最大值 将存入到redis中，后续按需要落地到关系数据中
    * @param rowKey
    * @param data
    * @return
    */
  def getHisMaxByType(rowKey:String, data : (Long,Long), ttlSecond : Long = 0) : (Long,Long) = {
    val hRowKey = "7g_hismax_"+rowKey.replace(",","_")

    val curDateTime = data._1
    val curCount = data._2

    var returnData : (Long,Long) = (curDateTime,curCount)

    val rMaxDates = RedisBusUtils.getByRowKey(hRowKey)

    if(rMaxDates != null && rMaxDates.toString.indexOf(",") != -1 &&  rMaxDates.toString.split(",")(1).toInt > curCount){
      val str =  rMaxDates.toString.split(",")
      returnData = (str(0).toLong,str(1).toLong)
    }else RedisBusUtils.saveByRowKey(hRowKey,returnData._1+","+returnData._2, ttlSecond)

    returnData
  }

  /**
    * 加载基础配置项
    */
  def getSideLen(): Double ={
    var area_side_dis = CommonConf.area_side_dis
    if (!RedisBusUtils.getRedisSetting().isEmpty) {
      val confSet = getRedisSetting().head
      area_side_dis = confSet.htGridSideLen
    }
    area_side_dis
  }

  /**
    * 加载区域缓存表
    * @return
    */
  def getRedisAreaMap(): List[SynAreaBean] = {

    var result = RedisUtils.getRedisClient().withClient[List[SynAreaBean]]( x => {

      x.hgetall(CommonConf.tBaseAreaRedisKey).toList.flatMap(map => {
        map.map(value => {
          GsonTools.gson.fromJson(value._2, classOf[SynAreaBean])
        })
      })

    })

    if(result == null || result.size <=0 ){
      result = CommDataTest.SynApAreaBeanTest
    }

    result
  }

  /**
    * 加载基础配置缓存表
    * @return
    */
  def getRedisSetting(): List[SettingRedisData] = {

    val result = RedisUtils.getRedisClient().withClient[List[SettingRedisData]](x => {

      x.hgetall(CommonConf.tBaseLocationRedisKey).toList.flatMap(map => {
        map.map(value => {
          GsonTools.gson.fromJson(value._2, classOf[SettingRedisData])
        })
      })
    })
    result
  }
  /**
    * 获取一个区域的信息
    * @param areaId
    * @return
    */
  def getAreaInfoByFloorAndAreaNo(areaId:Long): SynAreaBean ={
    val redisAreaMap: List[SynAreaBean] = getRedisAreaMap()
    val filter: List[SynAreaBean] = redisAreaMap.filter(xx => {
       xx.id == areaId
    })
    if(filter != null && filter.size > 0){
      return filter(0)
    }
    null
  }

  /**
    * 根据apMac查找点位信息
    * @param apMac
    * @return
    */
  def getApPointByApMac(apMac : String): SynApDeviceBean ={
    val apPoints : List[SynApDeviceBean] = getApDevicesFromRedis()
    apPoints.filter(_.apMac.equalsIgnoreCase(apMac)).head
  }


  /**
    * 根据当前经纬度找到所属区域
    * @param lon
    * @param lat
    * @return
    */
  def getAreaInfoByLonLat(lon : Double, lat : Double): List[String] ={
    getRedisAreaMap().map(a => {
      (a,ComUtils.isContainInArea2(lon,lat, a.points) )
    }).filter(_._2).sortBy(-_._1.areaLevel).map(_._1.id.toString)
  }


  /**
    * 从redis里拿到海关项目所有的AP信息
    *
    * @return
    */
  def getApDevicesFromRedis(): List[SynApDeviceBean] = {
    var result = RedisUtils.getRedisClient().withClient[List[SynApDeviceBean]](x => {
      x.hgetall(CommonConf.tBaseApRedisKey).toList.flatMap(map => {
        map.map(value => {
          val gson: Gson = new Gson
          gson.fromJson(value._2, classOf[SynApDeviceBean])
        })
      })
    })
    //加载测试数据
    if(result == null || result.size <= 0){
      result = CommDataTest.SynApDeviceBeanTest
    }
    result
  }


  def isApInList(arg: (String, Report)): Boolean = {
    var r: Boolean = false
    val macs = RedisBusUtils.getApDevicesFromRedis().filter(_.apMac.equalsIgnoreCase(arg._2.apMac))
    if (macs.length > 0) {
      r = true
    }
    r
  }

  def isApInList(report : Report): Boolean = {
    var r: Boolean = false
    val macs = RedisBusUtils.getApDevicesFromRedis().filter(_.apMac.equalsIgnoreCase(report.apMac))
    if (macs.length > 0) {
      r = true
    }
    r
  }


}
