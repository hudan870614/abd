package com.qjzh.abd.components.warn_water_line.caseview

/**
  * 准备存储到Hbase的结果视图案例类存放位置
  * Created by 92025 on 2017/2/20.
  */
object HbaseCaseResult {

}

/**
  * Ap警戒水位数据结构
  * @param serverTimeStamp  服务器计算时时间
  * @param apMac              设备的mac
  * @param cruSum             当前人数
  * @param hisMaxSum         历史最大值
  * @param awSum               告警值
  * @param ewSum               预警值
  * @param isAw               是否告警
  * @param isEw               是否预警
  */
case class WarnWaterLineApCaseToHbase(serverTimeStamp:Long,apMac: String,cruSum: Long,hisMaxSum: Long,awSum: Long,ewSum: Long,isAw:Boolean,isEw:Boolean) extends Serializable

/**
  * 区块警戒水位数据结构
  * @param serverTimeStamp  服务器计算时时间
  * @param areaNo             区域ID
  * @param cruSum             当前人数
  * @param hisMaxSum         历史最大值
  * @param awSum               告警值
  * @param ewSum               预警值
  * @param isAw               是否告警
  * @param isEw               是否预警
  */
case class WarnWaterLineAreaCaseToHbase(serverTimeStamp:Long,areaNo: String,cruSum: Long,hisMaxSum: Long,awSum: Long,ewSum: Long,isAw:Boolean,isEw:Boolean) extends Serializable
