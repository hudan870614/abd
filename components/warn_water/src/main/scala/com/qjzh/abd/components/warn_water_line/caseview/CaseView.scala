package com.qjzh.abd.components.warn_water_line.caseview

/**
  * 配置信息案例类存放位置
  * Created by 92025 on 2017/2/20.
  */
object CaseView {

}

/**
  * Ap配置对象
  * @param apMac    ap编号
  * @param awValue  警戒值
  * @param ewValue  预警值
  */
case class ApCase(val apMac: String,val awValue: Long,val ewValue: Long) extends Serializable