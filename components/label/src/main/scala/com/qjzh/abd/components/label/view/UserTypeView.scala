package com.qjzh.abd.components.label.view

/**
  * Created by hushuai on 16/12/30.
  */
object UserTypeView {

}

case class UserTypeClass(var timestamp:String=null,var ymd:String=null,var hour:String=null,var minute:String=null,
                         var usermac:String=null,var usertype:String=null) extends Serializable
/**
  * 解析对象
  * @param total          总人数
  * @param keyPass        重点旅客
  * @param firstPass      首次旅客
  * @param occasionalPass 偶尔旅客
  * @param activePass     活跃旅客
  * @param staff          工作人员
  */
case class UserTypeHbaseCass(var total:Long = 0 ,var keyPass:Long = 0,var firstPass:Long = 0,
                     var occasionalPass:Long = 0,var activePass:Long = 0,
                     var staff:Long=0) extends Serializable
