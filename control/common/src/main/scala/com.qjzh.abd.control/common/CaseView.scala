package com.qjzh.abd.control.common

/**
  * Created by 92025 on 2016/12/23.
  */
object CaseView {

}
case class CaseReport(val apMac : String,val timestamp: Long, val apType : String,val userMac:String,val pointX:Float, val pointY: Float, val count : Long = 1) extends Serializable
