package com.qjzh.abd.control.common.view

import com.qjzh.abd.control.common.utils.Tool
import com.qjzh.abd.function.common.DateUtils

/**
  * Created by hudan
  */
class Report extends Serializable{

  var userMac : String = null
  var apMac :  String = null
  var rssi : String = null
  var timeStamp : Long = 0
  var apType : String =  null
  var brand : String =  null
  var projectNo : String = null
  //当前最小维度区域编码
  var areaNo : String = null
  var isFake : Int  = 0
  var pointX : Double = 0.0f
  var pointY : Double  = 0.0f
  var pointZ : Double = 0.0f
  //点位编码
  var mapID : String = null
  //区域层级关系编码，多个层级以;号分隔
  var floor : String = null
  var serverTimeStamp: Long = System.currentTimeMillis()
  /**
    * 内部定位专用
    * @return
    */
  var coordinateX : Double = 0.0f
  var coordinateY : Double  = 0.0f
  var coordinateZ : Double  = 0.0f

  override def toString: String ={
    val jsonReport =  Tool.gson.toJson(this)
    jsonReport
  }

  def toRecodeStr: String = {
    this.userMac+","+this.apMac+","+this.rssi+","+this.timeStamp+","+this.apType+","+this.brand+","+this.projectNo +
      ","+this.areaNo+","+this.isFake
  }

  def toPointStr: String = {
    this.userMac+","+this.timeStamp+","+this.pointX+","+this.pointY+","+this.pointZ+","+this.areaNo+","+this.mapID
  }



}
