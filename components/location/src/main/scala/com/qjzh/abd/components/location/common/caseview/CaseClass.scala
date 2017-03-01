package com.qjzh.abd.components.location.common.caseview

/**
  * Created by yufeiwang on 19/12/2016.
  */
object CaseClass {

  case class SynApDeviceBean(id: Int, mac: String, lon: Float = -1, lat: Float = -1) extends Serializable

  case class APCircle(x: Double = 0.0d, y: Double = 0.0d, r: Double = 0.0d, mac: String = null, total: Int = 0, activeAngleList : List[(Double,Double)] = Nil) extends Serializable

  /**
    *
    * @param x lon
    * @param y lat
    * @param isReloc
    * @param apMac 信号最强的 ap_mac
    */
  case class Coordinate(x: Double = 0.0d, y: Double = 0.0d, isReloc: Boolean = false, apMac :  String = null) extends Serializable

  case class GPSCoordinate(lon: Double = 0.0d, lat: Double = 0.0d, isReloc: Boolean = false) extends Serializable

}
