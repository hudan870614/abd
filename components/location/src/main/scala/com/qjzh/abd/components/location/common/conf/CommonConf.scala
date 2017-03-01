package com.qjzh.abd.components.location.common.conf

import it.unimi.dsi.fastutil.objects.{Object2ObjectOpenHashMap, ObjectArrayList}

/**
  * Created by yufeiwang on 20/12/2016.
  */
object CommonConf {

  /**
    * Dao config settings
    */
  val hs_location_test = "7g_hs_position_test"

  /**
    * Connection config settings
    */
  var topics = "7g_mac_flow"

  /**
    * Default RSSI strength of 1 meter, RSSI-distance ratio
    */
  val DEFAULT_P0 = 50
  val DIST_RATIO = 1

  /**
    * The P0 and n for each mobile brand.
    */
  val MobileParams = new Object2ObjectOpenHashMap[String, (Double, Double)]()
  MobileParams.put("apple", (52, 0.65))


  //----------------------- HG project location specific rules. Refactor this. -------------------------------------//


  var apDevices = new Object2ObjectOpenHashMap[String, (String,String)]()
  apDevices.put("b0f9639e2e20".toUpperCase(),("444","431"))
  apDevices.put("b0f963e002c0".toUpperCase(),("284","431"))
  apDevices.put("b0f963e006a0".toUpperCase(),("109","431"))
  apDevices.put("B0F963E017A0".toUpperCase(),("166","587"))
  apDevices.put("B0F963E017C0".toUpperCase(),("259","507"))
  apDevices.put("B0F963E017E0".toUpperCase(),("226","586"))
  apDevices.put("B0F963E01820".toUpperCase(),("199","508"))
  apDevices.put("b0f963e018c0".toUpperCase(),("165","431"))
  apDevices.put("B0F963E01980".toUpperCase(),("136","509"))
  apDevices.put("b0f963e01b20".toUpperCase(),("346","431"))
  apDevices.put("b0f963e01e20".toUpperCase(),("222","431"))


  /**
    * Area names for HG project
    */
  val SHENGANG = "AREA_ShenGangTongDao"
  val BIANJIAN = "AREA_BianJian"
  val HUANCHONG = "AREA_HuanChongQu"
  val XITONG = "AREA_XiTongDao"
  val CHAYAN = "AREA_ChaYanQu"
  val DONGTONG = "AREA_DongTongDao"
  val GUOJIAN = "AREA_GuoJianQu"
  val DATING = "AREA_DaTingWaiWei"
  val DEFAULT = "AREA_Default"

  // Area conner coordiantes
  val areaCoors = new Object2ObjectOpenHashMap[String, List[List[Double]]]()
  areaCoors.put(SHENGANG, List(List(95.0, 159.0, 0.0, 32.0)))
  areaCoors.put(BIANJIAN, List(List(26.0, 360.0, 32.0, 352.0)))
  areaCoors.put(HUANCHONG, List(List(26.0, 360.0, 352.0, 386.0)))
  areaCoors.put(XITONG, List(List(226.0, 293.0, 386.0, 554.0)))
  areaCoors.put(CHAYAN, List(List(159.0, 226.0, 386.0, 554.0)))
  areaCoors.put(DONGTONG, List(List(92.0, 159.0, 386.0, 554.0)))
  areaCoors.put(GUOJIAN, List(List(92.0, 293.0, 554.0, 637.0)))
  areaCoors.put(DATING, List(List(92.0, 293.0, 637.0, 721.0)))


  //The relationship between RSSI strength and distance
  val pointMile: Float = 5.6F
  var pointRange = new ObjectArrayList[((Float, Float), Float)]()
  pointRange.add(((0, -27), 50 * pointMile))
  pointRange.add(((-28, -36), 60 * pointMile))
  pointRange.add(((-37, -44), 70 * pointMile))
  pointRange.add(((-45, -52), 80 * pointMile))
  pointRange.add(((-53, -60), 90 * pointMile))
  pointRange.add(((-61, -69), 100 * pointMile))
  pointRange.add(((-70, -1000), 110 * pointMile))

  /**
    * Location area rule. Should be project-dependent
    * This is for Haiguan project
    */

  val apMacGroupA = new Object2ObjectOpenHashMap[Int, String]()
  apMacGroupA.put(12, "b0f9639e2e20".toUpperCase)
  apMacGroupA.put(11, "b0f963e006a0".toUpperCase)
  apMacGroupA.put(10, "b0f963e018c0".toUpperCase)
  apMacGroupA.put(9, "b0f963e01e20".toUpperCase)
  apMacGroupA.put(8, "b0f963e002c0".toUpperCase)
  apMacGroupA.put(7, "b0f963e01b20".toUpperCase)

  val apMacGroupB = new Object2ObjectOpenHashMap[Int, String]()
  apMacGroupB.put(6, "b0f963e01ac0".toUpperCase)
  apMacGroupB.put(5, "b0f963e01980".toUpperCase)
  apMacGroupB.put(4, "b0f963e01820".toUpperCase)
  apMacGroupB.put(3, "b0f963e017c0".toUpperCase)

  val apMacGroupBPlus = new Object2ObjectOpenHashMap[Int, String]()
  apMacGroupB.put(6, "b0f963e01ac0".toUpperCase)
  apMacGroupB.put(5, "b0f963e01980".toUpperCase)
  apMacGroupB.put(4, "b0f963e01820".toUpperCase)

  val apMacGroupC = new Object2ObjectOpenHashMap[Int, String]()
  apMacGroupC.put(1, "b0f963e017e0".toUpperCase)
  apMacGroupC.put(2, "b0f963e017a0".toUpperCase)

  //查验区规则
  val apMacGroupSpecialA = new Object2ObjectOpenHashMap[Int, String]()
  apMacGroupSpecialA.put(11, "b0f963e006a0".toUpperCase)
  apMacGroupSpecialA.put(10, "b0f963e018c0".toUpperCase)
  apMacGroupSpecialA.put(5, "b0f963e01980".toUpperCase)
  apMacGroupSpecialA.put(4, "b0f963e01820".toUpperCase)

  //东通道规则
  val apMacGroupSpecialB = new Object2ObjectOpenHashMap[Int, String]()
  apMacGroupSpecialB.put(6, "b0f963e01ac0".toUpperCase)
  apMacGroupSpecialB.put(11, "b0f963e006a0".toUpperCase)
  apMacGroupSpecialB.put(12, "b0f9639e2e20".toUpperCase)


  //西通道规则
  val apMacGroupSpecialC = new Object2ObjectOpenHashMap[Int, String]()
  apMacGroupSpecialC.put(8, "b0f963e002c0".toUpperCase)
  apMacGroupSpecialC.put(7, "b0f963e01b20".toUpperCase)
  apMacGroupSpecialC.put(3, "b0f963e017c0".toUpperCase)

  //-----------------------------------------------------------------------------------------------------------------//

  val eastOrWestTable : String = "ew_history"
  var westNotEast : Boolean = true

  //东通道判定人数汇总redis rowkey
  val eastRedisKey = "count_dongtong"
  //西通道判定人数汇总redis rowkey
  val westRedisKey = "count_xitong"
  //东通西通开关
  val forceSwitch = "force_switch"//"east";"west"


  val xitongArea :List[Double] = List(226,360,397,554)
  val dongtongArea : List[Double] = List(25,159,397,554)

  val testHWOrWestTable : String = "hw_location_test"

}
