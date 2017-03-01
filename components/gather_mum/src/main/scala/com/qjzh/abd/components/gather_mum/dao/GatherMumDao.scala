package com.qjzh.abd.components.gather_mum.dao


import com.google.gson.Gson
import com.qjzh.abd.components.comp_common.common._
import com.qjzh.abd.components.comp_common.utils.GsonTools
import com.qjzh.abd.components.gather_mum.caseview._
import com.qjzh.abd.function.hbase.utils.HbaseUtils
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes

/**
  * Created by Fred on 2016/12/26.
  */
object GatherMumDao {

  /**
    * 存储 按分＼时＼天统计的人流总数＼最大值＼最小值＼历史最大值＼平均停驻时间
    *
    * @param rowKey
    * @param mSum
    * @param hSum
    * @param dSum
    * @param hisMax
    */
  def saveStaUserMacNumberCount(rowKey: String, mSum: Long, hSum: Long = 0, dSum: Long = 0, holdTime: Long = 0, hisMax: Long = 0, hisMaxTime: Long = 0) = {

    val hRowKey = rowKey.replace(",", "_")

    val valueMap = new Object2ObjectOpenHashMap[String, Long]()
    valueMap.put("mSum", mSum)
    valueMap.put("hSum", hSum)
    valueMap.put("dSum", dSum)
    valueMap.put("hisMax", hisMax)
    valueMap.put("hisMaxTime", hisMaxTime)
    valueMap.put("holdTimes", holdTime)
    HbaseUtils.writeTable(HbaseBusUtils.sta_umac_nber, hRowKey, "data", new Gson().toJson(valueMap))
  }

  //hbase 转换函数
  def staMemBerBatchWirteToHbaseFun(data: StaMemberCaseInfo): Put ={
    val put = new Put(Bytes.toBytes( data.curMinue+"_"+data.busNo ))
    put.add(Bytes.toBytes(HbaseBusUtils.hbase_common_cloumn_family), Bytes.toBytes(HbaseBusUtils.hbase_common_cloumn_family),
      Bytes.toBytes(GsonTools.gson.toJson(data)))
    put
  }

  //hbase 转换函数
  def pointMemberBatchWirteToHbaseFun(data: PointMember): Put ={
    val put = new Put(Bytes.toBytes(data.curMinue))
    put.add(Bytes.toBytes(HbaseBusUtils.hbase_common_cloumn_family), Bytes.toBytes(HbaseBusUtils.hbase_common_cloumn_family),
      Bytes.toBytes(GsonTools.gson.toJson(data)))
    put
  }


}
