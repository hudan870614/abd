package com.qjzh.abd.components.comp_common.common

import com.qjzh.abd.components.comp_common.conf.CommonConf
import com.qjzh.abd.control.common.view.Report
import com.qjzh.abd.function.hbase.utils.HbaseUtils


/**
  * Created by hushuai on 16/12/17.
  */
object HbaseBusUtils {


  /**
    * 全量手机设备标签表
    * rowKey:userMac
    * column=data:label
    */
  val user_label_info=CommonConf.project_no+"_7g_user_label_info"

  val sta_umac_nber = CommonConf.project_no+"_7g_sta_umac_nber"

  val sta_user_move_out = CommonConf.project_no+"_7g_sta_user_move_out"

  val user_point_track = CommonConf.project_no+"_7g_user_point_track"

  //保存探针数据  rowKey=userMac colunmName:日期精确到分钟 value={"apMac":"000000000003","rssi":"3"}
  val user_recode_track = CommonConf.project_no+"_7g_user_recode_track"

  val sta_impr_day_nber = CommonConf.project_no+"_7g_sta_impr_day_nber"

  val sta_user_type=CommonConf.project_no+"_7g_sta_user_type"

  val sta_umac_brand_nber=CommonConf.project_no+"_7g_sta_umac_brand_nber"

  val forecast_umac_nber=CommonConf.project_no+"_7g_forecast_umac_nber"

  val forecast_umac_nber_history=CommonConf.project_no+"_7g_forecast_umac_nber_history"

  val forecast_para        = CommonConf.project_no+"_7g_forecast_para"

  val forecast_final_para  = CommonConf.project_no+"_7g_forecast_final_para"

  val HBASE_WARN_TABLE = CommonConf.project_no+"_7g_warnwaterlevel"   //保存在hbase的骤增表    //rowkey = 201611111041_OUTER_warnwaterlevel_7

  val HBASE_TABLE_DETAIL_USER_MAC = CommonConf.project_no+"_7g_Hours_summary_list"  //保存每小时  ,  每分钟    用户mac汇总清单

  val user_gather_descri = CommonConf.project_no+"_7g_user_gather_descri"    //移动终端迁入迁出明细表

  val user_inout_descri = CommonConf.project_no+"_7g_user_inout_descri"   //存储用户出入关记录表
  val forecast_umac_nber_analyse = CommonConf.project_no+"_7g_forecast_umac_nber_analyse"
  /**
    * 统一列族名称
    */
  val hbase_common_cloumn_family = "data"

  val da_umac_behaviour=CommonConf.project_no+"_7g_da_umac_behaviour"
  val da_umac_brand=CommonConf.project_no+"_7g_da_umac_brand"

  //历史热力图
  val HBASE_7gG_STA_HEAT_AREA_HIS = CommonConf.project_no+"_7g_sta_heat_area_his"


  val LIMIT_ONE_DAY_TTL_TIME = 60 * 60 * 24


  /**
    * get the labe lof current User. One of the following
    * @param report
    * @return
    */
  def getUserLabel(report: Report): String = {

    var label: String = ""

    val userLabelInfoMaps = HbaseUtils.readTable(HbaseBusUtils.user_label_info, report.userMac)
    if (userLabelInfoMaps != null && userLabelInfoMaps.containsKey("data")) {
      val macLableJson = userLabelInfoMaps.get("data")
      if (macLableJson.indexOf(CommonConf.LABLE_WORRKER_DATA) != -1) {
        label = "staff"
      } else if (macLableJson.indexOf(CommonConf.LABLE_CORE_ALL_DATA) != -1) {
        label = "keyPass"
      } else if (macLableJson.indexOf(CommonConf.LABLE_GUEST_ONALLY_DATA) != -1) {
        label = "occasionalPass"
      } else if (macLableJson.indexOf(CommonConf.LABLE_GUEST_ACT_DATA) != -1) {
        label = "activePass"
      } else if (macLableJson.indexOf(CommonConf.LABLE_GUEST_FIR_DATA) != -1) {
        label = "firstPass"
      }
    } else {
      label = "firstPass"
    }

    label
  }
}
