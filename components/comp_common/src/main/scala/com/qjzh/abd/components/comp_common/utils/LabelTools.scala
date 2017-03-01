package com.qjzh.abd.components.comp_common.utils

import com.qjzh.abd.components.comp_common.caseciew.hdfs_view_label
import com.qjzh.abd.components.comp_common.common.HbaseBusUtils
import com.qjzh.abd.components.comp_common.conf.CommonConf
import com.qjzh.abd.function.hbase.utils.HbaseUtils

/**
  * Created by 92025 on 2016/10/12.
  */
object LabelTools {

    /**
      * 根据当前标签得到当前规则编码，若不是，则返回 －1
      * @param userLabels
      * @return
      */
   def getLabelRoleId(userLabels : String) : String ={
       var roleId = "-1"
       if(userLabels != null  && !userLabels.isEmpty
          &&  userLabels.indexOf(CommonConf.LABLE_CORE_RULE_DATA) != -1){
           val labels = userLabels.split("\\;").filter(_.indexOf(CommonConf.LABLE_CORE_RULE_DATA) != -1).takeRight(1).head
           if(labels.split("_").size == 3){
               roleId = labels.split("_").takeRight(1).head
           }
       }
       roleId
   }
    /**
      * get the labe lof current User. One of the following
      * @param userMac
      * @return
      */
    def getUserLabel(userMac: String): (String,String) = {
        var label_code: String = "firstPass"
        var label : String = "-1"
        val userLabelInfoMaps = HbaseUtils.readTable(HbaseBusUtils.user_label_info, userMac)
        if (userLabelInfoMaps != null && userLabelInfoMaps.containsKey("data")) {
            val viewLabel =  userLabelInfoMaps.get("data")
            label = viewLabel
            if (viewLabel.indexOf(CommonConf.LABLE_WORRKER_DATA) != -1) {
                label_code = "staff"
            } else if (viewLabel.indexOf(CommonConf.LABLE_CORE_DATA) != -1) {
                label_code = "keyPass"
            } else if (viewLabel.indexOf(CommonConf.LABLE_GUEST_ONALLY_DATA) != -1) {
                label_code = "occasionalPass"
            } else if (viewLabel.indexOf(CommonConf.LABLE_GUEST_ACT_DATA) != -1) {
                label_code = "activePass"
            } else if (viewLabel.indexOf(CommonConf.LABLE_GUEST_FIR_DATA) != -1) {
                label_code = "firstPass"
            }
        }
        (label_code,label)
    }

    /**
      * 根据当前用户得到当前的角色
      * @param userMac
      * @return
      */
    def getUserLabelType(userMac: String): (Boolean,Boolean,Boolean,Boolean,Boolean) = {
        //是否是工作人员
        var isStaContains: Boolean = false
        //是否是重点人
        var isImpContains: Boolean = false
        //是否活跃用户
        var isActContains: Boolean = false
        //是否是偶客
        var isOccContains: Boolean = false
        //是否是初次访客
        var isFistContains: Boolean = false

        val userLabelInfoMaps = HbaseUtils.readTable(HbaseBusUtils.user_label_info, userMac)
        if (userLabelInfoMaps != null && userLabelInfoMaps.containsKey("data")) {
            val macLableJson = userLabelInfoMaps.get("data")
            if (macLableJson.indexOf(CommonConf.LABLE_WORRKER_DATA) != -1) {
                isStaContains = true
            } else if (macLableJson.indexOf(CommonConf.LABLE_CORE_DATA) != -1) {
                isImpContains = true
            } else if (macLableJson.indexOf(CommonConf.LABLE_GUEST_ONALLY_DATA) != -1) {
                isOccContains = true
            } else if (macLableJson.indexOf(CommonConf.LABLE_GUEST_ACT_DATA) != -1) {
                isActContains = true
            }else if (macLableJson.indexOf(CommonConf.LABLE_GUEST_FIR_DATA) != -1) {
                isFistContains = true
            }
        } else {
            isFistContains = true
        }
        (isStaContains,isImpContains,isActContains,isOccContains,isFistContains)
    }

    /**
      * 判断是否是重点人
      * @param userMac
      * @return
      */
    def isImpCoreLabel(userMac: String) : Boolean = {
        var isRun = true
        val userLabelInfoMaps = HbaseUtils.readTable(HbaseBusUtils.user_label_info,userMac)
        if(userLabelInfoMaps != null && userLabelInfoMaps.containsKey(HbaseBusUtils.hbase_common_cloumn_family)){
            val macLableJson = userLabelInfoMaps.get(HbaseBusUtils.hbase_common_cloumn_family)
            if(macLableJson.indexOf(CommonConf.LABLE_CORE_DATA) != -1){
                isRun = false
            }
        }
        isRun
    }
}
