package com.qjzh.abd.components.label.service
import com.google.gson.reflect.TypeToken
import com.qjzh.abd.components.comp_common.caseciew.{MessageKafka, SynBaseRuleBean}
import com.qjzh.abd.components.comp_common.common._
import com.qjzh.abd.components.comp_common.conf.CommonConf
import com.qjzh.abd.components.comp_common.utils.{CompRedisClientUtils, GsonTools}
import com.qjzh.abd.components.label.view._
import com.qjzh.abd.control.common.view.Report
import com.qjzh.abd.function.common.DateUtils
import com.qjzh.abd.function.hbase.utils.HbaseUtils
import it.unimi.dsi.fastutil.objects.{Object2ObjectOpenHashMap, ObjectArrayList}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * 标签按天离线汇总挖掘逻辑
  * Created by hdshuai on 2016/10/24.
  */
object LabelDayService {

  /**
    * 执行当前离线任务
    * @param exeDay
    * @param sc
    */
  def dealDay(exeDay:String,sc:SparkContext): Unit = {
    //加载当天的所有记录
    val curDayRecode = loadFileByDay(exeDay,sc)
    curDayRecode.cache()
    //执行当天全量汇总
    dealDurationByDay(exeDay,curDayRecode)
    //挖掘并且保存关联标签
    dealDigLabelByDay(exeDay,curDayRecode)
  }

  /**
    * 加载当前的数据
    * @param exeDay
    * @param sc
    * @return
    */
  def loadFileByDay(exeDay:String,sc:SparkContext): RDD[(String,Report)] = {
    val allLimitApDevices = RedisBusUtils.getApDevicesFromRedis().map(_.mac)
    this.loadReCodeHdfsFileByDay(exeDay,sc).map(report => {
      (report.userMac, report)
    }).filter(x => {
      allLimitApDevices.contains(x._2.apMac)
    }).filter(_._2.isFake == 0)
//      .filter(!_._2.apMac.equalsIgnoreCase("000000000000"))

  }

  /**
    * 挖掘标签
    * @param exeDay
    * @param rdd
    */
  def dealDigLabelByDay(exeDay:String,rdd:RDD[(String,Report)]): Unit ={

    val formartExeDay = exeDay.replaceAll("\\/","")

    val pre90DaysList = HdfsUtils.getPreDayFilePath(formartExeDay, -90,"yyyyMMdd")

    val pre60DaysList = HdfsUtils.getPreDayFilePath(formartExeDay, -60,"yyyyMMdd")

    //当天所有用户
    val allUserMac = rdd.map(x => {
      (x._1,x._2.brand)
    }).distinct()
    //当天所有用户的所有90天内的历史记录
    val allUserMacDetailBeHaviour = allUserMac.map(x => {
      val durArray = HbaseUtils.findByRowKey(HbaseBusUtils.da_umac_behaviour,x._1).map(x => {
        val ic = GsonTools.gson.fromJson(x._2,classOf[ImportCrowd])
        ic.curDay = x._1
        ic
      }).filter(dur => {
        pre90DaysList.contains(dur.curDay)
      }).toArray
      (x._1,durArray)
    })

    /**
      * 首次旅客：90天内，首次出现且当天没有出现超过两次通关记录的人。
      偶尔旅客：90天内，出现天数大于一天，小于等于三天且单天没有出现超过两次通关记录的人
      活跃旅客：90天内，出现天数大于3天，小于等于12天且单天没有出现超过两次通关记录的人
      */
    val firstUserMac = allUserMacDetailBeHaviour.filter(_._2.size <= 1).map(_._1)


    val onllGenUserMac = allUserMacDetailBeHaviour.filter(x => {
      (x._2.size > 1 && x._2.size <= 3)
    }).map(_._1)


    val actGenUserMac = allUserMacDetailBeHaviour.filter(x => {
      (x._2.size > 3 && x._2.size <= 12)
    }).map(_._1)


    /**
      * 重点旅客：90天内，出现天数大于12天的人（平均每15天通关记录超过2次记录的人）
      * 新增重点旅客，每日通过规则或手动添加最新生成的重点旅客人群
      * 活跃重点旅客，最近60天内，出现天数大于等于8天的重点旅客
      * 沉默重点旅客，最近60天内，出现天数小于8天的重点旅客
      */
    val impAllUserMac = allUserMacDetailBeHaviour.filter(_._2.size > 12)

    //新增重点旅客
    val firstImpUserMac = impAllUserMac.filter(x => {
      !HbaseUtils.containRowKey(HbaseBusUtils.user_label_info,x._1)
    }).map(_._1)

    //识别最近60天的数据
    val per60DaysImpUserMac = impAllUserMac.filter(x => {
      HbaseUtils.containRowKey(HbaseBusUtils.user_label_info,x._1)
    }).map(x => {
      val durList = x._2.filter(dur => {
        pre60DaysList.contains(dur.curDay)
      })
      (x._1,durList)
    })

    //活跃重点旅客
    val actImpUserMac = per60DaysImpUserMac.filter(_._2.size >= 8).map(_._1)
    //沉默重点旅客
    val occImpUserMac = per60DaysImpUserMac.filter(_._2.size < 8).map(_._1)

    //手工规则
    val allRuleImpUserList = dealParsyHandleRule(formartExeDay,allUserMacDetailBeHaviour)

    val allRuleImpUserMac = allRuleImpUserList.flatMap(_._2.toLocalIterator.toList).distinct

    /**
      * 工作人员规则
        //工作人员：90天内，每天在线时长超过3小时或者出现频率超过5次 且出现的总天数比例大于60%，小于等于100%的人
      */
    val workerUserMac = allUserMacDetailBeHaviour.filter(_._2.size > 0.6 * 90).filter(x => {
      var isRun = false
      val result = x._2.filter(dur => {
        (dur.times < 5 || dur.duration < 3 * 60)
      })
      if(result.size ==  x._2.size){
        isRun = true
      }
      isRun
    }).map(_._1)

    //自定义规则的结果入库
    allRuleImpUserList.foreach(rule => {
      toSaveHbaseRuleAndKafka(rule._1,rule._2.toLocalIterator.toArray, allUserMacDetailBeHaviour,pre60DaysList,formartExeDay)
    })
    //特殊关注人群
    CompRedisClientUtils.getRedisRuleMacs.filter(x => {
      (x.apStatus.equals("0") && x.isUseFo == 0)
    }).map(rule => {
      val ruleMacs = impAllUserMac.map(_._1).filter(!allRuleImpUserMac.contains(_)).toLocalIterator.toArray
      toSaveHbaseRuleAndKafka(rule.id,ruleMacs, allUserMacDetailBeHaviour,
        pre60DaysList,formartExeDay)
    })

    //填充到hbase
    HbaseUtils.writeTable(HbaseBusUtils.sta_impr_day_nber, formartExeDay, "ALL",
      GsonTools.gson.toJson(HbaseImpStaCountCase(impAllUserMac.count())))
    HbaseUtils.writeTable(HbaseBusUtils.sta_impr_day_nber, formartExeDay, "NEW_ALL",
      GsonTools.gson.toJson(HbaseImpStaCountCase(firstImpUserMac.count())))
    HbaseUtils.writeTable(HbaseBusUtils.sta_impr_day_nber, formartExeDay, "ACT_ALL",
      GsonTools.gson.toJson(HbaseImpStaCountCase(actImpUserMac.count())))
    HbaseUtils.writeTable(HbaseBusUtils.sta_impr_day_nber, formartExeDay, "OCC_ALL",
      GsonTools.gson.toJson(HbaseImpStaCountCase(occImpUserMac.count())))
    HbaseUtils.writeTable(HbaseBusUtils.sta_impr_day_nber, formartExeDay, "RULE_ALL",
      GsonTools.gson.toJson(HbaseImpStaCountCase(allRuleImpUserMac.size)))


    //重点人发送到kafka
    val offLineParamKafaMap = new Object2ObjectOpenHashMap[String,Any]
    offLineParamKafaMap.put("ALL",impAllUserMac.count())
    offLineParamKafaMap.put("NEW_ALL",firstImpUserMac.count())
    offLineParamKafaMap.put("ACT_ALL",actImpUserMac.count())
    offLineParamKafaMap.put("OCC_ALL",occImpUserMac.count())
    offLineParamKafaMap.put("RULE_ALL",allRuleImpUserMac.size)
    offLineParamKafaMap.put("rowKey",formartExeDay)
    KafkaUtils.sendMsgToKafka(KafkaUtils.off_line_one_day_mub,
      MessageKafka(CommonConf.OFFLINE_IMP_STA_BY_DAY,
        DateUtils.getTime(System.currentTimeMillis(),"yyyyMMddHHmm").toLong,offLineParamKafaMap))


    //关联标签入库
    saveHbaseWithLabel(firstUserMac,CommonConf.LABLE_GUEST_FIR_DATA)
    saveHbaseWithLabel(onllGenUserMac,CommonConf.LABLE_GUEST_ONALLY_DATA)
    saveHbaseWithLabel(actGenUserMac,CommonConf.LABLE_GUEST_ACT_DATA)
    saveHbaseWithLabel(firstImpUserMac,CommonConf.LABLE_CORE_NEW_DATA)
    saveHbaseWithLabel(actImpUserMac,CommonConf.LABLE_CORE_ACT_DATA)
    saveHbaseWithLabel(occImpUserMac,CommonConf.LABLE_CORE_ACC_DATA)
    saveHbaseWithLabel(workerUserMac,CommonConf.LABLE_WORRKER_NORMAL_DATA)

//    allRuleImpUserRdd.foreach(rule => {
//      saveHbaseWithLabel(rule._2.map(_._1),CommonConf.LABLE_CORE_RULE_DATA+"_"+rule._1)
//    })

  }

  /**
    * 自定义规则入库＼kafka
    * @param ruleId
    * @param ruleMacs
    * @param allUserMacDetailBeHaviour
    */
  def toSaveHbaseRuleAndKafka(ruleId : Int , ruleMacs : Array[String],
                              allUserMacDetailBeHaviour : RDD[(String,Array[ImportCrowd])],
                              pre60DaysList : List[String], formartExeDay : String ){
    val allRuleUserMac = allUserMacDetailBeHaviour.filter(x => {
      ruleMacs.contains(x._1)
    })
    //新增重点旅客
    val ruleFirstImpUserMac = allRuleUserMac.filter(x => {
      !HbaseUtils.containRowKey(HbaseBusUtils.user_label_info,x._1)
    }).map(_._1)

    //识别最近60天的数据
    val rulePer60DaysImpUserMac = allRuleUserMac.filter(x => {
      HbaseUtils.containRowKey(HbaseBusUtils.user_label_info,x._1)
    }).map(x => {
      val durList = x._2.filter(dur => {
        pre60DaysList.contains(dur.curDay)
      })
      (x._1,durList)
    })

    //活跃重点旅客
    val ruleActImpUserMac = rulePer60DaysImpUserMac.filter(_._2.size >= 8).map(_._1)
    //沉默重点旅客
    val ruleOccImpUserMac = rulePer60DaysImpUserMac.filter(_._2.size < 8).map(_._1)

    //填充到hbase
    HbaseUtils.writeTable(HbaseBusUtils.sta_impr_day_nber, formartExeDay, ruleId+"_ALL",
      GsonTools.gson.toJson(HbaseImpStaCountCase(allRuleUserMac.count())))
    HbaseUtils.writeTable(HbaseBusUtils.sta_impr_day_nber, formartExeDay, ruleId+"_NEW",
      GsonTools.gson.toJson(HbaseImpStaCountCase(ruleFirstImpUserMac.count())))
    HbaseUtils.writeTable(HbaseBusUtils.sta_impr_day_nber, formartExeDay, ruleId+"_ACT",
      GsonTools.gson.toJson(HbaseImpStaCountCase(ruleActImpUserMac.count())))
    HbaseUtils.writeTable(HbaseBusUtils.sta_impr_day_nber, formartExeDay, ruleId+"_OCC",
      GsonTools.gson.toJson(HbaseImpStaCountCase(ruleOccImpUserMac.count())))


    //重点人发送到kafka
    val ruleOffLineParamKafaMap = new Object2ObjectOpenHashMap[String,Any]
    ruleOffLineParamKafaMap.put(ruleId+"_ALL",allRuleUserMac.count())
    ruleOffLineParamKafaMap.put(ruleId+"_NEW",ruleFirstImpUserMac.count())
    ruleOffLineParamKafaMap.put(ruleId+"_ACT",ruleActImpUserMac.count())
    ruleOffLineParamKafaMap.put(ruleId+"_OCC",ruleOccImpUserMac.count())
    ruleOffLineParamKafaMap.put("rowKey",formartExeDay)
    KafkaUtils.sendMsgToKafka(KafkaUtils.off_line_one_day_mub,
      MessageKafka(CommonConf.OFFLINE_IMP_STA_BY_DAY,
        DateUtils.getTime(System.currentTimeMillis(),"yyyyMMddHHmm").toLong,ruleOffLineParamKafaMap))
  }

  /**
    * 根据人工维护规则，查询出重点人群
    * @param exeDay
    * @param allUserMacDetailBeHaviour
    */
  def dealParsyHandleRule(exeDay:String, allUserMacDetailBeHaviour : RDD[(String,Array[ImportCrowd])]): List[(Int,RDD[String])] ={

    //手工规则
    CompRedisClientUtils.getRedisRuleMacs.filter(x => {
      (x.apStatus.equals("0") && x.isUseFo == 1)
    }).map(r => {
      var allMacRdd = allUserMacDetailBeHaviour

      val dateType = r.dateType//日期类型(1:所有日期{默认},2:工作日,3:周末)
      //排除日期类型
      allMacRdd = allMacRdd.map(x => {
        val durList = x._2.filter(dur => {
          DateUtils.isInWorkOrHoliday(DateUtils.getLongByTime(dur.curDay),dateType)
        })
        (x._1,durList)
      })

      //解析多个时段
      if(r.timeSegs != null && r.timeSegs.size() > 0) {
        //格式化
        val timeSegsList = r.timeSegs.elements().filter(_ != null).map(t => {
          (t.begin.replaceAll(":",""),t.end.replaceAll(":",""))
        })

        allMacRdd = allMacRdd.map(x => {
          val durIcList = x._2.map(dur => {
            //排除多个时段 {"times":2,"duration":15.0,"brand":"xerox ","details":"1104_1114;2228_2233"}
            val restDetails = dur.details.split(";").map(x => {
              (x.split("_")(0),x.split("_")(1))
            }).filter(x => {
              timeSegsList.filter(t => {
                (x._1.toInt >= t._1.toInt   &&  x._2.toInt <= t._2.toInt)
              }).size > 0
            })

            val times = restDetails.size
            var duration = 0.0d
            if(times > 0){
              duration = restDetails.map(r => {
                DateUtils.getDiffMinue(dur.curDay+r._1,dur.curDay+r._2)
              }).reduce(_+_)
            }
            //重构视图
            ImportCrowd(times,duration,dur.brand,dur.details,dur.curDay)
          })
          (x._1,durIcList.filter(_.times > 0))
        })
      }

      val isUseFi = r.isUseFi //是否启用规则一(0:启用 1:不启用)
      if(isUseFi == 0) {
        val fiLaterDay = r.fiLaterDay// 规则一最近天数
        val fiEnDayBegin = r.fiEnDayBegin//规则一入境天数开始
        val fiEnDayEnd = r.fiEnDayEnd//规则一入境天数结束
        val fiCheckedSet = r.fiCheckedSet //规则一设置在线时长/频次(0:选中 1:不选中)

        allMacRdd =  exclusiveDaysNum(exeDay,fiLaterDay,allMacRdd).filter(m => {
          m._2.size >= fiEnDayBegin && m._2.size <= fiEnDayEnd
        })

        if(fiCheckedSet == 0){
          allMacRdd = dealOnAppOnHour(allMacRdd,r)
        }

      }

      val isUseSe = r.isUseSe  // 是否启用规则二(0:启用 1:不启用)
      val seLaterDay = r.seLaterDay // 规则二最近天数
      val seSigDayBegin = r.seSigDayBegin // 规则二单日入境次数开始
      val seSigDayEnd = r.seSigDayBegin // 规则二单日入境次数结束
      val seCheckedSet = r.seCheckedSet //规则二是否设置在线时长(0:选中 1:不选中)

      if(isUseSe == 0){
        allMacRdd = exclusiveDaysNum(exeDay,seLaterDay,allMacRdd).filter(m => {
          m._2.filter(t => {
            t.times < seSigDayBegin || t.times > seSigDayEnd
          }).size == 0
        })

        if(seCheckedSet == 0){
          allMacRdd = dealOnAppOnHour(allMacRdd,r)
        }

      }

      val isUseTh = r.isUseTh // 是否启用规则三(0:启用 1:不启用)
      val thLaterDay = r.thLaterDay // 规则三今日入境次数
      val thCheckedSet = r.thCheckedSet // 规则三是否设置在线时长(0:选中 1:不选中)

      if(isUseTh == 0){
        allMacRdd = exclusiveDaysNum(exeDay,0,allMacRdd).filter(m => {
            m._2.filter(r => {
              var isRun = true
              if (thLaterDay == 3) {
                isRun = r.times > 2
              } else {
                isRun = r.times == thLaterDay
              }
              isRun
            }).size > 0
        })
        if(thCheckedSet == 0){
          allMacRdd = dealOnAppOnHour(allMacRdd,r)
        }

      }

      (r.id,allMacRdd.map(_._1))

    })
  }

  /**
    * 过滤在线时长＼次数
    * @param allMacRdd
    * @param r
    * @return
    */
  def dealOnAppOnHour(allMacRdd: RDD[(String,Array[ImportCrowd])], r :  SynBaseRuleBean): RDD[(String,Array[ImportCrowd])] ={
    //      val onlDHourBegin = r.onlDHourBegin// 每天在线时长开始
    //      val onlDHourEnd = r.onlDHourEnd// 每天在线时长结束
    //      val onlDAppearBegin = r.onlDAppearBegin// 每天出现频次开始
    //      val onlDAppearEnd = r.onlDAppearEnd // 每天出现频次结束
    allMacRdd.filter(m => {
      m._2.filter(t => {
        t.times < r.onlDAppearBegin || t.times > r.onlDAppearEnd ||
          t.duration < r.onlDHourBegin * 60 || t.duration > r.onlDHourEnd * 60
      }).size == 0
    })
  }

  /**
    * 过滤天数
    * @param exeDay
    * @param dayNum
    * @param allMacRdd
    * @return
    */
  def exclusiveDaysNum(exeDay:String,dayNum : Int, allMacRdd : RDD[(String,Array[ImportCrowd])]): RDD[(String,Array[ImportCrowd])] ={
    val seLaterDayDaysList = HdfsUtils.getPreDayFilePath(exeDay, -1 * dayNum,"yyyyMMdd")
    allMacRdd.map(x => {
      val durList = x._2.filter(dur => {
        seLaterDayDaysList.contains(dur.curDay)
      })
      (x._1,durList)
    })
  }

  def saveHbaseWithLabel(userMacRDD: RDD[String], label : String): Unit ={
    userMacRDD.foreachPartition(p => {
      p.foreach(usMac => {
        HbaseUtils.writeTable(HbaseBusUtils.user_label_info, usMac, "data", label)
      })
    })
  }

  /**
    * 将当天的数据按usMac分组后拼成指定格式存到hbase
    * 1:按usMac分组
    * 2:计算次数＼时长（一个小时内的间隔为有效时长）
    * 3:指定格式存入到行为表
    * 4:指定格式存入到品牌表
    * @param exeDay
    * @param rdd
    */
  def dealDurationByDay(exeDay:String,rdd:RDD[(String,Report)]): Unit ={
    val curDay = exeDay.replaceAll("\\/","")
    rdd.groupByKey()
      .map(x => {
        val iteMap : List[String] = x._2.toList.sortBy(_.timeStamp).map(_.timeStamp.toString).distinct

        var duration : Double = 0.0F
        val detailList = new ObjectArrayList[String]

        var (b_time,e_time,next_time) : (String,String,String) = (iteMap.head,iteMap.head,null)
        //计算有效时长
        iteMap.foreach(curTime => {
          next_time = curTime
          val spe_min = DateUtils.getDiffMinue(e_time,next_time)
          if(spe_min >= 60){

            duration = duration + DateUtils.getDiffMinue(b_time,e_time)
            detailList.add(b_time+"_"+e_time)

            b_time = curTime
            e_time = curTime
            next_time = null

          }else{
            e_time = next_time
          }
        })

        duration = duration + DateUtils.getDiffMinue(b_time,e_time)
        detailList.add(b_time+"_"+e_time)
        val times = detailList.size()

        val detailStr = detailList.elements().filter(_ != null).map(_.replaceAll(curDay,"")).mkString(";")

        val resultIC =  ImportCrowd(times,duration,x._2.head.brand,detailStr,curDay)
        (x._1,resultIC)
    }).map(d => {
      val userMac = d._1
      val impIC = d._2
      //存入用户天行为表
      HbaseUtils.writeTable(HbaseBusUtils.da_umac_behaviour, userMac, curDay, GsonTools.gson.toJson(impIC))

      (d._2.brand,userMac)

    }).filter(_._1 != null).groupByKey().map(x =>{

      (x._1,x._2.mkString(";"))

    }).foreachPartition(p => {
      p.foreach(d => {
        //存入用户天品牌表
        HbaseUtils.writeTable(HbaseBusUtils.da_umac_brand, curDay, d._1, d._2)
      })
    })
  }



  /**
    * 加载最近指定日期的探针明细数据
    * @param exeDay 执行日
    * @param sc
    * @return
    */
  def loadReCodeHdfsFileByDay(exeDay : String,sc : SparkContext): RDD[Report] = {

    //生成当前的文件路径
    val filePath = HdfsUtils.getHdfsFilePath(HdfsUtils.recode_load_hdfs_file_path,exeDay+"/*/*/part*")
    //路径
    val caseRdd: RDD[Report] = sc.textFile(filePath).map(lines => {
      HdfsUtils.recode_hdfs_parser(lines.split(","))
    })
    caseRdd
  }

  def main(args: Array[String]): Unit = {

  }








}