package com.qjzh.abd.components.location.service

import com.qjzh.abd.components.location.common.caseview.CaseClass.Coordinate
import com.qjzh.abd.components.location.common.utils.LocationPropertiesUtils
import com.qjzh.abd.control.common.view.Report
import org.apache.spark.streaming.dstream.DStream

/**
  * Created by yufeiwang on 15/02/2017.
  */
object LocationCommonService {

  /**
    * 借口方法，把所有项目名对应的类，用反射方法调calculatePos方法，每个类计算的结果Union起来返回。
    * @param userRssiList
    * @param projNames
    * @return
    */
  def calculatePos(userRssiList: DStream[(String, Iterable[Report])],projNames : List[String]): DStream[(String, String,String, (Coordinate,Coordinate))] = {
    var result :DStream[(String, String,String, (Coordinate,Coordinate))] = null

    projNames.foreach(name => {
      val className = LocationPropertiesUtils.getClassNameByProjName(name)
      val locClass = Class.forName(className)
      val method = locClass.getDeclaredMethod("calculatePos", classOf[String])
      result = result.union(method.invoke(locClass.newInstance(), userRssiList).asInstanceOf[DStream[(String, String,String, (Coordinate,Coordinate))]])
    })

    result
  }
}
