package com.qjzh.function.log.utils

import com.qjzh.abd.function.common.{DateUtils, PropertiesUtils}
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.SparkContext
import org.slf4j.LoggerFactory

/**
  * Created by Administrator on 2016/11/23.
  */
object Syslog {

  val properties  = PropertiesUtils.init()
  PropertyConfigurator.configure( properties );
  var infomsg:String  = ""
  var debugmsg:String = ""
  var errormsg:String = ""
  var warnmsg:String  = ""
  var taskName:String = ""
  val LOG = LoggerFactory.getLogger("dba")
  var _sc:SparkContext              = _

  def Init(sparkcontent:SparkContext,taskname:String):Unit={
    _sc         = sparkcontent
    taskName    = taskname
  }
  def info(msg:String):Unit={
    // LOG.info(msg)
    infomsg += (msg+"\n")
  }

  def debug(msg:String):Unit={
    //LOG.debug(msg)
    debugmsg += (msg+"\n")
  }

  def warn(msg:String):Unit={
    //LOG.warn(msg)
    warnmsg += (msg+"\n")
  }
  def error(msg:String,ex:Exception):Unit={
    // LOG.error(msg,ex)
    errormsg += (msg+"\n"+ex.toString+"\n")
  }

  def error(msg:String):Unit={
    //LOG.error(msg)
    errormsg += (msg+"\n")
  }

  /**
    * 默认路径 /temp/log/taskname/timesmap
    */
  def Close():Unit={
    if( _sc != null)
    {
      val logtime = DateUtils.getCurTime()
      _sc.parallelize(Seq(infomsg),1).saveAsTextFile("/new7g/temp/log/"+taskName+"/"+logtime+"/info")
      _sc.parallelize(Seq(debugmsg),1).saveAsTextFile("/new7g/temp/log/"+taskName+"/"+logtime+"/debug")
      _sc.parallelize(Seq(errormsg),1).saveAsTextFile("/new7g/temp/log/"+taskName+"/"+logtime+"/error")
      _sc.parallelize(Seq(warnmsg),1).saveAsTextFile("/new7g/temp/log/"+taskName+"/"+logtime+"/warn")
    }
  }
}
