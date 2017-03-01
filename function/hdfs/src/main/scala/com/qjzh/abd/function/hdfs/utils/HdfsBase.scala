package com.qjzh.abd.function.hdfs.utils

import java.net.URI

import com.qjzh.abd.function.common.PropertiesUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
/**
  * Created by 92025 on 2016/12/15.
  */
object HdfsBase {

  val kafkaProperties = PropertiesUtils.init()

  val FUN_HDFS_URL = kafkaProperties.getProperty("fun.hdfs.url")

  /**
    * 删除某个文件
    * @param path
    * @return
    */
  def delFile(path:String) =  {
    val conf = new Configuration()
    val dsf = FileSystem.get(URI.create(FUN_HDFS_URL),conf)
    if(dsf.exists(new Path(path))){
      dsf.delete(new Path(path),true)
    }
  }
  /**
    * 修改文件的名称
    * @param oldName
    * @param newName
    * @return
    */
  def reNameFile(oldName:String,newName:String): Boolean ={
    val conf = new Configuration()
    val hdfs = FileSystem.get(URI.create(FUN_HDFS_URL), conf)
    val rename: Boolean = hdfs.rename(new Path(oldName),new Path (newName))
    rename
  }
  /**
    * 获取文件夹下的文件列表
    * @param path
    * @return
    */
  def getListFilePath(path:String):Array[Path] ={
    val conf = new Configuration()
    val hdfs = FileSystem.get(URI.create(FUN_HDFS_URL), conf)
    val fs = hdfs.listStatus(new Path(path))
    FileUtil.stat2Paths(fs)
  }

  /**
    * 是否存在当前目录
    * @param path
    * @return
    */
  def exitsFile(path:String) =  {
    val conf = new Configuration()
    val dsf = FileSystem.get(URI.create(FUN_HDFS_URL),conf)
    val isExists = dsf.exists(new Path(path))
    isExists
  }
  def lsFile(path:String) =  {
    val conf = new Configuration()
    val dsf = FileSystem.get(URI.create(FUN_HDFS_URL),conf)
    val fs = dsf.listStatus(new Path(path))
    FileUtil.stat2Paths(fs,new Path(path))
  }
  def createFile(path:String) =  {
    val conf = new Configuration()
    val dsf = FileSystem.get(URI.create(FUN_HDFS_URL),conf)
    dsf.createNewFile(new Path(path))
  }
  def createPath(path:String) =  {
    val conf = new Configuration()
    val dsf = FileSystem.get(URI.create(FUN_HDFS_URL),conf)
    dsf.create(new Path(path))
  }

  def main(args: Array[String]): Unit = {
    println(FUN_HDFS_URL)
  }

}
