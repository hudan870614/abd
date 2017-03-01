#!/bin/bash

master="yarn-cluster"
current=`cd $(dirname $0)/../;pwd`

spark-submit --master $master --conf spark.speculation=true --executor-memory 1G --executor-cores 2 --driver-memory 1G --class com.qjzh.abd.control.offline.job.UserTypeJob --conf spark.executor.extraClassPath=$current/lib/* --conf spark.driver.extraClassPath=$current/lib/* $current/lib/offline-1.0-SNAPSHOT.jar
