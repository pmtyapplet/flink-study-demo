#!/bin/bash

case $1 in
"start") {

  echo " --------启动 hadoop3 采集flume-------"
  ssh hadoop3 "nohup /opt/module/flume/bin/flume-ng agent -n a1 -c /opt/module/flume/conf/ -f /opt/module/flume/job/kafka_to_hdfs_log.conf >/dev/null 2>&1 &"

} ;;
"stop") {

  echo " --------停止 hadoop3 采集flume-------"
  ssh hadoop3 "ps -ef | grep kafka_to_hdfs_log | grep -v grep |awk  '{print \$2}' | xargs -n1 kill -9 "

} ;;
esac
