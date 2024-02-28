#! /bin/bash
case $1 in
"start")

  for i in hadoop1 hadoop2 hadoop3 ; do
      echo "======================启动" $i "kafka =============================="
      ssh $i "/opt/module/kafka/bin/kafka-server-start.sh -daemon /opt/module/kafka/config/server.properties"
  done
  ;;
"stop")

  for i in hadoop1 hadoop2 hadoop3 ; do
      echo "======================停止" $i "kafka =============================="
      ssh $i "/opt/module/kafka/bin/kafka-server-stop.sh"
  done
  ;;
esac