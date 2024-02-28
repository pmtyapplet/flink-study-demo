#! /bin/bash
case $1 in
"start")
  for i in  hadoop1 hadoop2 hadoop3 ; do
      echo "======================启动" $i "zookeeper =============================="
      ssh $i "/opt/module/zookeeper/bin/zkServer.sh start"
  done
  ;;
"stop")
  for i in  hadoop1 hadoop2 hadoop3 ; do
      echo "======================启动" $i "zookeeper =============================="
      ssh $i "/opt/module/zookeeper/bin/zkServer.sh stop"
  done
  ;;
"status")
  for i in  hadoop1 hadoop2 hadoop3 ; do
      echo "======================状态" $i "zookeeper =============================="
      ssh $i "/opt/module/zookeeper/bin/zkServer.sh status"
  done
  ;;
esac