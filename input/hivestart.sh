#!/bin/bash

case $1 in
"start")
  nohup hiveserver2 >> /opt/module/hive/log/hiveserver2.log 2>&1 &
  ;;
esac