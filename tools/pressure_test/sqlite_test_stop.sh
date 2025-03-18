#!/bin/bash 

pid=`pidof -s sqlite_test`
echo $pid
if [ ! -z $pid ]; then
  kill -10 $pid
else
  echo "no sqlite_test start"
fi

#等进程停止后删除目录
#pid=`pidof -s sqlite_test`  
#while [ ! -z $pid ]; do
#  sleep 0.1
#  pid=`pidof -s sqlite_test`
#done
#rm -rf test/sqlites.db*