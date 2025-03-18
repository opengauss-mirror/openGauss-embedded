#!/bin/bash 

pid=`pidof -s pressure_test`
echo $pid
if [ ! -z $pid ]; then
  kill -10 $pid
else
  echo "no pressure_test start"
fi

##等进程停止后删除目录
#pid=`pidof -s pressure_test`  
#while [ ! -z $pid ]; do
#  sleep 0.1
#  pid=`pidof -s pressure_test`
#done
#rm -rf test/gstor
##创建库文件，防止启动过慢
#./pressure_test test
#echo "create db file"
