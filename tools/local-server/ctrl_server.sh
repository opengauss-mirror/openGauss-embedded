#!/bin/sh

if [ "$1" == "start" ]; then
    mkdir -p $2
    nohup ./local-server $2 $3 > $2/out.log 2>&1  &   # $2 为库路径 $3 为port $4为host
elif  [ "$1" == "stop" ]; then
    if [ "$2" == "all" ]; then 
        echo stop all
        pids=`pgrep local-server`
        for pid in $pids
        do
            kill -10 $pid
        done
    else
        kill -10 $2  # $2 为pid
    fi
elif [ "$1" == "list" ]; then
    netstat -nltp|grep "local-server"
    ps axu|grep "local-server"
fi

