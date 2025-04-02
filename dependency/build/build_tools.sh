#!/bin/bash
# *************************************************************************
# Copyright: (c) Huawei Technologies Co., Ltd. 2020. All rights reserved
#
#  description: the script that make install dependency
#  date: 2020-10-21
#  version: 1.0
#  history:
#
# *************************************************************************
set -e

ARCH=`uname -m`
ROOT_DIR=$(cd `dirname $0`/..; pwd)
PLATFORM="$(bash ${ROOT_DIR}/build/get_platform.sh)"

[ -f build_tools.log ] && rm -rf build_tools.lo

echo --------------------------------python2-------------------------------------------------
if [ -z `command -v python2` ]; then
    if [ -d "${ROOT_DIR}/python2" ]; then
        start_tm=$(date +%s%N)
        cd ${ROOT_DIR}/python2
        sh ./build.sh >> build_tools.log
        end_tm=$(date +%s%N)
        use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
        echo "[python] $use_tm"
    fi
else
    echo 'python2 had already installed. Skip the build process of python2.'
fi

echo ---------------------------license_control---------------------------------------------
if [ -d "${ROOT_DIR}/license_control" ]; then
    start_tm=$(date +%s%N)
    cd ${ROOT_DIR}/license_control
    sh ./build.sh >> build_tools.log
    end_tm=$(date +%s%N)
    use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
    echo "[license_control] $use_tm"
fi

# Use by dependency/pljava, dependency/postgresql-hll on 
# cp -a $(pwd)/../server_key $(pwd)/../../output/buildtools/
