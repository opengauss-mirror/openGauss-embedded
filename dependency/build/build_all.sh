#!/bin/bash
# *************************************************************************
# Copyright: (c) Huawei Technologies Co., Ltd. 2020. All rights reserved
#
#  description: the script that make install dependency
#  date: 2023-1-6
#  version: 1.0.0
#  history:
#    2023-1-6 fisrt version
# *************************************************************************
set -e

ROOT_DIR=$(cd `dirname $0`/..; pwd)
CURR_DIR=$(cd `dirname $0`; pwd)

# check gcc min version
let gcc_min_version=7
gcc_version=$(gcc -v 2>&1 | tail -1 | awk '{print $3}')
ver_num=(${gcc_version//\./ })
if [[ ${ver_num[0]} -lt $gcc_min_version ]]; then
    echo "[ERROR] Current gcc version is: ${gcc_version}, to low. (No less then gcc $gcc_min_version)"
    exit 1
fi

sh ${CURR_DIR}/build_tools.sh
sh ${CURR_DIR}/build_platform.sh
sh ${CURR_DIR}/build_dependency.sh
sh ${CURR_DIR}/build_libpg.sh

[ -f "${ROOT_DIR}/demo.log" ] && rm -f "${ROOT_DIR}/demo.log"
