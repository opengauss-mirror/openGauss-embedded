#!/bin/bash
# *************************************************************************
# Copyright: (c) Huawei Technologies Co., Ltd. 2020. All rights reserved
#
#  description: the script that make install platform
#  date: 2020-10-21
#  version: 1.0
#  history:
#
# *************************************************************************
set -e

ROOT_DIR=$(cd `dirname $0`/..; pwd)
PLATFORM_PATH=${ROOT_DIR}

# build huawei secure c lib
cd ${PLATFORM_PATH}/Huawei_Secure_C
sh build.sh -m all

# build huawei jdk
# cd ${PLATFORM_PATH}/openjdk8
# sh ./build.sh
