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

ARCH=$(uname -m)
ROOT_DIR=$(cd `dirname $0`/..; pwd)

[ -f build_all.log ] && rm -rf build_all.log

function build_module() {
    local module_name
    local build_cmd='sh build.sh -m all'
    local log_file="$ROOT_DIR/build/build_result.log"
    case "$#" in
        3)
            log_file=$3
            build_cmd=$2
            module_name=$1
            ;;
        2)
            build_cmd=$2
            module_name=$1
            ;;
        1)
            module_name=$1
            ;;
        0)
            return
        ;;
    esac
    
    echo "------------------------------ $module_name ------------------------------------------------------"
    start_tm=$(date +%s%N)
    cd ${ROOT_DIR}/${module_name}
    ${build_cmd} 2>&1 >> "${log_file}"
    end_tm=$(date +%s%N)
    use_tm=$(echo $end_tm $start_tm | awk '{ print ($1 - $2) / 1000000000}' | xargs printf "%.2f")
    echo "[${module_name}] use ${use_tm} second." 
}


[ -f "${ROOT_DIR}/openssl/openssl/test/certs/embeddedSCTs1_issuer-key.pem" ] && rm -f "${ROOT_DIR}/openssl/openssl/test/certs/embeddedSCTs1_issuer-key.pem"
#build_module  openssl 'python3 build.py -m all -f openssl-OpenSSL_1_1_1n.tar.gz -t comm,llt'  "${ROOT_DIR}/build/demo.log"
#build_module  cJSON
#build_module  lz4
build_module  zlib
#build_module  zstd
