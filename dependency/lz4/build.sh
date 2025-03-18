#!/bin/bash
#  Copyright (c) Huawei Technologies Co., Ltd. 2010-2018. All rights reserved.
#  description: the script that make install lz4
#  date: 2019-12-28
#  version: 1.11
#  history:
#    2019-5-5 update to lz4 1.8.3 from 1.7.5
#    2019-12-12 update to lz4 1.9.2 from 1.8.3
#    2019-12-28 change formatting and add copyright notice

set -e

WORK_PATH="$(dirname $0)"

source "${WORK_PATH}/build_common.sh"

source "${WORK_PATH}/build_component.sh"

source "${WORK_PATH}/shrink_component.sh"

source "${WORK_PATH}/dist_component.sh"

source "${WORK_PATH}/clean_component.sh"

#######################################################################
#######################################################################
#######################################################################
# main
#######################################################################
#######################################################################
#######################################################################
function main()
{
    case "${BUILD_OPTION}" in
        build)
            build_component
            ;;
        shrink)
            shrink_component
            ;;
        dist)
            dist_component            
            ;;
        clean)
            clean_component    
            ;;
        all)
            build_component
            shrink_component
            dist_component
            clean_component
            ;;
       *)
            log "Internal Error: option processing error: $2"   
            log "please input right paramenter values build, shrink, dist or clean "
    esac
}


########################################################################
if [ $# = 0 ] ; then
        log "missing option"
        print_help
        exit 1
fi

##########################################################################
#read command line paramenters
##########################################################################
while [ $# -gt 0 ]; do
    case "$1" in
        -h|--help)
            print_help
            exit 1
            ;;
        -m|--build_option)
            if [ "$2"X = X ];then
                    die "no given version number values"
            fi
            BUILD_OPTION=$2
            shift 2
            ;;
        *)
            log "Internal Error: option processing error: $1" 1>&2
            log "please input right paramtenter, the following command may help you"
            log "./build.sh --help or ./build.sh -h"
            exit 1
    esac
done

###########################################################################
main
