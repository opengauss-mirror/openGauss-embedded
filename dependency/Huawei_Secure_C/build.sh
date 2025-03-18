#!/bin/bash
#  Copyright (c) Huawei Technologies Co., Ltd. 2020. All rights reserved.
#  description: the script that make install securec
#  date: 2019-12-28
#  version: 1.01
#  history:
#	2019-01-16 modify for securec_V100R001C01SPC006B002
#	2019-12-28 change formatting and add copyright notice

set -e
######################################################################
# Parameter setting
######################################################################
LOCAL_PATH=${0}
FIRST_CHAR=$(expr substr "$LOCAL_PATH" 1 1)
if [ "$FIRST_CHAR" = "/" ]; then
    LOCAL_PATH=${0}
else
    LOCAL_PATH="$(pwd)/${LOCAL_PATH}"
fi

LOCAL_DIR=$(dirname "${LOCAL_PATH}")
CONFIG_FILE_NAME=config.ini
BUILD_OPTION=release
SOURCE_CODE_PATH=.
LOG_FILE=${LOCAL_DIR}/build_securec.log
BUILD_FAILED=1

ls ${LOCAL_DIR}/${CONFIG_FILE_NAME} > /dev/null 2>&1
if [ $? -ne 0 ]; then
    die "[Error] the file ${CONFIG_FILE_NAME} not exist."
fi

COMPLIE_TYPE_LIST=$(cat ${LOCAL_DIR}/${CONFIG_FILE_NAME} | grep -v '#' | grep -v '^$' | awk -F '=' '{print $2}' | sed 's/|/ /g')
COMPONENT_NAME=$(cat ${LOCAL_DIR}/${CONFIG_FILE_NAME} | grep -v '#' | grep -v '^$' | awk -F '=' '{print $1}' | awk -F '@' '{print $2}')
COMPONENT_TYPE=$(cat ${LOCAL_DIR}/${CONFIG_FILE_NAME} | grep -v '#' | grep -v '^$' | awk -F '@' '{print $1}')

if [ "${COMPONENT_NAME}"X = ""X ]; then
    die "[Error] get component name failed!"
fi

if [ "${COMPONENT_TYPE}"X = ""X ]; then
    die "[Error] get component type failed!"
fi

ROOT_DIR="${LOCAL_DIR}/../../../"
INSTALL_COMPOENT_PATH_NAME="${LOCAL_DIR}/../../output/kernel/${COMPONENT_TYPE}/${COMPONENT_NAME}"

#######################################################################
## print help information
#######################################################################
function print_help() {
    echo "Usage: $0 [OPTION]
        -h|--help               show help information
        -m|--build_option       provode type of operation, values of paramenter is build, shrink, dist or clean
	"
}

#######################################################################
#  Print log.
#######################################################################
log() {
    echo "[Build securec] $(date +%y-%m-%d' '%T): $@"
    echo "[Build securec] $(date +%y-%m-%d' '%T): $@" >> "$LOG_FILE" 2>&1
}

#######################################################################
#  print log and exit.
#######################################################################
die() {
    log "$@"
    echo "$@"
    exit $BUILD_FAILED
}

#######################################################################
# build and install component
#######################################################################
function build_component() {
    cd ${LOCAL_DIR}
    cp MakefileCustom ${LOCAL_DIR}/${SOURCE_CODE_PATH}/src
    cd ${LOCAL_DIR}/${SOURCE_CODE_PATH}/src
    if [ $? -ne 0 ]; then
        die "[Error] change dir to $SRC_DIR failed."
    fi

    for COMPILE_TYPE in ${COMPLIE_TYPE_LIST}; do
        case "${COMPILE_TYPE}" in
            release)
                die "[Error] securec not supported build type."
                ;;
            debug)
                die "[Error] securec not supported build type."
                ;;
            comm)
                log "[Notice] securec \"make -f MakefileCustom securecstatic\" build started"
                make -f MakefileCustom securecstatic
                if [ $? -ne 0 ]; then
                    die "securec make failed."
                fi
                make -f MakefileCustom securecshare
                if [ $? -ne 0 ]; then
                    die "securec make failed."
                fi
                log "[Notice] securec build using \"${COMPILE_TYPE}\" has been finished"
                ;;
            release_llt)
                die "[Error] securec not supported build type."
                ;;
            debug_llt)
                die "[Error] securec not supported build type."
                ;;
            llt)
                die "[Error] securec not supported build type."
                ;;
            *)
                log "Internal Error: option processing error: $1"
                log "please write right paramenter in ${CONFIG_FILE_NAME}"
                exit 1
                ;;
        esac
    done
}

#######################################################################
# choose the real files
#######################################################################
function shrink_component() {
    for COMPILE_TYPE in ${COMPLIE_TYPE_LIST}; do
        case "${COMPILE_TYPE}" in
            comm)
                mkdir -p ${LOCAL_DIR}/install_comm_dist/lib
                cp ${LOCAL_DIR}/${SOURCE_CODE_PATH}/src/libsecurec.a ${LOCAL_DIR}/install_comm_dist/lib
                mkdir -p ${LOCAL_DIR}/install_comm/lib
                cp ${LOCAL_DIR}/${SOURCE_CODE_PATH}/src/libsecurec.a ${LOCAL_DIR}/install_comm/lib
                if [ $? -ne 0 ]; then
                    die "[Error] \"cp ${LOCAL_DIR}/${SOURCE_CODE_PATH}/src/libsecurec.a ${LOCAL_DIR}/install_comm_dist/lib\" failed"
                fi
                ;;
            release) ;;

            debug) ;;

            llt) ;;

            release_llt) ;;

            debug_llt) ;;

            *) ;;
        esac
        log "[Notice] securec shrink using \"${COMPILE_TYPE}\" has been finished!"
    done
}

##############################################################################################################
# dist the real files to the matched path
#	we could makesure that $INSTALL_COMPOENT_PATH_NAME is not null, '.' or '/'
##############################################################################################################
function dist_component() {
    for COMPILE_TYPE in ${COMPLIE_TYPE_LIST}; do
        case "${COMPILE_TYPE}" in
            comm)
                mkdir -p ${INSTALL_COMPOENT_PATH_NAME}/comm
                rm -rf ${INSTALL_COMPOENT_PATH_NAME}/comm/*
                cp -arxf ${LOCAL_DIR}/${SOURCE_CODE_PATH}/include  ${INSTALL_COMPOENT_PATH_NAME}/comm
                cp -r ${LOCAL_DIR}/install_comm_dist/* ${INSTALL_COMPOENT_PATH_NAME}/comm
                if [ $? -ne 0 ]; then
                    die "[Error] \"cp -r ${LOCAL_DIR}/install_comm_dist/* ${INSTALL_COMPOENT_PATH_NAME}/comm\" failed."
                fi
                mkdir -p ${INSTALL_COMPOENT_PATH_NAME}/Dynamic_Lib
                rm -rf ${INSTALL_COMPOENT_PATH_NAME}/Dynamic_Lib/*
                cp -r ${LOCAL_DIR}/${SOURCE_CODE_PATH}/src/libsecurec.so  ${INSTALL_COMPOENT_PATH_NAME}/Dynamic_Lib/
                if [ $? -ne 0 ]; then
                    die "[Error] cp dynamic  failed."
                fi                
                ;;
            release) ;;

            debug) ;;

            *) ;;
        esac
        log "[Notice] securec dist using \"${COMPILE_TYPE}\" has been finished!"
    done
}

#######################################################################
# clean component
#######################################################################
function clean_component() {
    cd ${LOCAL_DIR}/${SOURCE_CODE_PATH}/src
    if [ $? -ne 0 ]; then
        die "[Error] cd ${LOCAL_DIR}/${SOURCE_CODE_PATH} failed."
    fi

    make clean

    cd ${LOCAL_DIR}
    if [ $? -ne 0 ]; then
        die "[Error] cd ${LOCAL_DIR} failed."
    fi
    [ -n "${SOURCE_CODE_PATH}" ] && rm -rf ${SOURCE_CODE_PATH}
    rm -rf install_*

    log "[Notice] securec clean has been finished!"
}
#######################################################################
#######################################################################
#######################################################################
# main
#######################################################################
#######################################################################
#######################################################################
function main() {
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
            #clean_component
            ;;
        *)
            log "Internal Error: option processing error: $2"
            log "please input right paramenter values build, shrink, dist or clean "
            ;;
    esac
}

########################################################################
if [ $# = 0 ]; then
    log "missing option"
    print_help
    exit 1
fi

##########################################################################
#read command line paramenters
##########################################################################
while [ $# -gt 0 ]; do
    case "$1" in
        -h | --help)
            print_help
            exit 1
            ;;
        -m | --build_option)
            if [ "$2"X = X ]; then
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
            ;;
    esac
done

###########################################################################
main
