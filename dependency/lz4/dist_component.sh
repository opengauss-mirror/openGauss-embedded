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

##############################################################################################################
# dist the real files to the matched path
# we could makesure that $INSTALL_COMPOENT_PATH_NAME is not null, '.' or '/'
##############################################################################################################
function dist_component()
{
    for COMPILE_TYPE in ${COMPLIE_TYPE_LIST}
    do
        case "${COMPILE_TYPE}" in
            comm)
                mkdir -p ${INSTALL_COMPOENT_PATH_NAME}/comm
                rm -rf "${INSTALL_COMPOENT_PATH_NAME}"/comm/*
                cp -dr ${LOCAL_DIR}/install_comm_dist/* ${INSTALL_COMPOENT_PATH_NAME}/comm
                if [ $? -ne 0 ]; then
                    die "[Error] \"cp -r ${LOCAL_DIR}/install_comm_dist/* ${INSTALL_COMPOENT_PATH_NAME}/comm\" failed."
                fi
                ;;
            llt)
                mkdir -p ${INSTALL_COMPOENT_PATH_NAME}/llt
                rm -rf "${INSTALL_COMPOENT_PATH_NAME}"/llt/*
                cp -dr ${LOCAL_DIR}/install_llt_dist/* ${INSTALL_COMPOENT_PATH_NAME}/llt
                if [ $? -ne 0 ]; then
                       die "[Error] \"cp -r ${LOCAL_DIR}/install_llt_dist/* ${INSTALL_COMPOENT_PATH_NAME}/llt\" failed."
                fi
                ;;
            release)
                ;;
            debug)
                ;;
            release_llt)
                ;;
            debug_llt)
                ;;
            *)
        esac
        log "[Notice] lz4 dist using \"${COMPILE_TYPE}\" has been finished!"
    done
}

