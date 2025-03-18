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

#######################################################################
# choose the real files 
#######################################################################
function shrink_component()
{
    for COMPILE_TYPE in ${COMPLIE_TYPE_LIST}
    do
       case "${COMPILE_TYPE}" in
           release)
               ;;
           comm)
                mkdir ${LOCAL_DIR}/install_comm_dist
                cp -r ${LOCAL_DIR}/install_comm/* ${LOCAL_DIR}/install_comm_dist
                if [ $? -ne 0 ]; then
                    die "[Error] \"cp -r ${LOCAL_DIR}/install_comm/* ${LOCAL_DIR}/install_comm_dist\" failed."
                fi
                rm -rf ${LOCAL_DIR}/install_comm_dist/share
                rm ${LOCAL_DIR}/install_comm_dist/lib/liblz4.a
		rm ${LOCAL_DIR}/install_comm_dist/bin/lz4c
		rm ${LOCAL_DIR}/install_comm_dist/bin/lz4cat
		rm ${LOCAL_DIR}/install_comm_dist/bin/unlz4
		rm -rf ${LOCAL_DIR}/install_comm_dist/lib/pkgconfig
                ;;
            llt)    
                mkdir ${LOCAL_DIR}/install_llt_dist
                cp -r ${LOCAL_DIR}/install_llt/* ${LOCAL_DIR}/install_llt_dist
                if [ $? -ne 0 ]; then
                    die "[Error] \"cp -r ${LOCAL_DIR}/install_llt/* ${LOCAL_DIR}/install_llt_dist\" failed."
                fi
                rm -rf ${LOCAL_DIR}/install_llt_dist/share
                rm ${LOCAL_DIR}/install_llt_dist/lib/liblz4.a
		rm ${LOCAL_DIR}/install_llt_dist/bin/lz4c
                rm ${LOCAL_DIR}/install_llt_dist/bin/lz4cat
                rm ${LOCAL_DIR}/install_llt_dist/bin/unlz4
                rm -rf ${LOCAL_DIR}/install_llt_dist/lib/pkgconfig
                ;;
            debug)
                ;;
            release_llt)
                ;;
            debug_llt)
                ;;
            *)
        esac
        log "[Notice] lz4 shrink using \"${COMPILE_TYPE}\" has been finished!"
    done
}
