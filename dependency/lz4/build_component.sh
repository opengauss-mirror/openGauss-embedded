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
# build and install component
#######################################################################
function build_component()
{
    cd ${LOCAL_DIR}

    if [ -d ${SOURCE_CODE_PATH} ]; then
        rm -rf ${SOURCE_CODE_PATH}
    fi
    rm -rf install_*
    mkdir ${SOURCE_CODE_PATH}
    tar -zxf $TAR_FILE_NAME -C $SOURCE_CODE_PATH --strip-components 1

    cd ${LOCAL_DIR}/${SOURCE_CODE_PATH}
    if [ $? -ne 0 ]; then
        die "[Error] change dir to $SRC_DIR failed."
    fi

    # copy and apply patch
    log "[Notice] begin apply patch to lz4 "
    patch -p1 < ../lz4.patch
    # cd lib/
    # cp ../../huawei_lz4.patch ./
    # patch -p0 < huawei_lz4.patch
    # if [ $? -ne 0 ]; then
    #     die "[Error] apply patch failed."
    # fi
    # log "[Notice] finish apply patch to lz4 "

    # chmod +x configure
    for COMPILE_TYPE in ${COMPLIE_TYPE_LIST}
    do
        case "${COMPILE_TYPE}" in
            release)
                die "[Error] lz4 not supported build type."
                ;;
            debug)
                die "[Error] lz4 not supported build type."
                ;;
            release_llt)
                die "[Error] lz4 not supported build type."
                ;;
            debug_llt)
                die "[Error] lz4 not supported build type."
                ;;
            comm|llt)
                mkdir -p ${LOCAL_DIR}/install_${COMPILE_TYPE}

                log "[Notice] lz4 using \"${COMPILE_TYPE}\" Begin make"
                if [ "${COMPILE_TYPE}"X = "comm"X ]; then
                    sed -i '53a CFLAGS += -fPIC -fstack-protector-strong' lib/Makefile
                    sed -i '54a LDFLAGS += -Wl,-z,relro,-z,now,-z,noexecstack -pie' lib/Makefile
		
                    sed -i '52a CFLAGS += -fstack-protector-strong -fPIE' programs/Makefile
                    sed -i '53a LDFLAGS += -Wl,-z,relro,-z,now,-z,noexecstack -pie' programs/Makefile
                    make -j4 
                else
                    make -j4
                fi
                ;;
            *)
                log "Internal Error: option processing error: $1"
                log "please write right paramenter in ${CONFIG_FILE_NAME}"
                exit 1
        esac

        if [ $? -ne 0 ]; then
            die "lz4 make failed."
        fi
        log "[Notice] lz4 End make"

        log "[Notice] lz4 using \"${COMPILE_TYPE}\" Begin make install"
        make install prefix=${LOCAL_DIR}/install_${COMPILE_TYPE}
        if [ $? -ne 0 ]; then
            die "[Error] lz4 make install failed."
        fi
        log "[Notice] lz4 using \"${COMPILE_TYPE}\" End make install"
        make clean -s
        log "[Notice] lz4 build using \"${COMPILE_TYPE}\" has been finished"
    done
}
