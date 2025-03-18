#!/bin/bash
# *************************************************************************
# Copyright: (c) Huawei Technologies Co., Ltd. 2020. All rights reserved
#
#  description: the script that make install zstd
#  date: 2020-01-16
#  version: 1.0
#  history:
#
# *************************************************************************
set -e
LOCAL_DIR=$(pwd)

TAR_SOURCE_FILE=zstd-1.5.2.tar.gz
export PACKAGE=zstd
[ -n "${PACKAGE}" ] && rm -rf ${PACKAGE} 
mkdir ${PACKAGE}
tar -zxf $TAR_SOURCE_FILE -C $PACKAGE --strip-components 1
cd $PACKAGE
mkdir -p ../install_comm/lib/
cd build/cmake/
mkdir build
cd build
cmake -DZSTD_BUILD_STATIC=on -DCMAKE_INSTALL_PREFIX=../../../../install_comm ..
sed -i 's/-std=c99/-std=c99 -Wl,-z,relro,-z,now,-z,noexecstack -fPIC -fstack-protector-strong/g' ./programs/CMakeFiles/zstd.dir/flags.make
sed -i 's/-std=c99/-std=c99 -Wl,-z,relro,-z,now,-z,noexecstack -fPIC -fstack-protector-strong/g' ./programs/CMakeFiles/zstd.dir/link.txt
sed -i 's/-std=c99/-std=c99 -Wl,-z,relro,-z,now,-z,noexecstack -fPIC -fstack-protector-strong/g' ./programs/CMakeFiles/zstd-frugal.dir/flags.make
sed -i 's/-std=c99/-std=c99 -Wl,-z,relro,-z,now,-z,noexecstack -fPIC -fstack-protector-strong/g' ./programs/CMakeFiles/zstd-frugal.dir/link.txt
sed -i 's/-std=c99/-std=c99 -Wl,-z,relro,-z,now,-z,noexecstack -fPIC -fstack-protector-strong/g' ./lib/CMakeFiles/libzstd_static.dir/flags.make
sed -i 's/-std=c99/-std=c99 -Wl,-z,relro,-z,now,-z,noexecstack -fPIC -fstack-protector-strong/g' ./lib/CMakeFiles/libzstd_static.dir/link.txt
sed -i 's/-std=c99/-std=c99 -Wl,-z,relro,-z,now,-z,noexecstack -fPIC -fstack-protector-strong/g' ./lib/CMakeFiles/libzstd_shared.dir/flags.make
sed -i 's/-std=c99/-std=c99 -Wl,-z,relro,-z,now,-z,noexecstack -fPIC -fstack-protector-strong/g' ./lib/CMakeFiles/libzstd_shared.dir/link.txt
make -j4
make install

if [ -d "../../../../install_comm/lib64" ]; then
    mv  ../../../../install_comm/lib64/libzstd* ../../../../install_comm/lib/
fi

INSTALL_DIR=${LOCAL_DIR}/../../output/kernel/dependency/zstd
# copy lib to destination
mkdir -p ${INSTALL_DIR}/bin
mkdir -p ${INSTALL_DIR}/include
mkdir -p ${INSTALL_DIR}/lib

cp ${LOCAL_DIR}/install_comm/bin/* ${INSTALL_DIR}/bin/
cp ${LOCAL_DIR}/install_comm/include/* ${INSTALL_DIR}/include/
cp -d ${LOCAL_DIR}/install_comm/lib/libzstd.* ${INSTALL_DIR}/lib/

