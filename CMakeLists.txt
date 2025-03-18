cmake_minimum_required(VERSION 3.14.1)

if (OS_ARCH)

    if (NOT ANDROID)
        if (${OS_ARCH} STREQUAL "arm32")
            if(NOT DEFINED OHOS_SYSROOT_PATH OR OHOS_SYSROOT_PATH STREQUAL "")
                set(CMAKE_SYSTEM_NAME Linux)
                set(CMAKE_SYSTEM_PROCESSOR arm)
                #交叉编译工具链
                set(CMAKE_C_COMPILER arm-linux-gnueabihf-gcc)
                set(CMAKE_CXX_COMPILER arm-linux-gnueabihf-g++)
                set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -march=armv7-a -mfpu=neon")
                set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=armv7-a -mfpu=neon") 
            endif()
        endif()
        if(OHOS_SYSROOT_PATH)
            message(STATUS "BUILD OS ARCH = ${OS_ARCH}")

            set(CMAKE_CROSSCOMPILING TRUE)
            # set(CMAKE_SYSTEM_NAME Generic)
            set(CMAKE_CXX_COMPILER_ID Clang)
            set(CMAKE_TOOLCHAIN_PREFIX ${TOOLCHAIN_PATH}/bin/llvm-)
            #指定c编译工具（确保工具链所在路径已经添加到了PATH环境变量中）和编译标志，使用clang编译时标志中必须指定--target，否则无法交叉编译。
            set(CMAKE_C_COMPILER ${TOOLCHAIN_PATH}/bin/clang)
            set(CMAKE_CXX_COMPILER ${TOOLCHAIN_PATH}/bin/clang++)
            if (${OS_ARCH} STREQUAL "arm32")
		        set(CMAKE_C_FLAGS "--target=arm-linux-ohos  -D__clang__ -march=armv7-a -w")
                #指定c++编译工具（确保工具链所在路径已经添加到了PATH环境变量中）和编译标志，必须指定--target，否则无法交叉编译。
                set(CMAKE_CXX_FLAGS "--target=arm-linux-ohos  -D__clang__ -march=armv7-a -w")
                #指定链接工具和链接标志，必须指定--target和--sysroot，其中OHOS_ROOT_PATH可通过cmake命令后缀参数来指定。
                set(MY_LINK_FLAGS "--target=arm-linux-ohos  --sysroot=${OHOS_SYSROOT_PATH}")
            elseif(${OS_ARCH} STREQUAL "aarch64")
                add_compile_options(-D__aarch64__)
                set(CMAKE_C_FLAGS "--target=aarch64-linux-ohos -D__clang__ -march=armv8-a -w ")
                #指定c++编译工具（确保工具链所在路径已经添加到了PATH环境变量中）和编译标志，必须指定--target，否则无法交叉编译。
                set(CMAKE_CXX_FLAGS "--target=aarch64-linux-ohos -D__clang__ -march=armv8-a -w ")
                #指定链接工具和链接标志，必须指定--target和--sysroot，其中OHOS_ROOT_PATH可通过cmake命令后缀参数来指定。
                set(MY_LINK_FLAGS "--target=aarch64-linux-ohos --sysroot=${OHOS_SYSROOT_PATH}")
            endif()
            set(CMAKE_LINKER ${TOOLCHAIN_PATH}/bin/clang)
            set(CMAKE_CXX_LINKER ${TOOLCHAIN_PATH}/bin/clang++)
            set(CMAKE_C_LINKER ${TOOLCHAIN_PATH}/bin/clang)
            set(CMAKE_C_LINK_EXECUTABLE "${CMAKE_C_LINKER} ${MY_LINK_FLAGS} <FLAGS> <LINK_FLAGS> <OBJECTS> -o <TARGET> <LINK_LIBRARIES>")
            set(CMAKE_CXX_LINK_EXECUTABLE "${CMAKE_CXX_LINKER} ${MY_LINK_FLAGS} <FLAGS> <LINK_FLAGS> <OBJECTS> -o <TARGET> <LINK_LIBRARIES>")
            #指定链接库的查找路径。
            set(CMAKE_SYSROOT ${OHOS_SYSROOT_PATH})
        endif()
    endif ()
endif()

project(intarkdb)

if(MSVC)
    if (CMAKE_BUILD_TYPE STREQUAL "Debug")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /MDd")  # Debug: Multi-threaded Debug DLL
    elseif (CMAKE_BUILD_TYPE STREQUAL "Release")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /MD")   # Release: Multi-threaded DLL
    endif()
endif(MSVC)

if(NOT MSVC)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-error=int-conversion")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-error=implicit-function-declaration")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-error=incompatible-pointer-types")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-error=implicit-int")
endif()

if (ANDROID)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-error=format-security")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-error=-Wnull-dereference")
endif ()

if(NOT MSVC)
# for libfmt warning
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-dangling-reference")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-tautological-compare")
endif()

## output path
if (${CMAKE_BUILD_TYPE} STREQUAL "Release")
    if (ENABLE_INTARK_LITE)
        set (EXECUTABLE_OUTPUT_PATH     ${CMAKE_CURRENT_SOURCE_DIR}/output/release_lite/bin)
        set (LIBRARY_OUTPUT_PATH        ${CMAKE_CURRENT_SOURCE_DIR}/output/release_lite/lib)    
    else ()
        set (EXECUTABLE_OUTPUT_PATH     ${CMAKE_CURRENT_SOURCE_DIR}/output/release/bin)
        set (LIBRARY_OUTPUT_PATH        ${CMAKE_CURRENT_SOURCE_DIR}/output/release/lib)
    endif()
else ()
    if (ENABLE_INTARK_LITE)
        set (EXECUTABLE_OUTPUT_PATH     ${CMAKE_CURRENT_SOURCE_DIR}/output/debug_lite/bin)
        set (LIBRARY_OUTPUT_PATH        ${CMAKE_CURRENT_SOURCE_DIR}/output/debug_lite/lib)    
    else ()
        set (EXECUTABLE_OUTPUT_PATH     ${CMAKE_CURRENT_SOURCE_DIR}/output/debug/bin)
        set (LIBRARY_OUTPUT_PATH        ${CMAKE_CURRENT_SOURCE_DIR}/output/debug/lib)
    endif()
endif()

set(INTARKDB_HOME                   ${CMAKE_CURRENT_SOURCE_DIR})
## intarkdb lib path
set(INTARKDB_LIB_PATH               ${LIBRARY_OUTPUT_PATH})
## thrid inc path
set(INTARKDB_THRID_INC_PATH         ${INTARKDB_HOME}/output/inc)
## thrid lib path
# set(INTARKDB_THRID_LIB_PATH ${INTARKDB_HOME}/output/lib)
set(INTARKDB_THRID_LIB_PATH         ${INTARKDB_LIB_PATH})

## other dependency include  for copy destination directory
set(INTARKDB_DST_SECUREC_INC_PATH   ${INTARKDB_THRID_INC_PATH}/huawei_security)
set(INTARKDB_DST_PGQUERY_INC_PATH   ${INTARKDB_THRID_INC_PATH}/libpg_query)
set(INTARKDB_DST_UTF8PROC_INC_PATH  ${INTARKDB_THRID_INC_PATH}/utf8proc/include)
set(INTARKDB_DST_FMT_INC_PATH       ${INTARKDB_THRID_INC_PATH}/fmt)
## other dependency include  for target include directory
set(INTARKDB_SECUREC_INC_PATH       ${INTARKDB_HOME}/dependency/Huawei_Secure_C/include)
set(INTARKDB_PGQUERY_INC_PATH       ${INTARKDB_HOME}/dependency/libpg_query/include)
set(INTARKDB_UTF8PROC_INC_PATH      ${INTARKDB_HOME}/dependency/utf8proc/include)
set(INTARKDB_FMT_INC_PATH           ${INTARKDB_HOME}/dependency/fmt/include)
set(INTARKDB_GMSSL_INC_PATH             ${INTARKDB_HOME}/dependency/GmSSL/include)

set(INTARKDB_OPENSSL_PATH           ${INTARKDB_THRID_INC_PATH}/openssl)
set(INTARKDB_OPENSSL_EX_PATH        ${INTARKDB_THRID_INC_PATH}/openssl/openssl)
set(INTARKDB_CJSON_PATH             ${INTARKDB_THRID_INC_PATH}/cJSON)
if (ENABLE_LZ4)
set(INTARKDB_LZ4_INC_PATH           ${INTARKDB_THRID_INC_PATH}/lz4)
endif()
set(INTARKDB_ZLIB_INC_PATH          ${INTARKDB_THRID_INC_PATH}/zlib)
if (ENABLE_ZSTD)
set(INTARKDB_ZSTANDARD_INC_PATH     ${INTARKDB_THRID_INC_PATH}/zstd)
endif()
if (ENABLE_LIBAIO)
set(INTARKDB_LIBAIO_INC_PATH        ${INTARKDB_THRID_INC_PATH}/libaio)
endif()

## lib
set(SECUREC_LIB_PATH                ${INTARKDB_THRID_LIB_PATH})
set(OPENSSL_LIB_PATH                ${INTARKDB_THRID_LIB_PATH})
set(CJSON_LIB_PATH                  ${INTARKDB_THRID_LIB_PATH})
set(INTARKDB_LZ4_LIB_PATH           ${INTARKDB_THRID_LIB_PATH})
set(INTARKDB_ZLIB_LIB_PATH          ${INTARKDB_THRID_LIB_PATH})
set(INTARKDB_ZSTANDARD_LIB_PATH     ${INTARKDB_THRID_LIB_PATH})

## src include_directories
set(INTARKDB_SRC_PATH               ${INTARKDB_HOME}/src)

set(INTARKDB_INCLUDE_PATH           ${INTARKDB_SRC_PATH}/include)
set(INTARKDB_GSTOR_INC_PATH         ${INTARKDB_SRC_PATH}/storage/gstor)

set(INTARKDB_ZEKERNEL_COMMON_INC_PATH           ${INTARKDB_GSTOR_INC_PATH}/zekernel/common)
set(INTARKDB_ZEKERNEL_COMMON_VARIANT_INC_PATH   ${INTARKDB_ZEKERNEL_COMMON_INC_PATH}/variant)

set(INTARKDB_ZEKERNEL_KERNEL_PATH               ${INTARKDB_GSTOR_INC_PATH}/zekernel/kernel)
set(INTARKDB_ZEKERNEL_KERNEL_COMMON_INC_PATH    ${INTARKDB_ZEKERNEL_KERNEL_PATH}/common)
set(INTARKDB_ZEKERNEL_KERNEL_TABLESPACE_PATH    ${INTARKDB_ZEKERNEL_KERNEL_PATH}/tablespace)
set(INTARKDB_ZEKERNEL_KERNEL_PERSIST_PATH       ${INTARKDB_ZEKERNEL_KERNEL_PATH}/persist)
set(INTARKDB_ZEKERNEL_KERNEL_BUFFER_PATH        ${INTARKDB_ZEKERNEL_KERNEL_PATH}/buffer)
set(INTARKDB_ZEKERNEL_KERNEL_BACKUP_PATH        ${INTARKDB_ZEKERNEL_KERNEL_PATH}/backup)
set(INTARKDB_ZEKERNEL_KERNEL_TABLE_PATH         ${INTARKDB_ZEKERNEL_KERNEL_PATH}/table)
set(INTARKDB_ZEKERNEL_KERNEL_INDEX_PATH         ${INTARKDB_ZEKERNEL_KERNEL_PATH}/index)
set(INTARKDB_ZEKERNEL_KERNEL_XACT_PATH          ${INTARKDB_ZEKERNEL_KERNEL_PATH}/xact)
set(INTARKDB_ZEKERNEL_KERNEL_FLASHBACK_PATH     ${INTARKDB_ZEKERNEL_KERNEL_PATH}/flashback)
set(INTARKDB_ZEKERNEL_KERNEL_CATALOG_PATH       ${INTARKDB_ZEKERNEL_KERNEL_PATH}/catalog)
set(INTARKDB_ZEKERNEL_KERNEL_LOB_PATH           ${INTARKDB_ZEKERNEL_KERNEL_PATH}/lob)
set(INTARKDB_ZEKERNEL_KERNEL_DAEMON_PATH        ${INTARKDB_ZEKERNEL_KERNEL_PATH}/daemon)
set(INTARKDB_ZEKERNEL_KERNEL_REPLICATION_PATH   ${INTARKDB_ZEKERNEL_KERNEL_PATH}/replication)
set(INTARKDB_ZEKERNEL_KERNEL_SEQUENCE_PATH      ${INTARKDB_ZEKERNEL_KERNEL_PATH}/sequence)
set(INTARKDB_ZEKERNEL_KERNEL_STATISTICS_PATH    ${INTARKDB_ZEKERNEL_KERNEL_PATH}/statistics)
set(INTARKDB_ZEKERNEL_KERNEL_INC_PATH           ${INTARKDB_ZEKERNEL_KERNEL_PATH}/include)

set(INTARKDB_KV_STORAGE_PATH        ${PROJECT_SOURCE_DIR}/src/storage/kv_executor)

set(INTARKDB_COMPUTE_SQL_INC_PATH   ${INTARKDB_HOME}/src/compute/sql/include)

set(INTARKDB_PARSE_PATH             ${INTARKDB_HOME}/src/utils/parse)
set(INTARKDB_UTILS_PATH             ${INTARKDB_HOME}/src/utils)

# Determine build rpath
set(CMAKE_BUILD_RPATH "${INTARKDB_LIB_PATH}")

# Determine install rpath
set(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/${INSTALL_LIB_DIR}")
set(INSTALL_RPATH_USE_LINK_PATH TRUE)

# Determine install paths
set(INSTALL_LIB_DIR
    lib
    CACHE PATH "Installation directory for libraries")
set(INSTALL_BIN_DIR
    bin
    CACHE PATH "Installation directory for executables")
set(INSTALL_INCLUDE_DIR
    include
    CACHE PATH "Installation directory for header files")

# dependency compile
add_subdirectory(dependency)

include(
    "${PROJECT_SOURCE_DIR}/build/cmake_include/CMakeListsInclude.txt"
)

# add_compile_options(-DTIMESTAT)

string(TOLOWER ${CMAKE_BUILD_TYPE}  CMAKE_BUILD_TYPE_LOWER)
message(STATUS "BUILD_TYPE = ${CMAKE_BUILD_TYPE}")
if (${CMAKE_BUILD_TYPE_LOWER} STREQUAL "debug"
    OR ${CMAKE_BUILD_TYPE_LOWER} STREQUAL "")
    set(CMAKE_BUILD_TYPE Debug)
    add_compile_definitions(_DEBUG)
    set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -O0 -g2 -ggdb")
    SET(CMAKE_CXX_FLAGS_DEBUG "$ENV{CXXFLAGS} -O0 -Wall -g2 -ggdb")
elseif (${CMAKE_BUILD_TYPE_LOWER} STREQUAL "release")
    message(STATUS "CMAKE_BUILD_TYPE is Release")
    set(CMAKE_BUILD_TYPE Release)
    set(CMAKE_CXX_FLAGS_RELEASE "-O2  -DNDEBUG")
    if (OS_ARCH)
	    if (${OS_ARCH} STREQUAL "arm32")
            set(CMAKE_CXX_FLAGS_RELEASE "-O0  -DNDEBUG")
            set(CMAKE_C_FLAGS_RELEASE "-O0 -DNDEBUG")
    	endif()
    endif()
else ()
    message(FATAL_ERROR "unknown CMAKE_BUILD_TYPE = " ${CMAKE_BUILD_TYPE})
endif ()

set(CMAKE_INSTALL_PREFIX $ENV{dcc_install_prefix})
if(UNIX)
	set(CMAKE_C_FLAGS "-std=gnu99 ${CMAKE_C_FLAGS}")
else()
	set(CMAKE_C_FLAGS " ${CMAKE_C_FLAGS}")
endif()
option(STATISTICS "enable statistics" ON)


#version
execute_process(
	COMMAND git describe --tags --abbrev=0
	WORKING_DIRECTORY ${PROJECT_SOURCE_DIR} 
	OUTPUT_VARIABLE GIT_VERSION
)
if (NOT GIT_VERSION STREQUAL "") 
    string (REGEX REPLACE "[\n\t\r]" "" GIT_VERSION ${GIT_VERSION})
    MESSAGE(STATUS "GIT_VERSION = ${GIT_VERSION}")
    set(git_version ${GIT_VERSION})
else()
    set(git_version "0.0.0")
endif()

execute_process(
	COMMAND git log --pretty=format:%h -1
	WORKING_DIRECTORY ${PROJECT_SOURCE_DIR} 
	OUTPUT_VARIABLE GIT_COMMIT_ID
)
if (NOT GIT_COMMIT_ID STREQUAL "") 
    string (REGEX REPLACE "[\n\t\r]" "" GIT_COMMIT_ID ${GIT_COMMIT_ID})
    MESSAGE(STATUS "GIT_COMMIT_ID = ${GIT_COMMIT_ID}")
    set(git_commit_id ${GIT_COMMIT_ID})
else()
    set(git_commit_id "Error:no git")
endif()

add_definitions(-DINTARKDB_SOURCE_ID="\""${git_commit_id}"\"")
add_definitions(-DINTARKDB_VERSION="\""${git_version}"\"")

# configure_file(
#     "${PROJECT_SOURCE_DIR}/build/VERSION.h.in"
#     "${PROJECT_SOURCE_DIR}/VERSION.h"
# )
if (NOT ANDROID)
    add_compile_options(-lpthread)
endif ()
add_compile_options(-fPIC -Wall -MMD -fno-strict-aliasing -fsigned-char -fms-extensions)
add_compile_definitions(_GNU_SOURCE _LARGEFILE64_SOURCE KNL_PREFIX _REENTRANT __PERF_STAT__)
if(UNIX)
	add_link_options(-pie)
	add_link_options(-Wl,-z,noexecstack -Wl,-z,relro -Wl,-z,now)
	add_compile_options(-fvisibility=default -fstack-protector-strong --param ssp-buffer-size=4)
endif()

if(WIN32 AND NOT MSVC)
	add_compile_options(-fvisibility=default -fno-stack-protector --param ssp-buffer-size=4)
endif()
# 安全编译选项
# set(CMAKE_SKIP_RPATH TRUE)
if(UNIX)
    if (${CMAKE_BUILD_TYPE} STREQUAL "Release")
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -ldl -pthread -Wl,-z,noexecstack -Wl,-z,relro -Wl,-z,now")
    else ()
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -g -ldl -pthread -Wl,-z,noexecstack -Wl,-z,relro -Wl,-z,now")
    endif ()
elseif(ANDROID)
    if (${CMAKE_BUILD_TYPE} STREQUAL "Release")
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -ldl -Wl,-z,noexecstack -Wl,-z,relro -Wl,-z,now")
    else ()
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -g -ldl -Wl,-z,noexecstack -Wl,-z,relro -Wl,-z,now")
    endif ()
else(NOT MSVC)
    if (${CMAKE_BUILD_TYPE} STREQUAL "Release")
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS}  -pthread")
    else ()
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -g -pthread")
    endif ()
endif()

if(NOT MSVC)
add_compile_options(-fno-common)
add_compile_options(-Wtrampolines)
endif(NOT MSVC)
if (OS_ARCH)
    if (NOT ((${OS_ARCH} STREQUAL "arm32") OR (${OS_ARCH} STREQUAL "aarch64")))
        add_compile_options(-freg-struct-return)
    endif()
else()
    add_compile_options(-freg-struct-return)
endif()
if(NOT MSVC)
add_compile_options(-pipe)
add_compile_options(-Wl,-Bsymbolic)
endif(NOT MSVC)

if (UNIX)
    add_compile_options(-rdynamic)
endif()

if (ANDROID)
    # 安卓ndk下STATISTICS定义有冲突选项使用STATISTICS_O控制
    if (NOT STATISTICS_O)
        set(STATISTICS OFF)
    endif ()
    # 安卓宏分支
    add_compile_options(-D_ANDROID)
endif ()
# 内核模块条件编译选项
if(STATISTICS)
    add_compile_options(-D_STATISTICS)
endif()

if (ENABLE_BACKUP)
    add_compile_options(-D_BACKUP)
endif()

if(ENABLE_ICONV)
    add_compile_options(-D_ICONV)
endif()

OPTION(ENABLE_GCOV "Enable gcov" OFF)
message(STATUS "INTARKDB ENABLE_GCOV = ${ENABLE_GCOV}")
IF (ENABLE_GCOV AND NOT WIN32)
    message(STATUS "gcov INTARKDB enable")
    SET(CMAKE_EXE_LINKER_FLAGS_DEBUG "${CMAKE_EXE_LINKER_FLAGS_DEBUG} -fprofile-arcs -ftest-coverage -lgcov")
    SET(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -fprofile-arcs -ftest-coverage")
    Add_Definitions(-DENABLE_GCOV)
    SET(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fprofile-arcs -ftest-coverage")
ENDIF()

OPTION(ENABLE_EXPORT_API "Enable hidden internal api" OFF)
message(STATUS "ENABLE_EXPORT_API = ${ENABLE_EXPORT_API}")
IF (ENABLE_EXPORT_API)
    add_compile_options(-fvisibility=hidden)
ENDIF()

option(ENABLE_DCC_LITE OFF)
IF (ENABLE_DCC_LITE)
    add_compile_definitions(DCC_LITE)
    message(STATUS "DCC lite enable")
ENDIF()

option(ENABLE_INTARK_LITE OFF)
IF (ENABLE_INTARK_LITE)
    add_compile_definitions(INTARK_LITE)
    message(STATUS "intarkdb lite enable")
ENDIF()

option(ENABLE_PG_QUERY OFF)
IF (ENABLE_PG_QUERY)
    add_compile_definitions(ENABLE_PG_QUERY)
    message(STATUS "PG_QUERY enable")
ENDIF()

message(STATUS "UT = ${UT}")
if (UT)
    message("Turn off hidden libs for UT")
    set(HIDDEN_LIBS "")
else ()
    message("Turn on hidden libs")
    set(HIDDEN_LIBS "-Wl,--exclude-libs,ALL")
endif()

if (NOT OS_ARCH)
    execute_process(
        COMMAND uname -m
        OUTPUT_VARIABLE OS_ARCH
        OUTPUT_STRIP_TRAILING_WHITESPACE
    )
    set(CMAKE_SYSTEM_PROCESSOR ${OS_ARCH})
endif()

if (${OS_ARCH} STREQUAL "aarch64")
    option(USE_H1620 OFF)
    if (USE_H1620)
        add_compile_options(-march=armv8-a+crc+lse)
        message(STATUS "Toolchain: Build aarch64 USE_H1620")
    else ()
        # add_compile_options(-march=armv8-a+crc)
    endif (USE_H1620)

    add_compile_options( -fsigned-char -funwind-tables)
elseif(${OS_ARCH} STREQUAL "x86_64")
    add_compile_options(-msse4.2 )
endif ()
Add_Definitions(-DWSEC_COMPILE_CAC_OPENSSL -DWSEC_AES_GCM_SUPPORT -DWSEC_USE_OPENSSL_110 -DWSEC_COMPILE_SDP)

set(3RD_PATH $ENV{THIRD_BIN_PATH})
set(PLAT_FORM_NAME $ENV{PLAT_FORM_STR})
message(STATUS "ENABLE_MEMCHECK = ${ENABLE_MEMCHECK}")
if (ENABLE_MEMCHECK)
    message("add memcheck dependencies.")
    if ("x${GCC_VERSION}" STREQUAL "x")
        set(GCC_VERSION "7.3.0")
    endif ()
    if (("x${3RD_PATH}" STREQUAL "x" ))
        message("3rd path is empty")
    else ()
        message("3rd path is ${3RD_PATH}")
        set(3RD_DEPENDENCY_ROOT ${3RD_PATH}/kernel/dependency)
        set(MEMCHECK_HOME ${3RD_DEPENDENCY_ROOT}/memcheck/debug)
        set(MEMCHECK_LIB_PATH ${MEMCHECK_HOME}/gcc${GCC_VERSION}/lib)
        set(MEMCHECK_LINK_DIRECTORIES ${MEMCHECK_LIB_PATH})
    endif ()
    set(MEMCHECK_FLAGS -fsanitize=address -fsanitize=leak -fno-omit-frame-pointer)
    if (NOT ANDROID)
        set(MEMCHECK_LIBS libasan.a rt dl)
    else()
        set(MEMCHECK_LIBS libasan.a dl)
    endif ()

    add_compile_options(${MEMCHECK_FLAGS})
endif ()
set(G_BIN_EXT_LIBS ${MEMCHECK_LIBS})

link_directories(${LIBRARY_OUTPUT_PATH} ${SECUREC_LIB_PATH} ${INTARKDB_ZLIB_LIB_PATH} ${INTARKDB_ZSTANDARD_LIB_PATH} ${MEMCHECK_LINK_DIRECTORIES})

# set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,-rpath=${CMAKE_INSTALL_PREFIX}/${INSTALL_LIB_DIR}")

set(vpp_libsecurec "securec")
if (ENABLE_LZ4)
set(3rd_liblz4 "lz4")
endif()
if (ENABLE_ZSTD)
set(3rd_libzstd "zstd")
endif()
if (ENABLE_SSL)
set(3rd_libssl "ssl")
endif()
if (ENABLE_CRYPTO)
set(3rd_lib_crypto "crypto")
endif()
set(3rd_libjson "cjson")

if(UNIX)
	set(3rd_libz "z")
else()	
	set(3rd_libz "zlib")
endif()

if(WIN32)
    add_definitions(-D_WIN32 -DWIN32)
    add_compile_options(-w)
    add_compile_options(-mfma)
    add_compile_definitions(_WIN32_WINNT=0x0A00 O_SYNC=0  WIN32_STACK_RLIMIT=4194304 O_DIRECT=0 O_DSYNC=0)
endif(WIN32)

if(WIN64)
    message(STATUS "================= WIN64 =================")
endif(WIN64)

add_subdirectory(src)
add_subdirectory(interface)
if (NOT ANDROID)
    add_subdirectory(tools)
    add_subdirectory(examples)
endif ()



install(
    TARGETS intarkdb sqlite3_api_wrapper securec
    LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
    ARCHIVE DESTINATION "${INSTALL_LIB_DIR}"
    RUNTIME DESTINATION "${INSTALL_BIN_DIR}")

if(UNIX)
    IF (ENABLE_PG_QUERY)
        install(
            TARGETS intarkdb_cli
            RUNTIME DESTINATION "${INSTALL_BIN_DIR}")
    ENDIF()

    install(FILES ${INTARKDB_LIB_PATH}/libz.so.1.2.12
                ${INTARKDB_LIB_PATH}/libz.so.1
                ${INTARKDB_LIB_PATH}/libz.so
                ${INTARKDB_LIB_PATH}/libcjson.so
                ${INTARKDB_LIB_PATH}/libcjson.so.1
                ${INTARKDB_LIB_PATH}/libcjson.so.1.7.15
                ${INTARKDB_LIB_PATH}/libgmssl.so
                ${INTARKDB_LIB_PATH}/libgmssl.so.3
                ${INTARKDB_LIB_PATH}/libgmssl.so.3.0
                ${INTARKDB_LIB_PATH}/libintarkdb.a
        DESTINATION ${INSTALL_LIB_DIR})
else()
    install(FILES ${INTARKDB_LIB_PATH}/libzlib1.dll
                ${INTARKDB_LIB_PATH}/libcjson.dll
                ${INTARKDB_LIB_PATH}/libgmssl.dll
                ${INTARKDB_LIB_PATH}/libintarkdb.dll.a
        DESTINATION ${INSTALL_LIB_DIR})
endif(UNIX)
# uninstall target
set(INTARKDB_BUILD_DIR "${PROJECT_BINARY_DIR}")
message(STATUS "INTARKDB_BUILD_DIR = ${INTARKDB_BUILD_DIR}")

configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake_uninstall.cmake.in"
    "${PROJECT_BINARY_DIR}/cmake_uninstall.cmake" @ONLY)

add_custom_target(uninstall
    COMMAND ${CMAKE_COMMAND} -P ${PROJECT_BINARY_DIR}/cmake_uninstall.cmake)
