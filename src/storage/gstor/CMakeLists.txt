# 指定了需要的 CMake 最低版本
cmake_minimum_required(VERSION 3.14.1)

# 定义项目的名称
# project(gstor)

# 判断操作系统是否为 UNIX 系统
if (UNIX)
    # 打印出当前构建类型（CMAKE_BUILD_TYPE）的信息
    message(STATUS "BUILD Type is ${CMAKE_BUILD_TYPE}")
    #  Debug 构建或空字符串（未指定构建类型）
    if ("${CMAKE_BUILD_TYPE}" STREQUAL "Debug"
         OR "${CMAKE_BUILD_TYPE}" STREQUAL "")
        message(STATUS "CMAKE_BUILD_TYPE is Debug")
        # 设置 CMAKE_BUILD_TYPE 为 Debug
        set(CMAKE_BUILD_TYPE Debug)
        # 添加编译定义 _DEBUG
        add_compile_definitions(_DEBUG)
    # Release 构建
    elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "Release")
        message(STATUS "CMAKE_BUILD_TYPE is Release")
        # 将 CMAKE_BUILD_TYPE 设置为 Release
        set(CMAKE_BUILD_TYPE Release)
        # -O2 是指定编译器进行优化的级别，常见的级别包括 -O0、-O1、-O2 和 -O3。
        # -O2 表示较高的优化级别，编译器将尽可能地优化生成的代码，以提高执行效率和性能。
        # -DNDEBUG 是定义了一个名为 NDEBUG 的宏,通常用于禁用调试相关的代码。
        # 如果定义了 NDEBUG 宏，编译器会忽略与调试相关的代码（如 assert 宏），以提高代码的执行效率。
        # set(CMAKE_CXX_FLAGS_RELEASE "-O2 -DNDEBUG")
    else ()
        message(STATUS "unknown CMAKE_BUILD_TYPE = " ${CMAKE_BUILD_TYPE})
    endif ()

    # 定义了一个开关选项 ENABLE_GCOV，用于是否启用 gcov（用于代码覆盖率分析）
    OPTION(ENABLE_GCOV "GSTORE Enable gcov (debug, Linux builds only)" OFF)
    message(STATUS "GSTORE ENABLE_GCOV = ${ENABLE_GCOV}")
    # 在 Linux 系统下且开启了 ENABLE_GCOV，非 win32 与 apple
    IF (ENABLE_GCOV AND NOT WIN32 AND NOT APPLE)
        message(STATUS "GSTORE Enable gcov (debug, Linux builds only).")
        # -g 选项用于在编译过程中生成调试信息。它允许在程序崩溃或出现错误时进行源代码级的调试，包括查看变量的值、函数调用栈等
        # -O0 选项表示关闭优化。它告诉编译器不进行任何优化，生成的代码与源代码一一对应，以便于调试
        # -fprofile-arcs 选项告诉编译器在编译时收集代码的执行信息，包括每个分支和函数的执行次数。这些信息存储在程序运行时生成的 .gcda 文件中
        # -ftest-coverage 选项告诉编译器生成代码覆盖率信息。编译器会在生成的可执行文件中插入额外的代码，用于跟踪每个代码块（如语句、分支等）的执行情况。这些信息存储在程序运行时生成的 .gcno 文件中
        add_compile_options(-g -O0 -fprofile-arcs -ftest-coverage)
    ENDIF()

    # Use "-fPIC" / "-fPIE" for all targets by default, including static libs
    # 生成与位置无关的代码，可在内存中的任意位置加载和执行
    set(CMAKE_POSITION_INDEPENDENT_CODE ON)
    # CMake doesn't add "-pie" by default for executables
    # 生成位置无关的可执行文件，使其能够在内存中的任意位置加载和执行
    add_link_options(-pie)
    if (NOT OS_ARCH)
    execute_process(
            # 获取当前系统的处理器架构信息
            COMMAND uname -p
            # 输出结果将通过 OUTPUT_VARIABLE 参数存储在 OS_ARCH 变量中
            OUTPUT_VARIABLE OS_ARCH
            # 去除输出结果末尾的空白字符（如换行符）
            OUTPUT_STRIP_TRAILING_WHITESPACE
    )
    # 将 CMake 的 CMAKE_SYSTEM_PROCESSOR 变量设置为 OS_ARCH 的值
    set(CMAKE_SYSTEM_PROCESSOR ${OS_ARCH})
    endif()
    if (${OS_ARCH} STREQUAL "aarch64")
        message(STATUS "aarch64 CPU")
        add_compile_options(-D__aarch64__)
        option(USE_H1620 OFF)
        if (USE_H1620)
            add_compile_options(-march=armv8-a+crc+lse)
            message(STATUS "Toolchain: Build aarch64 USE_H1620")
        else ()
            # add_compile_options(-march=armv8-a+crc)
        endif (USE_H1620)

        add_compile_options( -fsigned-char -funwind-tables)
    elseif(${OS_ARCH} STREQUAL "x86_64")
        message(STATUS "x86_64 CPU")
        add_compile_options(-mtune=nocona -fsigned-char -msse4.2 )
    endif ()
else ()
    # set(CMAKE_BUILD_TYPE Debug)
    # add_compile_definitions(_DEBUG)
endif ()
if(NOT DEFINED OS_ARCH)
    STRING(FIND ${OS_ARCH} "64" pos)
    if (pos GREATER -1)
        message("++ 64 bit architecture")
        set(ARCHBIT "64")
    else ()
        message("++ 32 bit architecture")
        set(ARCHBIT "32")
        add_compile_options(-m32)
    endif ()
endif ()
if (UNIX)
    set_property(TARGET PROPERTY C_STANDARD 99)
    add_compile_options(-fPIC -Wall -MMD -fno-strict-aliasing -fsigned-char -fms-extensions)
    add_compile_definitions(_GNU_SOURCE _LARGEFILE64_SOURCE KNL_PREFIX _REENTRANT)

    add_compile_options(-fvisibility=default -fstack-protector-strong --param ssp-buffer-size=4)
    add_link_options(-Wl,-z,noexecstack -Wl,-z,relro -Wl,-z,now)
    set(CMAKE_SKIP_RPATH TRUE)
endif ()

# open source include
set(DCC_HOME ${CMAKE_CURRENT_SOURCE_DIR}/../../../)
set(GSTOR_SRC_HOME ${CMAKE_CURRENT_SOURCE_DIR}/../../)

# zekernel include_directories
set(ZEKERNEL_KERNEL_PATH     ${CMAKE_CURRENT_SOURCE_DIR}/zekernel/kernel)
set(ZEKERNEL_PROTOCOL_PATH   ${CMAKE_CURRENT_SOURCE_DIR}/zekernel/protocol)
set(ZEKERNEL_COMMON_PATH     ${CMAKE_CURRENT_SOURCE_DIR}/zekernel/common)

set(SECUREC_INC_PATH        "${INTARKDB_THRID_INC_PATH}/huawei_security")
set(OPENSSL_PATH            "${INTARKDB_THRID_INC_PATH}/openssl/")
set(OPENSSL_PATH_EX         "${INTARKDB_THRID_INC_PATH}/openssl/openssl")
set(LZ4_INC_PATH            "${INTARKDB_THRID_INC_PATH}/lz4")
set(ZLIB_INC_PATH           "${INTARKDB_THRID_INC_PATH}/zlib")
set(ZSTANDARD_INC_PATH      "${INTARKDB_THRID_INC_PATH}/zstd")
if (ENABLE_LIBAIO)
set(LIBAIO_INC_PATH         "${INTARKDB_THRID_INC_PATH}/libaio")
endif()
# open source lib
set(SECUREC_LIB_PATH        "${INTARKDB_THRID_LIB_PATH}")
set(OPENSSL_LIB_PATH        "${INTARKDB_THRID_LIB_PATH}")
set(LZ4_LIB_PATH            "${INTARKDB_THRID_LIB_PATH}")
set(ZLIB_LIB_PATH           "${INTARKDB_THRID_LIB_PATH}")
set(ZSTANDARD_LIB_PATH      "${INTARKDB_THRID_LIB_PATH}")

# output library path
message(STATUS "LIBRARY_OUTPUT_PATH: ${LIBRARY_OUTPUT_PATH}")
message(STATUS "EXECUTABLE_OUTPUT_PATH: ${EXECUTABLE_OUTPUT_PATH}")
set(vpp_libsecurec "securec")
set(3rd_liblz4 "lz4")
if (ENABLE_ZSTD)
set(3rd_libzstd "zstd")
endif()
if (ENABLE_SSL)
set(3rd_libssl "ssl")
endif()
if (ENABLE_CRYPTO)
set(3rd_lib_crypto "crypto")
endif()
if(UINX)
    if (${CMAKE_BUILD_TYPE} STREQUAL "Release")
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -std=c99 -ldl -pthread")
    else ()
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -std=c99 -g -ldl -pthread")
    endif ()
elseif(ANDROID)
    if (${CMAKE_BUILD_TYPE} STREQUAL "Release")
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -std=c99 -ldl")
    else ()
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -std=c99 -g -ldl")
    endif ()
else()
    if (${CMAKE_BUILD_TYPE} STREQUAL "Release")
        if(NOT MSVC)
            set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS}  -pthread")
        endif()
    else ()
        if(NOT MSVC)
            set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS}  -g  -pthread")
        endif()
    endif ()
endif()
if(NOT MSVC)
add_compile_options(-fno-common)
add_compile_options(-Wtrampolines)
endif()
if (NOT ((${OS_ARCH} STREQUAL "arm32") OR (${OS_ARCH} STREQUAL "aarch64")))
    add_compile_options(-freg-struct-return)
else()
endif()
add_compile_options(-pipe)
add_link_options(-Wl,-Bsymbolic)
if (UNIX)
    add_link_options(-rdynamic)
endif()

include_directories(${GSTOR_SRC_HOME})
include_directories(${SECUREC_INC_PATH})
if (ENABLE_SSL)
include_directories(${OPENSSL_PATH})
include_directories(${OPENSSL_PATH_EX})
endif()
include_directories(${LZ4_INC_PATH})
include_directories(${ZLIB_INC_PATH})
if (ENABLE_ZSTD)
include_directories(${ZSTANDARD_INC_PATH})
endif()
if (ENABLE_LIBAIO)
include_directories(${LIBAIO_INC_PATH})
endif()
include_directories(${PCRE_INC_PATH})
include_directories(${ZEKERNEL_COMMON_PATH})
include_directories(${ZEKERNEL_COMMON_PATH}/variant)
include_directories(${ZEKERNEL_KERNEL_PATH})
include_directories(${ZEKERNEL_PROTOCOL_PATH})
#if (ENABLE_BACKUP)
    include_directories(${ZEKERNEL_KERNEL_PATH}/backup)
#endif()
include_directories(${ZEKERNEL_KERNEL_PATH}/buffer)
include_directories(${ZEKERNEL_KERNEL_PATH}/common)
include_directories(${ZEKERNEL_KERNEL_PATH}/daemon)
include_directories(${ZEKERNEL_KERNEL_PATH}/flashback)
include_directories(${ZEKERNEL_KERNEL_PATH}/include)
include_directories(${ZEKERNEL_KERNEL_PATH}/index)
include_directories(${ZEKERNEL_KERNEL_PATH}/lob)
include_directories(${ZEKERNEL_KERNEL_PATH}/persist)
#include_directories(${ZEKERNEL_KERNEL_PATH}/replication)
include_directories(${ZEKERNEL_KERNEL_PATH}/sequence)
if(STATISTICS)
include_directories(${ZEKERNEL_KERNEL_PATH}/statistics)
endif()
include_directories(${ZEKERNEL_KERNEL_PATH}/table)
include_directories(${ZEKERNEL_KERNEL_PATH}/xact)
include_directories(${ZEKERNEL_KERNEL_PATH}/tablespace)
# include_directories(${ZEKERNEL_KERNEL_PATH}/statistics)
include_directories(${ZEKERNEL_KERNEL_PATH}/catalog)

add_subdirectory(zekernel)
#link_directories(${SECUREC_LIB_PATH} ${OPENSSL_LIB_PATH} ${LZ4_LIB_PATH} ${ZLIB_LIB_PATH} ${ZSTANDARD_LIB_PATH})
link_directories(${SECUREC_LIB_PATH}  ${ZLIB_LIB_PATH})

aux_source_directory(. GSTOR_SRC)

if(WIN32)
    if(NOT MSVC)
    add_compile_options(-Wno-incompatible-pointer-types)
    endif()
endif()
add_compile_options(-w)
# add_library(gstor SHARED ${GSTOR_SRC} $<TARGET_OBJECTS:zekernel> $<TARGET_OBJECTS:zecommon> )
# #target_link_libraries(gstor PRIVATE zekernel z ${3rd_libzstd} ${3rd_libssl} ${3rd_lib_crypto} ${3rd_liblz4} -Wl,--whole-archive ${vpp_libsecurec} ${G_BIN_EXT_LIBS} -Wl,--no-whole-archive)
# target_link_libraries(gstor PUBLIC zekernel zecommon ${3rd_libz} -Wl,--whole-archive ${vpp_libsecurec} ${G_BIN_EXT_LIBS} -Wl,--no-whole-archive)

add_library(gstor_obj OBJECT ${GSTOR_SRC} $<TARGET_OBJECTS:zekernel> $<TARGET_OBJECTS:zecommon> )
# target_link_libraries(gstor_obj PUBLIC zekernel zecommon ${3rd_libz} -Wl,--whole-archive ${vpp_libsecurec} ${G_BIN_EXT_LIBS} -Wl,--no-whole-archive)