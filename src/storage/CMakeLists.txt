cmake_minimum_required(VERSION 3.14.1)

# project(storage)

# 设置生成编译命令的输出文件
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(WIN32)
    if(NOT MSVC)
    add_compile_options(-Wno-incompatible-pointer-types)
    endif()
endif()

# open source include
include_directories(${INTARKDB_SRC_PATH})
include_directories(${INTARKDB_ZEKERNEL_COMMON_INC_PATH})
include_directories(${INTARKDB_ZEKERNEL_COMMON_VARIANT_INC_PATH})
include_directories(${INTARKDB_ZEKERNEL_KERNEL_PATH})
include_directories(${INTARKDB_ZEKERNEL_KERNEL_COMMON_INC_PATH})
include_directories(${INTARKDB_ZEKERNEL_KERNEL_INC_PATH})
include_directories(${INTARKDB_ZEKERNEL_KERNEL_BACKUP_PATH})
include_directories(${INTARKDB_ZEKERNEL_KERNEL_BUFFER_PATH})
include_directories(${INTARKDB_ZEKERNEL_KERNEL_CATALOG_PATH})
include_directories(${INTARKDB_ZEKERNEL_KERNEL_DAEMON_PATH})
include_directories(${INTARKDB_ZEKERNEL_KERNEL_FLASHBACK_PATH})
include_directories(${INTARKDB_ZEKERNEL_KERNEL_INDEX_PATH})
include_directories(${INTARKDB_ZEKERNEL_KERNEL_LOB_PATH})
include_directories(${INTARKDB_ZEKERNEL_KERNEL_PERSIST_PATH})
include_directories(${INTARKDB_ZEKERNEL_KERNEL_REPLICATION_PATH})
include_directories(${INTARKDB_ZEKERNEL_KERNEL_SEQUENCE_PATH})
include_directories(${INTARKDB_ZEKERNEL_KERNEL_STATISTICS_PATH})
include_directories(${INTARKDB_ZEKERNEL_KERNEL_TABLE_PATH})
include_directories(${INTARKDB_ZEKERNEL_KERNEL_TABLESPACE_PATH})
include_directories(${INTARKDB_ZEKERNEL_KERNEL_XACT_PATH})
include_directories(${INTARKDB_GSTOR_INC_PATH})
include_directories(${INTARKDB_SECUREC_INC_PATH})

# 3rd
include_directories(${INTARKDB_SECUREC_INC_PATH})
include_directories(${INTARKDB_OPENSSL_PATH})
include_directories(${INTARKDB_OPENSSL_EX_PATH})
include_directories(${INTARKDB_LZ4_INC_PATH})
include_directories(${INTARKDB_ZLIB_INC_PATH})
include_directories(${INTARKDB_ZSTANDARD_INC_PATH})
if (ENABLE_LIBAIO)
include_directories(${INTARKDB_LIBAIO_INC_PATH})
endif()

add_subdirectory(gstor)
# add_subdirectory(kv_executor)
# add_subdirectory(relational_executor)

# no need storage.c storage.h
# aux_source_directory(. CURR_SRC)
# aux_source_directory(kv_executor KV_EXECUTOR_SRC)
# aux_source_directory(relational_executor RELA_EXECUTOR_SRC)
# set(STORAGE_SRC ${CURR_SRC} ${KV_EXECUTOR_SRC} ${RELA_EXECUTOR_SRC})

add_library(storage_obj OBJECT ${STORAGE_SRC} $<TARGET_OBJECTS:gstor_obj> $<TARGET_OBJECTS:zekernel> $<TARGET_OBJECTS:zecommon>)
