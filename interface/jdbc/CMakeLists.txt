cmake_minimum_required(VERSION 3.14.1)

project(jdbc)

# open source include
set(SQL_C_API ${INTARKDB_SRC_PATH}/compute/sql/include/main/capi)

include_directories(${INTARKDB_SRC_PATH})
include_directories(${INTARKDB_ZEKERNEL_COMMON_INC_PATH})
include_directories(${INTARKDB_ZEKERNEL_COMMON_INC_PATH}/variant)
include_directories(${SECUREC_INC_PATH})
include_directories(${SQL_C_API})
include_directories(${INTARKDB_SECUREC_INC_PATH})
include_directories($ENV{JAVA_HOME}/include)
if (WIN32)
include_directories($ENV{JAVA_HOME}/include/win32)
else()
include_directories($ENV{JAVA_HOME}/include/linux)
endif(WIN32)
include_directories(${INTARKDB_HOME}/interface/c)

set(INTARKDB_LINK_LIBS
    fmt::fmt
    duckdb_pg_query
    utf8proc
    gmssl
    m)

link_directories(${INTARKDB_LIB_PATH} ) 

set(JAVA_SVR ${CMAKE_CURRENT_SOURCE_DIR}/org_intarkdb_core_IntarkdbNative.c)

if(ANDROID)
    add_library(intarkdb_jni SHARED ${JAVA_SVR}
    $<TARGET_OBJECTS:sql_object>
    $<TARGET_OBJECTS:kv_object>
    $<TARGET_OBJECTS:storage_obj> $<TARGET_OBJECTS:gstor_obj>
    $<TARGET_OBJECTS:zekernel> $<TARGET_OBJECTS:zecommon>
    $<TARGET_OBJECTS:securec_obj>
    $<TARGET_OBJECTS:zlib_obj>
    $<TARGET_OBJECTS:ts_object>
    $<TARGET_OBJECTS:status>)
else()
    add_library(intarkdb_jni SHARED ${JAVA_SVR}
    $<TARGET_OBJECTS:sql_object>
    $<TARGET_OBJECTS:kv_object>
    $<TARGET_OBJECTS:storage_obj> $<TARGET_OBJECTS:gstor_obj>
    $<TARGET_OBJECTS:zekernel> $<TARGET_OBJECTS:zecommon>
    $<TARGET_OBJECTS:network_server_protocol_obj>
    $<TARGET_OBJECTS:network_network_obj>
    $<TARGET_OBJECTS:network_login_obj>
    $<TARGET_OBJECTS:network_server_obj>
    $<TARGET_OBJECTS:securec_obj>
    $<TARGET_OBJECTS:zlib_obj>
    $<TARGET_OBJECTS:status>)
endif()


target_link_libraries(intarkdb_jni PUBLIC ${INTARKDB_LINK_LIBS})

target_link_directories(intarkdb_jni PUBLIC ${INTARKDB_LIB_PATH} ${INTARKDB_THRID_LIB_PATH})

if(ANDROID)
    target_link_libraries(intarkdb_jni PUBLIC dl ${G_BIN_EXT_LIBS})
elseif(UNIX)
    target_link_libraries(intarkdb_jni PUBLIC dl rt ${G_BIN_EXT_LIBS} pthread)
else()
    target_link_libraries(intarkdb_jni PUBLIC wsock32 ws2_32 pthread)
endif()

if (WIN32)
    target_link_options(intarkdb_jni PUBLIC "-Wl,--export-all-symbols")
endif(WIN32)
                                
target_link_libraries(intarkdb_jni PUBLIC)
