
cmake_minimum_required(VERSION 3.14.1)
# project(sql_engine)

set(BUILD_SHARED_LIBS OFF)

if(CMAKE_COMPILER_IS_GNUCXX)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-attributes")
endif()
if(NOT MSVC)
add_compile_options(-fPIC)
# add_compile_options(-fPIC -MMD -fno-strict-aliasing -fsigned-char -fms-extensions)
add_compile_options(-fpermissive)
endif(NOT MSVC)
set(CMAKE_CXX_STANDARD 17)
# create compile_command.json
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include(FetchContent)
FetchContent_Declare(
    fmtlib
    # GIT_REPOSITORY https://github.com/fmtlib/fmt.git
    # GIT_TAG 9.1.0
    SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../../../dependency/fmt
)
FetchContent_MakeAvailable(fmtlib)

file(GLOB main_source main/*.cpp main/capi/*.cpp)
IF (ENABLE_PG_QUERY)
    file(GLOB binder_source binder/*.cpp binder/*.h binder/binder_helper/*.cpp binder/statement/*.cpp binder/expression/*.cpp binder/bypass/*.cpp)
ELSE()
    file(GLOB binder_source binder/binder_helper/*.cpp binder/statement/*.cpp binder/expression/*.cpp binder/bypass/*.cpp)
ENDIF()
file(GLOB catalog_source catalog/*.cpp catalog/*.h)
file(GLOB_RECURSE common_source common/*.cpp)
file(GLOB_RECURSE planner_source planner/*.cpp)
file(GLOB data_source datasource/*.cpp)
file(GLOB type_source type/*.cpp type/operator/*.cpp)
file(GLOB_RECURSE function_source function/*.cpp)

include_directories(${INTARKDB_COMPUTE_SQL_INC_PATH})
include_directories(${INTARKDB_SECUREC_INC_PATH})
include_directories(${INTARKDB_PGQUERY_INC_PATH})
include_directories(${INTARKDB_SRC_PATH})
include_directories(${INTARKDB_ZEKERNEL_KERNEL_PATH})
include_directories(${INTARKDB_ZEKERNEL_COMMON_INC_PATH})
include_directories(${INTARKDB_ZEKERNEL_KERNEL_INC_PATH})
include_directories(${INTARKDB_ZEKERNEL_KERNEL_COMMON_INC_PATH})
include_directories(${INTARKDB_ZEKERNEL_KERNEL_TABLESPACE_PATH})
include_directories(${INTARKDB_ZEKERNEL_KERNEL_PERSIST_PATH})
include_directories(${INTARKDB_ZEKERNEL_KERNEL_BUFFER_PATH})
include_directories(${INTARKDB_ZEKERNEL_KERNEL_TABLE_PATH})
include_directories(${INTARKDB_ZEKERNEL_KERNEL_INDEX_PATH})
include_directories(${INTARKDB_ZEKERNEL_KERNEL_XACT_PATH})
include_directories(${INTARKDB_ZEKERNEL_KERNEL_CATALOG_PATH})
include_directories(${INTARKDB_ZEKERNEL_KERNEL_LOB_PATH})
include_directories(${INTARKDB_ZEKERNEL_KERNEL_SEQUENCE_PATH})
include_directories(${INTARKDB_ZEKERNEL_KERNEL_DAEMON_PATH})
include_directories(${INTARKDB_ZEKERNEL_KERNEL_BACKUP_PATH})
include_directories(${INTARKDB_ZEKERNEL_KERNEL_FLASHBACK_PATH})
include_directories(${INTARKDB_ZEKERNEL_KERNEL_REPLICATION_PATH})
include_directories(${INTARKDB_ZEKERNEL_KERNEL_STATISTICS_PATH})
include_directories(${INTARKDB_UTF8PROC_INC_PATH})
include_directories(${INTARKDB_CJSON_PATH})
include_directories(${INTARKDB_GMSSL_INC_PATH})
include_directories(${INTARKDB_ZLIB_INC_PATH})
include_directories(${INTARKDB_COMPUTE_TS_INC_PATH})

# sql_object
add_library(sql_object OBJECT ${binder_source} ${catalog_source} ${main_source} ${common_source} ${execution_source} ${planner_source} ${data_source} ${type_source} ${function_source})
target_link_libraries(sql_object fmt::fmt)
target_link_directories(sql_object PUBLIC ${INTARKDB_LIB_PATH} ${INTARKDB_THRID_LIB_PATH})

# sql_storage_object
add_library(sql_storage_object OBJECT $<TARGET_OBJECTS:sql_object> $<TARGET_OBJECTS:storage_obj> $<TARGET_OBJECTS:gstor_obj> $<TARGET_OBJECTS:zekernel> $<TARGET_OBJECTS:zecommon>)
target_link_directories(sql_storage_object PUBLIC ${INTARKDB_LIB_PATH} ${INTARKDB_THRID_LIB_PATH})

# # static
# add_library(sql_engine STATIC $<TARGET_OBJECTS:sql_object> $<TARGET_OBJECTS:storage_obj> $<TARGET_OBJECTS:gstor_obj> $<TARGET_OBJECTS:zekernel> $<TARGET_OBJECTS:zecommon>)
# target_link_libraries(sql_engine PUBLIC ${INTARKDB_LINK_LIBS} )
# if(UNIX)
# 	target_link_libraries(sql_engine PUBLIC dl rt)
# else()
#     target_link_libraries(sql_engine PUBLIC wsock32 ws2_32)
# endif()

# if(WIN32)
#     target_link_options(sql_engine PUBLIC "-Wl,--export-all-symbols")
# endif(WIN32)
# set_target_properties(sql_engine PROPERTIES OUTPUT_NAME "intarkdb")

# # dynamic
# add_library(intarkdb SHARED $<TARGET_OBJECTS:sql_object> $<TARGET_OBJECTS:storage_obj> $<TARGET_OBJECTS:gstor_obj> $<TARGET_OBJECTS:zekernel> $<TARGET_OBJECTS:zecommon>)
# target_link_libraries(intarkdb PUBLIC ${INTARKDB_LINK_LIBS})
# if(UNIX)
# 	target_link_libraries(intarkdb PUBLIC dl rt)
# else()
#     target_link_libraries(intarkdb PUBLIC wsock32 ws2_32)
# endif()

# if (WIN32)
#     target_link_options(intarkdb PUBLIC "-Wl,--export-all-symbols")
# endif(WIN32)


option(BUILD_TESTS "Build tests" OFF)
if(BUILD_TESTS)
    add_subdirectory(test)
endif()
# TDD
# enable_testing()
# add_executable(xxx_test test/**.cpp)
# target_link_libraries(xxx_test GTest::gtest_main)
# 链接其他库

# 注册test
# include(GoogleTest)
# gtest_discover_tests(xxx_test)
