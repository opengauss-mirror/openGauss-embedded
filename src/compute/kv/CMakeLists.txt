
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

file(GLOB KV_source ./*.cpp)

include_directories(${INTARKDB_COMPUTE_SQL_INC_PATH})
include_directories(${INTARKDB_SECUREC_INC_PATH})
include_directories(${INTARKDB_PGQUERY_INC_PATH})
include_directories(${INTARKDB_SRC_PATH})
include_directories(${INTARKDB_ZEKERNEL_COMMON_INC_PATH})
include_directories(${INTARKDB_UTF8PROC_INC_PATH})
include_directories(${INTARKDB_CJSON_PATH})

# kv_object
add_library(kv_object OBJECT ${KV_source})
target_link_libraries(kv_object fmt::fmt)
target_link_directories(kv_object PUBLIC ${INTARKDB_LIB_PATH} ${INTARKDB_THRID_LIB_PATH})
