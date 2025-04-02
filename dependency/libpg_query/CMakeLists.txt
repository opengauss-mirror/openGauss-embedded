cmake_minimum_required(VERSION 2.8.12)

# project(duckdb_pg_query CXX C)
if(POLICY CMP0063)
    cmake_policy(SET CMP0063 NEW)
endif()
set(CMAKE_CXX_VISIBILITY_PRESET hidden)
#set(LIBRARY_OUTPUT_PATH  ${CMAKE_CURRENT_SOURCE_DIR}/build)
SET(LIBRARY_OUTPUT_PATH ${INTARKDB_THRID_LIB_PATH})
if (OS_ARCH)
    if (NOT ((${OS_ARCH} STREQUAL "arm32") OR (${OS_ARCH} STREQUAL "aarch64")))
        SET(CMAKE_CXX_FLAGS "$ENV{CXXFLAGS} -O3")
        SET(CMAKE_C_FLAGS "$ENV{CFLAGS} -O3")
    endif()
else()
SET(CMAKE_CXX_FLAGS "$ENV{CXXFLAGS} -O3")
SET(CMAKE_C_FLAGS "$ENV{CFLAGS} -O3")
endif()

add_library(duckdb_pg_query STATIC
            postgres_parser.cpp
            pg_functions.cpp
            src_backend_parser_parser.cpp
            src_backend_parser_gram.cpp
            src_backend_nodes_list.cpp
            src_backend_parser_scansup.cpp
            src_backend_nodes_makefuncs.cpp
            src_backend_nodes_value.cpp
            src_backend_parser_scan.cpp
            src_common_keywords.cpp)

target_include_directories(
  duckdb_pg_query
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)
set_target_properties(duckdb_pg_query PROPERTIES EXPORT_NAME duckdb_duckdb_pg_query)

install(TARGETS duckdb_pg_query
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

#disable_target_warnings(duckdb_pg_query)

set_target_properties(duckdb_pg_query PROPERTIES POSITION_INDEPENDENT_CODE ON)

# cp to output 
set(libpg_query_include ${INTARKDB_DST_PGQUERY_INC_PATH})
set(libpg_query_lib ${INTARKDB_THRID_LIB_PATH})

# 创建 libpg_query_include 指向的文件夹
execute_process( COMMAND ${CMAKE_COMMAND} -E make_directory ${libpg_query_include})

add_custom_command(
TARGET duckdb_pg_query POST_BUILD
COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_CURRENT_SOURCE_DIR}/include   ${libpg_query_include}
#COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/build/libintarkdb_pg_query.a   ${libpg_query_lib}
)
