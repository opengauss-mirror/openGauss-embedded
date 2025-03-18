
MESSAGE(STATUS "=============== CMAKE BUILD BEGIN=============")

cmake_minimum_required(VERSION 3.14.1)

# open source include
include_directories(${INTARKDB_SRC_PATH})
include_directories(${INTARKDB_GSTOR_INC_PATH})
include_directories(${INTARKDB_SECUREC_INC_PATH})
include_directories(${INTARKDB_COMPUTE_SQL_INC_PATH})

link_directories(${INTARKDB_THRID_LIB_PATH})

set(SQLITE_API_WRAPPER_SRC
        ${CMAKE_CURRENT_SOURCE_DIR}/sqlite3_api_wrapper.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/printf.c
)

add_library(sqlite3_api_wrapper SHARED ${SQLITE_API_WRAPPER_SRC})
if(UNIX)
    target_link_libraries(sqlite3_api_wrapper PUBLIC intarkdb ${G_BIN_EXT_LIBS})
else()
    target_link_libraries(sqlite3_api_wrapper PUBLIC intarkdb ${G_BIN_EXT_LIBS})
endif()
target_include_directories(sqlite3_api_wrapper PUBLIC ${INTARKDB_LIB_PATH})
