## patch
set(GSTOR_SRC_HOME  ${INTARKDB_SRC_PATH})
set(GSTOR_LIB_PATH  ${INTARKDB_THRID_LIB_PATH})
set(SQL_LIB_PATH    ${INTARKDB_LIB_PATH})
## include
include_directories(${GSTOR_SRC_HOME})
include_directories(${INTARKDB_COMPUTE_SQL_INC_PATH})

## include 3rd
include_directories(${INTARKDB_SECUREC_INC_PATH})
MESSAGE(STATUS "INTARKDB_SECUREC_INC_PATH:  ${INTARKDB_SECUREC_INC_PATH}")

link_directories(${GSTOR_LIB_PATH})
link_directories(${SQL_LIB_PATH})
# set(CMAKE_BUILD_TYPE Debug)
set(SQLITE3_API_TEST
        ${CMAKE_CURRENT_SOURCE_DIR}/sqlite3_api_test.c
)

add_executable(sqlite3_api_test ${SQLITE3_API_TEST})
target_link_libraries(sqlite3_api_test sqlite3_api_wrapper)
target_include_directories(sqlite3_api_test PUBLIC ${GSTOR_LIB_PATH} ${SQL_LIB_PATH} )
set_target_properties(sqlite3_api_test PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${EXECUTABLE_OUTPUT_PATH})
set_target_properties(sqlite3_api_test PROPERTIES LINKER_LANGUAGE "C")
