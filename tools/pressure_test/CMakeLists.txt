## patch
set(GSTOR_SRC_HOME ${INTARKDB_SRC_PATH})
set(GSTOR_LIB_PATH ${INTARKDB_THRID_LIB_PATH})
set(ZEKERNEL_COMMON_INC_PATH ${GSTOR_SRC_HOME}/storage/gstor/zekernel/common)
set(CJSON_INC_PATH ${INTARKDB_CJSON_PATH})

## other dependency include
include_directories(${INTARKDB_SECUREC_INC_PATH})
include_directories(${INTARKDB_OPENSSL_PATH})

## source
include_directories(${GSTOR_SRC_HOME})
include_directories(${ZEKERNEL_COMMON_INC_PATH})
include_directories(${ZEKERNEL_COMMON_INC_PATH}/variant)
include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${INTARKDB_KV_STORAGE_PATH})
include_directories(${INTARKDB_UTILS_PATH})
include_directories(${INTARKDB_PARSE_PATH})
include_directories(${CJSON_INC_PATH})
include_directories(${GSTOR_SRC_HOME}/network/common)
include_directories(${GSTOR_SRC_HOME}/network/protocol)
include_directories(${GSTOR_SRC_HOME}/interface/sqlite3_api_wrapper/include)

link_directories(${GSTOR_LIB_PATH})

set(TEST_SRC
        ${CMAKE_CURRENT_SOURCE_DIR}/pressure_test.c
)
add_compile_options(-w -ggdb)

add_executable(pressure_test ${TEST_SRC})
if (NOT ANDROID)
    target_link_libraries(pressure_test intarkdb pthread dl rt ${HIDDEN_LIBS}  ${3rd_libssl}
            ${3rd_lib_crypto} ${3rd_liblz4}
            -Wl,--whole-archive ${vpp_libsecurec} -Wl,--no-whole-archive)
else()
    target_link_libraries(pressure_test intarkdb dl ${HIDDEN_LIBS}  ${3rd_libssl}
            ${3rd_lib_crypto} ${3rd_liblz4}
            -Wl,--whole-archive ${vpp_libsecurec} -Wl,--no-whole-archive)
endif ()
target_include_directories(intarkdb PUBLIC ${GSTOR_LIB_PATH})
set_target_properties(pressure_test PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${EXECUTABLE_OUTPUT_PATH})
set_target_properties(pressure_test PROPERTIES LINKER_LANGUAGE "C")

#DEMO
set(DEMO_SRC
        ${CMAKE_CURRENT_SOURCE_DIR}/demo.c
)
add_executable(demo ${DEMO_SRC})
if (NOT ANDROID)
    target_link_libraries(demo intarkdb intarkdb_server_thread server_common pthread sqlite3 dl rt
            ${HIDDEN_LIBS}  ${3rd_libssl} ${3rd_lib_crypto} ${3rd_liblz4}
            -Wl,--whole-archive ${vpp_libsecurec} -Wl,--no-whole-archive)
else()
    target_link_libraries(demo intarkdb intarkdb_server_thread server_common sqlite3 dl
            ${HIDDEN_LIBS}  ${3rd_libssl} ${3rd_lib_crypto} ${3rd_liblz4}
            -Wl,--whole-archive ${vpp_libsecurec} -Wl,--no-whole-archive)
endif ()


target_include_directories(intarkdb PUBLIC ${GSTOR_LIB_PATH})
set_target_properties(demo PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${EXECUTABLE_OUTPUT_PATH})
set_target_properties(demo PROPERTIES LINKER_LANGUAGE "C")


#SQLite
set(SQLITE_TEST_SRC
    ${CMAKE_CURRENT_SOURCE_DIR}/sqlite_test.c
)
add_executable(sqlite_test ${SQLITE_TEST_SRC})
target_link_libraries(sqlite_test sqlite3 -w)