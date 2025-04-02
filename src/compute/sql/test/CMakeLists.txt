cmake_minimum_required(VERSION 3.15)
# project(test)

cmake_policy(SET CMP0135 NEW)
set(BUILD_SHARED_LIBS OFF)

set (EXECUTABLE_OUTPUT_PATH     ${ORIGIN_OUTPUT_PATH})

include(FetchContent)
FetchContent_Declare(
    googletest 
    URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
    DOWNLOAD_EXTRACT_TIMESTAMP
)
FetchContent_MakeAvailable(googletest)

FetchContent_Declare(
    fmtlib
    GIT_REPOSITORY https://github.com/fmtlib/fmt.git
    GIT_TAG 9.1.0
)
FetchContent_MakeAvailable(fmtlib)
if (NOT ((${OS_ARCH} STREQUAL "arm32") OR (${OS_ARCH} STREQUAL "aarch64")))
    if(MSVC)
         set(INSTARDB_TEST_LINK_LIBS
                intarkdb
                -Wl,--whole-archive ${vpp_libsecurec} -Wl,--no-whole-archive
                ${3rd_libz}
                fmt::fmt
                duckdb_pg_query
                utf8proc
                GTest::gtest_main
                # Windows 不需要 pthread
            )
    else()
        set(INSTARDB_TEST_LINK_LIBS
        intarkdb
        -Wl,--whole-archive ${vpp_libsecurec} -Wl,--no-whole-archive
        ${3rd_libz}
        fmt::fmt
        duckdb_pg_query
        utf8proc
        pthread
        GTest::gtest_main
        -Wl,--copy-dt-needed-entries)
    endif()
elseif(ANDROID)
    set(INSTARDB_TEST_LINK_LIBS
    intarkdb
    -Wl,--whole-archive ${vpp_libsecurec} -Wl,--no-whole-archive
    ${3rd_libz}
    fmt::fmt
    duckdb_pg_query
    utf8proc
    GTest::gtest_main
    )
elseif(MSVC)
    set(INSTARDB_TEST_LINK_LIBS
    intarkdb
    -Wl,--whole-archive ${vpp_libsecurec} -Wl,--no-whole-archive
    ${3rd_libz}
    fmt::fmt
    duckdb_pg_query
    utf8proc
    GTest::gtest_main
    )
else()
    set(INSTARDB_TEST_LINK_LIBS
        intarkdb
        -Wl,--whole-archive ${vpp_libsecurec} -Wl,--no-whole-archive
        ${3rd_libz}
        fmt::fmt
        duckdb_pg_query
        utf8proc
        pthread
        GTest::gtest_main
        )
endif()

enable_testing()

add_executable(pg_query_test pg_query_test.cpp)
target_include_directories(pg_query_test PUBLIC ${INTARKDB_PGQUERY_INC_PATH})
if (NOT ANDROID AND NOT MSVC)
    target_link_libraries(pg_query_test pthread)
endif ()
target_link_libraries(pg_query_test GTest::gtest_main)
target_link_libraries(pg_query_test ${INTARKDB_THRID_LIB_PATH}/libduckdb_pg_query.a)

add_executable(create_test create_test.cpp)
target_link_libraries(create_test PUBLIC ${INSTARDB_TEST_LINK_LIBS})

add_executable(index_test index_test.cpp)
target_link_libraries(index_test PUBLIC ${INSTARDB_TEST_LINK_LIBS})

add_executable(alter_test alter_test.cpp)
target_link_libraries(alter_test PUBLIC ${INSTARDB_TEST_LINK_LIBS})

add_executable(ctas_test ctas_test.cpp)
target_link_libraries(ctas_test PUBLIC ${INSTARDB_TEST_LINK_LIBS})

add_executable(pg_type_test pg_type_test.cpp)
target_link_libraries(pg_type_test PUBLIC ${INSTARDB_TEST_LINK_LIBS})

add_executable(assist_test assist_test.cpp)
target_link_libraries(assist_test PUBLIC ${INSTARDB_TEST_LINK_LIBS})

add_executable(type_test type_test.cpp $<TARGET_OBJECTS:sql_object> 
                        $<TARGET_OBJECTS:storage_obj> $<TARGET_OBJECTS:gstor_obj> 
                        $<TARGET_OBJECTS:zekernel> $<TARGET_OBJECTS:zecommon> )
target_link_libraries(type_test PUBLIC ${INSTARDB_TEST_LINK_LIBS})

add_executable(select_test select_test.cpp)
target_link_libraries(select_test PUBLIC ${INSTARDB_TEST_LINK_LIBS})

add_executable(copy_test copy_test.cpp)
target_link_libraries(copy_test PUBLIC ${INSTARDB_TEST_LINK_LIBS})

add_executable(insert_test insert_test.cpp)
target_link_libraries(insert_test PUBLIC ${INSTARDB_TEST_LINK_LIBS})

add_executable(show_test show_test.cpp)
target_link_libraries(show_test PUBLIC ${INSTARDB_TEST_LINK_LIBS})

add_executable(like_test like_test.cpp)
target_link_libraries(like_test PUBLIC ${INSTARDB_TEST_LINK_LIBS})

add_executable(constraint_test constraint_test.cpp)
target_link_libraries(constraint_test PUBLIC ${INSTARDB_TEST_LINK_LIBS})

add_executable(partition_test partition_test.cpp)
target_link_libraries(partition_test PUBLIC ${INSTARDB_TEST_LINK_LIBS})

add_executable(view_test view_test.cpp)
target_link_libraries(view_test PUBLIC ${INSTARDB_TEST_LINK_LIBS})

add_executable(update_delete_test update_del_test.cpp)
target_link_libraries(update_delete_test PUBLIC ${INSTARDB_TEST_LINK_LIBS})

add_executable(prepare_test prepare_test.cpp)
target_link_libraries(prepare_test PUBLIC ${INSTARDB_TEST_LINK_LIBS})

add_executable(multidb_test multidb_test.cpp)
target_link_libraries(multidb_test PUBLIC ${INSTARDB_TEST_LINK_LIBS})

add_executable(tsl_slm_test tsl_slm_test.cpp)
target_link_libraries(tsl_slm_test PUBLIC ${INSTARDB_TEST_LINK_LIBS})

add_executable(date_function_test date_function_test.cpp)
target_link_libraries(date_function_test PUBLIC ${INSTARDB_TEST_LINK_LIBS})

add_executable(c_api_test c_api_test.cpp)
target_link_libraries(c_api_test PUBLIC ${INSTARDB_TEST_LINK_LIBS})

add_executable(archive_test archive_test.cpp)
target_link_libraries(archive_test PUBLIC ${INSTARDB_TEST_LINK_LIBS} -lstdc++fs)

# find_package(benchmark REQUIRED)
# add_executable(benchmark_createtable benchmark_createtable.cpp)
# target_link_libraries(benchmark_createtable PUBLIC ${INSTARDB_TEST_LINK_LIBS} benchmark::benchmark)

# add_executable(benchmark_insert benchmark_insert.cpp)
# target_link_libraries(benchmark_insert PUBLIC ${INSTARDB_TEST_LINK_LIBS} benchmark::benchmark)

#add_executable(statement_test statement_test.cpp)
#target_include_directories(statement_test PUBLIC ${INTARKDB_PGQUERY_INC_PATH})
#target_include_directories(statement_test PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/../include)
#target_link_libraries(statement_test GTest::gtest_main)
#target_link_libraries(statement_test ${INTARKDB_THRID_LIB_PATH}/libduckdb_pg_query.a)
#target_link_libraries(statement_test ${CMAKE_CURRENT_SOURCE_DIR}/../build/libsql_engine.a)


# 链接其他库

# 注册test
add_test(pg_query_test pg_query_test)
add_test(create_test create_test)
add_test(index_test index_test)
add_test(alter_test alter_test)
add_test(ctas_test ctas_test)
#add_test(pg_type_test pg_type_test)
add_test(type_test type_test)
add_test(copy_test copy_test)
add_test(select_test select_test)
add_test(insert_test insert_test)
add_test(like_test like_test)
add_test(constraint_test constraint_test)
add_test(partition_test partition_test)

add_test(view_test view_test) 
add_test(update_delete_test update_delete_test) 
add_test(prepare_test prepare_test) 
add_test(multidb_test multidb_test) 
add_test(date_function_test date_function_test) 
add_test(c_api_test c_api_test) 
add_test(archive_test archive_test) 
add_test(show_test show_test) # 请保持次用例在最后
# include(googletest)
#gtest_discover_tests(all_test)
# gtest_discover_tests(pg_query_test)
#gtest_discover_tests(ddl_test)
# gtest_discover_tests(type_test)
#gtest_discover_tests(statement_test)
