MESSAGE(STATUS "=============== CMAKE BUILD BEGIN=============")

cmake_minimum_required(VERSION 3.14.1)

# add_compile_options(-DENABLE_PRECOMPILED_HEADERS=OFF)
if (MSVC)
    set(INTARKDB_LINK_LIBS
    fmt::fmt
    utf8proc
    -Wl,--whole-archive ${vpp_libsecurec} -Wl,--no-whole-archive
    ${G_BIN_EXT_LIBS}
    gmssl)
elseif(ANDROID)
    set(INTARKDB_LINK_LIBS
        fmt::fmt
        utf8proc
        m
        -Wl,--whole-archive ${vpp_libsecurec} -Wl,--no-whole-archive
        ${G_BIN_EXT_LIBS}
        gmssl)
else()
    set(INTARKDB_LINK_LIBS
        fmt::fmt
        utf8proc
        pthread
        m
        -Wl,--whole-archive ${vpp_libsecurec} -Wl,--no-whole-archive
        ${G_BIN_EXT_LIBS}
        gmssl)
endif ()

## shared
add_library(intarkdb SHARED ${INTARKDB_DEV_C_SRC} $<TARGET_OBJECTS:sql_object> 
                            $<TARGET_OBJECTS:storage_obj> $<TARGET_OBJECTS:gstor_obj> 
                            $<TARGET_OBJECTS:zekernel> $<TARGET_OBJECTS:zecommon>
                            $<TARGET_OBJECTS:kv_object> 
                            $<TARGET_OBJECTS:status> )
IF (ENABLE_PG_QUERY)
target_link_libraries(intarkdb PUBLIC ${INTARKDB_LINK_LIBS} duckdb_pg_query)
ELSE()
target_link_libraries(intarkdb PUBLIC ${INTARKDB_LINK_LIBS})
ENDIF()
if (NOT ANDROID AND NOT MSVC)
    if(CMAKE_SYSTEM_PROCESSOR MATCHES "arm")
        target_link_libraries(intarkdb PUBLIC pthread atomic)
    else()
        target_link_libraries(intarkdb PUBLIC pthread)
    endif ()
endif ()
if(MSVC)
target_link_libraries(intarkdb PUBLIC  -Wl,--no-whole-archive ${G_BIN_EXT_LIBS} ${3rd_libz})
else()
target_link_libraries(intarkdb PUBLIC m -Wl,--no-whole-archive ${G_BIN_EXT_LIBS} ${3rd_libz})
endif()
target_link_directories(intarkdb PUBLIC ${INTARKDB_LIB_PATH} ${INTARKDB_THRID_LIB_PATH})
target_include_directories(intarkdb PUBLIC ${INTARKDB_SRC_PATH}
                                            ${INTARKDB_GSTOR_INC_PATH} 
                                            ${INTARKDB_SECUREC_INC_PATH}
                                            ${INTARKDB_COMPUTE_SQL_INC_PATH}
                                            ${INTARKDB_STREAMAGG_INCLUDE_PATH})
if(ANDROID)
    target_link_libraries(intarkdb PUBLIC dl ${G_BIN_EXT_LIBS})
elseif(UNIX)
    target_link_libraries(intarkdb PUBLIC dl rt ${G_BIN_EXT_LIBS})
elseif(WIN32)
    target_link_libraries(intarkdb PUBLIC wsock32 ws2_32)
else()
    target_link_libraries(intarkdb PUBLIC dl rt ${G_BIN_EXT_LIBS})
endif()

if (WIN32)
    target_link_options(intarkdb PUBLIC "-Wl,--export-all-symbols")
endif(WIN32)

## server shared 
if (NOT ANDROID AND NOT MSVC)
add_library(intarkdb_server_dynamic SHARED ${INTARKDB_DEV_C_SRC}
                                           $<TARGET_OBJECTS:sql_object> 
                                           $<TARGET_OBJECTS:storage_obj> $<TARGET_OBJECTS:gstor_obj> 
                                           $<TARGET_OBJECTS:zekernel> $<TARGET_OBJECTS:zecommon>
                                           $<TARGET_OBJECTS:network_server_protocol_obj>
                                           $<TARGET_OBJECTS:network_network_obj>  
                                           $<TARGET_OBJECTS:network_login_obj> 
                                           $<TARGET_OBJECTS:network_server_obj>
                                           $<TARGET_OBJECTS:securec_obj>   
                                           $<TARGET_OBJECTS:zlib_obj>
                                           $<TARGET_OBJECTS:status> )
target_link_libraries(intarkdb_server_dynamic PUBLIC fmt::fmt duckdb_pg_query utf8proc m pthread gmssl)

target_link_directories(intarkdb_server_dynamic PUBLIC ${INTARKDB_LIB_PATH} ${INTARKDB_THRID_LIB_PATH})
target_include_directories(intarkdb_server_dynamic PUBLIC ${INTARKDB_SRC_PATH}
                                            ${INTARKDB_GSTOR_INC_PATH} 
                                            ${INTARKDB_SECUREC_INC_PATH}
                                            ${INTARKDB_COMPUTE_SQL_INC_PATH})
if(UNIX)
    target_link_libraries(intarkdb_server_dynamic PUBLIC  status rt ${G_BIN_EXT_LIBS})
elseif(WIN32)
    target_link_libraries(intarkdb_server_dynamic PUBLIC wsock32 ws2_32)
endif()
set_target_properties(intarkdb_server_dynamic PROPERTIES OUTPUT_NAME "intarkdb_server")
if (WIN32)
    target_link_options(intarkdb PUBLIC "-Wl,--export-all-symbols")
endif(WIN32)
endif ()
if(NOT MSVC)
## static
add_library(intarkdbstatic STATIC ${INTARKDB_DEV_C_SRC} $<TARGET_OBJECTS:sql_object> 
                            $<TARGET_OBJECTS:storage_obj> $<TARGET_OBJECTS:gstor_obj> 
                            $<TARGET_OBJECTS:zekernel> $<TARGET_OBJECTS:zecommon>
                            $<TARGET_OBJECTS:kv_object> 
                            $<TARGET_OBJECTS:status> )
target_link_libraries(intarkdbstatic PUBLIC ${INTARKDB_LINK_LIBS})
if (NOT ANDROID)
    target_link_libraries(intarkdbstatic PUBLIC pthread)
endif ()
target_link_libraries(intarkdbstatic PUBLIC m -Wl,--whole-archive ${vpp_libsecurec} -Wl,--no-whole-archive
                                            ${G_BIN_EXT_LIBS} ${3rd_libz})
target_link_directories(intarkdbstatic PUBLIC ${INTARKDB_LIB_PATH} ${INTARKDB_THRID_LIB_PATH})
target_include_directories(intarkdbstatic PUBLIC ${INTARKDB_SRC_PATH}
                                            ${INTARKDB_GSTOR_INC_PATH} 
                                            ${INTARKDB_SECUREC_INC_PATH}
                                            ${INTARKDB_STREAMAGG_INCLUDE_PATH})
if(ANDROID)
    target_link_libraries(intarkdbstatic PUBLIC dl ${G_BIN_EXT_LIBS})
elseif(UNIX)
    target_link_libraries(intarkdbstatic PUBLIC dl rt ${G_BIN_EXT_LIBS})
else()
    target_link_libraries(intarkdbstatic PUBLIC wsock32 ws2_32)
endif()
set_target_properties(intarkdbstatic PROPERTIES OUTPUT_NAME "intarkdb")

if (WIN32)
    target_link_options(intarkdbstatic PUBLIC "-Wl,--export-all-symbols")
endif(WIN32)
endif(NOT MSVC)