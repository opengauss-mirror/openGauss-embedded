## patch
set(GSTOR_SRC_HOME ${INTARKDB_SRC_PATH})
set(GSTOR_LIB_PATH ${INTARKDB_THRID_LIB_PATH})
set(ZEKERNEL_COMMON_INC_PATH ${GSTOR_SRC_HOME}/storage/gstor/zekernel/common)
set(CJSON_INC_PATH ${INTARKDB_CJSON_PATH})

## other dependency include
include_directories(${DCC_SECUREC_INC_PATH})
include_directories(${DCC_OPENSSL_PATH})

## source
include_directories(${GSTOR_SRC_HOME})
include_directories(${ZEKERNEL_COMMON_INC_PATH})
include_directories(${ZEKERNEL_COMMON_INC_PATH}/variant)
include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${DCC_KV_STORAGE_PATH})
include_directories(${DCC_UTILS_PATH})
# include_directories(${DCC_INTERFACE_PATH})
include_directories(${DCC_PARSE_PATH})
include_directories(${CJSON_INC_PATH})

aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}  LOCAL_SERVER_SRC)

MESSAGE(STATUS  "CMAKE_CURRENT_SOURCE_DIR: " ${CMAKE_CURRENT_SOURCE_DIR})
MESSAGE(STATUS  "LOCAL_SERVER_SRC: " ${LOCAL_SERVER_SRC})

MESSAGE(STATUS "DCC LIB VERSION  "  ${DD_DCC_LIB_VERSION})
add_compile_definitions(g_DCC_LIB_VERSION=${DD_DCC_LIB_VERSION})
add_compile_definitions(GETLIBVERSION=${DD_GETLIBVERSION})


link_directories(${GSTOR_LIB_PATH})

set(DCC_EXE_SRC
        ${LOCAL_SERVER_SRC}
)

add_executable(local-server ${DCC_EXE_SRC})
if(ANDROID)
	target_link_libraries(local-server intarkdb_server_thread intarkdb dl ${HIDDEN_LIBS}
			-Wl,--whole-archive ${vpp_libsecurec} -Wl,--no-whole-archive)
elseif(UNIX)
	target_link_libraries(local-server intarkdb_server_thread intarkdb pthread dl rt ${HIDDEN_LIBS}    
                                -Wl,--whole-archive ${vpp_libsecurec} -Wl,--no-whole-archive)
else()
	target_link_libraries(local-server intarkdb_server_thread intarkdb pthread  ${HIDDEN_LIBS}    
                                -Wl,--whole-archive ${vpp_libsecurec} -Wl,--no-whole-archive)
endif()
target_include_directories(local-server PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(intarkdb PUBLIC ${GSTOR_LIB_PATH})
set_target_properties(local-server PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${EXECUTABLE_OUTPUT_PATH})
set_target_properties(local-server PROPERTIES LINKER_LANGUAGE "C")
