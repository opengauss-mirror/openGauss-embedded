# compile options
if(CMAKE_COMPILER_IS_GNUCXX)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-attributes")
endif()
add_compile_options(-fPIC)
add_compile_options(-fpermissive)

set(CMAKE_CXX_STANDARD 17)
# create compile_command.json
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

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
# include_directories(${DCC_INTERFACE_PATH})
include_directories(${INTARKDB_PARSE_PATH})
include_directories(${CJSON_INC_PATH})
include_directories(${GSTOR_SRC_HOME})
include_directories(${GSTOR_SRC_HOME}/network/common)
include_directories(${GSTOR_SRC_HOME}/network/om_agent)
include_directories(${GSTOR_SRC_HOME}/network/network)


MESSAGE(STATUS "INTARKDB LIB VERSION  "  ${DD_INTARKDB_LIB_VERSION})
add_compile_definitions(g_INTARKDB_LIB_VERSION=${DD_INTARKDB_LIB_VERSION})
add_compile_definitions(GETLIBVERSION=${DD_GETLIBVERSION})

link_directories(${GSTOR_LIB_PATH})
link_directories(${LIBRARY_OUTPUT_PATH})

set(INTARKDB_EXE_SRC
    ${CMAKE_CURRENT_SOURCE_DIR}/server.c
)

# libraries for linking
set(LINK_LIBS intarkdb
              ${HIDDEN_LIBS}
              ${3rd_libjson} )

add_executable(local-db ${INTARKDB_EXE_SRC})
# target_link_libraries(dcc_server storage dcf executor utils pthread dl rt ${HIDDEN_LIBS} ${3rd_libzstd} ${3rd_libssl} ${3rd_lib_crypto} ${3rd_liblz4} ${3rd_libjson} -Wl,--whole-archive ${vpp_libsecurec} ${3rd_libcbb} -Wl,--no-whole-archive)
if (ANDROID)
    target_link_libraries(local-db PRIVATE ${LINK_LIBS} dl)
elseif(UNIX)
    target_link_libraries(local-db PRIVATE ${LINK_LIBS} pthread dl rt)
elseif(WIN32)
    target_link_libraries(local-db PRIVATE ${LINK_LIBS} pthread wsock32 ws2_32)
    target_link_options(local-db PRIVATE "-Wl,--export-all-symbols")
endif()


target_include_directories(local-db PUBLIC ${CMAKE_CURRENT_SOURCE_DIR} ${LIBRARY_OUTPUT_PATH})
set_target_properties(local-db PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${EXECUTABLE_OUTPUT_PATH})
set_target_properties(local-db PROPERTIES LINKER_LANGUAGE "C")
