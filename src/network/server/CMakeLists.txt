# other dependency include

# common naming rules
set(LIB_NAME "network_server")
set(OBJECT_NAME "${LIB_NAME}_obj")
set(STATIC_LIB_NAME "${LIB_NAME}_static")
set(DYNAMIC_LIB_NAME "${LIB_NAME}_dynamic")

if(WIN32)
    if(NOT MSVC)
    add_compile_options(-Wno-incompatible-pointer-types)
    endif()
endif()

include_directories(${DCC_UTILS_PATH}
                    ${INTARKDB_SRC_PATH}/utils
                    ${NETWORK_NETWORK_PATH}
                    ${NETWORK_COMMON_PATH}
                    ${CJSON_INC_PATH}
                    ${INTARKDB_PGQUERY_INC_PATH}
)

# source
file(GLOB SOURCES *.cpp *.c *.h)

# object
add_library(${OBJECT_NAME} OBJECT ${SOURCES})

#set(OTHER_OBJECTS
#    $<TARGET_OBJECTS:storage_obj>
#    $<TARGET_OBJECTS:gstor_obj>
#    $<TARGET_OBJECTS:zekernel>
#    $<TARGET_OBJECTS:zecommon>
#    $<TARGET_OBJECTS:network_server_protocol_obj>
#    $<TARGET_OBJECTS:network_common_obj>
#    $<TARGET_OBJECTS:network_network_obj>
#    $<TARGET_OBJECTS:network_login_obj>
#)
#
## libs for link
#set(LINK_LIBS
#        gmssl
#        fmt::fmt
#        duckdb_pg_query
#        #intarkdb
#        utf8proc
#        ${3rd_libz}
#        ${3rd_libjson}
#        m
#        network_login_obj
#        -Wl,--whole-archive ${vpp_libsecurec} -Wl,--no-whole-archive
#        ${G_BIN_EXT_LIBS}
#    )
#
## static library
#add_library(${STATIC_LIB_NAME} STATIC $<TARGET_OBJECTS:${OBJECT_NAME}> ${OTHER_OBJECTS})
#
## target_include_directories(${STATIC_LIB_NAME} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
#
#target_link_libraries(${STATIC_LIB_NAME} PUBLIC ${LINK_LIBS})
#target_link_directories(${STATIC_LIB_NAME} PUBLIC  ${INTARKDB_LIB_PATH})
#target_link_libraries(${STATIC_LIB_NAME} PUBLIC gmssl)
#
#if (ANDROID)
#    target_link_libraries(${STATIC_LIB_NAME} PUBLIC dl)
#elseif(UNIX)
#    target_link_libraries(${STATIC_LIB_NAME} PUBLIC dl rt)
#else()
#    target_link_libraries(${STATIC_LIB_NAME} PUBLIC wsock32 ws2_32)
#endif()
#
#
#if(WIN32)
#    target_link_options(${STATIC_LIB_NAME} PUBLIC "-Wl,--export-all-symbols")
#endif(WIN32)

# dynamic library
#add_library(${DYNAMIC_LIB_NAME} SHARED $<TARGET_OBJECTS:${OBJECT_NAME}> ${OTHER_OBJECTS})
#
#target_link_libraries(${DYNAMIC_LIB_NAME} PUBLIC ${LINK_LIBS})
#target_link_directories(${DYNAMIC_LIB_NAME} PUBLIC ${INTARKDB_LIB_PATH})
#target_link_libraries(${DYNAMIC_LIB_NAME} PUBLIC gmssl)
#
#if (ANDROID)
#    target_link_libraries(${DYNAMIC_LIB_NAME} PUBLIC dl)
#elseif(UNIX)
#    target_link_libraries(${DYNAMIC_LIB_NAME} PUBLIC dl rt)
#else()
#    target_link_libraries(${DYNAMIC_LIB_NAME} PUBLIC wsock32 ws2_32)
#endif()
#
#
#if(WIN32)
#    target_link_options(${DYNAMIC_LIB_NAME} PUBLIC "-Wl,--export-all-symbols")
#endif(WIN32)
