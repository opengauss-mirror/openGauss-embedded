

# common naming rules
set(SERVER_LIB_NAME "network_server_protocol_obj")
set(CLINET_LIB_NAME "network_client_protocol_obj")


# source
file(GLOB SOURCES *.cpp *.c *.h)

# include
include_directories(${INTARKDB_SRC_PATH}/storage/gstor/zekernel/common
                    ${NETWORK_NETWORK_PATH}
                    ${NETWORK_COMMON_PATH}
                    ${CJSON_INC_PATH}
)


# server object
add_library(${SERVER_LIB_NAME} OBJECT ${SOURCES})
target_link_libraries(${SERVER_LIB_NAME} PUBLIC gmssl)

#add_compile_options(-DCLIENT_LOG)
#add_library(${CLINET_LIB_NAME} OBJECT ${SOURCES})
#target_link_libraries(${CLINET_LIB_NAME} PUBLIC gmssl)

# # dynamic library
# if (ANDROID)
#     set(INTARKDB_LINK_LIBS
#             fmt::fmt
#             utf8proc
#             ${3rd_libzstd}
#             ${3rd_libz}
#             ${3rd_liblz4}
#             ${3rd_libssl}
#             ${3rd_libjson}
#             m
#             -Wl,--whole-archive ${vpp_libsecurec} -Wl,--no-whole-archive
#             )
# else()
#     set(INTARKDB_LINK_LIBS
#         fmt::fmt
#         utf8proc
#         pthread
#         ${3rd_libzstd}
#         ${3rd_libz}
#         ${3rd_liblz4}
#         ${3rd_libssl}
#         ${3rd_libjson}
#         m
#         -Wl,--whole-archive ${vpp_libsecurec} -Wl,--no-whole-archive
#     )
# endif ()

# add_library(${DYNAMIC_LIB_NAME} SHARED $<TARGET_OBJECTS:${OBJECT_NAME}>
#                                        $<TARGET_OBJECTS:sql_object>
#                                        $<TARGET_OBJECTS:storage_obj>
#                                        $<TARGET_OBJECTS:gstor_obj>
#                                        $<TARGET_OBJECTS:zekernel>
#                                        $<TARGET_OBJECTS:network_network_obj>
#                                        $<TARGET_OBJECTS:zecommon>
#                                        $<TARGET_OBJECTS:zekernel>
# )

# target_link_libraries(${DYNAMIC_LIB_NAME} PUBLIC ${INTARKDB_LINK_LIBS})

# if(UNIX)
#     target_link_libraries(${DYNAMIC_LIB_NAME} PUBLIC dl rt)
# else()
#     target_link_libraries(${DYNAMIC_LIB_NAME} PUBLIC wsock32 ws2_32)
# endif()
