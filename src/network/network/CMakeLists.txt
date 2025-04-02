set(LIB_NAME "network_network")
set(OBJECT_NAME "${LIB_NAME}_obj")
set(STATIC_LIB_NAME "${LIB_NAME}_static")
set(DYNAMIC_LIB_NAME "${LIB_NAME}_dynamic")

# source
file(GLOB SOURCES *.cpp *.c *.h)

# include
if (NOT ANDROID)
    set(INTARKDB_LINK_LIBS
            fmt::fmt
            duckdb_pg_query
            utf8proc
            pthread
            ${3rd_libzstd}
            ${3rd_libz}
            ${3rd_libjson}
            m
            -Wl,--whole-archive securec -Wl,--no-whole-archive
            )
else()
    set(INTARKDB_LINK_LIBS
            fmt::fmt
            duckdb_pg_query
            utf8proc
            ${3rd_libzstd}
            ${3rd_libz}
            ${3rd_libjson}
            m
            -Wl,--whole-archive securec -Wl,--no-whole-archive
            )
endif ()


# object
add_library(${OBJECT_NAME} OBJECT ${SOURCES})

# # git version definitions
# add_compile_definitions(g_DCC_LIB_VERSION=${DD_DCC_LIB_VERSION})
# add_compile_definitions(GETLIBVERSION=${DD_GETLIBVERSION})

# # dynamic library
# add_library(${DYNAMIC_LIB_NAME} SHARED $<TARGET_OBJECTS:${OBJECT_NAME}>
#                                        $<TARGET_OBJECTS:sql_object>
#                                        $<TARGET_OBJECTS:storage_obj>
#                                        $<TARGET_OBJECTS:gstor_obj>
#                                        $<TARGET_OBJECTS:zekernel>
#                                        $<TARGET_OBJECTS:zecommon>
#                                        $<TARGET_OBJECTS:zekernel>
# )

# target_link_libraries(${DYNAMIC_LIB_NAME} PUBLIC ${INTARKDB_LINK_LIBS})

# if(UNIX)
#     target_link_libraries(${DYNAMIC_LIB_NAME} PUBLIC dl rt)
# else()
#     target_link_libraries(${DYNAMIC_LIB_NAME} PUBLIC wsock32 ws2_32)
# endif()
