# copy config to bin
file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/server_config.json DESTINATION ${EXECUTABLE_OUTPUT_PATH})

# common naming rules
set(BIN_NAME "intarkdb_server")
set(CJSON_INC_PATH "${INTARKDB_HOME}/output/inc/cJSON")

## source
include_directories(${INTARKDB_SRC_PATH})
include_directories(${INTARKDB_ZEKERNEL_COMMON_INC_PATH})
include_directories(${INTARKDB_ZEKERNEL_COMMON_INC_PATH}/variant)
include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${INTARKDB_SRC_PATH}/utils)
include_directories(${INTARKDB_SRC_PATH}/network/common)
include_directories(${INTARKDB_SRC_PATH}/network/network)
include_directories(${INTARKDB_SRC_PATH}/network/server)
include_directories(${CJSON_INC_PATH})

aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}  InstarDB_DB_SRC)

set(EXE_SRC
        ${CMAKE_CURRENT_SOURCE_DIR}/server.c
)

add_executable(${BIN_NAME} ${EXE_SRC})
target_link_libraries(${BIN_NAME} ${3rd_libjson})
if(UNIX)
	target_link_libraries(${BIN_NAME} intarkdb_server_dynamic $<TARGET_OBJECTS:network_common_obj> pthread dl rt ${HIDDEN_LIBS} )
else()
	target_link_libraries(${BIN_NAME} intarkdb_server_dynamic $<TARGET_OBJECTS:network_common_obj> pthread  ${HIDDEN_LIBS})
endif()
target_include_directories(${BIN_NAME} PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
set_target_properties(${BIN_NAME} PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${EXECUTABLE_OUTPUT_PATH})

