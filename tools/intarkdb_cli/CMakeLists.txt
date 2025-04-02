cmake_minimum_required(VERSION 3.14.1)
# project(intarkdb_cli)

add_compile_options(-fPIC)
# # add_compile_options(-fPIC -MMD -fno-strict-aliasing -fsigned-char -fms-extensions)

set(CMAKE_CXX_STANDARD 17)
## create compile_command.json
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
# 设置编译器标志以支持 UTF-8
if (MSVC)
    add_compile_options(/utf-8)
endif()


if(MSVC)
# 设置运行时库
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} /MDd")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} /MD")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} /MD")
set(CMAKE_CXX_FLAGS_MINSIZEREL "${CMAKE_CXX_FLAGS_MINSIZEREL} /MD")
endif()
include_directories(${INTARKDB_ZEKERNEL_COMMON_INC_PATH})

set(INTARKDB_LINK_LIBS
intarkdb
cjson
)

file(GLOB CLI_SRC ./*.cpp)
    
add_executable(intarkdb_cli ${CLI_SRC})
target_link_libraries(intarkdb_cli $<TARGET_OBJECTS:cl_linenoise>)
  
if(WIN32)
    if(MSVC)
        target_link_libraries(intarkdb_cli intarkdb cjson)
    else()
        target_link_libraries(intarkdb_cli libintarkdb.dll cjson)
    endif()
elseif ((${OS_ARCH} STREQUAL "arm32") OR (${OS_ARCH} STREQUAL "aarch64"))
    target_link_libraries(intarkdb_cli ${INTARKDB_LINK_LIBS})
else()
    target_link_libraries(intarkdb_cli ${INTARKDB_LINK_LIBS} -Wl,--copy-dt-needed-entries)
endif()

#target_link_directories(intarkdb_cli PUBLIC ${INTARKDB_LIB_PATH} )
target_include_directories(intarkdb_cli PUBLIC ${INTARKDB_COMPUTE_SQL_INC_PATH} 
                                               ${INTARKDB_PGQUERY_INC_PATH}
                                               ${INTARKDB_SECUREC_INC_PATH}
                                               ${INTARKDB_SRC_PATH}
                                               ${INTARKDB_CJSON_PATH}
                                               ${CMAKE_CURRENT_SOURCE_DIR}
                                               ${INTARKDB_THRID_INC_PATH}
                                               ${INTARKDB_FMT_INC_PATH}
                                               ${CMAKE_CURRENT_SOURCE_DIR}/../tools_dependency/linenoise-ng/include)