cmake_minimum_required(VERSION 3.14.1)
# project(cmd)

add_compile_options(-fPIC)
# # add_compile_options(-fPIC -MMD -fno-strict-aliasing -fsigned-char -fms-extensions)

set(CMAKE_CXX_STANDARD 17)
# # create compile_command.json
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include_directories(${INTARKDB_INCLUDE_PATH})

set(INSTARDB_LINK_LIBS intarkdb)

## example_kv_c
file(GLOB c_main example_kv.c)
add_executable(example_kv_c ${c_main})
if ((${OS_ARCH} STREQUAL "arm32") OR (${OS_ARCH} STREQUAL "aarch64"))
target_link_libraries(example_kv_c intarkdb)
else()
target_link_libraries(example_kv_c intarkdb -Wl,--copy-dt-needed-entries )
endif()

## example_kv_cc
file(GLOB cc_main example_kv.cpp)
add_executable(example_kv_cc ${cc_main})
if ((${OS_ARCH} STREQUAL "arm32") OR (${OS_ARCH} STREQUAL "aarch64"))
target_link_libraries(example_kv_cc intarkdb)
else()
target_link_libraries(example_kv_cc intarkdb -Wl,--copy-dt-needed-entries )
endif()
