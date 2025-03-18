cmake_minimum_required(VERSION 3.14.1)
# project(cmd)

add_compile_options(-fPIC)
# # add_compile_options(-fPIC -MMD -fno-strict-aliasing -fsigned-char -fms-extensions)

set(CMAKE_CXX_STANDARD 17)
# # create compile_command.json
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include_directories(${INTARKDB_INCLUDE_PATH})
include_directories(${INTARKDB_ZEKERNEL_COMMON_INC_PATH})

set(INSTARDB_LINK_LIBS
intarkdb
fmt::fmt
cjson)

## example_cc
file(GLOB cc_main example_cc.cpp)
add_executable(example_cc ${cc_main})
if ((${OS_ARCH} STREQUAL "arm32") OR (${OS_ARCH} STREQUAL "aarch64"))
    target_link_libraries(example_cc intarkdb)
else()
    target_link_libraries(example_cc intarkdb -Wl,--copy-dt-needed-entries )
endif()

## example_prepared_cc
file(GLOB cc_mexample_preparedain example_prepared_cc.cpp)
add_executable(example_prepared_cc ${cc_mexample_preparedain})
if ((${OS_ARCH} STREQUAL "arm32") OR (${OS_ARCH} STREQUAL "aarch64"))
    target_link_libraries(example_prepared_cc intarkdb)
else()
    target_link_libraries(example_prepared_cc intarkdb -Wl,--copy-dt-needed-entries )
endif()
