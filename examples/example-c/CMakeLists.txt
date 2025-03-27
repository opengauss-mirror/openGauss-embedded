cmake_minimum_required(VERSION 3.14.1)
# project(cmd)

add_compile_options(-fPIC)
# # add_compile_options(-fPIC -MMD -fno-strict-aliasing -fsigned-char -fms-extensions)

set(CMAKE_CXX_STANDARD 17)
# # create compile_command.json
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include_directories(${INTARKDB_INCLUDE_PATH} 
                    ${INTARKDB_SRC_PATH}
                    ${INTARKDB_HOME}/interface/c)

set(INSTARDB_LINK_LIBS intarkdb)

## example_c
file(GLOB c_main example_c.c)
add_executable(example_c ${c_main})
if ((${OS_ARCH} STREQUAL "arm32") OR (${OS_ARCH} STREQUAL "aarch64"))
target_link_libraries(example_c intarkdb)
else()
target_link_libraries(example_c intarkdb -Wl,--copy-dt-needed-entries )
endif()

## example_prepareed_c
file(GLOB example_prepared example_prepared_c.c)
add_executable(example_prepared_c ${example_prepared})
if ((${OS_ARCH} STREQUAL "arm32") OR (${OS_ARCH} STREQUAL "aarch64"))
target_link_libraries(example_prepared_c intarkdb)
else()
target_link_libraries(example_prepared_c intarkdb -Wl,--copy-dt-needed-entries )
endif()

