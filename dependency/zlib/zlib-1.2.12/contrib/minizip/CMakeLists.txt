cmake_minimum_required(VERSION 2.4.4)
# project(minizip C)

set(MINIZIP_HDS zip.h unzip.h ioapi.h mztools.h)
set(MINIZIP_SRCS zip.c unzip.c ioapi.c mztools.c)
set(MINIZ_SRCS zip.c ioapi.c minizip.c)
set(MINIUNZ_SRCS unzip.c ioapi.c miniunz.c)

# zlib
include_directories(${CMAKE_SOURCE_DIR}/../..)
add_subdirectory(${CMAKE_SOURCE_DIR}/../.. zlib_build)

# minizip library
include_directories(${zlib_BINARY_DIR})
add_library(minizshared SHARED ${MINIZIP_HDS} ${MINIZIP_SRCS})
add_library(minizstatic STATIC ${MINIZIP_HDS} ${MINIZIP_SRCS})
target_link_libraries(minizshared PRIVATE zlib)
target_link_libraries(minizstatic PRIVATE zlibstatic)
set_target_properties(minizshared minizstatic PROPERTIES OUTPUT_NAME miniz)

# minizip and miniunz
add_executable(minizip ${MINIZ_SRCS})
add_executable(miniunz ${MINIUNZ_SRCS})
target_link_libraries(minizip PRIVATE zlibstatic)
target_link_libraries(miniunz PRIVATE zlibstatic)

# test
macro(ADDTEST name)
    add_executable(${name} tests/${name}.c)
    target_include_directories(${name} PUBLIC ${CMAKE_SOURCE_DIR})
    target_link_libraries(${name} PRIVATE minizshared PRIVATE zlib)
    add_test(${name} ${name})
endmacro()
enable_testing()
ADDTEST(test_miniz_use)
ADDTEST(test_memory_func)
ADDTEST(test_custom_io)

# install
if(NOT SKIP_INSTALL_LIBRARIES AND NOT SKIP_INSTALL_ALL )
    install(TARGETS minizshared minizstatic
        RUNTIME DESTINATION "${CMAKE_INSTALL_PREFIX}/bin"
        ARCHIVE DESTINATION "${CMAKE_INSTALL_PREFIX}/lib"
        LIBRARY DESTINATION "${CMAKE_INSTALL_PREFIX}/lib" )
endif()
if(NOT SKIP_INSTALL_HEADERS AND NOT SKIP_INSTALL_ALL )
    install(FILES ${MINIZIP_HDS} DESTINATION "${CMAKE_INSTALL_PREFIX}/include")
endif()
