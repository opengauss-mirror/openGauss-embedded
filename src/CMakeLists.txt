MESSAGE(STATUS "INTARKDB CMAKE BUILD BEGIN")

add_subdirectory(storage)
add_subdirectory(compute)
if (NOT ANDROID AND NOT MSVC)
    add_subdirectory(network)
endif()
add_subdirectory(monitor)
# add_subdirectory(utils)
