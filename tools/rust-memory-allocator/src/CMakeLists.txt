cmake_minimum_required(VERSION 3.0)

project(main VERSION 1.0)

link_directories(
	${CMAKE_CURRENT_SOURCE_DIR}/../target/debug	
)

message(STATUS "[INFO]: ${CMAKE_CURRENT_SOURCE_DIR}")

add_executable(main main.c)
target_link_libraries(main c_call_rust)
