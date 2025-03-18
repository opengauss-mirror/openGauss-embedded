# common naming rules
set(LIB_NAME "network_login")
set(OBJECT_NAME "${LIB_NAME}_obj")
set(STATIC_LIB_NAME "${LIB_NAME}_static")
set(DYNAMIC_LIB_NAME "${LIB_NAME}_dynamic")

# source
file(GLOB SOURCES *.cpp *.c *.h)

# object
add_library(${OBJECT_NAME} OBJECT ${SOURCES})
