# common naming rules
set(LIB_NAME "network_common")
set(OBJECT_NAME "${LIB_NAME}_obj")
set(STATIC_LIB_NAME "${LIB_NAME}_static")
set(DYNAMIC_LIB_NAME "${LIB_NAME}_dynamic")

# source
# file(GLOB SOURCES *.cpp *.c *.h)
aux_source_directory(./json SOURCES)
aux_source_directory(./hashmap SOURCES)

# include
include_directories(${NETWORK_NETWORK_PATH}
                    ${CJSON_INC_PATH}
)

# object
add_library(${OBJECT_NAME} OBJECT ${SOURCES})
