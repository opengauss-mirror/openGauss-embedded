file(GLOB_RECURSE CURRENT_SOURCE *.c)

#source_group("Include" FILES ${CURRENT_HEADERS})

include_directories(
        ${INTARKDB_SRC_PATH}
        ${INTARKDB_SECUREC_INC_PATH}
        ${INTARKDB_ZEKERNEL_COMMON_INC_PATH}
)

add_library(status OBJECT ${CURRENT_SOURCE} )