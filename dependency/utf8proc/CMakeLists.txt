if(POLICY CMP0063)
    cmake_policy(SET CMP0063 NEW)
endif()

SET(LIBRARY_OUTPUT_PATH ${INTARKDB_THRID_LIB_PATH})

include_directories(include)

add_library(utf8proc STATIC utf8proc.cpp utf8proc_wrapper.cpp)
set(INSTARDB_EXPORT_SET "InstarDBExports")

install(TARGETS utf8proc
        EXPORT "${INSTARDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

set_target_properties(utf8proc PROPERTIES POSITION_INDEPENDENT_CODE ON)

# cp to output 
set(libutf8proc_include ${INTARKDB_DST_UTF8PROC_INC_PATH})

# 创建 libutf8proc_include 指向的文件夹
execute_process( COMMAND ${CMAKE_COMMAND} -E make_directory ${libutf8proc_include})

add_custom_command(
TARGET utf8proc POST_BUILD
COMMAND ${CMAKE_COMMAND} -E copy_directory ${PROJECT_SOURCE_DIR}/dependency/utf8proc/include   ${libutf8proc_include}
)