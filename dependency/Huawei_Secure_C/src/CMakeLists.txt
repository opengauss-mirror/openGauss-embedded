set(SECUREC_INC_PATH        ${CMAKE_CURRENT_SOURCE_DIR}/../include)
set(LIBRARY_OUTPUT_PATH     ${INTARKDB_THRID_LIB_PATH})

include_directories(${SECUREC_INC_PATH})
if(NOT MSVC)
add_compile_options(-fPIC)
add_compile_options(-w)
add_compile_options(-Wall -DNDEBUG -DSECUREC_WARP_OUTPUT -s -fno-stack-protector -O2 -fPIC)
endif(NOT MSVC)
aux_source_directory(. SECUREC_SRC)

# add_library(securec SHARED ${SECUREC_SRC})

add_library(securec_obj OBJECT ${SECUREC_SRC})
add_library(securec STATIC ${SECUREC_SRC})
set_target_properties(securec PROPERTIES OUTPUT_NAME "securec")
