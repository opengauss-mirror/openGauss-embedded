aux_source_directory(. COMMON_SRC)
aux_source_directory(variant COMMON_VARIANT_SRC)

set(COMMON_ALL_SRC ${COMMON_VARIANT_SRC} ${COMMON_SRC})

# generate static library
#add_library(zecommon STATIC ${COMMON_ALL_SRC})
add_library(zecommon OBJECT ${COMMON_ALL_SRC})
if(WIN32)
    if(NOT MSVC)
    add_compile_options(-Wno-incompatible-pointer-types)
    endif()
    target_link_libraries(zecommon PUBLIC wsock32 ws2_32)
endif()