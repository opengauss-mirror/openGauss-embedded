if(STATISTICS)
include_directories(
        "."
        "catalog"
        "${CMAKE_CURRENT_SOURCE_DIR}/common/variant"
        "common"
        "daemon"
        "flashback"
        "include"
        "index"
        "lob"
        "persist"
        #"replication"
        "sequence"
        "statistics"
        "table"
        "tablespace"
        "xact"
        "tsl"
)
else()
include_directories(
        "."
        "buffer"
        "catalog"
        "${CMAKE_CURRENT_SOURCE_DIR}/common/variant"
        "common"
        "daemon"
        "flashback"
        "include"
        "index"
        "lob"
        "persist"
        #"replication"
        "sequence"
        "table"
        "tablespace"
        "xact"
        "tsl"
)
endif()

if (ENABLE_BACKUP)
	include_directories("backup")
	message(STATUS "with BACKUP")
else()
	message(STATUS "without BACKUP")
endif()

aux_source_directory(. KERNEL_SRC)
if (ENABLE_BACKUP)
aux_source_directory(backup KERNEL_BAK_SRC)
endif()
aux_source_directory(buffer KERNEL_BUF_SRC)
aux_source_directory(catalog KERNEL_CATA_SRC)
aux_source_directory(common KERNEL_CM_SRC)
aux_source_directory(daemon KERNEL_DAE_SRC)
aux_source_directory(flashback KERNEL_FLASH_SRC)
aux_source_directory(include KERNEL_INCLUDE_SRC)
aux_source_directory(index KERNEL_IDX_SRC)
aux_source_directory(lob KERNEL_LOB_SRC)
aux_source_directory(persist KERNEL_PER_SRC)
#aux_source_directory(replication KERNEL_REP_SRC)
aux_source_directory(sequence KERNEL_SEQ_SRC)
if(STATISTICS)
aux_source_directory(statistics KERNEL_STAT_SRC)
endif()
aux_source_directory(table KERNEL_TBL_SRC)
aux_source_directory(tablespace KERNEL_TBLSP_SRC)
aux_source_directory(xact KERNEL_XACT_SRC)
aux_source_directory(tsl KERNEL_TSL_SRC)

if(STATISTICS)
        set(KERNEL_TOTAL_SRC ${KERNEL_BAK_SRC} ${KERNEL_BUF_SRC} ${KERNEL_CATA_SRC} ${KERNEL_CM_SRC} ${KERNEL_DAE_SRC}
                ${KERNEL_FLASH_SRC} ${KERNEL_INCLUDE_SRC} ${KERNEL_IDX_SRC} ${KERNEL_LOB_SRC} ${KERNEL_PER_SRC} #${KERNEL_REP_SRC}
                ${KERNEL_SEQ_SRC} ${KERNEL_STAT_SRC} ${KERNEL_TBL_SRC} ${KERNEL_TBLSP_SRC} ${KERNEL_XACT_SRC} ${KERNEL_SRC} ${KERNEL_TSL_SRC})
        message(STATUS "with STATISTICS")
else()
        set(KERNEL_TOTAL_SRC ${KERNEL_BAK_SRC} ${KERNEL_BUF_SRC} ${KERNEL_CATA_SRC} ${KERNEL_CM_SRC} ${KERNEL_DAE_SRC}
                ${KERNEL_FLASH_SRC} ${KERNEL_INCLUDE_SRC} ${KERNEL_IDX_SRC} ${KERNEL_LOB_SRC} ${KERNEL_PER_SRC} #${KERNEL_REP_SRC}
                ${KERNEL_SEQ_SRC} ${KERNEL_TBL_SRC} ${KERNEL_TBLSP_SRC} ${KERNEL_XACT_SRC} ${KERNEL_SRC} ${KERNEL_TSL_SRC})
        message(STATUS "without STATISTICS")
endif()

# 将收集到的源文件构建为一个目标对象库
add_library(zekernel OBJECT ${KERNEL_TOTAL_SRC})
#target_link_libraries(zekernel PRIVATE zecommon zeprotocol ${3rd_libzstd} ${3rd_libssl} ${3rd_lib_crypto} ${3rd_liblz4} ${3rd_libz} ${vpp_libsecurec})
# target_link_libraries(zekernel PUBLIC zecommon  ${3rd_libz} ${vpp_libsecurec})
