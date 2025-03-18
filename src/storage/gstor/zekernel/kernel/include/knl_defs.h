/*
 * Copyright (c) 2022 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * knl_defs.h
 * Kernel defines
 *
 * IDENTIFICATION
 * src/storage/gstor/zekernel/kernel/include/knl_defs.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef __KNL_DEFS_H__
#define __KNL_DEFS_H__

#include "cm_defs.h"
#include "cm_stack.h"
#include "cm_text.h"
#include "cm_list.h"
#include "cm_latch.h"
#include "cm_date.h"
#include "cm_config.h"
#include "cm_row.h"
#include "cm_atomic.h"
#include "cm_partkey.h"
#include "cm_hash.h"
#include "cm_hba.h"
#include "cm_vma.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef void *knl_handle_t;

/*
 * Kernel SCN type which is a 64 bit value divided into three parts as follow:
 *
 * uint64 SCN = |--second--|--usecond--|--serial--|
 * uint64 SCN = |---32bit--|---20bit---|--12bit---|
 */
typedef uint64 knl_scn_t;

#define KNL_INVALID_SCN 0

// gen scn with given seq
#define KNL_TIMESEQ_TO_SCN(time_val, init_time, seq) \
    ((uint64)((time_val)->tv_sec - (init_time)) << 32 | (uint64)(time_val)->tv_usec << 12 | (seq))

#define KNL_SCN_TO_TIMESEQ(scn, time_val, seq, init_time)                                 \
    do {                                                                                  \
        (time_val)->tv_sec = (long)(((scn) >> 32 & 0x00000000ffffffffULL) + (init_time)); \
        (time_val)->tv_usec = (long)((scn) >> 12 & 0x00000000000fffffULL);                \
        seq = (uint64)((scn)&0x0000000000000fffULL);                                      \
    } while (0)

// gen scn with 0x000 seq
#define KNL_TIME_TO_SCN(time_val, init_time) \
    ((uint64)((time_val)->tv_sec - (init_time)) << 32 | (uint64)(time_val)->tv_usec << 12)

#define KNL_SCN_TO_TIME(scn, time_val, init_time)                                                   \
    do {                                                                                            \
        (time_val)->tv_sec = (long)((((uint64)(scn)) >> 32 & 0x00000000ffffffffULL) + (init_time)); \
        (time_val)->tv_usec = (long)(((uint64)(scn)) >> 12 & 0x00000000000fffffULL);                \
    } while (0)

#if defined(WIN32) || defined(__arm__) || defined(__aarch64__)
#define KNL_GET_SCN(p_scn) ((knl_scn_t)cm_atomic_get(p_scn))
#define KNL_SET_SCN(p_scn, scn) (cm_atomic_set((atomic_t *)p_scn, (int64)scn))
#define KNL_INC_SCN(p_scn) ((knl_scn_t)cm_atomic_inc(p_scn))
#else
#define KNL_GET_SCN(p_scn) (knl_scn_t)(*(p_scn))
#define KNL_SET_SCN(p_scn, scn) (*(int64 *)(p_scn) = ((int64)(scn)))
#define KNL_INC_SCN(p_scn) (knl_scn_t)(++(*(p_scn)))
#endif

uint64 knl_current_scn(knl_handle_t session);
uint64 knl_next_scn(knl_handle_t session);
time_t knl_init_time(knl_handle_t session);

status_t knl_timestamp_to_scn(knl_handle_t session, timestamp_t tstamp, uint64 *scn);
void knl_scn_to_timeval(knl_handle_t session, knl_scn_t scn, timeval_t *time_val);

// Page ID type identify a physical position of a page
#pragma pack(4)
typedef union st_undo_page_id {
    uint32 value; // total size is 32 bit
    struct {
        uint32 file : 10;
        uint32 page : 22;
    };
} undo_page_id_t;

// Page list type
typedef struct st_undo_page_list {
    uint32 count;
    undo_page_id_t first;
    undo_page_id_t last;
} undo_page_list_t;

// Page list type
typedef struct st_undo_rowid {
    undo_page_id_t page_id;
    uint16 slot;
    uint16 aligned;
} undo_rowid_t;

typedef struct st_undo_page_info {
    undo_rowid_t undo_rid;   /* undo page */
    uint32 undo_fs;          /* freespace of urid->page_id */
    bool32 encrypt_enable;   /* curr undo page can encrypt */
    bool32 undo_log_encrypt; /* if redolog of curr undorow need encrypt */
} undo_page_info_t;

#define ROWID_FILE_BITS 10
#define ROWID_PAGE_BITS 30
#define ROWID_SLOT_BITS 12
#define ROWID_UNUSED_BITS 12
#define ROWID_VALUE_BITS 52

#define INVALID_FILE_ID (uint32)((1 << ROWID_FILE_BITS) - 1)

typedef union st_page_id {
    uint32 vmid;
    struct {
        uint32 page;
        uint16 file;
        uint16 aligned;
    };
} page_id_t;

// Page id buffer
typedef char pagid_data_t[6];

// Page list type
typedef struct st_page_list {
    uint32 count;
    page_id_t first;
    page_id_t last;
} page_list_t;

// Row ID type identify a physical position of a row
typedef union st_rowid {
    struct {
        uint64 value : ROWID_VALUE_BITS;
        uint64 unused1 : ROWID_UNUSED_BITS;
    };

    struct {
        uint64 file : ROWID_FILE_BITS; // file
        uint64 page : ROWID_PAGE_BITS; // page
        uint64 slot : ROWID_SLOT_BITS; // slot number
        uint64 unused2 : ROWID_UNUSED_BITS;
    };

    struct {
        uint64 vmid : 32;    // virtual memory page id, dynamic view item, ...
        uint64 vm_slot : 16; // slot of virtual memory page, sub item
        uint64 vm_tag : 16;
    };

    struct {
        uint32 tenant_id : 16;
        uint32 curr_ts_num : 16;
        uint32 ts_id;
    };

    struct {
        uint32 group_id;
        uint32 attr_id;
    };

    struct {
        uint32 pos;
        uint32 bucket_id : 12;
        uint32 sub_id : 20;
    };
} rowid_t;
#pragma pack()

static const text_t g_system = { (char *)"SYSTEM", 6 };
static const text_t g_temp = { (char *)"TEMP2", 5 }; // temp changes for new space type
static const text_t g_swap = { (char *)"TEMP", 4 };
static const text_t g_undo = { (char *)"UNDO", 4 };
static const text_t g_users = { (char *)"USERS", 5 };
static const text_t g_temp2 = { (char *)"TEMP2", 5 };
static const text_t g_temp_undo = { (char *)"TEMP2_UNDO", 10 }; // temp changes for new space type
static const text_t g_temp2_undo = { (char *)"TEMP2_UNDO", 10 };
static const text_t g_sysaux = { (char *)"SYSAUX", 6 };
static const text_t g_tenantroot = { (char *)"TENANT$ROOT", 11 };

extern const page_id_t g_invalid_pagid;
extern const rowid_t g_invalid_rowid;
extern const rowid_t g_invalid_temp_rowid;
extern const undo_page_id_t g_invalid_undo_pagid;
extern const undo_rowid_t g_invalid_undo_rowid;
// 默认用户
extern const text_t g_user;
// common page definition
#define AS_PAGID(data) (*(page_id_t *)(data))
#define AS_PAGID_PTR(data) ((page_id_t *)(data))
#define TO_PAGID_DATA(id, data)                       \
    do {                                              \
        AS_PAGID_PTR(data)->file = (uint16)(id).file; \
        AS_PAGID_PTR(data)->page = (uint32)(id).page; \
    } while (0) // pgid_buf_t
#define INVALID_PAGID g_invalid_pagid
#define INVALID_ROWID g_invalid_rowid
#define INVALID_TEMP_ROWID g_invalid_temp_rowid
#define INVALID_SLOT (uint16)(((uint64)1 << ROWID_SLOT_BITS) - 1)
#define INVALID_UNDO_PAGID g_invalid_undo_pagid
#define PAGE_ID_VALUE(id) (((uint64)(id).file << ROWID_PAGE_BITS) + (id).page)
#define IS_INVALID_PAGID(id) ((uint32)(id).file >= INVALID_FILE_ID)

#define KNL_MIN_ROW_SIZE (sizeof(row_head_t) + sizeof(rowid_t))
/* to decide whether the ROWID is an invalid heap-table rowid */
#define IS_INVALID_ROWID(rowid) ((uint32)(rowid).file >= INVALID_FILE_ID)

/* to decide whether the ROWID is an invalid temp-table rowid */
#define IS_INVALID_TEMP_TABLE_ROWID(rowid) ((uint32)((rowid)->vmid == GS_INVALID_ID32))

#define IS_SAME_PAGID(id1, id2) ((id1).page == (id2).page && (id1).file == (id2).file)
#define IS_SAME_PAGID_BY_ROWID(id1, id2) ((id1).page == (id2).page && (id1).file == (id2).file)
#define IS_SAME_TEMP_PAGEID(id1, id2) ((id1).vmid == (id2).vmid)
#define ROWID_LENGTH 18 /* rowid for outer users is a string with a fixed length ROWID_LENGTH. */
#define IS_SAME_ROWID(rid1, rid2) ((rid1).value == (rid2).value)
#define IS_SAME_UNDO_ROWID(rid1, rid2) ((rid1).page_id.value == (rid2).page_id.value && (rid1).slot == (rid2).slot)

#define ROWID_COPY(dst, src)       \
    {                              \
        (dst).value = (src).value; \
    }
#define MINIMIZE_ROWID(rid) \
    {                       \
        (rid).value = 0;    \
    }
#define MAXIMIZE_ROWID(rid)                                \
    {                                                      \
        (rid).value = ((uint64)1 << ROWID_VALUE_BITS) - 1; \
    }

typedef enum en_knl_dict_type {
    DICT_TYPE_UNKNOWN = 0,
    DICT_TYPE_TABLE = 1,
    DICT_TYPE_TEMP_TABLE_TRANS = 2,
    DICT_TYPE_TEMP_TABLE_SESSION = 3,
    DICT_TYPE_TABLE_NOLOGGING = 4,

    /* this is must be the last one of table type */
    DICT_TYPE_TABLE_EXTERNAL = 5,

    DICT_TYPE_VIEW = 6,
    DICT_TYPE_DYNAMIC_VIEW = 7,
    DICT_TYPE_GLOBAL_DYNAMIC_VIEW = 8,

    DICT_TYPE_SYNONYM = 9,
    DICT_TYPE_DISTRIBUTE_RULE = 10,
    DICT_TYPE_SEQUENCE = 11,
} knl_dict_type_t;

typedef struct st_knl_dictionary {
    knl_dict_type_t type;
    uint32 uid;
    uint32 oid;
    knl_handle_t handle;
    knl_handle_t kernel;
    knl_scn_t org_scn;
    knl_scn_t chg_scn;
    knl_scn_t syn_org_scn;
    knl_scn_t syn_chg_scn;
    knl_handle_t syn_handle;
    bool32 is_sysnonym;
    uint32 syn_orig_uid;
    uint32 stats_version;
} knl_dictionary_t;


/* drop table/index/table space/role/synonym */
typedef struct st_knl_drop_def {
    text_t owner;
    text_t name;
    bool32 purge;
    bool32 temp;
    uint32 options;
    text_t ex_owner;
    text_t ex_name;
} knl_drop_def_t;

#define IS_LOGGING_TABLE_BY_TYPE(type) ((type) != DICT_TYPE_TABLE_NOLOGGING)
#define IS_TABLE_BY_TYPE(type) ((type) >= DICT_TYPE_TABLE && (type) <= DICT_TYPE_TABLE_EXTERNAL)

bool32 knl_is_lob_table(knl_dictionary_t *dc);
status_t knl_open_dc(knl_handle_t session, text_t *user, text_t *name, knl_dictionary_t *dc);
status_t knl_open_dc_if_exists(knl_handle_t handle, text_t *user_name, text_t *obj_name, knl_dictionary_t *dc,
    bool32 *is_exists);
status_t knl_open_dc_with_public(knl_handle_t session, text_t *user, bool32 implicit_user, text_t *name,
    knl_dictionary_t *dc);
status_t knl_open_dc_with_public_ex(knl_handle_t session, text_t *user, bool32 implicit_user, text_t *name,
    knl_dictionary_t *dc);
status_t knl_open_seq_dc(knl_handle_t session, text_t *username, text_t *seqname, knl_dictionary_t *dc);
status_t knl_open_dc_by_id(knl_handle_t handle, uint32 uid, uint32 oid, knl_dictionary_t *dc, bool32 ex_recycled);
status_t knl_check_dc(knl_handle_t session, knl_dictionary_t *dc);
void knl_close_dc(knl_handle_t dc);
status_t knl_open_dc_by_index(knl_handle_t session, text_t *owner, text_t *table, text_t *idx_name,
    knl_dictionary_t *dc);
bool32 knl_find_dc_by_tmpidx(knl_handle_t se, text_t *owner, text_t *idx_name);
bool32 dc_object_exists2(knl_handle_t session, text_t *owner, text_t *name, knl_dict_type_t *type);
void knl_inc_dc_ver(knl_handle_t kernel);

memory_context_t *knl_get_dc_memory_context(knl_handle_t dc_entity);

typedef enum en_compress_type {
    COMPRESS_TYPE_NO = 0,
    COMPRESS_TYPE_GENERAL = 1,     // used for transparent table compression
    COMPRESS_TYPE_ALL = 2,         // not used
    COMPRESS_TYPE_DIRECT_LOAD = 3, // not used
} compress_type_t;

typedef enum en_compress_algo {
    COMPRESS_NONE = 0,
    COMPRESS_ZLIB = 1,
    COMPRESS_ZSTD = 2,
    COMPRESS_LZ4 = 3,
} compress_algo_t;

typedef enum en_compress_object_type {
    COMPRESS_OBJ_TYPE_TABLE = 0,
    COMPRESS_OBJ_TYPE_INDEX = 1,
    COMPRESS_OBJ_TYPE_TABLE_PART = 2,
    COMPRESS_OBJ_TYPE_INDEX_PART = 3,
    COMPRESS_OBJ_TYPE_TABLE_SUBPART = 4,
    COMPRESS_OBJ_TYPE_INDEX_SUBPART = 5,
} compress_object_type_t;

/*
 * CAUTION!!!: if add new type or modify old type's order,
 * please modify sql_func.c/g_tab_type_tab synchronously
 */
typedef enum en_table_type {
    TABLE_TYPE_HEAP = 0,
    TABLE_TYPE_IOT = 1,
    TABLE_TYPE_TRANS_TEMP = 2,
    TABLE_TYPE_SESSION_TEMP = 3,
    TABLE_TYPE_NOLOGGING = 4,
    TABLE_TYPE_EXTERNAL = 5,
} table_type_t;

#define IS_NOLOGGING_BY_TABLE_TYPE(type) ((type) == TABLE_TYPE_NOLOGGING)

typedef enum en_index_type {
    INDEX_TYPE_BTREE = 0,
    INDEX_TYPE_HASH = 1,
    INDEX_TYPE_BITMAP = 2,
} index_type_t;

#ifdef __cplusplus
}
#endif

#endif
