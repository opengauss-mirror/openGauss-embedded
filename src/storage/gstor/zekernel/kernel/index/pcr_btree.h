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
 * pcr_btree.h
 * kernel page consistent read access method definitions
 *
 * IDENTIFICATION
 * src/storage/gstor/zekernel/kernel/index/pcr_btree.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef __PCR_BTREE_H__
#define __PCR_BTREE_H__

#include "cm_defs.h"
#include "knl_interface.h"
#include "knl_session.h"
#include "knl_page.h"
#include "knl_lock.h"
#include "knl_index.h"
#include "knl_undo.h"
#include "rb_purge.h"
#include "rcr_btree.h"

#ifdef __cplusplus
extern "C" {
#endif

#pragma pack(4)
/* PCR btree key structure */
typedef struct st_pcrb_key {
    union {
        rowid_t rowid;
        struct {
            uint64 aligned : ROWID_VALUE_BITS;
            uint64 size : ROWID_UNUSED_BITS;
        };
    };

    uint8 is_deleted : 1;
    uint8 is_infinite : 1;
    uint8 is_cleaned : 1;
    uint8 unused : 5;
    uint8 itl_id;
    uint16 bitmap;
    /* === following is key data === */
    /* === child(branch) or part_no(global index)=== */
} pcrb_key_t;

typedef struct st_rd_pcrb_insert {
    uint32 ssn;
    undo_page_id_t undo_page;
    uint16 undo_slot;
    uint16 slot : 15;
    uint16 is_reuse : 1;
    char key[4];
} rd_pcrb_insert_t;

typedef struct st_rd_pcrb_delete {
    uint32 ssn;
    undo_page_id_t undo_page;
    uint16 undo_slot;
    uint16 slot;
    uint8 itl_id;
    uint8 unused1;
    uint16 unused2;
} rd_pcrb_delete_t;

typedef struct st_rd_pcrb_clean_itl {
    knl_scn_t scn;
    uint8 itl_id;
    uint8 is_owscn;
    uint8 is_copied;
    uint8 aligned;
} rd_pcrb_clean_itl_t;

typedef struct st_rd_pcrb_new_itl {
    uint32 ssn;
    xid_t xid;
    undo_rowid_t undo_rid;
} rd_pcrb_new_itl_t;

typedef struct st_rd_pcrb_reuse_itl {
    knl_scn_t min_scn;
    uint32 ssn;
    xid_t xid;
    union {
        undo_rowid_t undo_rid;
        struct {
            undo_page_id_t page_id;
            uint16 slot;
            uint16 itl_id;
        };
    };
} rd_pcrb_reuse_itl_t;

typedef struct st_rd_pcrb_undo {
    uint32 ssn;
    undo_page_id_t undo_page;
    uint16 undo_slot : 15;
    uint16 is_xfirst : 1;
    uint16 slot;
} rd_pcrb_undo_t;

typedef struct st_pcrb_undo_batch_insert {
    knl_part_locate_t part_loc;
    uint16 count;
    uint16 aligned;
    char keys[0];
} pcrb_undo_batch_insert_t;
#pragma pack()

typedef uint16 pcrb_dir_t;

#define PCRB_GET_KEY(page, dir) (pcrb_key_t *)((char *)(page) + *(dir))

#define PCRB_COST_SIZE(key) (((uint16)(key)->size) + sizeof(pcrb_dir_t))

// used for insert key, may cost an extra itl size
#define PCRB_MAX_COST_SIZE(key) (PCRB_COST_SIZE(key) + sizeof(pcr_itl_t))

static inline pcrb_dir_t *pcrb_get_dir(btree_page_t *page, uint32 slot)
{
    uint32 offset = (uint32)PAGE_SIZE(page->head) - sizeof(page_tail_t);
    offset -= page->itls * sizeof(pcr_itl_t);
    offset -= (slot + 1) * sizeof(pcrb_dir_t);
    return (pcrb_dir_t *)((char *)(page) + offset);
}

static inline pcr_itl_t *pcrb_get_itl(btree_page_t *page, uint8 slot)
{
    uint32 offset = (uint32)PAGE_SIZE(page->head) - sizeof(page_tail_t);
    knl_panic(slot < page->itls);
    offset -= (slot + 1) * sizeof(pcr_itl_t);
    return (pcr_itl_t *)((char *)(page) + offset);
}

static inline void pcrb_put_part_id(char *key_buf, uint32 part_id)
{
    *(uint32 *)(key_buf + ((pcrb_key_t *)key_buf)->size) = part_id;
    ((pcrb_key_t *)key_buf)->size += sizeof(uint32);
}

static inline void pcrb_set_part_id(pcrb_key_t *key, uint32 part_id)
{
    *(uint32 *)((char *)key + key->size - sizeof(uint32)) = part_id;
}

static inline void pcrb_set_subpart_id(pcrb_key_t *key, uint32 part_id)
{
    *(uint32 *)((char *)key + key->size - sizeof(uint32) - sizeof(uint32)) = part_id;
}

static inline uint32 pcrb_get_subpart_id(pcrb_key_t *key)
{
    return *(uint32 *)((char *)key + key->size - sizeof(uint32) - sizeof(uint32));
}

static inline uint32 pcrb_get_part_id(pcrb_key_t *key)
{
    return *(uint32 *)((char *)key + key->size - sizeof(uint32));
}

static inline void pcrb_remove_part_id(pcrb_key_t *key)
{
    if (!key->is_infinite) {
        key->size -= sizeof(uint32);
    }
}

static inline uint16 pcrb_get_key_size(char *key)
{
    return (uint16)((pcrb_key_t *)key)->size;
}

static inline void pcrb_set_key_rowid(pcrb_key_t *key, rowid_t *rid)
{
    ROWID_COPY(key->rowid, *rid);
}

static inline void pcrb_put_child(pcrb_key_t *key, page_id_t child)
{
    *(page_id_t *)((char *)key + key->size) = child;
    key->size += sizeof(page_id_t);
}

static inline void pcrb_set_child(pcrb_key_t *key, page_id_t child)
{
    *(page_id_t *)((char *)key + key->size - sizeof(page_id_t)) = child;
}

static inline page_id_t pcrb_get_child(pcrb_key_t *key)
{
    return *(page_id_t *)((char *)key + key->size - sizeof(page_id_t));
}

static inline void pcrb_minimize_unique_parent(index_t *index, pcrb_key_t *key)
{
    if (IS_UNIQUE_PRIMARY_INDEX(index) && !BTREE_KEY_IS_NULL(key)) {
        MINIMIZE_ROWID(key->rowid);
    }
}
status_t pcrb_insert(knl_session_t *session, knl_cursor_t *cursor);
status_t pcrb_batch_insert(knl_handle_t handle, knl_cursor_t *cursor);
status_t pcrb_delete(knl_session_t *session, knl_cursor_t *cursor);
status_t pcrb_insert_into_shadow(knl_session_t *session, knl_cursor_t *cursor);
void pcrb_insert_minimum_key(knl_session_t *session);

status_t pcrb_construct(btree_mt_context_t *ctx);

void pcrb_init_key(pcrb_key_t *key, rowid_t *rid);
void pcrb_put_key_data(char *key_buf, gs_type_t type, const char *data, uint16 len, uint16 id);
void pcrb_decode_key(index_t *index, pcrb_key_t *key, knl_scan_key_t *scan_key);
int32 pcrb_compare_key(index_t *index, knl_scan_key_t *scan_key, pcrb_key_t *key, bool32 cmp_rowid, bool32 *is_same);
void pcrb_convert_row(knl_session_t *session, knl_index_desc_t *desc, char *key_buf, row_head_t *row, uint16 *bitmap);
void pcrb_clean_lock(knl_session_t *session, lock_item_t *lock);
status_t pcrb_check_key_exist(knl_session_t *session, btree_t *btree, char *data, bool32 *exists);

bool32 pcrb_is_recycled_page(knl_session_t *session, btree_page_t *page, bool32 *is_sparse, bool32 *is_empty,
    knl_scn_t force_recycle_scn);
void pcrb_recycle_leaf(knl_session_t *session, btree_t *btree, page_id_t leaf_id, uint64 lsn,
    knl_part_locate_t part_loc);

void pcrb_undo_itl(knl_session_t *session, undo_row_t *ud_row, undo_page_t *ud_page, int32 ud_slot);
void pcrb_undo_insert(knl_session_t *session, undo_row_t *ud_row, undo_page_t *ud_page, int32 ud_slot,
    knl_dictionary_t *dc);
void pcrb_undo_batch_insert(knl_session_t *session, undo_row_t *ud_row, undo_page_t *ud_page, int32 ud_slot,
    knl_dictionary_t *dc);
void pcrb_undo_delete(knl_session_t *session, undo_row_t *ud_row, undo_page_t *ud_page, int32 ud_slot,
    knl_dictionary_t *dc);
status_t pcrb_dump_page(knl_session_t *session, page_head_t *page_head, cm_dump_t *dump);
uint8 pcrb_new_itl(knl_session_t *session, btree_page_t *page);
void pcrb_reuse_itl(knl_session_t *session, btree_page_t *page, pcr_itl_t *itl, uint8 itl_id, knl_scn_t min_scn);
void pcrb_insert_into_page(knl_session_t *session, btree_page_t *page, pcrb_key_t *key, rd_pcrb_insert_t *redo);
void pcrb_compact_page(knl_session_t *session, btree_page_t *page, knl_scn_t min_scn);
uint8 pcrb_copy_itl(knl_session_t *session, pcr_itl_t *src_itl, btree_page_t *dst_page);
status_t pcrb_compare_mtrl_key(mtrl_segment_t *segment, char *data1, char *data2, int32 *result);
void pcrb_clean_key(knl_session_t *session, btree_page_t *page, uint16 slot);
void pcrb_get_txn_info(knl_session_t *session, btree_page_t *page, pcrb_key_t *key, txn_info_t *txn_info);

#ifdef LOG_DIAG
void pcrb_validate_page(knl_session_t *session, page_head_t *page);
#endif

static inline void pcrb_copy_data(pcrb_key_t *dst_key, pcrb_key_t *src_key)
{
    knl_panic(src_key->size == dst_key->size);
    dst_key->rowid = src_key->rowid;

    size_t size = (size_t)(dst_key->size - sizeof(pcrb_key_t));
    if (size > 0) {
        errno_t ret = memcpy_sp((char *)dst_key + sizeof(pcrb_key_t), size, (char *)src_key + sizeof(pcrb_key_t), size);
        knl_securec_check(ret);
    }
}

#ifdef __cplusplus
}
#endif

#endif
