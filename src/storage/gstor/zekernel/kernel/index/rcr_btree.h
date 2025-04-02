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
 * rcr_btree.h
 * implement of btree index
 *
 * IDENTIFICATION
 * src/storage/gstor/zekernel/kernel/index/rcr_btree.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef __RCR_BTREE_H__
#define __RCR_BTREE_H__

#include "cm_defs.h"
#include "knl_common.h"
#include "knl_interface.h"
#include "knl_session.h"
#include "knl_page.h"
#include "knl_lock.h"
#include "knl_index.h"
#include "knl_undo.h"
#include "rb_purge.h"

#ifdef __cplusplus
extern "C" {
#endif

#define BTREE_CURR_PAGE ((btree_page_t *)((session)->curr_page))
#define BTREE_GET_SEGMENT ((btree_segment_t *)(CURR_PAGE + CM_ALIGN8(sizeof(btree_page_t))))
#define BTREE_NEED_COMPACT(page, cost_size) \
    ((page)->free_size >= (cost_size) && (page)->free_begin + (cost_size) > (page)->free_end)
#define BTREE_NEXT_DEL_PAGE (page_id_t *)((char *)CURR_PAGE + sizeof(btree_page_t) + space->ctrl->cipher_reserve_size)
#define BTREE_PCT_SIZE(btree) (uint16)(DEFAULT_PAGE_SIZE / 100 * ((btree)->segment->pctfree))

#define BTREE_ITL_ADDR(page) \
    ((itl_t *)((char *)(page) + PAGE_SIZE((page)->head) - sizeof(itl_t) * (page)->itls - sizeof(page_tail_t)))
#define BTREE_GET_ITL(page, id) (BTREE_ITL_ADDR(page) + (page)->itls - ((id) + 1))
#define BTREE_GET_KEY(page, dir) ((btree_key_t *)((char *)(page) + ((dir)->offset)))
#define BTREE_GET_DIR(page, pos) ((btree_dir_t *)((char *)(BTREE_ITL_ADDR(page)) - ((pos) + 1) * sizeof(btree_dir_t)))
#define BTREE_COST_SIZE(key) (((uint16)(key)->size) + sizeof(btree_dir_t))

// used for insert key, may cost an extra itl size
#define BTREE_MAX_COST_SIZE(key) (BTREE_COST_SIZE(key) + sizeof(itl_t))

#define BTREE_COPY_ROWID(src_key, dst_cur) ROWID_COPY((dst_cur)->rowid, (src_key)->rowid)

#define BTREE_COMPARE_SLOT_GAP 8
#define BTREE_ROOT_COPY_VALID(root_copy) (((root_copy) != NULL) && (!((index_page_item_t *)(root_copy))->is_invalid))
#define BTREE_GET_ROOT_COPY(root_copy) (((index_page_item_t *)(root_copy))->page)

#define BTREE_KEY_IS_NULL(key) ((key)->bitmap == 0)
#define BTREE_ROOT_COPY_SIZE ((uint32)(OFFSET_OF(index_page_item_t, page) + session->kernel->attr.page_size))
#define BTREE_GET_ITEM(area, id) (index_page_item_t *)((char *)(area)->items + (id)*BTREE_ROOT_COPY_SIZE)

#define BTREE_RESERVE_SIZE 500

#define BTREE_PAGE_BODY(page) ((char *)(page) + sizeof(page_head_t))
#define BTREE_PAGE_BODY_SIZE(page) (PAGE_SIZE((page)->head) - sizeof(page_head_t) - sizeof(page_tail_t))
#define BTREE_MIN_SKIP_COLUMNS 1
#define BTREE_SPLIT_PAGE_SIZE (PAGE_UNIT_SIZE * 2)
#define BTREE_PAGE_FREE_SIZE(page) (((page)->free_end) - ((page)->free_begin))

/*
 * if over 20% pages is empty, and over half of empty pages are not recycled, we need coalesce latter
 */
#define INDEX_NEED_COALESCE(stats) ((stats)->empty_pages > (stats)->recycled_pages)

#define BTREE_SEGMENT(pageid, segment)                                                       \
    ((buf_check_resident_page_version(session, (pageid))) ? ((btree_segment_t *)(segment)) : \
                                                            ((btree_segment_t *)(segment)))
typedef enum st_btree_find_type {
    BTREE_FIND_INSERT = 0,
    BTREE_FIND_DELETE,
    BTREE_FIND_INSERT_LOCKED,
    BTREE_FIND_DELETE_NEXT,
} btree_find_type;

#pragma pack(4)
typedef struct st_btree_page {
    page_head_t head;
    knl_scn_t seg_scn; // it is also org_scn on temp btree

    uint16 is_recycled : 1;
    uint16 unused : 15;
    uint16 keys;

    pagid_data_t prev;
    uint8 level;
    uint8 itls;

    pagid_data_t next;
    uint16 free_begin;

    uint16 free_end;
    uint16 free_size;
    knl_scn_t scn;     // max committed itl scn(except delayed itl)
    uint8 reserved[8]; // reserved for future use
} btree_page_t;

typedef struct st_btree_dir_t {
    uint16 offset;
    uint8 itl_id;
    uint8 unused;
} btree_dir_t;

typedef struct st_btree_key {
    union {
        knl_scn_t scn; // sql sequence number(txn in progress) or commit scn
        page_id_t child;
    };

    union {
        rowid_t rowid; // leaf node: rowid;
        struct {
            uint64 align_rowid : ROWID_VALUE_BITS;
            uint64 size : ROWID_UNUSED_BITS;
        };
    };

    undo_page_id_t undo_page;
    uint16 undo_slot : 12;
    uint16 is_deleted : 1;
    uint16 is_infinite : 1;
    uint16 is_owscn : 1;
    uint16 is_cleaned : 1;
    uint16 bitmap;
} btree_key_t;

typedef struct st_btree_segment {
    knl_tree_info_t tree_info;
    knl_scn_t org_scn;
    knl_scn_t seg_scn;
    uint32 table_id;
    uint16 uid;
    uint16 index_id;

    uint16 space_id;
    uint8 initrans;
    uint8 cr_mode;
    page_list_t extents;

    uint32 ufp_count;
    page_id_t ufp_first;
    page_id_t ufp_extent;

    knl_scn_t del_scn;
    page_list_t del_pages; // recycled page list
    uint32 pctfree;

    /* *
     * this is new variable for rocord page_count of this index
     * used for bitmap scenario when try to allow THE SIZE is not available,
     * then try to degrade size (eg 8192 -> 1024 ->128 -> 8), will update this vaule
     * otherwise, always be 0 (also elder version is 0).
     * scenarios(same usage for heap segment):
     * 1 page_count is 0, extent size and page count of this table should be count as before
     * 2 page_count is not 0, page count size must read for extent head (page_head_t)ext_size,
     * page count used this one.
     */
    uint32 page_count;
} btree_segment_t;

typedef struct st_rd_btree_insert {
    uint16 slot;
    uint8 is_reuse;
    uint8 itl_id;
    char key[0];
} rd_btree_insert_t;

typedef struct st_rd_btree_reuse_itl {
    knl_scn_t min_scn;
    xid_t xid;
    uint8 itl_id;
    uint8 unused1;  // for future use
    uint16 unused2; // for future use
} rd_btree_reuse_itl_t;

typedef struct st_rd_btree_clean_itl {
    knl_scn_t scn;
    uint8 itl_id;
    uint8 is_owscn;
    uint8 is_copied;
    uint8 aligned;
} rd_btree_clean_itl_t;

typedef struct st_rd_btree_delete {
    uint32 ssn;
    undo_page_id_t undo_page;
    uint16 undo_slot;
    uint16 slot;
    uint8 itl_id;
    uint8 unused1;  // for future use
    uint16 unused2; // for future use
} rd_btree_delete_t;

typedef struct st_rd_btree_clean_keys {
    uint16 keys;
    uint16 free_size;
} rd_btree_clean_keys_t;

typedef struct st_rd_btree_page_init {
    knl_scn_t seg_scn;
    page_id_t page_id;
    uint8 level;
    uint8 itls;
    uint8 cr_mode;
    uint8 extent_size;
    bool8 reserve_ext;
    uint8 aligned;
    uint16 unused;
} rd_btree_page_init_t;

typedef struct st_rd_btree_undo {
    knl_scn_t scn;
    rowid_t rowid;
    undo_page_id_t undo_page;
    uint16 undo_slot : 12;
    uint16 is_xfirst : 1;
    uint16 is_owscn : 1;
    uint16 unused : 2;
    uint16 slot; // btree slot
} rd_btree_undo_t;

typedef struct st_rd_update_btree_partid {
    uint32 part_id;
    uint32 parent_partid;
    uint16 slot;
    uint16 is_compart_table : 1;
    uint16 unused : 15;
} rd_update_btree_partid_t;

typedef struct st_undo_btree_create {
    uint32 space_id;
    page_id_t entry;
} undo_btree_create_t;

typedef struct st_rd_btree_init_entry {
    page_id_t page_id;
    uint32 extent_size;
} rd_btree_init_entry_t;

typedef struct st_rd_btree_info {
    knl_scn_t min_scn;
    uint32 uid;
    uint32 oid;
    uint32 idx_id;
    knl_part_locate_t part_loc;
} rd_btree_info_t;

#pragma pack()

typedef struct st_btree_key_data {
    btree_key_t *key;
    char *data[GS_MAX_INDEX_COLUMNS];
    uint16 size[GS_MAX_INDEX_COLUMNS];
} btree_key_data_t;

typedef struct st_btree_search {
    btree_t *btree;
    knl_scn_t seg_scn;
    knl_tree_info_t tree_info;
    knl_scn_t query_scn;
    uint32 ssn;
    bool8 is_dsc_scan;
    bool8 is_equal;
    bool8 is_full_scan;
    bool8 read_root_copy;
} btree_search_t;

typedef enum en_btree_alloc_type {
    BTREE_ALLOC_NEW_PAGE = 0,
    BTREE_ALLOC_NEW_EXTENT,
    BTREE_REUSE_STORAGE,
    BTREE_RECYCLE_DELETED,
} btree_alloc_type_t;

typedef struct st_btree_alloc_assist {
    btree_alloc_type_t type;
    page_id_t new_pageid;
    page_id_t next_pageid;
} btree_alloc_assist_t;

#define CURR_KEY_PTR(key) ((char *)(key) + (key)->size)

status_t btree_insert(knl_session_t *session, knl_cursor_t *cursor);
status_t btree_insert_into_shadow(knl_session_t *session, knl_cursor_t *cursor);
status_t btree_delete(knl_session_t *session, knl_cursor_t *cursor);
void btree_decode_key(index_t *index, btree_key_t *key, knl_scan_key_t *scan_key);

void btree_get_end_slot(knl_session_t *session, knl_cursor_t *cursor);
void btree_convert_row(knl_session_t *session, knl_index_desc_t *desc, char *key_buf, row_head_t *row, uint16 *bitmap);
void btree_construct_ancestors_finish(knl_session_t *session, btree_t *btree, btree_page_t **parent_page);

void btree_append_to_page(knl_session_t *session, btree_page_t *page, btree_key_t *key, uint8 itl_id);
void btree_init_key(btree_key_t *key, rowid_t *rid);
void btree_put_key_data(char *key_buf, gs_type_t type, const char *data, uint16 len, uint16 id);
void btree_clean_lock(knl_session_t *session, lock_item_t *lock);
int32 btree_compare_key(index_t *index, knl_scan_key_t *key1, btree_key_t *key2, bool32 cmp_rowid, bool32 *is_same);
status_t btree_construct(btree_mt_context_t *ctx);
status_t btree_check_key_exist(knl_session_t *session, btree_t *btree, char *data, bool32 *exists);
status_t btree_dump_page(knl_session_t *session, page_head_t *page_head, cm_dump_t *dump);

status_t btree_coalesce(knl_session_t *session, btree_t *btree, idx_recycle_stats_t *stats, knl_part_locate_t part_loc);
void btree_concat_next_to_prev(knl_session_t *session, page_id_t next_page_id, page_id_t prev_page_id);
status_t btree_fetch_depended(knl_session_t *session, knl_cursor_t *cursor);
bool32 btree_concat_del_pages(knl_session_t *session, btree_t *btree, page_id_t leaf_id, uint64 lsn,
    knl_part_locate_t part_loc);
void btree_get_parl_schedule(knl_session_t *session, index_t *index, knl_idx_paral_info_t paral_info,
    idx_range_info_t org_info, uint32 root_level, knl_index_paral_range_t *sub_range);
page_id_t btree_clean_copied_itl(knl_session_t *session, uint64 itl_xid, page_id_t page_id);
void btree_try_notify_recycle(knl_session_t *session, btree_t *btree, knl_part_locate_t part_loc);
knl_scn_t btree_get_recycle_min_scn(knl_session_t *session);
status_t btree_compare_mtrl_key(mtrl_segment_t *segment, char *data1, char *data2, int32 *result);
char *btree_get_column(knl_scan_key_t *key, gs_type_t type, uint32 id, uint16 *len, bool32 is_pcr);
uint16 btree_max_column_size(gs_type_t type, uint16 size, bool32 is_pcr);

#ifdef LOG_DIAG
void btree_validate_page(knl_session_t *session, page_head_t *page);
#endif
static inline void btree_put_part_id(char *key_buf, uint32 part_id)
{
    *(uint32 *)(key_buf + ((btree_key_t *)key_buf)->size) = part_id;
    ((btree_key_t *)key_buf)->size += sizeof(uint32);
}

static inline uint32 btree_get_subpart_id(btree_key_t *key)
{
    return *(uint32 *)((char *)key + key->size - sizeof(uint32) - sizeof(uint32));
}

static inline uint32 btree_get_part_id(btree_key_t *key)
{
    return *(uint32 *)((char *)key + key->size - sizeof(uint32));
}

static inline uint16 btree_get_key_size(char *key)
{
    return (uint16)((btree_key_t *)key)->size;
}

static inline void btree_set_bitmap(uint16 *bitmap, uint16 idx)
{
    (*bitmap) |= (0x8000 >> idx);
}

static inline bool32 btree_get_bitmap(uint16 *bitmap, uint16 id)
{
    return ((*bitmap) & (0x8000 >> id));
}

static inline void btree_set_key_rowid(btree_key_t *key, rowid_t *rid)
{
    ROWID_COPY(key->rowid, *rid);
}

// set err_code outside
static inline status_t btree_check_segment_scn(btree_page_t *page, page_type_t type, knl_scn_t seg_scn)
{
    if (page->head.type != type || page->seg_scn != seg_scn) {
        return GS_ERROR;
    }

    return GS_SUCCESS;
}

static inline status_t btree_check_min_scn(knl_scn_t query_scn, knl_scn_t min_scn, uint16 level)
{
    if (level == 0 && query_scn < min_scn) {
        GS_LOG_RUN_ERR("snapshot too old, detail: query_scn %llu, btree_min_scn %llu", query_scn, min_scn);
        GS_THROW_ERROR(ERR_SNAPSHOT_TOO_OLD);
        return GS_ERROR;
    }

    return GS_SUCCESS;
}

static inline btree_t *knl_cursor_btree(knl_cursor_t *cursor)
{
    if (IS_PART_INDEX(cursor->index)) {
        return &((index_part_t *)cursor->index_part)->btree;
    } else {
        return &((index_t *)cursor->index)->btree;
    }
}

#define CURSOR_BTREE(cursor) knl_cursor_btree(cursor)

static inline btree_segment_t *btree_get_segment(knl_session_t *session, index_t *index, uint32 part_no)
{
    btree_segment_t *segment = NULL;

    if (IS_PART_INDEX(index)) {
        index_part_t *index_part = INDEX_GET_PART(index, part_no);
        segment = BTREE_SEGMENT(index_part->btree.entry, index_part->btree.segment);
    } else {
        segment = BTREE_SEGMENT(index->btree.entry, index->btree.segment);
    }

    return segment;
}

static inline uint32 btree_get_extents_count(knl_session_t *session, index_t *index, uint32 part_no)
{
    btree_segment_t *segment = btree_get_segment(session, index, part_no);

    return segment->extents.count;
}

void btree_get_txn_info(knl_session_t *session, bool32 is_scan, btree_page_t *page, btree_dir_t *dir, btree_key_t *key,
    txn_info_t *txn_info);
void btree_cache_reset(knl_session_t *session);
space_t *btree_get_space(knl_session_t *session, index_t *index, uint32 part_no);
void btree_undo_insert(knl_session_t *session, undo_row_t *ud_row, undo_page_t *ud_page, int32 ud_slot,
    knl_dictionary_t *dc);
void btree_undo_delete(knl_session_t *session, undo_row_t *ud_row, undo_page_t *ud_page, int32 ud_slot,
    knl_dictionary_t *dc);

void btree_compact_page(knl_session_t *session, btree_page_t *page, knl_scn_t min_scn);
void btree_insert_into_page(knl_session_t *session, btree_page_t *page, btree_key_t *key, rd_btree_insert_t *redo);
void btree_reuse_itl(knl_session_t *session, btree_page_t *page, itl_t *itl, uint8 itl_id, knl_scn_t min_scn);
uint8 btree_copy_itl(knl_session_t *session, itl_t *src_itl, btree_page_t *dst_page);
void btree_set_match_cond(knl_cursor_t *cursor);
void btree_init_match_cond(knl_cursor_t *cursor, bool32 is_pcr);
status_t btree_open_mtrl_cursor(btree_mt_context_t *ctx, mtrl_sort_cursor_t *cur1, mtrl_sort_cursor_t *cur2,
    mtrl_cursor_t *cursor);
status_t btree_fetch_mtrl_sort_key(btree_mt_context_t *ctx, mtrl_sort_cursor_t *cur1, mtrl_sort_cursor_t *cur2,
    mtrl_cursor_t *cursor);
void btree_close_mtrl_cursor(btree_mt_context_t *ctx, mtrl_sort_cursor_t *cur1, mtrl_sort_cursor_t *cur2,
    mtrl_cursor_t *cursor);
static inline void btree_clean_key(knl_session_t *session, btree_page_t *page, uint16 slot)
{
    btree_dir_t *dir = BTREE_GET_DIR(page, slot);
    btree_key_t *key = BTREE_GET_KEY(page, dir);

    for (uint16 j = slot; j < page->keys - 1; j++) {
        *BTREE_GET_DIR(page, j) = *BTREE_GET_DIR(page, j + 1);
    }

    page->free_size += ((uint16)key->size + sizeof(btree_dir_t));
    key->is_cleaned = (uint16)GS_TRUE;
    page->keys--;
}

static inline void btree_delete_key(knl_session_t *session, btree_page_t *page, rd_btree_delete_t *redo)
{
    btree_dir_t *dir = BTREE_GET_DIR(page, redo->slot);
    btree_key_t *key = BTREE_GET_KEY(page, dir);

    key->is_deleted = GS_TRUE;
    dir->itl_id = redo->itl_id;
    key->scn = redo->ssn;
    key->undo_page = redo->undo_page;
    key->undo_slot = redo->undo_slot;
    key->is_owscn = GS_FALSE;
}

static inline uint8 btree_new_itl(knl_session_t *session, btree_page_t *page)
{
    char *src = (char *)page + page->free_end;
    char *dst = src - sizeof(itl_t);

    errno_t err =
        memmove_s(dst, PAGE_SIZE(page->head) - page->free_end + sizeof(itl_t), src, page->keys * sizeof(btree_dir_t));
    knl_securec_check(err);

    uint8 itl_id = page->itls;
    page->itls++;
    page->free_end -= sizeof(itl_t);
    page->free_size -= sizeof(itl_t);

    return itl_id;
}

static inline uint32 btree_get_segment_page_count(space_t *space, btree_segment_t *segment)
{
    if (segment->page_count == 0) {
        return spc_pages_by_ext_cnt(space, segment->extents.count, PAGE_TYPE_BTREE_HEAD);
    }
    return segment->page_count;
}

static inline void btree_try_update_segment_pagecount(btree_segment_t *segment, uint32 ext_size)
{
    if (segment->page_count == 0) {
        return;
    }
    segment->page_count += ext_size;
}

static inline void btree_try_init_segment_pagecount(space_t *space, btree_segment_t *segment)
{
    if (segment->page_count == 0) {
        // print log when first degrade happened
        GS_LOG_RUN_INF("btree segment degraded alloc extent, space id: %u, uid: %u, table id: %u, index id: %u.",
            (uint32)segment->space_id, (uint32)segment->uid, segment->table_id, (uint32)segment->index_id);
        segment->page_count = spc_pages_by_ext_cnt(space, segment->extents.count, PAGE_TYPE_BTREE_HEAD);
    }
}

#ifdef __cplusplus
}
#endif

#endif
