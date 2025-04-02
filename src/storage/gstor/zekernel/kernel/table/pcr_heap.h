/*
 * Copyright (c) 2022 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * pcr_heap.h
 *    kernel page consistent read access method definitions
 *
 * IDENTIFICATION
 *    src/storage/gstor/zekernel/kernel/table/pcr_heap.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef __KNL_PCR_HEAP_H__
#define __KNL_PCR_HEAP_H__

#include "cm_defs.h"
#include "cm_row.h"
#include "knl_index.h"
#include "knl_lock.h"
#include "knl_map.h"
#include "knl_page.h"
#include "knl_session.h"
#include "knl_tran.h"
#include "knl_undo.h"
#include "rb_purge.h"
#include "knl_heap.h"

#ifdef __cplusplus
extern "C" {
#endif

#pragma pack(4)
typedef uint16 pcr_row_dir_t;

/*
 * max row count is 32K / (HEAP_MIN_ROW_SIZE + sizeof(pcr_row_dir_t), about 1821
 * When row_dir is valid, it points to the position of row in page, 32K needs all 2bytes.
 * When row_dir is free, it points to the slot of next free row_dir, the max value
 * is 1821, 11 bits is enough for this.
 */
#define PCRH_DIR_FREE_MASK 0x8000
#define PCRH_DIR_NEW_MASK  0x4000
#define PCRH_NO_FREE_DIR   0x3FFF

#define PCRH_NEXT_FREE_DIR(dir) ((*dir) & (~PCRH_DIR_NEW_MASK) & (~PCRH_DIR_FREE_MASK))
#define PCRH_DIR_IS_FREE(dir)   (((*dir) & PCRH_DIR_FREE_MASK) != 0)
#define PCRH_DIR_IS_NEW(dir)    (((*dir) & PCRH_DIR_NEW_MASK) != 0)

#define PCRH_GET_ROW(page, dir) (row_head_t *)((char *)(page) + *(dir))

#define PCRH_MIN_ROW_SIZE    HEAP_MIN_ROW_SIZE  /* same with RCR heap */
#define PCRH_MAX_COST_SIZE   HEAP_MAX_COST_SIZE /* same with RCR heap */
#define PCRH_MAX_MIGR_SIZE   (PCRH_MAX_COST_SIZE - sizeof(pcr_row_dir_t) - sizeof(pcr_itl_t))
#define PCRH_MAX_ROW_SIZE    (PCRH_MAX_COST_SIZE - sizeof(pcr_row_dir_t) - sizeof(pcr_itl_t) - sizeof(rowid_t))
#define PCRH_INSERT_MAX_CHAIN_COUNT 16

#define PCRH_MERGE_CHAIN_COUNT     (GS_MAX_CHAIN_COUNT - PCRH_INSERT_MAX_CHAIN_COUNT + 1)
#define PCRH_NEXT_ROWID(row) \
    (rowid_t *)((char *)(row) + (((row)->is_csf) ? (cm_row_init_size((row)->is_csf, ROW_COLUMN_COUNT(row))) : \
    (sizeof(row_head_t) + ((row)->is_link ? 0 : ROW_BITMAP_EX_SIZE((row))))))

#define PCRH_UPDATE_INPAGE_SIZE(cols) (sizeof(rd_pcrh_update_inpage_t) + CM_ALIGN4(sizeof(uint16) * (cols)))

typedef struct st_pcrh_undo_itl {
    xid_t xid;
    knl_part_locate_t part_loc;
} pcrh_undo_itl_t;

typedef struct st_rd_pcrh_clean_itl {
    knl_scn_t scn;
    uint8 itl_id;
    uint8 is_owscn;
    uint8 is_fast;
    uint8 aligned;
} rd_pcrh_clean_itl_t;

typedef struct st_rd_pcrh_new_itl {
    uint32 ssn;
    xid_t xid;
    undo_rowid_t undo_rid;
} rd_pcrh_new_itl_t;

typedef struct st_rd_pcrh_reuse_itl {
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
} rd_pcrh_reuse_itl_t;

typedef struct st_rd_pcrh_insert {
    uint32 ssn;
    undo_page_id_t undo_page;
    uint16 undo_slot;
    uint8  new_dir;
    uint8 aligned;
    char data[4];
} rd_pcrh_insert_t;

typedef struct st_rd_prch_lock_row {
    uint16 slot;
    uint8 itl_id;
    uint8 aligned;
} rd_pcrh_lock_row_t;

typedef struct st_rd_pcrh_update_link_ssn {
    uint32 ssn;
    undo_page_id_t undo_page;
    uint16 undo_slot;
    uint16 slot;
} pcrh_update_link_ssn_t;

typedef struct st_rd_pcrh_delete {
    uint32 ssn;
    undo_page_id_t undo_page;
    uint16 undo_slot;
    uint16 slot;
} rd_pcrh_delete_t;

typedef struct st_rd_pcrh_set_next_rid {
    uint32 ssn;
    undo_page_id_t undo_page;
    uint16 undo_slot;
    uint16 slot;
    rowid_t next_rid;
} pcrh_set_next_rid_t;

typedef struct st_rd_pcrh_update_inplace {
    uint32 ssn;
    undo_page_id_t undo_page;
    uint16 undo_slot;
    uint16 slot;
    uint16 count; /* < update columns */
    uint16 aligned;
    /* ==== above aligned by 4 bytes === */
    uint16 columns[0]; /* < following is update column data */
} rd_pcrh_update_inplace_t;

typedef struct st_rd_pcrh_update_inpage {
    uint32 ssn;
    undo_page_id_t undo_page;
    uint16 undo_slot;
    uint16 slot;
    uint16 new_cols; /* < new columns */
    int16 inc_size;
    uint16 count; /* < update columns */
    uint16 aligned;
    /* ==== above aligned by 4 bytes === */
    uint16 columns[0]; /* < following is update column data */
} rd_pcrh_update_inpage_t;

typedef struct st_rd_pcrh_undo_update {
    uint32 ssn;
    undo_page_id_t undo_page;
    uint16 undo_slot;
    uint16 slot;
    uint8 is_xfirst;
    uint8 type;
    uint16 aligned;
} rd_pcrh_undo_update_t;

typedef struct st_rd_pcrh_undo {
    uint32 ssn;
    undo_page_id_t undo_page;
    uint16 slot;
    uint16 undo_slot : 15;
    uint16 is_xfirst : 1;
} rd_pcrh_undo_t;

typedef struct st_pcrh_batch_undo {
    uint16 slot : 15;
    uint16 is_xfirst : 1;
} pcrh_batch_undo_t;

typedef struct st_pcrh_undo_batch_insert {
    uint16 count;
    uint16 aligned;
    pcrh_batch_undo_t undos[0];
} pcrh_undo_batch_insert_t;
#pragma pack()

/* structures using in compact page */
typedef struct st_compact_item {
    uint16 offset;
    uint16 prev;
    uint16 next;
    uint16 aligned;
} compact_item_t;

typedef struct st_compact_list {
    uint16 count;
    uint16 first;
    uint16 last;
    uint16 aligned;
} compact_list_t;

static inline pcr_row_dir_t *pcrh_get_dir(heap_page_t *page, uint16 slot)
{
    uint32 offset = (uint32)PAGE_SIZE(page->head) - sizeof(page_tail_t);
    offset -= page->itls * sizeof(pcr_itl_t);
    offset -= (slot + 1) * sizeof(pcr_row_dir_t);
    return (pcr_row_dir_t *)((char *)(page) + offset);
}

static inline pcr_itl_t *pcrh_get_itl(heap_page_t *page, uint8 id)
{
    uint32 offset = (uint32)PAGE_SIZE(page->head) - sizeof(page_tail_t);
    knl_panic(id < page->itls);
    offset -= (id + 1) * sizeof(pcr_itl_t);
    return (pcr_itl_t *)((char *)(page) + offset);
}

status_t pcrh_fetch(knl_handle_t session, knl_cursor_t *cursor);
status_t pcrh_fetch_page(knl_handle_t session , knl_cursor_t *cursor,int64_t* row_count);
status_t pcrh_rowid_fetch(knl_handle_t session, knl_cursor_t *cursor);
status_t pcrh_fetch_by_rowid(knl_session_t *session, knl_cursor_t *cursor);
status_t pcrh_lock_row(knl_session_t *session, knl_cursor_t *cursor, bool32 *is_locked);
status_t pcrh_insert(knl_session_t *session, knl_cursor_t *cursor);
status_t pcrh_update(knl_session_t *session, knl_cursor_t *cursor);
status_t pcrh_delete(knl_session_t *session, knl_cursor_t *cursor);
uint32 pcrh_calc_insert_cost(knl_session_t *session, heap_segment_t *segment, uint16 row_size);

status_t pcrh_prefetch_cr_page(knl_session_t *session, knl_cursor_t *cursor, knl_scn_t query_scn, page_id_t page_id,
                               char *page_buf, uint8 *fb_mark);

void pcrh_undo_insert(knl_session_t *session, undo_row_t *ud_row, undo_page_t *ud_page, int32 ud_slot);
void pcrh_undo_batch_insert(knl_session_t *session, undo_row_t *ud_row, undo_page_t *ud_page, int32 ud_slot);
void pcrh_undo_delete(knl_session_t *ession, undo_row_t *ud_row, undo_page_t *ud_page, int32 slot);
void pcrh_undo_update(knl_session_t *ession, undo_row_t *ud_row, undo_page_t *ud_page, int32 slot);
void pcrh_undo_update_link_ssn(knl_session_t *session, undo_row_t *ud_row, undo_page_t *ud_page, int32 ud_slot);
void pcrh_undo_update_next_rid(knl_session_t *ession, undo_row_t *ud_row, undo_page_t *ud_page, int32 slot);
void pcrh_undo_itl(knl_session_t *ession, undo_row_t *ud_row, undo_page_t *ud_page, int32 slot,
                   knl_dictionary_t *dc, heap_undo_assist_t *heap_assist);
void pcrh_validate_page(knl_session_t *session, page_head_t *page);
void pcrh_clean_lock(knl_session_t *session, lock_item_t *item);

uint8 pcrh_new_itl(knl_session_t *session, heap_page_t *page);
void pcrh_reuse_itl(knl_session_t *session, heap_page_t *page, pcr_itl_t *itl, uint8 itl_id);
void pcrh_clean_itl(knl_session_t *session, heap_page_t *page, rd_pcrh_clean_itl_t *redo);
void pcrh_insert_into_page(knl_session_t *session, heap_page_t *page, row_head_t *row,
                           undo_data_t *undo, rd_pcrh_insert_t *rd, uint16 *slot);
void pcrh_update_inpage(knl_session_t *session, heap_page_t *page, heap_update_assist_t *ua);
void pcrh_compact_page(knl_session_t *session, heap_page_t *page);
void pcrh_reset_self_changed(knl_session_t *session, heap_page_t *page, uint8 itl_id);
void pcrh_cleanout_itls(knl_session_t *session, knl_cursor_t *cursor, heap_page_t *page, bool32 *changed);

status_t pcrh_dump_page(knl_session_t *session, page_head_t *page_head, cm_dump_t *dump);

#ifdef __cplusplus
}
#endif

#endif

