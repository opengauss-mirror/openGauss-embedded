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
 * knl_xact_log.c
 *    kernel undo redo interface routines
 *
 * IDENTIFICATION
 *    src/storage/gstor/zekernel/kernel/xact/knl_xact_log.c
 *
 * -------------------------------------------------------------------------
 */
#include "knl_xact_log.h"
#include "knl_context.h"
#include "knl_xa.h"

/*
 * redo function for change undo segment
 */
void rd_undo_change_segment(knl_session_t *session, log_entry_t *log)
{   
    undo_segment_t *segment = (undo_segment_t *)(CURR_PAGE + PAGE_HEAD_SIZE);
    segment->page_list = *(undo_page_list_t *)log->data;
} 

void print_undo_change_segment(log_entry_t *log)
{   
    undo_page_list_t *page_list = (undo_page_list_t *)log->data;
    page_id_t first = PAGID_U2N(page_list->first);
    page_id_t last = PAGID_U2N(page_list->last);

    printf("count %u, first %u-%u, last %u-%u\n", page_list->count,
           (uint32)first.file, (uint32)first.page, (uint32)last.file, (uint32)last.page);
}

/*
 * redo function for change txn
 */
void rd_undo_change_txn(knl_session_t *session, log_entry_t *log)
{   
    rd_undo_chg_txn_t *redo = (rd_undo_chg_txn_t *)log->data;
    txn_t *txn_page = (txn_t *)(CURR_PAGE + PAGE_HEAD_SIZE);
    txn_t *txn = txn_page + redo->xmap.slot % TXN_PER_PAGE;

    txn->undo_pages = redo->undo_pages;
} 

/*
 * log analysis function for change txn
 */
void gbp_aly_undo_change_txn(knl_session_t *session, log_entry_t *log, uint64 lsn)
{
    knl_panic(BUF_IS_RESIDENT(session->curr_page_ctrl));
    rd_undo_change_txn(session, log);
}

void gbp_undo_format_txn(knl_session_t *session, log_entry_t *log, uint64 lsn)
{
    knl_panic(BUF_IS_RESIDENT(session->curr_page_ctrl));
}

void print_undo_change_txn(log_entry_t *log)
{
    rd_undo_chg_txn_t *redo = (rd_undo_chg_txn_t *)log->data;
    page_id_t first = PAGID_U2N(redo->undo_pages.first);
    page_id_t last = PAGID_U2N(redo->undo_pages.last);

    printf("xmap %u-%u, undo_pages(count %u, first %u-%u, last %u-%u)\n",
           (uint32)redo->xmap.seg_id, (uint32)redo->xmap.slot,
           (uint32)redo->undo_pages.count, (uint32)first.file, (uint32)first.page,
           (uint32)last.file, (uint32)last.page);
} 

/*
 * redo function for format undo page
 */
void rd_undo_format_page(knl_session_t *session, log_entry_t *log)
{   
    undo_page_t *page = (undo_page_t *)CURR_PAGE;
    rd_undo_fmt_page_t *redo = (rd_undo_fmt_page_t *)log->data;

    undo_format_page(session, page, PAGID_U2N(redo->page_id), redo->prev, redo->next);
}

void print_undo_format_page(log_entry_t *log)
{   
    rd_undo_fmt_page_t *redo = (rd_undo_fmt_page_t *)log->data;
    page_id_t page_id = PAGID_U2N(redo->page_id);
    page_id_t prev = PAGID_U2N(redo->prev);
    page_id_t next = PAGID_U2N(redo->next);

    printf("page %u-%u, prev %u-%u, next %u-%u\n", (uint32)page_id.file, (uint32)page_id.page,
           (uint32)prev.file, (uint32)prev.page, (uint32)next.file, (uint32)next.page);
}

void rd_undo_cipher_reserve(knl_session_t *session, log_entry_t *log)
{
    undo_page_t *page = (undo_page_t *)CURR_PAGE;
    rd_undo_cipher_reserve_t *redo = (rd_undo_cipher_reserve_t *)log->data;

    page->free_size -= redo->cipher_reserve_size;
    page->free_begin += redo->cipher_reserve_size;
}

void print_undo_cipher_reserve(log_entry_t *log)
{
    rd_undo_cipher_reserve_t *redo = (rd_undo_cipher_reserve_t *)log->data;

    printf("cipher_reserve_size %u\n", (uint32)redo->cipher_reserve_size);
}

/*
 * redo function for change undo page
 */
void rd_undo_change_page(knl_session_t *session, log_entry_t *log)
{   
    undo_page_t *page = (undo_page_t *)CURR_PAGE;
    rd_undo_chg_page_t *redo = (rd_undo_chg_page_t *)log->data;

    TO_PAGID_DATA(INVALID_PAGID, page->head.next_ext);
    page->prev = redo->prev;
    page->begin_slot = redo->slot;
}

void print_undo_change_page(log_entry_t *log)
{   
    rd_undo_chg_page_t *redo = (rd_undo_chg_page_t *)log->data;
    page_id_t prev = PAGID_U2N(redo->prev);

    printf("prev %u-%u, begin_slot %u\n",
           (uint32)prev.file, (uint32)prev.page, (uint32)redo->slot);
}

/*
 * redo function for undo write
 */
void rd_undo_write(knl_session_t *session, log_entry_t *log) 
{
    rd_undo_write_t *redo = (rd_undo_write_t *)log->data;
    undo_page_t *page = (undo_page_t *)CURR_PAGE;
    undo_row_t *temp;
    undo_row_t *row;
    uint16 *slot = NULL;
    uint16 actual_size;
    errno_t errcode = 0;
    temp = (undo_row_t *)redo->data;
    row = (undo_row_t *)((char *)page + page->free_begin);

    /* undo_data->size is less than page size(8192), the sum will not exceed max value of uint16 */
    actual_size = (uint16)(UNDO_ROW_HEAD_SIZE + temp->data_size);
    if (actual_size != 0) {
        errcode = memcpy_sp(row, actual_size, temp, actual_size);
        knl_securec_check(errcode);
    }
    slot = UNDO_SLOT(page, page->rows); 
    *slot = page->free_begin;

    /*
     * free_size less than DEFAULT_PAGE_SIZE(8192),
     * actual_size is less than page size(8192) + UNDO_ROW_HEAD_SIZE,
     * the sum is less than max value(65535) of uint16
     */
    page->free_begin += actual_size;
    page->free_size -= (uint16)(actual_size + sizeof(uint16));
    page->ss_time = redo->time;
    page->rows++;
}

void print_undo_write(log_entry_t *log)
{   
    rd_undo_write_t *redo = (rd_undo_write_t *)log->data;
    undo_row_t *row = (undo_row_t *)redo->data;
    undo_page_id_t seg_id;
    page_id_t prev;

    printf("%s, data_size %u, ", undo_type((uint8)row->type), row->data_size); 
    printf("is_cleaned %u, is_xfirst %u, scn %llu, is_owscn %u, xmap %u-%u, xnum %u, ssn %u ",
           (uint32)row->is_cleaned, (uint32)row->is_xfirst, row->scn, (uint32)row->is_owscn,
           (uint32)row->xid.xmap.seg_id, (uint32)row->xid.xmap.slot, (uint32)row->xid.xnum, row->ssn); 

    if (row->type == UNDO_BTREE_INSERT || row->type == UNDO_BTREE_DELETE ||
        row->type == UNDO_PCRB_INSERT || row->type == UNDO_PCRB_DELETE) {
        seg_id.file = (uint32)row->seg_file;
        seg_id.page = (uint32)row->seg_page;
        printf("seg_id %u-%u, is_shadow %d, ",
               (uint32)seg_id.file, (uint32)seg_id.page, (row->index_id == GS_SHADOW_INDEX_ID ? 1 : 0));
    } else if (row->type == UNDO_TEMP_BTREE_INSERT || row->type == UNDO_TEMP_BTREE_DELETE) {
        printf("user_id %u, table_id %u, index_id %u, ",
               (uint32)row->user_id, (uint32)row->seg_page, (uint32)row->index_id);
    } else {
        printf("row_page %u-%u, row_slot %u, ",
               (uint32)row->rowid.file, (uint32)row->rowid.page, (uint32)row->rowid.slot);
    }

    prev = PAGID_U2N(row->prev_page);
    printf("prev_undo %u-%u, prev_slot %u\n", (uint32)prev.file, (uint32)prev.page, (uint32)row->prev_slot);
}

/*
 * redo function for clean undo row during txn rollback
 */
void rd_undo_clean(knl_session_t *session, log_entry_t *log) 
{   
    int32 slot = *(int32 *)log->data;
    undo_page_t *page = (undo_page_t *)CURR_PAGE;
    undo_row_t *row = NULL;

    knl_panic_log(slot >= 0 && slot < (int32)page->rows, "slot is invalid, panic info: page %u-%u type %u slot %u",
                  AS_PAGID(page->head.id).file, AS_PAGID(page->head.id).page, page->head.type, slot);
    row = UNDO_ROW(page, slot); 

    if (!row->is_cleaned) {
        row->is_cleaned = 1;
    }
} 

void print_undo_clean(log_entry_t *log) 
{   
    int32 slot = *(int32 *)log->data;
    printf("slot %d\n", slot);
}


void rd_tx_begin(knl_session_t *session, log_entry_t *log)
{
    xid_t *xid = (xid_t *)log->data;
    txn_page_t *txn_page = (txn_page_t *)CURR_PAGE;
    txn_t *txn = &txn_page->items[xid->xmap.slot % TXN_PER_PAGE];
    tx_id_t tx_id = tx_xmap_get_txid(session, xid->xmap);
    undo_t *undo = &session->kernel->undo_ctx.undos[tx_id.seg_id];
    tx_item_t *tx_item = &undo->items[tx_id.item_id];
    tx_area_t *area = &session->kernel->tran_ctx;

    txn->xnum = xid->xnum;
    txn->status = (uint8)XACT_BEGIN;
    txn->undo_pages.count = 0;
    txn->undo_pages.first = INVALID_UNDO_PAGID;
    txn->undo_pages.last = INVALID_UNDO_PAGID;

#ifdef LOG_DIAG
    if (session->log_diag) {
        return;
    }
#endif

    if (!DB_IS_PRIMARY(&session->kernel->db) && !DB_NOT_READY(session)) {
        cm_spin_lock(&tx_item->lock, &session->stat_txn);
        tx_item->rmid = session->kernel->sessions[SESSION_ID_ROLLBACK]->rmid;
        cm_spin_unlock(&tx_item->lock);

        cm_spin_lock(&undo->lock, &session->stat_txn_list);
        knl_panic(undo->free_items.count > 0);
        undo->free_items.count--;
        area->rollback_num = session->kernel->attr.tx_rollback_proc_num;
        if (GS_INVALID_ID32 != tx_item->prev) {
            undo->items[tx_item->prev].next = tx_item->next;
        }
        if (GS_INVALID_ID32 != tx_item->next) {
            undo->items[tx_item->next].prev = tx_item->prev;
        }
        if (undo->free_items.first == tx_id.item_id) {
            undo->free_items.first = tx_item->next;
        }
        if (undo->free_items.last == tx_id.item_id) {
            undo->free_items.last = tx_item->prev;
        }
        cm_spin_unlock(&undo->lock);
    }
}

void gbp_aly_tx_begin(knl_session_t *session, log_entry_t *log, uint64 lsn)
{
    /* redo tx page when do log analysis */
    knl_panic(BUF_IS_RESIDENT(session->curr_page_ctrl));
    rd_tx_begin(session, log);
}

void print_tx_begin(log_entry_t *log)
{
    xid_t *xid = (xid_t *)log->data;
    printf("xmap %u-%u, xnum %u\n", (uint32)xid->xmap.seg_id, (uint32)xid->xmap.slot, xid->xnum);
}

/*
 * rd_tx_end will not skip when GBP enable, because log analyze proc will repaly rd_tx_end
 * log analyze proc maintain txn area but do not change scn, scn will changed by lrpl proc
 * Notice:
 *     must judge is_skip when modify txn area and page, and judge SESSION_IS_LOG_ANALYZE when modify scn
 */
void rd_tx_end(knl_session_t *session, log_entry_t *log)
{
    rd_tx_end_t *redo = (rd_tx_end_t *)log->data;
    txn_page_t *txn_page = (txn_page_t *)CURR_PAGE;
    txn_t *txn = &txn_page->items[redo->xmap.slot % TXN_PER_PAGE];
    tx_id_t tx_id = tx_xmap_get_txid(session, redo->xmap);
    undo_t *undo = &session->kernel->undo_ctx.undos[tx_id.seg_id];
    tx_item_t *tx_item = &undo->items[tx_id.item_id];
    bool32 is_skip = session->page_stack.is_skip[session->page_stack.depth - 1];

#ifdef LOG_DIAG
    if (session->log_diag) {
        txn->scn = redo->scn;
        txn->status = (uint8)XACT_END;
        return;
    }
#endif

    if (!redo->is_auton && txn->scn > undo->ow_scn && !SESSION_IS_LOG_ANALYZE(session) &&
        txn->status != (uint8)XACT_PHASE1) {
        KNL_SET_SCN(&undo->ow_scn, txn->scn);
    }

    if (!is_skip) {
        txn->scn = redo->scn;
        txn->status = (uint8)XACT_END;
    }

    if (redo->scn > DB_CURR_SCN(session) && !SESSION_IS_LOG_ANALYZE(session)) {
        KNL_SET_SCN(&session->kernel->scn, redo->scn);
    }

    if (!is_skip && !DB_IS_PRIMARY(&session->kernel->db) && !DB_NOT_READY(session)) {
        cm_spin_lock(&tx_item->lock, &session->stat_txn);
        tx_item->rmid = GS_INVALID_ID16;
        cm_spin_unlock(&tx_item->lock);

        cm_spin_lock(&undo->lock, &session->stat_txn_list);
        if (undo->free_items.count == 0) {
            undo->free_items.count = 1;
            undo->free_items.first = tx_id.item_id;
            undo->free_items.last = tx_id.item_id;
            undo->items[tx_id.item_id].prev = GS_INVALID_ID32;
        } else {
            undo->items[undo->free_items.last].next = tx_id.item_id;
            undo->items[tx_id.item_id].prev = undo->free_items.last;
            undo->free_items.last = tx_id.item_id;
            undo->free_items.count++;
        }
        undo->items[tx_id.item_id].next = GS_INVALID_ID32;
        cm_spin_unlock(&undo->lock);
    }
    if (!SESSION_IS_LOG_ANALYZE(session)) {
        session->query_scn = redo->scn;
    }
}

void gbp_aly_tx_end(knl_session_t *session, log_entry_t *log, uint64 lsn)
{
    knl_panic(BUF_IS_RESIDENT(session->curr_page_ctrl));
    /* redo tx page when do log analysis */
    rd_tx_end(session, log);
}

void print_tx_end(log_entry_t *log)
{
    rd_tx_end_t *redo = (rd_tx_end_t *)log->data;
    printf("xmap %u-%u, is_auton %u, is_commit %u, scn %llu\n",
        (uint32)redo->xmap.seg_id, (uint32)redo->xmap.slot,
        (uint32)redo->is_auton, (uint32)redo->is_commit, redo->scn);
}

/*
 * rd_xa_phase1 will not skip when GBP enable, because log analyze proc will repaly rd_xa_phase1
 * log analyze proc maintain txn area but do not change scn, scn will changed by lrpl proc
 * Notice:
 *     must judge is_skip when modify txn area and page, and judge SESSION_IS_LOG_ANALYZE when modify scn
 */
void rd_xa_phase1(knl_session_t *session, log_entry_t *log)
{
    rd_xa_phase1_t *redo = (rd_xa_phase1_t *)log->data;
    txn_page_t *txn_page = (txn_page_t *)CURR_PAGE;
    txn_t *txn = &txn_page->items[redo->xmap.slot % TXN_PER_PAGE];
    tx_id_t tx_id = tx_xmap_get_txid(session, redo->xmap);
    undo_t *undo = &session->kernel->undo_ctx.undos[tx_id.seg_id];
    bool32 is_skip = session->page_stack.is_skip[session->page_stack.depth - 1];

#ifdef LOG_DIAG
    if (session->log_diag) {
        txn->scn = redo->scn;
        txn->status = (uint8)XACT_PHASE1;
        return;
    }
#endif

    if (txn->scn > undo->ow_scn && !SESSION_IS_LOG_ANALYZE(session)) {
        KNL_SET_SCN(&undo->ow_scn, txn->scn);
    }

    if (!is_skip) {
        txn->scn = redo->scn;
        txn->status = (uint8)XACT_PHASE1;
    }

    if (redo->scn > DB_CURR_SCN(session) && !SESSION_IS_LOG_ANALYZE(session)) {
        KNL_SET_SCN(&session->kernel->scn, redo->scn);
    }
}

void print_xa_phase1(log_entry_t *log)
{
    rd_xa_phase1_t *redo = (rd_xa_phase1_t *)log->data;
    printf("xmap %u-%u, scn %llu\n", (uint32)redo->xmap.seg_id, (uint32)redo->xmap.slot, redo->scn);
}

void gbp_aly_xa_phase1(knl_session_t *session, log_entry_t *log, uint64 lsn)
{
    knl_panic(BUF_IS_RESIDENT(session->curr_page_ctrl));
    /* redo tx page when do log analysis */
    rd_xa_phase1(session, log);
}

void rd_xa_rollback_phase2(knl_session_t *session, log_entry_t *log)
{
    xmap_t *xmap = (xmap_t *)log->data;
    txn_page_t *txn_page = (txn_page_t *)CURR_PAGE;
    txn_t *txn = &txn_page->items[xmap->slot % TXN_PER_PAGE];

    txn->status = (uint8)XACT_PHASE2;

#ifdef LOG_DIAG
    if (session->log_diag) {
        return;
    }
#endif
}

void print_xa_rollback_phase2(log_entry_t *log)
{
    xmap_t *xmap = (xmap_t *)log->data;
    printf("xmap %u-%u\n", (uint32)xmap->seg_id, (uint32)xmap->slot);
}

void gbp_aly_xa_rollback_phase2(knl_session_t *session, log_entry_t *log, uint64 lsn)
{
    knl_panic(BUF_IS_RESIDENT(session->curr_page_ctrl));
    /* redo tx page when do log analysis */
    rd_xa_rollback_phase2(session, log);
}

void rd_undo_alloc_segment(knl_session_t *session, log_entry_t *log)
{
    undo_page_id_t *undo_entry = NULL;
    rd_undo_alloc_seg_t *redo = (rd_undo_alloc_seg_t*)log->data;
    undo_entry = (undo_page_id_t *)(CURR_PAGE + PAGE_HEAD_SIZE + sizeof(space_head_t));
    undo_entry[redo->id] = redo->entry;
}

void print_undo_alloc_segment(log_entry_t *log)
{
    rd_undo_alloc_seg_t *redo = (rd_undo_alloc_seg_t*)log->data;

    printf("undo alloc the %u segment entry %u-%u\n", (uint32)redo->id, (uint32)redo->entry.file, 
        (uint32)redo->entry.page);
}

void rd_undo_create_segment(knl_session_t *session, log_entry_t *log)
{
    page_head_t *undo_head = (page_head_t *)log->data;
    page_id_t entry = AS_PAGID(undo_head->id);

    page_init(session, (page_head_t*)CURR_PAGE, entry, PAGE_TYPE_UNDO_HEAD);

    undo_segment_t *segment = UNDO_GET_SEGMENT;
    segment->page_list.count = 0;
    segment->page_list.first = INVALID_UNDO_PAGID;
    segment->page_list.last = INVALID_UNDO_PAGID;
    segment->txn_page_count = 0;
}

void print_undo_create_segment(log_entry_t *log)
{
    page_head_t *undo_head = (page_head_t *)log->data;
    page_id_t entry = AS_PAGID(undo_head->id);
    printf("undo create segment entry %u-%u\n", (uint32)entry.file, (uint32)entry.page);
}

void rd_undo_extend_txn(knl_session_t *session, log_entry_t *log)
{
    rd_undo_alloc_txn_page_t *rd = (rd_undo_alloc_txn_page_t*)log->data;
    undo_segment_t *seg = UNDO_GET_SEGMENT;
    seg->txn_page[rd->slot] = PAGID_N2U(rd->txn_extent);
    seg->txn_page_count = rd->slot + 1;
}

void print_undo_extend_txn(log_entry_t *log)
{
    rd_undo_alloc_txn_page_t *rd = (rd_undo_alloc_txn_page_t*)log->data;
    printf("undo extend the %u txn page entry %u-%u\n", 
        (uint32)rd->slot, (uint32)rd->txn_extent.file, (uint32)rd->txn_extent.page);
}

void rd_undo_format_txn(knl_session_t *session, log_entry_t *log)
{
    page_head_t *undo_head = (page_head_t *)log->data;
    page_id_t entry = AS_PAGID(undo_head->id);

    page_init(session, (page_head_t*)CURR_PAGE, entry, PAGE_TYPE_TXN);
}

void print_undo_format_txn(log_entry_t *log)
{
    page_head_t *undo_head = (page_head_t *)log->data;
    page_id_t entry = AS_PAGID(undo_head->id);
    printf("undo format txn page entry %u-%u\n", (uint32)entry.file, (uint32)entry.page);
}

void rd_undo_move_txn(knl_session_t *session, log_entry_t *log)
{
    txn_page_t *new_txnpage = (txn_page_t*)log->data;
    txn_page_t *dis_txn_page = (txn_page_t *)CURR_PAGE;
    errno_t ret;

    ret = memcpy_sp(dis_txn_page, DEFAULT_PAGE_SIZE, new_txnpage, DEFAULT_PAGE_SIZE);
    knl_securec_check(ret);
}

void print_undo_move_txn(log_entry_t *log)
{
    page_head_t *undo_head = (page_head_t *)log->data;
    page_id_t entry = AS_PAGID(undo_head->id);
    printf("undo move txn page entry %u-%u\n", (uint32)entry.file, (uint32)entry.page);
}

void rd_switch_undo_space(knl_session_t *session, log_entry_t *log)
{
    rd_switch_undo_space_t *rd = (rd_switch_undo_space_t*)log->data;
    uint32 space_id = rd->space_id;
    core_ctrl_t *core_ctrl = DB_CORE_CTRL(session);
    space_t *old_undo_space = SPACE_GET(core_ctrl->undo_space);
    space_t *new_undo_space = NULL;

    session->kernel->undo_ctx.is_switching = GS_TRUE;
    undo_invalid_segments(session);

    GS_LOG_RUN_INF("[RD] switch undo space start");
    undo_reload_segment(session, rd->space_entry);
    core_ctrl->undo_space = space_id;

    undo_init(session, 0, core_ctrl->undo_segments);

    if (tx_area_init(session, 0, core_ctrl->undo_segments) != GS_SUCCESS) {
        GS_LOG_RUN_ERR("rd_switch_undo_space failed");
        session->kernel->undo_ctx.is_switching = GS_FALSE;
        return;
    }

    tx_area_release(session);
    session->kernel->undo_ctx.is_switching = GS_FALSE;

    new_undo_space = SPACE_GET(space_id);
    new_undo_space->ctrl->type = SPACE_TYPE_UNDO | SPACE_TYPE_DEFAULT;
    old_undo_space->ctrl->type = SPACE_TYPE_UNDO;

    if (db_save_space_ctrl(session, old_undo_space->ctrl->id) != GS_SUCCESS) {
        CM_ABORT(0, "[DB] ABORT INFO: failed to save space ctrl file when load tablespace %s",
            old_undo_space->ctrl->name);
    }

    if (db_save_space_ctrl(session, new_undo_space->ctrl->id) != GS_SUCCESS) {
        CM_ABORT(0, "[DB] ABORT INFO: failed to save space ctrl file when load tablespace %s",
            new_undo_space->ctrl->name);
    }

    if (db_save_core_ctrl(session) != GS_SUCCESS) {
        CM_ABORT(0, "[DB] ABORT INFO: failed to save core ctrl file when load tablespace");
    }

    if (cm_alter_config(session->kernel->attr.config, "UNDO_TABLESPACE", new_undo_space->ctrl->name, 
                        CONFIG_SCOPE_MEMORY, GS_TRUE) != GS_SUCCESS) {
        GS_LOG_RUN_ERR("[UNDO] failed to alter undo tablespace config");
    }
    
    GS_LOG_RUN_INF("[UNDO] succeed to switch undo tablespace %s", new_undo_space->ctrl->name);
}

void print_switch_undo_space(log_entry_t *log)
{
    rd_switch_undo_space_t *rd = (rd_switch_undo_space_t*)log->data;
    (void)printf("undo space switch space space_id:%d\n", rd->space_id);
}

