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
 * knl_tran.h
 *    kernel transaction manager
 *
 * IDENTIFICATION
 *    src/storage/gstor/zekernel/kernel/xact/knl_tran.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef __KNL_TRAN_H__
#define __KNL_TRAN_H__

#include "knl_page.h"
#include "knl_session.h"
#include "knl_log.h"

#define TX_WAIT_INTERVEL      5  // in milliseconds
#define TXN_PER_PAGE          (uint32)((DEFAULT_PAGE_SIZE - PAGE_HEAD_SIZE - PAGE_TAIL_SIZE) / sizeof(txn_t))
#define TX_NEED_READ_WAIT(se) ((se)->wxid.value != GS_INVALID_ID64 && TX_XA_CONSISTENCY(se))
#define RECORD_SQL_SIZE  SIZE_K(16)

#pragma pack(4)
typedef struct st_itl {
    knl_scn_t scn;  // commit scn
    xid_t xid;      // txn id
    uint16 fsc;     // free space credit (bytes)

    uint16 is_active : 1;  // committed or not
    uint16 is_owscn : 1;   // txn scn overwrite or not
    uint16 is_copied : 1;  // itl is copied or not
    uint16 unused : 13;    // unused flags
} itl_t;

typedef struct st_pcr_itl {
    union {
        knl_scn_t scn;  // commit scn

        struct {
            uint32 ssn;      // txn ssn
            uint16 fsc;      // free space credit (bytes)
            uint16 aligned;  // aligned
        };
    };

    xid_t xid;                 // txn id
    undo_page_id_t undo_page;  // undo page for current transaction

    union {
        struct {
            uint16 undo_slot;  // undo slot
            uint16 flags;
        };
        struct {
            uint16 aligned1;
            uint16 is_active : 1;  // committed or not
            uint16 is_owscn : 1;   // txn scn overwrite or not
            uint16 is_copied : 1;  // itl is copied or not
            uint16 is_hist : 1;    // itl is historical or not (used in CR rollback)
            uint16 is_fast : 1;    // itl is fast committed or not
            uint16 unused : 11;
        };
    };
} pcr_itl_t;

// txn item in txn page
typedef struct st_txn {
    knl_scn_t scn;                // scn of the last commit
    undo_page_list_t undo_pages;  // undo page list held by current txn

    uint32 xnum;
    uint8 status;
    uint8 aligned[3];
} txn_t;

typedef struct st_txn_page {
    page_head_t head;
    txn_t items[1];
} txn_page_t;

typedef struct st_rd_tx_end {
    knl_scn_t scn;
    xmap_t xmap;
    uint8 is_auton;   // if is autonomous transaction
    uint8 is_commit;  // if is commit
    uint16 aligned;
} rd_tx_end_t;
#pragma pack()

typedef struct st_tx_item {
    spinlock_t lock;
    xmap_t xmap;
    uint32 prev;
    uint32 next;
    date_t systime;
    uint16 rmid;
    volatile uint8 in_progress;
} tx_item_t;

typedef struct st_txn_snapshot {
    knl_scn_t scn;
    uint32 xnum;
    uint16 rmid;
    uint8 status;
    uint8 in_progress;
} txn_snapshot_t;

/* transaction info structure */
typedef struct st_txn_info {
    knl_scn_t scn;
    bool8 is_owscn;
    uint8 status;
    uint8 unused[2];
} txn_info_t;

typedef struct st_tx_area {
    spinlock_t scn_lock;
    spinlock_t seri_lock;
    atomic_t rollback_num;     // txn rollback thread num
    bool32 is_xa_consistency;  // if use XA consistency read or write
    thread_t rollback_proc[GS_MAX_ROLLBACK_PROC];
} tx_area_t;

extern pcr_itl_t g_init_pcr_itl;

status_t tx_area_init(knl_session_t *session, uint32 lseg_no, uint32 rseg_no);
status_t tx_area_init_impl(knl_session_t *session, uint32 lseg_no, uint32 rseg_no, bool32 is_extend);
void tx_extend_deinit(knl_session_t *session);
void tx_area_release(knl_session_t *session);
void tx_area_release_impl(knl_session_t *session, uint32 lseg_no, uint32 rseg_no);
void tx_area_rollback(knl_session_t *session, thread_t *thread);
status_t tx_begin(knl_session_t *session);
status_t tx_wait(knl_session_t *session, uint32 timeout, wait_event_t event);
void tx_get_itl_info(knl_session_t *session, bool32 is_scan, itl_t *itl, txn_info_t *txn_info);
void tx_get_pcr_itl_info(knl_session_t *session, bool32 is_scan, pcr_itl_t *itl, txn_info_t *txn_info);

txn_t *txn_addr(knl_session_t *session, xmap_t xmap);
void txn_get_owner(knl_session_t *session, xmap_t xmap, undo_page_id_t *page_id);
void tx_get_snapshot(knl_session_t *session, xmap_t xmap, txn_snapshot_t *snapshot);

void tx_rollback_proc(thread_t *thread);
status_t tx_rollback_start(knl_session_t *session);
void tx_rollback_close(knl_session_t *session);
void tx_commit(knl_session_t *session, knl_scn_t xa_scn);
void tx_rollback(knl_session_t *session, knl_savepoint_t *savepoint);
knl_scn_t tx_inc_scn(knl_session_t *session, uint32 seg_id, txn_t *txn, knl_scn_t xa_scn);

status_t xa_recover(knl_session_t *session, tx_item_t *item, txn_t *txn, uint32 item_id);
tx_id_t tx_xmap_get_txid(knl_session_t *session, xmap_t xmap);
void tx_record_sql(knl_session_t *session);

#define TX_XA_CONSISTENCY(se) ((se)->kernel->tran_ctx.is_xa_consistency)

static inline void tx_rm_attach_trans(knl_rm_t *rm, tx_item_t *item, txn_t *txn, uint32 item_id)
{
    rm->tx_id.seg_id = item->xmap.seg_id;
    rm->tx_id.item_id = item_id;
    rm->txn = txn;
    rm->xid.xmap = item->xmap;
    rm->xid.xnum = txn->xnum;
    rm->undo_page_info.undo_rid.page_id = txn->undo_pages.last;
    rm->undo_page_info.undo_fs = 0;
    rm->undo_page_info.encrypt_enable = GS_FALSE;
    rm->undo_page_info.undo_log_encrypt = GS_FALSE;
    rm->noredo_undo_page_info.undo_rid.page_id = INVALID_UNDO_PAGID;
    rm->noredo_undo_page_info.undo_fs = 0;
    rm->noredo_undo_page_info.encrypt_enable = GS_FALSE;
    rm->noredo_undo_page_info.undo_log_encrypt = GS_FALSE;
}

static inline void tx_init_itl(knl_session_t *session, itl_t *itl, xid_t xid)
{
    itl->xid = xid;
    itl->fsc = 0;
    itl->is_active = 1;
    itl->is_owscn = 0;
    itl->is_copied = 0;
    itl->scn = 0;
}

static inline void tx_init_pcr_itl(knl_session_t *session, pcr_itl_t *itl,
                                   undo_rowid_t *undo_rid, xid_t xid, uint32 ssn)
{
    itl->xid = xid;
    itl->ssn = ssn;
    itl->fsc = 0;
    itl->is_owscn = 0;
    itl->is_active = 1;
    itl->is_copied = 0;
    itl->is_fast = 0;
    itl->undo_page = undo_rid->page_id;
    itl->undo_slot = undo_rid->slot;
}

static inline char *txn_status(xact_status_t status)
{
    switch (status) {
        case XACT_END:
            return (char*)"IN-ACTIVE";
        case XACT_BEGIN:
            return (char*)"ACTIVE";
        case XACT_PHASE1:
            return (char*)"PREPARED";
        case XACT_PHASE2:
            return (char*)"ROLLBACK PREPARED";
        default:
            return (char*)"INVALID";
    }
}

static inline void tx_record_rowid(rowid_t rid)
{
    int32 code = cm_get_error_code();
    if (code == ERR_LOCK_TIMEOUT) {
        GS_LOG_RUN_ERR("lock timeout while waiting for row %lld-%lld-%lld", rid.file, rid.page, rid.slot);
    }
}

status_t txn_dump_page(knl_session_t *session, page_head_t *page_head, cm_dump_t *dump);

#endif
