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
 * knl_interface.c
 * kernel interface manage
 *
 * IDENTIFICATION
 * src/storage/gstor/zekernel/kernel/knl_interface.c
 *
 * -------------------------------------------------------------------------
 */
#include "knl_interface.h"
#include "cm_hash.h"
#include "cm_file.h"
#include "cm_kmc.h"
#include "knl_lob.h"
#include "rcr_btree.h"
#include "rcr_btree_scan.h"
#include "pcr_btree.h"
#include "pcr_btree_scan.h"
#include "index_common.h"
#include "pcr_heap.h"
#include "knl_context.h"
#include "knl_sequence.h"
#include "knl_table.h"
#include "knl_user.h"
#include "knl_tenant.h"
#include "knl_log_file.h"
#include "temp_btree.h"
#ifdef _STATISTICS
#include "ostat_load.h"
#else
#include "rcr_btree_stat.h"
#include "pcr_btree_stat.h"
#endif
#ifdef DB_DEBUG_VERSION
#include "knl_syncpoint.h"
#endif /* DB_DEBUG_VERSION */
#include "knl_comment.h"
#include "knl_map.h"
#include "knl_tran.h"
#include "knl_database.h"
#include "knl_datafile.h"
#include "knl_external.h"
#include "knl_flashback.h"
#include "knl_db_alter.h"
#include "knl_ctrl_restore.h"
#include "dc_part.h"
#include "dc_dump.h"
#include "dc_util.h"
#ifdef _BACKUP
#include "bak_restore.h"
#endif

#ifdef __cplusplus
extern "C" {
#endif

// from knl_defs.h
 const page_id_t g_invalid_pagid = {
    .page = 0,
    .file = INVALID_FILE_ID,
    .aligned = 0
};
 const rowid_t g_invalid_rowid = {
    .file = INVALID_FILE_ID,
    .page = 0,
    .slot = 0,
    .unused2 = 0
};
 const rowid_t g_invalid_temp_rowid = {
    .vmid = GS_INVALID_ID32,
    .vm_slot = 0,
    .vm_tag = 0
};
 const undo_page_id_t g_invalid_undo_pagid = {
    .page = 0,
    .file = INVALID_FILE_ID
};
 const undo_rowid_t g_invalid_undo_rowid = {
    .page_id.file = INVALID_FILE_ID,
    .page_id.page = 0,
    .slot = GS_INVALID_ID16,
    .aligned = 0
};
// end from knl_defs.h

knl_callback_t g_knl_callback = { NULL, NULL, NULL, NULL, NULL };
#define HIGH_PRIO_ACT(act) ((act) == MAXIMIZE_STANDBY_DB || (act) == SWITCHOVER_STANDBY || (act) == FAILOVER_STANDBY)

init_cursor_t g_init_cursor = {
    .stmt = NULL,
    .temp_cache = NULL,
    .vm_page = NULL,
    .file = -1,
    .part_loc.part_no = 0,
    .part_loc.subpart_no = 0,
    .rowid_count = 0,
    .decode_count = GS_INVALID_ID16,
    .chain_count = 0,
    .index_slot = INVALID_INDEX_SLOT,
    .index_dsc = GS_FALSE,
    .index_only = GS_FALSE,
    .index_ffs = GS_FALSE,
    .index_prefetch_row = GS_FALSE,
    .index_ss = GS_FALSE,
    .skip_index_match = GS_FALSE,
    .set_default = GS_FALSE,
    .restrict_part = GS_FALSE,
    .restrict_subpart = GS_FALSE,
    .is_valid = GS_TRUE,
    .eof = GS_FALSE,
    .logging = GS_TRUE,
    .global_cached = GS_FALSE,
    .rowmark.value = 0,
    .is_splitting = GS_FALSE,
    .for_update_fetch = GS_FALSE,
};

knl_savepoint_t g_init_savepoint =
    { .urid = { .page_id.file = INVALID_FILE_ID, .page_id.page = 0, .slot = GS_INVALID_ID16, .aligned = 0 },
      .noredo_urid = { .page_id.file = INVALID_FILE_ID, .page_id.page = 0, .slot = GS_INVALID_ID16, .aligned = 0 },
      .lsn = GS_INVALID_ID64,
      .xid = GS_INVALID_ID64,

      .lob_items = { .count = 0, .first = NULL, .last = NULL },

      .key_lock.plocks = { .count = 0, .first = GS_INVALID_ID32, .last = GS_INVALID_ID32 },
      .key_lock.glocks = { .count = 0, .first = GS_INVALID_ID32, .last = GS_INVALID_ID32 },
      .key_lock.plock_id = GS_INVALID_ID32,

      .row_lock.plocks = { .count = 0, .first = GS_INVALID_ID32, .last = GS_INVALID_ID32 },
      .row_lock.glocks = { .count = 0, .first = GS_INVALID_ID32, .last = GS_INVALID_ID32 },
      .row_lock.plock_id = GS_INVALID_ID32,

      .sch_lock.plocks = { .count = 0, .first = GS_INVALID_ID32, .last = GS_INVALID_ID32 },
      .sch_lock.glocks = { .count = 0, .first = GS_INVALID_ID32, .last = GS_INVALID_ID32 },
      .sch_lock.plock_id = GS_INVALID_ID32,

      .alck_lock.plocks = { .count = 0, .first = GS_INVALID_ID32, .last = GS_INVALID_ID32 },
      .alck_lock.glocks = { .count = 0, .first = GS_INVALID_ID32, .last = GS_INVALID_ID32 },
      .alck_lock.plock_id = GS_INVALID_ID32 };

const wait_event_desc_t g_wait_event_desc[] = {
    { "idle wait", "", "Idle" },
    { "message from client", "", "Idle" },
    { "message to client", "", "Idle" },
    { "latch: large pool", "", "Concurrency" },
    { "latch: sql pool", "", "Concurrency" },
    { "latch: lock pool", "", "Concurrency" },
    { "latch: dictionary pool", "", "Concurrency" },
    { "latch: data buffer pool", "", "Concurrency" },
    { "latch: cache buffers chains", "", "Concurrency" },
    { "cursor: mutex", "", "Other" },
    { "library : mutex", "", "Other" },
    { "log file sync", "", "Commit" },
    { "buffer busy waits", "", "Concurrency" },
    { "enq: TX row lock contention", "", "Application" },
    { "enq: TX alloc itl entry", "", "Concurrency" },
    { "enq: TX index contention", "", "Application" },
    { "enq: TX table lock S", "", "Application" },
    { "enq: TX table lock X", "", "Application" },
    { "enq: TX read  wait", "", "Application" },
    { "db file scattered read", "", "User/IO" },
    { "db file sequential read", "", "User/IO" },
    { "db file gbp read", "", "User/IO" },
    { "mtrl segment sort", "", "User/IO" },
    { "log file switch(checkpoint incomplete)", "", "Configuration" },
    { "log file switch(archiving needed)", "", "Configuration" },
    { "read by other session", "", "Concurrency" },
    { "attached to agent", "", "Idle" },
    { "heap find map", "", "Concurrency" },
    { "heap extend segment", "", "Concurrency" },
    { "resmgr: io quantum", "", "User/IO" },
    { "direct path read temp", "", "User/IO" },
    { "direct path write temp", "", "User/IO" },
    { "advisory lock wait time", "", "Concurrency" },
    { "cn commit", "", "Commit" },
    { "cn execute request", "", "CN Execute" },
    { "cn execute ack", "", "CN Execute" },
    { "buf enter temp page with nolock", "", "Concurrency" },
    { "online redo log recycle", "", "Concurrency"},
    { "undo alloc page from space",             "", "Concurrency"},
    { "plsql object lock wait", "", "Concurrency"},
};

#ifdef _WIN32
__declspec(thread) void *tls_curr_sess = 0;
#else
__thread void *tls_curr_sess = 0;
#endif

void knl_set_curr_sess2tls(void *sess)
{
    tls_curr_sess = sess;
}

void *knl_get_curr_sess()
{
    return tls_curr_sess;
}

const wait_event_desc_t *knl_get_event_desc(const uint16 id)
{
    return &g_wait_event_desc[id];
}

status_t knl_ddl_latch_s(latch_t *latch, knl_handle_t session, latch_statis_t *stat)
{
    knl_session_t *se = (knl_session_t *)session;

    do {
        if (!cm_latch_timed_s(latch, 1, GS_FALSE, stat)) {
            if (se->canceled) {
                GS_THROW_ERROR(ERR_OPERATION_CANCELED);
                return GS_ERROR;
            }

            if (se->killed) {
                GS_THROW_ERROR(ERR_OPERATION_KILLED);
                return GS_ERROR;
            }
        } else {
            latch->sid = se->id;
            return GS_SUCCESS;
        }
    } while (1);
}

status_t knl_ddl_latch_x(latch_t *latch, knl_handle_t session, latch_statis_t *stat)
{
    knl_session_t *se = (knl_session_t *)session;

    do {
        if (!cm_latch_timed_x(latch, se->id, 1, stat)) {
            if (se->canceled) {
                GS_THROW_ERROR(ERR_OPERATION_CANCELED);
                return GS_ERROR;
            }

            if (se->killed) {
                GS_THROW_ERROR(ERR_OPERATION_KILLED);
                return GS_ERROR;
            }
        } else {
            return GS_SUCCESS;
        }
    } while (1);
}

status_t knl_match_cond(knl_handle_t session, knl_cursor_t *cursor, bool32 *matched)
{
    knl_match_cond_t match_cond = NULL;
    knl_session_t *se = (knl_session_t *)session;

    if (IS_INDEX_ONLY_SCAN(cursor)) {
        idx_decode_row(se, cursor, cursor->offsets, cursor->lens, &cursor->data_size);
        cursor->decode_cln_total = ((index_t *)cursor->index)->desc.column_count;
    } else {
        cm_decode_row_ex((char *)cursor->row, cursor->offsets, cursor->lens, cursor->decode_count, &cursor->data_size,
            &cursor->decode_cln_total);
    }

    match_cond = se->match_cond;

    if (cursor->stmt == NULL || match_cond == NULL) {
        *matched = GS_TRUE;
        return GS_SUCCESS;
    }

    return match_cond(cursor->stmt, matched);
}

/*
 * kernel interface for begin autonomous rm
 * @param handle pointer for kernel session
 * @note handle would be switch to autonomous rm after called.
 */
status_t knl_begin_auton_rm(knl_handle_t session)
{
    if (g_knl_callback.alloc_auton_rm(session) != GS_SUCCESS) {
        return GS_ERROR;
    }

    knl_set_session_scn(session, GS_INVALID_ID64);
    return GS_SUCCESS;
}

/*
 * kernel interface for end autonomous rm
 * @note end current transaction due to execution status
 * @note handle would be switch to parent rm after called.
 */
void knl_end_auton_rm(knl_handle_t handle, status_t status)
{
    knl_session_t *session = (knl_session_t *)(handle);

    if (status == GS_SUCCESS) {
        knl_commit(session);
    } else {
        knl_rollback(session, NULL);
    }

    (void)g_knl_callback.release_auton_rm(handle);
}

status_t knl_timestamp_to_scn(knl_handle_t session, timestamp_t tstamp, uint64 *scn)
{
    knl_session_t *se = (knl_session_t *)session;
    struct timeval time;
    time_t init_time;

    init_time = DB_INIT_TIME(se);
    cm_date2timeval(tstamp, &time);

    if (time.tv_sec < init_time) {
        GS_THROW_ERROR(ERR_TOO_OLD_SCN, "no snapshot found based on specified time");
        return GS_ERROR;
    }

    *scn = KNL_TIME_TO_SCN(&time, init_time);

    return GS_SUCCESS;
}

void knl_scn_to_timeval(knl_handle_t session, knl_scn_t scn, timeval_t *time_val)
{
    knl_session_t *se = (knl_session_t *)session;
    time_t init_time;

    init_time = DB_INIT_TIME(se);
    KNL_SCN_TO_TIME(scn, time_val, init_time);
}
#ifdef _REPLICATION
void knl_set_replica(knl_handle_t session, uint16 replica_port, bool32 is_start)
{
    knl_session_t *se = (knl_session_t *)session;
    arch_context_t *arch_ctx = &se->kernel->arch_ctx;
    lrcv_context_t *lrcv = &se->kernel->lrcv_ctx;
    se->kernel->attr.repl_port = replica_port;

    if (is_start) {
        cm_spin_lock(&arch_ctx->dest_lock, NULL);
        // set arch_dest_state_changed = GS_TRUE to trigger log sender init in srv loop
        arch_ctx->arch_dest_state_changed = GS_TRUE;
        while (arch_ctx->arch_dest_state_changed) {
            if (se->killed) {
                cm_spin_unlock(&arch_ctx->dest_lock);
                return;
            }
            cm_sleep(1);
        }
        cm_spin_unlock(&arch_ctx->dest_lock);
    }

    if (DB_IS_PRIMARY(&se->kernel->db)) {
        return;
    }

    if (lrcv->session == NULL) {
        return;
    }

    lrcv_close(se);
}
#endif
void knl_qos_begin(knl_handle_t session)
{
    knl_session_t *se = (knl_session_t *)session;
    uint32 threshold = se->kernel->attr.qos_threshold;
    uint32 sleep_time = se->kernel->attr.qos_sleep_time;
    uint32 random_range = se->kernel->attr.qos_random_range;
    uint32 total_wait_time = 0;
    uint32 sleep_in_us = 0;

    if (!se->kernel->attr.enable_qos) {
        return;
    }

    while (se->qos_mode != QOS_NOWAIT) {
        // running_sessions is smaller than uint32
        if ((uint32)se->kernel->running_sessions < threshold) {
            break;
        }

        if (total_wait_time > GS_MAX_QOS_WAITTIME_US) {
            break;
        }

        // wait time at once should be in ms level, and increase by exponential
        sleep_in_us = sleep_time * (MICROSECS_PER_MILLISEC + se->itl_id % random_range);
        cm_spin_sleep_ex(MICROSECS_PER_MILLISEC * sleep_in_us);
        total_wait_time = total_wait_time + sleep_in_us;
    }

    (void)cm_atomic32_inc(&se->kernel->running_sessions);
}

void knl_qos_end(knl_handle_t session)
{
    knl_session_t *se = (knl_session_t *)session;

    if (!se->kernel->attr.enable_qos) {
        return;
    }

    if (se->kernel->running_sessions > 0) {
        (void)cm_atomic32_dec(&se->kernel->running_sessions);
        if (se->kernel->running_sessions < 0) {
            se->kernel->running_sessions = 0;
        }
    }
    se->qos_mode = QOS_NORMAL;
    se->status = SESSION_ACTIVE;
}

void knl_set_repl_timeout(knl_handle_t handle, uint32 val)
{
    knl_session_t *session = (knl_session_t *)handle;
#ifdef _REPLICATION
    session->kernel->attr.repl_wait_timeout = val;

    session->kernel->lrcv_ctx.timeout = val;

    for (uint32 i = 0; i < GS_MAX_PHYSICAL_STANDBY; i++) {
        if (session->kernel->lsnd_ctx.lsnd[i] != NULL) {
            session->kernel->lsnd_ctx.lsnd[i]->timeout = val;
        }
    }
#endif
}

status_t knl_set_session_trans(knl_handle_t session, isolation_level_t level)
{
    knl_session_t *se = (knl_session_t *)session;

    if (DB_IS_READONLY(se)) {
        GS_THROW_ERROR(ERR_CAPABILITY_NOT_SUPPORT, "operation on read only mode");
        return GS_ERROR;
    }

    if (level < ISOLATION_READ_COMMITTED || level > ISOLATION_SERIALIZABLE) {
        GS_THROW_ERROR(ERR_INVALID_ISOLATION_LEVEL, level);
        return GS_ERROR;
    }

    if (se->rm->query_scn != GS_INVALID_ID64 || se->rm->txn != NULL) {
        GS_THROW_ERROR(ERR_TXN_IN_PROGRESS, "set transaction must be first statement of transaction");
        return GS_ERROR;
    }

    se->rm->isolevel = (uint8)level;
    se->rm->query_scn = DB_CURR_SCN(se);

    return GS_SUCCESS;
}

/*
 * set session query scn
 * Set the current session query scn as expected, if invalid
 * scn input, we set the query scn during to current isolation level.
 *
 * We use the query scn to judge the visibility of different transaction.
 * @param kernel session handle, expected query scn
 */
void knl_set_session_scn(knl_handle_t handle, uint64 scn)
{
    knl_session_t *session = (knl_session_t *)handle;
    knl_rm_t *rm = session->rm;

    if (SECUREC_UNLIKELY(scn != GS_INVALID_ID64)) {
        session->query_scn = scn;
    } else if (rm->isolevel != (uint8)ISOLATION_SERIALIZABLE) {
        session->query_scn = DB_CURR_SCN(session);
        if (rm->query_scn != GS_INVALID_ID64 &&
            session->kernel->attr.isolation_level == ISOLATION_CURR_COMMITTED) {
            session->query_scn = rm->query_scn;
        }
    } else {
        knl_panic(rm->query_scn != GS_INVALID_ID64);
        session->query_scn = rm->query_scn;
    }
}

/*
 * increase session SSN(sql sequence number)
 *
 * Increase current session SSN to separate different sql statement, so following
 * statement can see the results of previous statement in same transaction.
 *
 * @note Cursor would set its SSN to row/key to declare that it has changed it.
 * Different statement(include forked statement) has different SSN.

 * The statement can only modify the same row/key once during its lifetime, so
 * be careful if you got an error "row has been changed by current statement".
 * Cursor see the before version of row/key if it has changed it rather than
 * the after version. If you want to read the result just changed in transaction,
 * increase the SSN to indicate that it's a different statement.
 *
 * The SSN value of same row/key is strictly increasing.
 * @param kernel session handle
 */
void knl_inc_session_ssn(knl_handle_t handle)
{
    knl_session_t *session = (knl_session_t *)handle;

    session->ssn++;

    if (knl_xact_status(session) != XACT_END) {
        session->rm->ssn++;
    }
}

void knl_logic_log_put(knl_handle_t session, uint32 type, const void *data, uint32 size)
{
    knl_session_t *se = (knl_session_t *)session;
    logic_op_t logic_type = type + RD_SQL_LOG_BEGIN;

    log_put(se, RD_LOGIC_OPERATION, &logic_type, sizeof(logic_op_t), LOG_ENTRY_FLAG_NONE);
    log_append_data(se, data, size);
}

static inline void knl_init_session_stat(knl_session_t *session)
{
    session->stat.buffer_gets = 0;
    session->stat.disk_reads = 0;
    session->stat.disk_read_time = 0;
    session->stat.db_block_changes = 0;
    session->stat.con_wait_time = 0;
    session->stat.table_creates = 0;
    session->stat.table_drops = 0;
    session->stat.table_alters = 0;
    session->stat.hists_inserts = 0;
    session->stat.hists_updates = 0;
    session->stat.hists_deletes = 0;
    session->stat.table_part_drops = 0;
    session->stat.table_subpart_drops = 0;
    session->stat.spc_free_exts = 0;
    session->stat.spc_shrink_times = 0;
    session->stat.undo_free_pages = 0;
    session->stat.undo_shrink_times = 0;
    session->stat.auto_txn_alloc_times = 0;
    session->stat.auto_txn_page_waits = 0;
    session->stat.auto_txn_page_end_waits = 0;
    session->stat.txn_alloc_times = 0;
    session->stat.txn_page_waits = 0;
    session->stat.txn_page_end_waits = 0;
    session->stat.cr_pool_used = 0;

    if (session->kernel->attr.enable_table_stat) {
        session->stat_heap.enable = GS_TRUE;
        session->stat_btree.enable = GS_TRUE;
        session->stat_page.enable = GS_TRUE;
        session->stat_lob.enable = GS_TRUE;
        session->stat_interval.enable = GS_TRUE;
    } else {
        session->stat_heap.enable = GS_FALSE;
        session->stat_btree.enable = GS_FALSE;
        session->stat_page.enable = GS_FALSE;
        session->stat_lob.enable = GS_FALSE;
        session->stat_interval.enable = GS_FALSE;
    }
}

uint16 knl_get_rm_sid(knl_handle_t session, uint16 rmid)
{
    knl_session_t *knl_session = (knl_session_t *)session;
    knl_rm_t *rm = NULL;

    if (rmid == GS_INVALID_ID16) {
        return GS_INVALID_ID16;
    }

    rm = knl_session->kernel->rms[rmid];

    return (rm != NULL) ? rm->sid : GS_INVALID_ID16;
}

void knl_init_rm(knl_handle_t handle, uint16 rmid)
{
    knl_rm_t *rm = (knl_rm_t *)handle;

    rm->id = rmid;
    rm->uid = GS_INVALID_ID32;
    rm->txn = NULL;
    rm->svpt_count = 0;
    rm->tx_id.value = GS_INVALID_ID64;
    rm->large_page_id = GS_INVALID_ID32;
    rm->query_scn = GS_INVALID_ID64;
    rm->xid.value = GS_INVALID_ID64;
    rm->ssn = 0;
    rm->begin_lsn = GS_INVALID_ID64;
    cm_init_cond(&rm->cond);
    lock_init(rm);

    rm->temp_has_undo = GS_FALSE;
    rm->noredo_undo_pages.count = 0;
    rm->noredo_undo_pages.first = INVALID_UNDO_PAGID;
    rm->noredo_undo_pages.last = INVALID_UNDO_PAGID;

    rm->xa_flags = GS_INVALID_ID64;
    rm->xa_status = XA_INVALID;
    rm->xa_xid.fmt_id = GS_INVALID_ID64;
    rm->xa_xid.bqual_len = 0;
    rm->xa_xid.gtrid_len = 0;
    rm->xa_prev = GS_INVALID_ID16;
    rm->xa_next = GS_INVALID_ID16;
    rm->xa_rowid = INVALID_ROWID;
    rm->is_ddl_op = GS_FALSE;
}

void knl_set_session_rm(knl_handle_t handle, uint16 rmid)
{
    knl_session_t *session = (knl_session_t *)handle;
    knl_rm_t *rm = session->kernel->rms[rmid];

    rm->sid = session->id;
    rm->isolevel = session->kernel->attr.db_isolevel;
    rm->suspend_timeout = session->kernel->attr.xa_suspend_timeout;
    rm->txn_alarm_enable = GS_TRUE;

    session->rmid = rmid;
    session->rm = rm;
}

void knl_init_session(knl_handle_t kernel, uint32 sid, uint32 uid, char *plog_buf, cm_stack_t *stack)
{
    knl_instance_t *ctx = (knl_instance_t *)kernel;
    knl_session_t *session = ctx->sessions[sid];
    mtrl_context_t *temp_mtrl = session->temp_mtrl;
    knl_temp_cache_t *temp_table_cache = session->temp_table_cache;
    void *temp_dc_entries = session->temp_dc_entries;
    uint32 temp_table_capacity = session->temp_table_capacity;
    uint16 rmid = session->rmid;
    void *lnk_tab_entries = session->lnk_tab_entries;
    uint32 lnk_tab_capacity = session->lnk_tab_capacity;
    errno_t ret;

    ret = memset_sp(session, sizeof(knl_session_t), 0, sizeof(knl_session_t));
    knl_securec_check(ret);
    session->id = sid;
    session->serial_id = 1; /* ID 0 reserved for invalid ID */
    session->uid = uid;
    session->drop_uid = GS_INVALID_ID32;
    session->kernel = ctx;
    session->stack = stack;
    session->log_buf = plog_buf;
    session->wrmid = GS_INVALID_ID16;
    session->wpid = INVALID_PAGID;
    session->wtid.is_locking = GS_FALSE;
    session->wrid = g_invalid_rowid;
    session->wxid.value = GS_INVALID_ID64;
    session->curr_lfn = 0;
    session->ssn = 0;
    session->curr_lsn = DB_CURR_LSN(session);
    session->commit_batch = (bool8)ctx->attr.commit_batch;
    session->commit_nowait = (bool8)ctx->attr.commit_nowait;
    session->lock_wait_timeout = ctx->attr.lock_wait_timeout;
    session->autotrace = GS_FALSE;
    session->interactive_altpwd = GS_FALSE;
    knl_init_session_stat(session);
    ret = memset_sp(&session->datafiles, GS_MAX_DATA_FILES * sizeof(int32), 0xFF, GS_MAX_DATA_FILES * sizeof(int32));
    knl_securec_check(ret);
    session->temp_pool = &ctx->temp_pool[session->id % ctx->temp_ctx_count];
    session->temp_mtrl = temp_mtrl;
    session->temp_table_cache = temp_table_cache;
    session->temp_dc_entries = temp_dc_entries;
    session->temp_table_capacity = temp_table_capacity;
    session->temp_version = 0;
    session->lnk_tab_entries = lnk_tab_entries;
    session->lnk_tab_capacity = lnk_tab_capacity;
    temp_mtrl_init_context(session);
    session->index_root = NULL;
    KNL_SESSION_CLEAR_THREADID(session);
    cm_init_cond(&session->commit_cond);
    session->dist_ddl_id = NULL;
    session->has_migr = GS_FALSE;

    knl_set_session_rm(session, rmid);
    lock_init_group(&session->alck_lock_group);

    session->log_encrypt = GS_FALSE;
    session->atomic_op = GS_FALSE;
#ifdef LOG_DIAG
    session->log_diag = GS_FALSE;
    for (uint32 i = 0; i < KNL_MAX_ATOMIC_PAGES; i++) {
        session->log_diag_page[i] = (char *)malloc(ctx->attr.page_size);
        if (session->log_diag_page[i] == NULL) {
            GS_LOG_RUN_ERR("failed to malloc log_diag_page with size %u", ctx->attr.page_size);
            CM_ABORT(0, "ABORT INFO: failed to malloc log_diag_page");
        }
    }
#endif
}

void knl_reset_index_conflicts(knl_handle_t session)
{
    ((knl_session_t *)session)->rm->idx_conflicts = 0;
}

void knl_init_index_conflicts(knl_handle_t session, uint64 *conflicts)
{
    knl_session_t *se = (knl_session_t *)session;

    *conflicts = se->rm->idx_conflicts;
    se->rm->idx_conflicts = 0;
}

status_t knl_check_index_conflicts(knl_handle_t session, uint64 conflicts)
{
    knl_session_t *se = (knl_session_t *)session;

    if (se->rm->idx_conflicts == 0) {
        se->rm->idx_conflicts = conflicts;
        return GS_SUCCESS;
    }

    ((knl_session_t *)session)->rm->idx_conflicts = 0;
    GS_THROW_ERROR(ERR_DUPLICATE_KEY, "");
    return GS_ERROR;
}

void knl_destroy_session(knl_handle_t kernel, uint32 sid)
{
    knl_instance_t *ctx = (knl_instance_t *)kernel;
    knl_session_t *session = ctx->sessions[sid];

    if (session == NULL) {
        return;
    }

#ifdef LOG_DIAG
    for (uint32 i = 0; i < KNL_MAX_ATOMIC_PAGES; i++) {
        free(session->log_diag_page[i]);
        session->log_diag_page[i] = NULL;
    }
#endif
}

status_t knl_open_dc(knl_handle_t session, text_t *user, text_t *name, knl_dictionary_t *dc)
{
    if (dc_open((knl_session_t *)session, user, name, dc) != GS_SUCCESS) {
        return GS_ERROR;
    }
    return GS_SUCCESS;
}

/* this function won't open dc,it only get dc entry */
dc_entry_t *knl_get_dc_entry(knl_handle_t session, text_t *user, text_t *name, knl_dictionary_t *dc)
{
    return dc_get_entry_private((knl_session_t *)session, user, name, dc);
}

status_t knl_open_dc_with_public(knl_handle_t session, text_t *user, bool32 implicit_user, text_t *name,
    knl_dictionary_t *dc)
{
    knl_session_t *se = (knl_session_t *)session;
    bool32 is_found = GS_FALSE;
    text_t public_user = { PUBLIC_USER, (uint32)strlen(PUBLIC_USER) };

    if (GS_SUCCESS != knl_open_dc_if_exists(se, user, name, dc, &is_found)) {
        return GS_ERROR;
    }

    dc->syn_orig_uid = GS_INVALID_ID32;

    (void)knl_get_user_id(session, user, &dc->syn_orig_uid);
    /* find object in current user just return */
    if (is_found) {
        return GS_SUCCESS;
    }

    /* hit specify user or synonym exist but link object not exist scenario, return error */
    if (!implicit_user || se->kernel->db.status < DB_STATUS_OPEN) {
        GS_THROW_ERROR(ERR_TABLE_OR_VIEW_NOT_EXIST, T2S(user), T2S_EX(name));
        return GS_ERROR;
    }

    if (SYNONYM_EXIST(dc)) {
        GS_THROW_ERROR(ERR_OBJECT_INVALID, "SYNONYM", T2S(user), T2S_EX(name));
        return GS_ERROR;
    }

    if (GS_SUCCESS != knl_open_dc_if_exists(se, &public_user, name, dc, &is_found)) {
        return GS_ERROR;
    }

    if (!is_found) {
        GS_THROW_ERROR(ERR_TABLE_OR_VIEW_NOT_EXIST, T2S(user), T2S_EX(name));
        return GS_ERROR;
    }

    return GS_SUCCESS;
}

// there is not error information in this function
status_t knl_open_dc_with_public_ex(knl_handle_t session, text_t *user, bool32 implicit_user, text_t *name,
    knl_dictionary_t *dc)
{
    knl_session_t *se = (knl_session_t *)session;
    bool32 is_found = GS_FALSE;
    text_t public_user = { PUBLIC_USER, (uint32)strlen(PUBLIC_USER) };

    if (GS_SUCCESS != knl_open_dc_if_exists(se, user, name, dc, &is_found)) {
        cm_reset_error();
        return GS_ERROR;
    }

    dc->syn_orig_uid = GS_INVALID_ID32;

    (void)knl_get_user_id(session, user, &dc->syn_orig_uid);
    /* find object in current user just return */
    if (is_found) {
        return GS_SUCCESS;
    }

    cm_reset_error();
    /* hit specify user or synonym exist but link object not exist scenario, return error */
    if (!implicit_user || se->kernel->db.status < DB_STATUS_OPEN) {
        return GS_ERROR;
    }

    if (SYNONYM_EXIST(dc)) {
        return GS_ERROR;
    }

    if (GS_SUCCESS != knl_open_dc_if_exists(se, &public_user, name, dc, &is_found)) {
        cm_reset_error();
        return GS_ERROR;
    }

    if (!is_found) {
        return GS_ERROR;
    }

    return GS_SUCCESS;
}

status_t knl_open_seq_dc(knl_handle_t session, text_t *username, text_t *seqname, knl_dictionary_t *dc)
{
    return dc_seq_open((knl_session_t *)session, username, seqname, dc);
}

void knl_close_dc(knl_handle_t dc)
{
    knl_dictionary_t *pdc = (knl_dictionary_t *)dc;

    dc_close(pdc);
}

bool32 knl_is_table_csf(knl_handle_t dc_entity, uint32 part_no)
{
    dc_entity_t *entity = (dc_entity_t *)dc_entity;

    if (entity->type != DICT_TYPE_TABLE && entity->type != DICT_TYPE_TABLE_NOLOGGING) {
        return GS_FALSE;
    }

    if (knl_is_part_table(entity)) {
        if (knl_verify_interval_part(entity, part_no)) {
            return entity->table.desc.is_csf;
        }
        table_t *table = &entity->table;
        if (table->part_table->desc.partcnt==0){ // interval分区当还不存在分区时的表现
            return table->desc.is_csf;
        }
        table_part_t *table_part = TABLE_GET_PART(table, part_no);
        return table_part->desc.is_csf;
    }

    return entity->table.desc.is_csf;
}

uint32 knl_table_max_row_len(knl_handle_t dc_entity, uint32 max_col_size, knl_part_locate_t part_loc)
{
    dc_entity_t *entity = (dc_entity_t *)dc_entity;
    return heap_table_max_row_len(&entity->table, max_col_size, part_loc);
}

bool32 knl_is_part_table(knl_handle_t dc_entity)
{
    dc_entity_t *entity = (dc_entity_t *)dc_entity;

    if (entity->type != DICT_TYPE_TABLE && entity->type != DICT_TYPE_TABLE_NOLOGGING) {
        return GS_FALSE;
    } else {
        return entity->table.desc.parted;
    }
}

bool32 knl_is_compart_table(knl_handle_t dc_entity)
{
    dc_entity_t *entity = (dc_entity_t *)dc_entity;

    if (knl_is_part_table(dc_entity)) {
        return IS_COMPART_TABLE(&entity->table.part_table->desc);
    }

    return GS_FALSE;
}

part_type_t knl_part_table_type(knl_handle_t dc_entity)
{
    dc_entity_t *entity = (dc_entity_t *)dc_entity;
    table_t *table = &entity->table;

    if (!knl_is_part_table(dc_entity)) {
        return PART_TYPE_INVALID;
    }

    knl_panic_log(table->part_table != NULL, "the part_table is NULL, panic info: table %s", entity->table.desc.name);
    return table->part_table->desc.parttype;
}

part_type_t knl_subpart_table_type(knl_handle_t dc_entity)
{
    dc_entity_t *entity = (dc_entity_t *)dc_entity;
    table_t *table = &entity->table;

    if (!knl_is_part_table(dc_entity)) {
        return PART_TYPE_INVALID;
    }

    knl_panic_log(table->part_table != NULL, "the part_table is NULL, panic info: table %s", entity->table.desc.name);
    if (!knl_is_compart_table(dc_entity)) {
        return PART_TYPE_INVALID;
    }

    return table->part_table->desc.subparttype;
}

bool32 knl_table_nologging_enabled(knl_handle_t dc_entity)
{
    dc_entity_t *entity = (dc_entity_t *)dc_entity;
    table_t *table = &entity->table;

    if (table->desc.type != TABLE_TYPE_HEAP) {
        return GS_FALSE;
    }

    return table->desc.is_nologging;
}

bool32 knl_part_nologging_enabled(knl_handle_t dc_entity, knl_part_locate_t part_loc)
{
    dc_entity_t *entity = (dc_entity_t *)dc_entity;
    table_t *table = &entity->table;

    if (table->desc.type != TABLE_TYPE_HEAP) {
        return GS_FALSE;
    }

    table_part_t *compart = TABLE_GET_PART(table, part_loc.part_no);
    if (!IS_PARENT_TABPART(&compart->desc)) {
        return compart->desc.is_nologging;
    }

    knl_panic(part_loc.subpart_no != GS_INVALID_ID32);
    table_part_t *subpart = PART_GET_SUBENTITY(table->part_table, compart->subparts[part_loc.subpart_no]);

    return subpart->desc.is_nologging;
}

uint32 knl_part_count(knl_handle_t dc_entity)
{
    dc_entity_t *entity = (dc_entity_t *)dc_entity;
    part_table_t *part_table = entity->table.part_table;
    return part_table->desc.partcnt;
}

uint32 knl_subpart_count(knl_handle_t dc_entity, uint32 part_no)
{
    dc_entity_t *entity = (dc_entity_t *)dc_entity;
    part_table_t *part_table = entity->table.part_table;
    table_part_t *table_part = PART_GET_ENTITY(part_table, part_no);
    knl_panic_log(IS_PARENT_TABPART(&table_part->desc),
        "the table_part is not parent tabpart, panic info: "
        "table %s table_part %s",
        entity->table.desc.name, table_part->desc.name);
    return table_part->desc.subpart_cnt;
}

uint32 knl_total_subpart_count(knl_handle_t dc_entity)
{
    table_part_t *compart = NULL;
    table_part_t *subpart = NULL;
    dc_entity_t *entity = (dc_entity_t *)dc_entity;
    part_table_t *part_table = entity->table.part_table;

    knl_panic_log(IS_COMPART_TABLE(part_table), "the part_table is not compart table, panic info: table %s",
        entity->table.desc.name);
    uint32 total_subparts = 0;
    for (uint32 i = 0; i < part_table->desc.partcnt; i++) {
        compart = TABLE_GET_PART(&entity->table, i);
        if (!IS_READY_PART(compart)) {
            continue;
        }

        knl_panic_log(IS_PARENT_TABPART(&compart->desc),
            "the compart is not parent tabpart, panic info: "
            "table %s compart %s",
            entity->table.desc.name, compart->desc.name);
        for (uint32 j = 0; j < compart->desc.subpart_cnt; j++) {
            subpart = PART_GET_SUBENTITY(part_table, compart->subparts[j]);
            if (subpart == NULL) {
                continue;
            }

            total_subparts++;
        }
    }

    return total_subparts;
}

uint32 knl_real_part_count(knl_handle_t dc_entity)
{
    dc_entity_t *entity = (dc_entity_t *)dc_entity;
    part_table_t *part_table = entity->table.part_table;
    return PART_CONTAIN_INTERVAL(part_table) ? part_table->desc.real_partcnt : part_table->desc.partcnt;
}

uint16 knl_part_key_count(knl_handle_t dc_entity)
{
    dc_entity_t *entity = (dc_entity_t *)dc_entity;
    part_table_t *part_table = entity->table.part_table;
    return (uint16)part_table->desc.partkeys;
}

uint16 knl_part_key_column_id(knl_handle_t dc_entity, uint16 id)
{
    dc_entity_t *entity = (dc_entity_t *)dc_entity;
    part_table_t *part_table = entity->table.part_table;
    return part_table->keycols[id].column_id;
}

void knl_set_table_part(knl_cursor_t *cursor, knl_part_locate_t part_loc)
{
    cursor->part_loc = part_loc;
    cursor->table_part = TABLE_GET_PART(cursor->table, part_loc.part_no);
    if (IS_PARENT_TABPART(&((table_part_t *)(cursor->table_part))->desc)) {
        table_t *table = (table_t *)cursor->table;
        table_part_t *table_part = (table_part_t *)cursor->table_part;
        cursor->table_part = PART_GET_SUBENTITY(table->part_table, table_part->subparts[part_loc.subpart_no]);
    }
}

status_t knl_find_table_part_by_name(knl_handle_t dc_entity, text_t *name, uint32 *part_no)
{
    dc_entity_t *entity;
    part_table_t *part_table;
    table_part_t *table_part = NULL;

    entity = (dc_entity_t *)dc_entity;
    part_table = entity->table.part_table;

    if (!part_table_find_by_name(part_table, name, &table_part)) {
        GS_THROW_ERROR(ERR_PARTITION_NOT_EXIST, "table", T2S(name));
        return GS_ERROR;
    }

    *part_no = table_part->part_no;
    return GS_SUCCESS;
}

/*
 * fetch dynamic view interface
 * use the dynamic view registered fetch function to get a virtual cursor row.
 * @param kernel session, kernel cursor
 */
static status_t knl_fetch_dynamic_view(knl_handle_t session, knl_cursor_t *cursor)
{
    dc_entity_t *entity = (dc_entity_t *)cursor->dc_entity;

    for (;;) {
        if (entity->dview->fetch(session, cursor) != GS_SUCCESS) {
            return GS_ERROR;
        }

        if (cursor->eof) {
            return GS_SUCCESS;
        }

        if (knl_match_cond(session, cursor, &cursor->is_found) != GS_SUCCESS) {
            return GS_ERROR;
        }

        if (cursor->is_found) {
            return GS_SUCCESS;
        }
    }
}

status_t knl_get_index_par_schedule(knl_handle_t handle, knl_dictionary_t *dc, knl_idx_paral_info_t paral_info,
    knl_index_paral_range_t *sub_ranges)
{
    knl_session_t *session = (knl_session_t *)handle;
    dc_entity_t *entity = DC_ENTITY(dc);
    index_t *index = NULL;
    index_part_t *index_part = NULL;
    btree_t *btree = NULL;
    knl_scn_t org_scn;
    errno_t err;

    if (knl_check_dc(session, dc) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (dc->type != DICT_TYPE_TABLE && dc->type != DICT_TYPE_TABLE_NOLOGGING) {
        GS_THROW_ERROR(ERR_OPERATIONS_NOT_SUPPORT, "parallel scan", "temp table");
        return GS_ERROR;
    }

    knl_panic_log(paral_info.index_slot < entity->table.index_set.total_count,
        "the index_slot is not smaller than "
        "index_set's total count, panic info: table %s index_slot %u index_set's total_count %u",
        entity->table.desc.name, paral_info.index_slot, entity->table.index_set.total_count);
    index = entity->table.index_set.items[paral_info.index_slot];

    if (IS_PART_INDEX(index)) {
        knl_panic_log(paral_info.part_loc.part_no < index->part_index->desc.partcnt,
            "the part_no is not smaller than part count, panic info: table %s index %s part_no %u part count %u",
            entity->table.desc.name, index->desc.name, paral_info.part_loc.part_no, index->part_index->desc.partcnt);
        index_part = INDEX_GET_PART(index, paral_info.part_loc.part_no);
        if (index_part == NULL) {
            sub_ranges->workers = 0;
            return GS_SUCCESS;
        }

        if (IS_PARENT_IDXPART(&index_part->desc)) {
            uint32 subpart_no = paral_info.part_loc.subpart_no;
            index_part_t *subpart = PART_GET_SUBENTITY(index->part_index, index_part->subparts[subpart_no]);
            btree = &subpart->btree;
            org_scn = subpart->desc.org_scn;
        } else {
            btree = &index_part->btree;
            org_scn = index_part->desc.org_scn;
        }

        if (btree->segment == NULL && !IS_INVALID_PAGID(btree->entry)) {
            if (dc_load_index_part_segment(session, entity, index_part) != GS_SUCCESS) {
                return GS_ERROR;
            }
        }
    } else {
        btree = &index->btree;
        org_scn = index->desc.org_scn;
    }

    if (btree->segment == NULL) {
        sub_ranges->workers = 0;
        return GS_SUCCESS;
    }

    if (paral_info.is_index_full) {
        for (uint32 i = 0; i < index->desc.column_count; i++) {
            knl_set_key_flag(&paral_info.org_range->l_key, SCAN_KEY_LEFT_INFINITE, i);
            knl_set_key_flag(&paral_info.org_range->r_key, SCAN_KEY_RIGHT_INFINITE, i);
        }
    }

    if (idx_get_paral_schedule(session, btree, org_scn, paral_info, sub_ranges) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (sub_ranges->workers == 1) {
        err = memcpy_sp(sub_ranges->index_range[0], sizeof(knl_scan_range_t), paral_info.org_range,
            sizeof(knl_scan_range_t));
        knl_securec_check(err);
    }
    return GS_SUCCESS;
}

void knl_set_index_scan_range(knl_cursor_t *cursor, knl_scan_range_t *sub_range)
{
    errno_t err;

    err = memcpy_sp(cursor->scan_range.l_buf, GS_KEY_BUF_SIZE, sub_range->l_buf, GS_KEY_BUF_SIZE);
    knl_securec_check(err);
    cursor->scan_range.l_key = sub_range->l_key;
    cursor->scan_range.l_key.buf = cursor->scan_range.l_buf;
    err = memcpy_sp(cursor->scan_range.r_buf, GS_KEY_BUF_SIZE, sub_range->r_buf, GS_KEY_BUF_SIZE);
    knl_securec_check(err);

    cursor->scan_range.r_key = sub_range->r_key;
    cursor->scan_range.r_key.buf = cursor->scan_range.r_buf;
    cursor->scan_range.is_equal = sub_range->is_equal;
}

void knl_set_table_scan_range(knl_handle_t handle, knl_cursor_t *cursor, page_id_t left, page_id_t right)
{
    knl_session_t *session = (knl_session_t *)handle;

    if (!spc_validate_page_id(session, left)) {
        cursor->scan_range.l_page = INVALID_PAGID;
    } else {
        cursor->scan_range.l_page = left;
    }

    if (!spc_validate_page_id(session, right)) {
        cursor->scan_range.r_page = INVALID_PAGID;
    } else {
        cursor->scan_range.r_page = right;
    }

    SET_ROWID_PAGE(&cursor->rowid, cursor->scan_range.l_page);
    cursor->rowid.slot = INVALID_SLOT;
}

void knl_init_table_scan(knl_handle_t handle, knl_cursor_t *cursor)
{
    heap_segment_t *segment;
    knl_session_t *session = (knl_session_t *)handle;

    segment = (heap_segment_t *)(CURSOR_HEAP(cursor)->segment);

    if (segment == NULL) {
        cursor->scan_range.l_page = INVALID_PAGID;
        cursor->scan_range.r_page = INVALID_PAGID;
    } else {
        cursor->scan_range.l_page = segment->data_first;
        if (!spc_validate_page_id(session, cursor->scan_range.l_page)) {
            cursor->scan_range.l_page = INVALID_PAGID;
        }

        cursor->scan_range.r_page = segment->data_last;
        if (!spc_validate_page_id(session, cursor->scan_range.r_page)) {
            cursor->scan_range.r_page = INVALID_PAGID;
        }
    }

    SET_ROWID_PAGE(&cursor->rowid, cursor->scan_range.l_page);
    cursor->rowid.slot = INVALID_SLOT;
}

static status_t dc_load_part_segments(knl_session_t *session, knl_cursor_t *cursor, knl_dictionary_t *dc)
{
    table_part_t *table_part = NULL;
    table_t *table = (table_t *)cursor->table;
    table_part = (table_part_t *)TABLE_GET_PART(table, cursor->part_loc.part_no);
    if (!IS_READY_PART(table_part)) {
        return GS_SUCCESS;
    }

    if (IS_PARENT_TABPART(&table_part->desc)) {
        knl_panic_log(cursor->part_loc.subpart_no != GS_INVALID_ID32,
            "the subpart_no record on cursor is invalid, "
            "panic info: table %s table_part %s",
            table->desc.name, table_part->desc.name);
        table_part = PART_GET_SUBENTITY(table->part_table, table_part->subparts[cursor->part_loc.subpart_no]);
        if (table_part == NULL) {
            return GS_SUCCESS;
        }
    }

    if (table_part->heap.loaded) {
        return GS_SUCCESS;
    }

    if (dc_load_table_part_segment(session, cursor->dc_entity, table_part) != GS_SUCCESS) {
        return GS_ERROR;
    }

    return GS_SUCCESS;
}

/*
 * open table cursor interface
 * Open a normal table cursor in request scan mode, register the fetch method
 * @param kernel session, kernel cursor, kernel dictionary
 */
static status_t knl_open_table_cursor(knl_session_t *session, knl_cursor_t *cursor, knl_dictionary_t *dc)
{
    cursor->table_part = NULL;
    cursor->index_part = NULL;
    cursor->chain_count = 0;
    cursor->cleanout = GS_FALSE;
    cursor->is_locked = GS_FALSE;
    cursor->ssi_conflict = GS_FALSE;
    cursor->ssn = session->rm->ssn;

    table_t *table = (table_t *)(cursor->table);
    if (IS_PART_TABLE(table)) {
        if (dc_load_part_segments(session, cursor, dc) != GS_SUCCESS) {
            return GS_ERROR;
        }
    }

    if (cursor->action == CURSOR_ACTION_INSERT) {
        cursor->rowid_count = 0;
        cursor->rowid_no = 0;
        cursor->row_offset = 0;
        cursor->index = NULL;

        if (IS_PART_TABLE(table)) {
            knl_panic_log(cursor->part_loc.part_no != GS_INVALID_ID32,
                          "the part_no record on cursor is invalid, panic info: table %s", table->desc.name);
            cursor->table_part = TABLE_GET_PART(table, cursor->part_loc.part_no);
            if (!IS_READY_PART(cursor->table_part)) {
                cursor->eof = GS_TRUE;
                return GS_SUCCESS;
            }

            if (IS_PARENT_TABPART(&((table_part_t *)(cursor->table_part))->desc)) {
                uint32 subpart_no = cursor->part_loc.subpart_no;
                knl_panic_log(subpart_no != GS_INVALID_ID32,
                              "the subpart_no record on cursor is invalid, panic info: table %s", table->desc.name);
                table_part_t *table_part = (table_part_t *)cursor->table_part;
                cursor->table_part = PART_GET_SUBENTITY(table->part_table, table_part->subparts[subpart_no]);
                if (cursor->table_part == NULL) {
                    cursor->eof = GS_TRUE;
                    return GS_SUCCESS;
                }
            }
        }
        return GS_SUCCESS;
    }

    if (cursor->scan_mode == SCAN_MODE_INDEX) {
        index_t *index = DC_INDEX(dc, cursor->index_slot);

        cursor->index = index;
        cursor->fetch = index->acsor->do_fetch;
        cursor->key_loc.is_initialized = GS_FALSE;

        if (IS_PART_INDEX(cursor->index)) {
            knl_panic_log(cursor->part_loc.part_no != GS_INVALID_ID32,
                "the part_no record on cursor is invalid, "
                "panic info: table %s index %s",
                table->desc.name, index->desc.name);
            cursor->table_part = TABLE_GET_PART(cursor->table, cursor->part_loc.part_no);
            cursor->index_part = INDEX_GET_PART(cursor->index, cursor->part_loc.part_no);
            if (!IS_READY_PART(cursor->table_part)) {
                cursor->eof = GS_TRUE;
                return GS_SUCCESS;
            }

            if (IS_PARENT_IDXPART(&((index_part_t *)(cursor->index_part))->desc)) {
                knl_panic_log(cursor->part_loc.subpart_no != GS_INVALID_ID32,
                    "the subpart_no record on cursor is "
                    "invalid, panic info: table %s index %s",
                    table->desc.name, index->desc.name);
                index_part_t *index_part = (index_part_t *)cursor->index_part;
                table_part_t *table_part = (table_part_t *)cursor->table_part;
                uint32 subpart_no = cursor->part_loc.subpart_no;
                cursor->index_part = PART_GET_SUBENTITY(index->part_index, index_part->subparts[subpart_no]);
                cursor->table_part = PART_GET_SUBENTITY(table->part_table, table_part->subparts[subpart_no]);
                if (cursor->index_part == NULL) {
                    cursor->eof = GS_TRUE;
                    return GS_SUCCESS;
                }
            }
        }
    } else if (cursor->scan_mode == SCAN_MODE_TABLE_FULL) {
        cursor->fetch = TABLE_ACCESSOR(cursor)->do_fetch;

        if (IS_PART_TABLE(table)) {
            knl_panic_log(cursor->part_loc.part_no != GS_INVALID_ID32,
                "the part_no record on cursor is invalid, panic info: table %s", table->desc.name);
            cursor->table_part = TABLE_GET_PART(table, cursor->part_loc.part_no);
            if (!IS_READY_PART(cursor->table_part)) {
                cursor->eof = GS_TRUE;
                return GS_SUCCESS;
            }

            if (IS_PARENT_TABPART(&((table_part_t *)(cursor->table_part))->desc)) {
                uint32 subpart_no = cursor->part_loc.subpart_no;
                knl_panic_log(subpart_no != GS_INVALID_ID32,
                    "the subpart_no record on cursor is invalid, panic info: table %s", table->desc.name);
                table_part_t *table_part = (table_part_t *)cursor->table_part;
                cursor->table_part = PART_GET_SUBENTITY(table->part_table, table_part->subparts[subpart_no]);
                if (cursor->table_part == NULL) {
                    cursor->eof = GS_TRUE;
                    return GS_SUCCESS;
                }
            }
        }

        knl_init_table_scan(session, cursor);

        cursor->index = NULL;
    } else if (cursor->scan_mode == SCAN_MODE_ROWID) {
        cursor->fetch = TABLE_ACCESSOR(cursor)->do_rowid_fetch;
        cursor->index = NULL;
        cursor->rowid_no = 0;
    }

    return GS_SUCCESS;
}

void knl_init_cursor_buf(knl_handle_t handle, knl_cursor_t *cursor)
{
    knl_session_t *session = (knl_session_t *)handle;
    char *ext_buf;
    uint32 ext_size;

    /* 2 pages, one is for cursor->row, one is for cursor->page_buf */
    ext_buf = cursor->buf + 2 * DEFAULT_PAGE_SIZE;
    ext_size = session->kernel->attr.max_column_count * sizeof(uint16);

    cursor->offsets = (uint16 *)ext_buf;
    cursor->lens = (uint16 *)(ext_buf + ext_size);
    cursor->update_info = session->trig_ui == NULL ? session->update_info : *session->trig_ui;
    cursor->update_info.count = 0;
    cursor->update_info.data = NULL;

    cursor->insert_info.data = NULL;
    cursor->insert_info.lens = NULL;
    cursor->insert_info.offsets = NULL;
}

/*
 * create kernel cursor and initialize it
 * when call this interface, need to add CM_SAVE_STACK and CM_RESTORE_STACK around it
 */
knl_cursor_t *knl_push_cursor(knl_handle_t handle)
{
    knl_session_t *session = (knl_session_t *)handle;
    char *ext_buf = NULL;
    uint32 ext_size;

    knl_cursor_t *cursor = (knl_cursor_t *)cm_push(session->stack, session->kernel->attr.cursor_size);

    knl_panic_log(cursor != NULL, "cursor is NULL.");

    /* 2 pages, one is for cursor->row, one is for cursor->page_buf */
    ext_buf = cursor->buf + 2 * DEFAULT_PAGE_SIZE;
    ext_size = session->kernel->attr.max_column_count * sizeof(uint16);
    cursor->offsets = (uint16 *)ext_buf;
    cursor->lens = (uint16 *)(ext_buf + ext_size);

    cursor->update_info.columns = (uint16 *)cm_push(session->stack, ext_size);
    knl_panic_log(cursor->update_info.columns != NULL,
        "update_info's columns record on cursor is NULL, panic info: "
        "stack's size %u heap_offset %u push_offset %u table %s",
        session->stack->size, session->stack->heap_offset, session->stack->push_offset,
        ((table_t *)cursor->table)->desc.name);

    cursor->update_info.offsets = (uint16 *)cm_push(session->stack, ext_size);
    knl_panic_log(cursor->update_info.offsets != NULL,
        "update_info's offsets record on cursor is NULL, panic info: "
        "stack's size %u heap_offset %u push_offset %u table %s",
        session->stack->size, session->stack->heap_offset, session->stack->push_offset,
        ((table_t *)cursor->table)->desc.name);

    cursor->update_info.lens = (uint16 *)cm_push(session->stack, ext_size);
    knl_panic_log(cursor->update_info.lens != NULL,
        "update_info's lens record on cursor is NULL, panic info: "
        "stack's size %u heap_offset %u push_offset %u table %s",
        session->stack->size, session->stack->heap_offset, session->stack->push_offset,
        ((table_t *)cursor->table)->desc.name);

    KNL_INIT_CURSOR(cursor);

    return cursor;
}

/*
 * pop the stack when used by kernel cursor
 * only used by sql_update_depender_status interface
 */
void knl_pop_cursor(knl_handle_t handle)
{
    knl_session_t *session = (knl_session_t *)handle;

    cm_pop(session->stack); // pop cursor->update_info.lens
    cm_pop(session->stack); // pop cursor->update_info.offsets
    cm_pop(session->stack); // cursor->update_info.columns
    cm_pop(session->stack); // pop cursor
}

/*
 * create kernel cursor and initialize it., only used by sharing layer and sql layer
 * when call this interface, need to add SQL_SAVE_STACK and SQL_RESTORE_STACK around it
 * @param kernel handle, knl_cursor_t ** cursor
 */
status_t sql_push_knl_cursor(knl_handle_t handle, knl_cursor_t **cursor)
{
    knl_session_t *session = (knl_session_t *)handle;
    char *ext_buf = NULL;
    uint32 ext_size;

    *cursor = (knl_cursor_t *)cm_push(session->stack, session->kernel->attr.cursor_size);

    if (*cursor == NULL) {
        GS_THROW_ERROR(ERR_STACK_OVERFLOW);
        return GS_ERROR;
    }

    /* 2 pages, one is for cursor->row, one is for cursor->page_buf */
    ext_buf = (*cursor)->buf + 2 * session->kernel->attr.page_size;
    ext_size = session->kernel->attr.max_column_count * sizeof(uint16);
    (*cursor)->offsets = (uint16 *)ext_buf;
    (*cursor)->lens = (uint16 *)(ext_buf + ext_size);

    (*cursor)->update_info.columns = (uint16 *)cm_push(session->stack, ext_size);
    if ((*cursor)->update_info.columns == NULL) {
        GS_THROW_ERROR(ERR_STACK_OVERFLOW);
        return GS_ERROR;
    }

    (*cursor)->update_info.offsets = (uint16 *)cm_push(session->stack, ext_size);
    if ((*cursor)->update_info.offsets == NULL) {
        GS_THROW_ERROR(ERR_STACK_OVERFLOW);
        return GS_ERROR;
    }

    (*cursor)->update_info.lens = (uint16 *)cm_push(session->stack, ext_size);
    if ((*cursor)->update_info.lens == NULL) {
        GS_THROW_ERROR(ERR_STACK_OVERFLOW);
        return GS_ERROR;
    }

    KNL_INIT_CURSOR((*cursor));

    return GS_SUCCESS;
}

void knl_open_sys_cursor(knl_session_t *session, knl_cursor_t *cursor, knl_cursor_action_t action, uint32 table_id,
    uint32 index_slot)
{
    knl_rm_t *rm = session->rm;
    heap_segment_t *segment = NULL;
    index_t *index = NULL;
    knl_dictionary_t dc;

    knl_inc_session_ssn(session);

    db_get_sys_dc(session, table_id, &dc);
    if (DB_IS_UPGRADE(session) && dc.handle == NULL) {
        CM_ABORT(0, "[UPGRADE] ABORT INFO: System table %u is not available during upgrade processs", table_id);
    }

    cursor->table = DC_TABLE(&dc);
    cursor->dc_entity = dc.handle;
    cursor->dc_type = dc.type;
    cursor->action = action;
    cursor->row = (row_head_t *)cursor->buf;
    cursor->page_buf = cursor->buf + DEFAULT_PAGE_SIZE;
    cursor->update_info.data = session->update_info.data;
    cursor->isolevel = ISOLATION_READ_COMMITTED;
    if (DB_IS_PRIMARY(&session->kernel->db)) {
        cursor->query_scn = DB_CURR_SCN(session);
    } else {
        cursor->query_scn = session->query_scn;
    }
    cursor->query_lsn = DB_CURR_LSN(session);
    cursor->xid = rm->xid.value;
    cursor->ssn = rm->ssn;
    cursor->is_locked = GS_FALSE;
    cursor->cleanout = GS_FALSE;
    cursor->eof = GS_FALSE;
    cursor->is_valid = GS_TRUE;
    cursor->stmt = NULL;
    cursor->restrict_part = GS_FALSE;
    cursor->restrict_subpart = GS_FALSE;
    cursor->decode_count = GS_INVALID_ID16;
    cursor->is_xfirst = GS_FALSE;
    cursor->disable_pk_update = GS_TRUE;

    if (index_slot != GS_INVALID_ID32) {
        index = DC_INDEX(&dc, index_slot);
        cursor->index = index;

        cursor->index_slot = index_slot;
        cursor->fetch = index->acsor->do_fetch;
        cursor->scan_mode = SCAN_MODE_INDEX;
        cursor->index_dsc = GS_FALSE;
        cursor->index_only = GS_FALSE;
        cursor->key_loc.is_initialized = GS_FALSE;
    } else {
        cursor->fetch = TABLE_ACCESSOR(cursor)->do_fetch;
        cursor->scan_mode = SCAN_MODE_TABLE_FULL;
        cursor->index = NULL;

        segment = (heap_segment_t *)CURSOR_HEAP(cursor)->segment;
        if (segment == NULL) {
            cursor->scan_range.l_page = INVALID_PAGID;
        } else {
            cursor->scan_range.l_page = segment->data_first;
            if (!spc_validate_page_id(session, cursor->scan_range.l_page)) {
                cursor->scan_range.l_page = INVALID_PAGID;
            }
        }
        cursor->scan_range.r_page = INVALID_PAGID;
        SET_ROWID_PAGE(&cursor->rowid, cursor->scan_range.l_page);
        cursor->rowid.slot = INVALID_SLOT;
    }
}

static inline void knl_update_cursor_isolevel(knl_session_t *session, knl_cursor_t *cursor)
{
    table_t *table = (table_t *)cursor->table;
    if (table->ashrink_stat == ASHRINK_COMPACT && cursor->isolevel == ISOLATION_CURR_COMMITTED &&
        !session->compacting) {
        cursor->isolevel = ISOLATION_READ_COMMITTED;
    }
}

static status_t knl_check_nologging_attr(table_t *table, knl_cursor_t *cursor)
{
    if (!IS_PART_TABLE(table)) {
        if (SECUREC_UNLIKELY(table->desc.is_nologging)) {
            GS_THROW_ERROR(ERR_OPERATIONS_NOT_ALLOW, "update, delete on the table with nologging insert attribute");
            return GS_ERROR;
        }
    } else {
        table_part_t *table_part = PART_GET_ENTITY(table->part_table, cursor->part_loc.part_no);
        if (!IS_READY_PART(table_part)) {
            return GS_SUCCESS;
        }

        if (SECUREC_UNLIKELY(table_part->desc.is_nologging)) {
            GS_THROW_ERROR(ERR_OPERATIONS_NOT_ALLOW, "update, delete on the part with nologging insert attribute");
            return GS_ERROR;
        }

        if (!IS_PARENT_TABPART(&table_part->desc)) {
            return GS_SUCCESS;
        }

        uint32 subpart_no = cursor->part_loc.subpart_no;
        if (subpart_no != GS_INVALID_ID32) {
            table_part_t *subpart = PART_GET_SUBENTITY(table->part_table, table_part->subparts[subpart_no]);
            if (SECUREC_UNLIKELY(subpart->desc.is_nologging)) {
                GS_THROW_ERROR(ERR_OPERATIONS_NOT_ALLOW, "update, delete on the part with nologging insert attribute");
                return GS_ERROR;
            }
        }
    }

    return GS_SUCCESS;
}

/*
 * open kernel cursor interface
 * @note if query wants to set query scn and ssn, please set after open cursor
 * @param kernel session handle, kernel cursor, kernel dictionary
 */
status_t knl_open_cursor(knl_handle_t handle, knl_cursor_t *cursor, knl_dictionary_t *dc)
{
    knl_session_t *session = (knl_session_t *)handle;
    knl_rm_t *rm = session->rm;
    dc_entity_t *entity = DC_ENTITY(dc);

    if (DB_IS_READONLY(session) && cursor->action > CURSOR_ACTION_SELECT) {
        GS_THROW_ERROR(ERR_DATABASE_ROLE, "operation", "in readonly mode");
        return GS_ERROR;
    }

    if (cursor->action != CURSOR_ACTION_SELECT && !cursor->skip_lock) {
        if (cursor->action != CURSOR_ACTION_INSERT) {
            if (knl_check_nologging_attr(&entity->table, cursor) != GS_SUCCESS) {
                return GS_ERROR;
            }
        }

        /* in case of select for update, wait time depend on input */
        if (cursor->action == CURSOR_ACTION_UPDATE && cursor->rowmark.type != ROWMARK_WAIT_BLOCK) {
            if (lock_table_shared(session, entity, cursor->rowmark.wait_seconds) != GS_SUCCESS) {
                return GS_ERROR;
            }
        } else {
            if (lock_table_shared(session, entity, LOCK_INF_WAIT) != GS_SUCCESS) {
                return GS_ERROR;
            }
        }
    }

    cursor->dc_type = dc->type;
    cursor->dc_entity = entity;
    cursor->table = &entity->table;
    cursor->page_buf = cursor->buf + DEFAULT_PAGE_SIZE;
    cursor->xid = rm->xid.value;
    cursor->isolevel = rm->isolevel;
    cursor->query_scn = session->query_scn;
    cursor->query_lsn = session->curr_lsn;
    cursor->cc_cache_time = KNL_NOW(session);
    cursor->eof = GS_FALSE;
    cursor->is_valid = GS_TRUE;
    cursor->row = (row_head_t *)cursor->buf;
    cursor->chain_info = cursor->buf;
    if ( cursor->update_info.data == NULL ){
        cursor->update_info.data = session->update_info.data;
    }
    cursor->disable_pk_update = GS_FALSE;

    knl_update_cursor_isolevel(session, cursor);

    switch (dc->type) {
        case DICT_TYPE_DYNAMIC_VIEW:
        case DICT_TYPE_GLOBAL_DYNAMIC_VIEW:
            cursor->fetch = knl_fetch_dynamic_view;
            return entity->dview->dopen(session, cursor);

        case DICT_TYPE_TEMP_TABLE_SESSION:
        case DICT_TYPE_TEMP_TABLE_TRANS:
            return knl_open_temp_cursor(session, cursor, dc);

        case DICT_TYPE_TABLE_EXTERNAL:
            return knl_open_external_cursor(session, cursor, dc);
        default:
            return knl_open_table_cursor(session, cursor, dc);
    }
}

/*
 * in the case of rescan, it means a re-open of a existing cursor.
 * this may happen in the operation like JOIN and the re-opened cursor already
 * got an query_scn and ssn, it should not be update.
 */
status_t knl_reopen_cursor(knl_handle_t session, knl_cursor_t *cursor, knl_dictionary_t *dc)
{
    knl_scn_t query_scn;
    uint64 query_lsn;
    uint64 ssn;
    uint8 isolevel;
    row_head_t *row_buf;
    char *upd_buf;
    uint64 xid;

    query_scn = cursor->query_scn;
    query_lsn = cursor->query_lsn;
    ssn = cursor->ssn;
    isolevel = cursor->isolevel;
    row_buf = cursor->row;
    upd_buf = cursor->update_info.data;
    xid = cursor->xid;

    if (knl_open_cursor(session, cursor, dc) != GS_SUCCESS) {
        return GS_ERROR;
    }

    cursor->query_scn = query_scn;
    cursor->query_lsn = query_lsn;
    cursor->ssn = ssn;
    cursor->isolevel = isolevel;
    cursor->row = row_buf;
    cursor->update_info.data = upd_buf;
    cursor->xid = xid;

    return GS_SUCCESS;
}

void knl_close_cursor(knl_handle_t session, knl_cursor_t *cursor)
{
    knl_session_t *se = (knl_session_t *)session;
    if (cursor->vm_page == NULL) {
        return;
    }

    vm_close_and_free(se, se->temp_pool, cursor->vm_page->vmid);
    cursor->vm_page = NULL;
}

status_t knl_cursor_use_vm(knl_handle_t handle, knl_cursor_t *cursor, bool32 replace_row)
{
    knl_session_t *session = (knl_session_t *)handle;
    uint32 vmid;

    if (cursor->vm_page == NULL) {
        if (vm_alloc(session, session->temp_pool, &vmid) != GS_SUCCESS) {
            return GS_ERROR;
        }

        if (vm_open(session, session->temp_pool, vmid, &cursor->vm_page) != GS_SUCCESS) {
            vm_free(session, session->temp_pool, vmid);
            return GS_ERROR;
        }
    }

    if (replace_row) {
        cursor->row = (row_head_t *)cursor->vm_page->data;
    }

    return GS_SUCCESS;
}

static inline void knl_init_pcr_index_scan(knl_cursor_t *cursor, index_t *index, bool32 is_equal)
{
    pcrb_key_t *l_key = NULL;
    pcrb_key_t *r_key = NULL;
    errno_t ret;

    cursor->scan_range.is_equal = is_equal;
    cursor->scan_range.l_key.buf = cursor->scan_range.l_buf;
    l_key = (pcrb_key_t *)cursor->scan_range.l_key.buf;
    ret = memset_sp(l_key, sizeof(pcrb_key_t), 0, sizeof(pcrb_key_t));
    knl_securec_check(ret);
    l_key->size = sizeof(pcrb_key_t);
    cursor->key_loc.is_initialized = GS_FALSE;

    if (!is_equal) {
        cursor->scan_range.r_key.buf = cursor->scan_range.r_buf;
        r_key = (pcrb_key_t *)cursor->scan_range.r_key.buf;
        ret = memset_sp(r_key, sizeof(pcrb_key_t), 0, sizeof(pcrb_key_t));
        knl_securec_check(ret);

        /* PCR does not support index for temp table */
        r_key->rowid = INVALID_ROWID;
        r_key->size = sizeof(pcrb_key_t);
    }
}

static inline void knl_init_rcr_index_scan(knl_cursor_t *cursor, index_t *index, bool32 is_equal)
{
    btree_key_t *l_key = NULL;
    btree_key_t *r_key = NULL;
    errno_t ret;

    cursor->scan_range.is_equal = is_equal;
    cursor->scan_range.l_key.buf = cursor->scan_range.l_buf;
    l_key = (btree_key_t *)cursor->scan_range.l_key.buf;
    ret = memset_sp(l_key, sizeof(btree_key_t), 0, sizeof(btree_key_t));
    knl_securec_check(ret);
    l_key->size = sizeof(btree_key_t);
    cursor->key_loc.is_initialized = GS_FALSE;

    if (!is_equal) {
        cursor->scan_range.r_key.buf = cursor->scan_range.r_buf;
        r_key = (btree_key_t *)cursor->scan_range.r_key.buf;
        ret = memset_sp(r_key, sizeof(btree_key_t), 0, sizeof(btree_key_t));
        knl_securec_check(ret);

        if (cursor->dc_type == DICT_TYPE_TEMP_TABLE_SESSION || cursor->dc_type == DICT_TYPE_TEMP_TABLE_TRANS) {
            r_key->rowid = INVALID_TEMP_ROWID;
        } else {
            r_key->rowid = INVALID_ROWID;
        }
        r_key->size = sizeof(btree_key_t);
    }
}

void knl_init_index_scan(knl_cursor_t *cursor, bool32 is_equal)
{
    index_t *index = (index_t *)cursor->index;

    if (index->desc.cr_mode == CR_PAGE) {
        knl_init_pcr_index_scan(cursor, index, is_equal);
    } else {
        knl_init_rcr_index_scan(cursor, index, is_equal);
    }
}

uint32 knl_get_key_size(knl_index_desc_t *desc, const char *buf)
{
    if (desc->cr_mode == CR_PAGE) {
        // size is 12 bit and is smammler than uint32
        return (uint32)((pcrb_key_t *)buf)->size;
    } else {
        // size is 12 bit and is smammler than uint32
        return (uint32)((btree_key_t *)buf)->size;
    }
}

void knl_set_key_size(knl_index_desc_t *desc, knl_scan_key_t *key, uint32 size)
{
    if (desc->cr_mode == CR_PAGE) {
        ((pcrb_key_t *)key->buf)->size = size;
    } else {
        ((btree_key_t *)key->buf)->size = size;
    }
}

uint32 knl_scan_key_size(knl_index_desc_t *desc, knl_scan_key_t *key)
{
    if (desc->cr_mode == CR_PAGE) {
        // size is 12 bit, and add sizeof(knl_scan_key_t) is smammler than uint32
        return (uint32)(sizeof(knl_scan_key_t) + ((pcrb_key_t *)key->buf)->size);
    } else {
        // size is 12 bit, and add sizeof(knl_scan_key_t) is smammler than uint32
        return (uint32)(sizeof(knl_scan_key_t) + ((btree_key_t *)key->buf)->size);
    }
}

void knl_init_key(knl_index_desc_t *desc, char *buf, rowid_t *rid)
{
    if (desc->cr_mode == CR_PAGE) {
        pcrb_init_key((pcrb_key_t *)buf, rid);
    } else {
        btree_init_key((btree_key_t *)buf, rid);
    }
}

void knl_set_key_rowid(knl_index_desc_t *desc, char *buf, rowid_t *rid)
{
    if (desc->cr_mode == CR_PAGE) {
        pcrb_set_key_rowid((pcrb_key_t *)buf, rid);
    } else {
        btree_set_key_rowid((btree_key_t *)buf, rid);
    }
}

void knl_put_key_data(knl_index_desc_t *desc, char *buf, gs_type_t type, const void *data, uint16 len, uint16 id)
{
    if (desc->cr_mode == CR_PAGE) {
        pcrb_put_key_data(buf, type, (const char *)data, len, id);
    } else {
        btree_put_key_data(buf, type, (const char *)data, len, id);
    }
}

void knl_set_scan_key(knl_index_desc_t *desc, knl_scan_key_t *scan_key, gs_type_t type, const void *data, uint16 len,
    uint16 id)
{
    pcrb_key_t *pcr_key = NULL;
    btree_key_t *rcr_key = NULL;

    scan_key->flags[id] = SCAN_KEY_NORMAL;

    if (desc->cr_mode == CR_PAGE) {
        pcr_key = (pcrb_key_t *)scan_key->buf;
        scan_key->offsets[id] = (uint16)pcr_key->size;
        pcrb_put_key_data(scan_key->buf, type, (const char *)data, len, id);
    } else {
        rcr_key = (btree_key_t *)scan_key->buf;
        scan_key->offsets[id] = (uint16)rcr_key->size;
        btree_put_key_data(scan_key->buf, type, (const char *)data, len, id);
    }
}

void knl_set_key_flag(knl_scan_key_t *border, uint8 flag, uint16 id)
{
    border->flags[id] = flag;
}

status_t knl_find_table_of_index(knl_handle_t se, text_t *user, text_t *index, text_t *table)
{
    uint32 uid, tid;
    knl_cursor_t *cursor = NULL;
    knl_session_t *session = (knl_session_t *)se;
    dc_user_t *dc_user = NULL;

    if (DB_STATUS(session) != DB_STATUS_OPEN) {
        GS_THROW_ERROR(ERR_DATABASE_NOT_AVAILABLE);
        return GS_ERROR;
    }

    knl_set_session_scn(session, GS_INVALID_ID64);
    if (!dc_get_user_id(session, user, &uid)) {
        return GS_ERROR;
    }

    CM_SAVE_STACK(session->stack);

    cursor = knl_push_cursor(session);

    knl_open_sys_cursor(session, cursor, CURSOR_ACTION_SELECT, SYS_INDEX_ID, IX_SYS_INDEX_002_ID);
    knl_init_index_scan(cursor, GS_TRUE);
    /* find the tuple by uid only */
    knl_set_scan_key(INDEX_DESC(cursor->index), &cursor->scan_range.l_key, GS_TYPE_INTEGER, (void *)&uid,
        sizeof(uint32), 0);
    knl_set_scan_key(INDEX_DESC(cursor->index), &cursor->scan_range.l_key, GS_TYPE_STRING, index->str, index->len, 1);

    if (GS_SUCCESS != knl_fetch(session, cursor)) {
        CM_RESTORE_STACK(session->stack);
        return GS_ERROR;
    }

    if (cursor->eof) {
        CM_RESTORE_STACK(session->stack);
        return GS_ERROR;
    }

    tid = *(uint32 *)(CURSOR_COLUMN_DATA(cursor, SYS_INDEX_COLUMN_ID_TABLE));
    CM_RESTORE_STACK(session->stack);

    if (dc_open_user_by_id(session, uid, &dc_user) != GS_SUCCESS) {
        return GS_ERROR;
    }

    table->str = DC_GET_ENTRY(dc_user, tid)->name;
    table->len = (uint32)strlen(table->str);
    return GS_SUCCESS;
}

/*
 * if index is invalid and it is primary/unique index, then report error
 */
static status_t knl_check_index_operate_state(index_t *index, knl_cursor_t *cursor, bool32 *need_operate)
{
    if (index->desc.is_invalid) {
        // if spliting partition is ongoing, don't report error
        if (INDEX_IS_UNSTABLE(index, cursor->is_splitting)) {
            GS_THROW_ERROR(ERR_INDEX_NOT_STABLE, index->desc.name);
            return GS_ERROR;
        } else {
            *need_operate = GS_FALSE;
        }

        return GS_SUCCESS;
    }

    if (!IS_PART_INDEX(index)) {
        return GS_SUCCESS;
    }

    index_part_t *index_part = (index_part_t *)cursor->index_part;

    if (index_part->desc.is_invalid) {
        if (INDEX_IS_UNSTABLE(index, cursor->is_splitting)) {
            GS_THROW_ERROR(ERR_INDEX_PART_UNUSABLE, index_part->desc.name, index->desc.name);
            return GS_ERROR;
        } else {
            *need_operate = GS_FALSE;
        }
    }

    return GS_SUCCESS;
}

static status_t knl_insert_index_key(knl_handle_t session, knl_cursor_t *cursor)
{
    index_t *index = (index_t *)cursor->index;
    bool32 need_insert = GS_TRUE;

    if (knl_check_index_operate_state(index, cursor, &need_insert) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (SECUREC_UNLIKELY(!need_insert)) {
        return GS_SUCCESS;
    }

    if (knl_make_key(session, cursor, index, cursor->key) != GS_SUCCESS) {
        return GS_ERROR;
    }

    return index->acsor->do_insert((knl_session_t *)session, cursor);
}

static status_t knl_batch_insert_index_keys(knl_handle_t session, knl_cursor_t *cursor)
{
    index_t *index = (index_t *)cursor->index;
    bool32 need_insert = GS_TRUE;

    if (knl_check_index_operate_state(index, cursor, &need_insert) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (SECUREC_UNLIKELY(!need_insert)) {
        return GS_SUCCESS;
    }

    if (cursor->dc_type == DICT_TYPE_TEMP_TABLE_SESSION || cursor->dc_type == DICT_TYPE_TEMP_TABLE_TRANS) {
        return temp_btree_batch_insert(session, cursor);
    }

    return pcrb_batch_insert(session, cursor);
}
/*
 * if index is invalid and it is primary/unique index, then report error
 */
static status_t knl_delete_index_key(knl_handle_t session, knl_cursor_t *cursor)
{
    index_t *index = (index_t *)cursor->index;
    bool32 need_delete = GS_TRUE;

    if (knl_check_index_operate_state(index, cursor, &need_delete) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (SECUREC_UNLIKELY(!need_delete)) {
        return GS_SUCCESS;
    }

    if (knl_make_key(session, cursor, index, cursor->key) != GS_SUCCESS) {
        knl_panic_log(0, "knl_make_key is failed, panic info: page %u-%u type %u table %s index %s", cursor->rowid.file,
            cursor->rowid.page, ((page_head_t *)cursor->page_buf)->type,
            ((dc_entity_t *)cursor->dc_entity)->table.desc.name, index->desc.name);
    }

    return index->acsor->do_delete(session, cursor);
}

static inline void knl_check_icol_changed(knl_cursor_t *cursor, index_t *index, uint16 *map, uint32 i,
    bool32 *is_changed)
{
    knl_icol_info_t *icol;
    uint32 j, k, col_id;

    icol = &index->desc.columns_info[i];
    for (k = 0; k < icol->arg_count; k++) {
        col_id = icol->arg_cols[k];

        for (j = 0; j < cursor->update_info.count; j++) {
            if (col_id == cursor->update_info.columns[j]) {
                map[i] = j;
                *is_changed = GS_TRUE;
            }
        }
    }
}

static bool32 knl_check_index_changed(knl_session_t *session, knl_cursor_t *cursor, index_t *index, uint16 *map)
{
    uint32 i, j, col_id;
    dc_entity_t *entity = (dc_entity_t *)(cursor->dc_entity);
    bool32 is_changed = GS_FALSE;
    knl_column_t *index_col = NULL;

    for (i = 0; i < index->desc.column_count; i++) {
        map[i] = GS_INVALID_ID16;
        col_id = index->desc.columns[i];
        index_col = dc_get_column(entity, col_id);

        if (KNL_COLUMN_IS_VIRTUAL(index_col)) {
            knl_check_icol_changed(cursor, index, map, i, &is_changed);
        }

        for (j = 0; j < cursor->update_info.count; j++) {
            if (col_id == cursor->update_info.columns[j]) {
                map[i] = j;
                is_changed = GS_TRUE;
            }
        }
    }

    return is_changed;
}

static void knl_restore_cursor_index(knl_cursor_t *cursor, knl_handle_t org_index, uint8 index_slot)
{
    cursor->index_slot = index_slot;
    cursor->index = org_index;
    if (cursor->index != NULL && IS_PART_INDEX(cursor->index)) {
        knl_panic_log(cursor->part_loc.part_no != GS_INVALID_ID32,
            "the part_no record on cursor is invalid, panic info: table %s index %s",
            ((dc_entity_t *)cursor->dc_entity)->table.desc.name, ((index_t *)cursor->index)->desc.name);
        index_part_t *index_part = INDEX_GET_PART(cursor->index, cursor->part_loc.part_no);
        if (IS_PARENT_IDXPART(&index_part->desc)) {
            knl_panic_log(cursor->part_loc.subpart_no != GS_INVALID_ID32,
                "the subpart_no record on cursor is invalid,"
                " panic info: table %s index %s",
                ((dc_entity_t *)cursor->dc_entity)->table.desc.name, ((index_t *)cursor->index)->desc.name);
            index_t *index = (index_t *)cursor->index;
            uint32 subpart_no = cursor->part_loc.subpart_no;
            cursor->index_part = PART_GET_SUBENTITY(index->part_index, index_part->subparts[subpart_no]);
        } else {
            cursor->index_part = index_part;
        }
    }
}

status_t knl_insert_indexes(knl_handle_t handle, knl_cursor_t *cursor)
{
    knl_session_t *session = (knl_session_t *)handle;
    table_t *table = (table_t *)cursor->table;
    index_t *index = NULL;
    seg_stat_t temp_stat;
    btree_t *btree = NULL;

    table = (table_t *)cursor->table;

    for (uint32 i = 0; i < table->index_set.total_count; i++) {
        cursor->index_slot = i;
        index = table->index_set.items[i];
        cursor->index = index;

        if (IS_PART_INDEX(cursor->index)) {
            knl_panic_log(cursor->part_loc.part_no != GS_INVALID_ID32,
                "the part_no record on cursor is invalid, "
                "panic info: table %s index %s",
                table->desc.name, ((index_t *)cursor->index)->desc.name);
            cursor->index_part = INDEX_GET_PART(index, cursor->part_loc.part_no);
            if (IS_PARENT_IDXPART(&((index_part_t *)cursor->index_part)->desc)) {
                knl_panic_log(cursor->part_loc.subpart_no != GS_INVALID_ID32,
                    "the subpart_no record on cursor is "
                    "invalid, panic info: table %s index %s",
                    table->desc.name, ((index_t *)cursor->index)->desc.name);
                index_part_t *index_part = (index_part_t *)cursor->index_part;
                uint32 subpart_no = cursor->part_loc.subpart_no;
                cursor->index_part = PART_GET_SUBENTITY(index->part_index, index_part->subparts[subpart_no]);
            }
        }

        btree = CURSOR_BTREE(cursor);
#ifdef _STATISTICS
        SEG_STATS_INIT(session, &temp_stat);
#endif
        if (knl_insert_index_key(session, cursor) != GS_SUCCESS) {
            return GS_ERROR;
        }
#ifdef _STATISTICS
        SEG_STATS_RECORD(session, temp_stat, &btree->stat);
#endif
    }

    return GS_SUCCESS;
}

static inline bool32 knl_check_ref_changed(knl_cursor_t *cursor, ref_cons_t *ref, uint16 *map)
{
    uint32 i, j;
    bool32 is_changed = GS_FALSE;

    for (i = 0; i < ref->col_count; i++) {
        map[i] = GS_INVALID_ID16;
        for (j = 0; j < cursor->update_info.count; j++) {
            if (ref->cols[i] == cursor->update_info.columns[j]) {
                map[i] = j;
                is_changed = GS_TRUE;
                break;
            }
        }
    }

    return is_changed;
}

static status_t knl_verify_ref_cons(knl_session_t *session, knl_cursor_t *cursor, ref_cons_t *cons)
{
    knl_dictionary_t dc;
    uint32 i, col_id, uid;
    index_t *index = NULL;
    table_t *table = NULL;
    dc_entity_t *ref_entity = NULL;
    knl_column_t *column = NULL;
    char *key = NULL;
    char *data = NULL;
    uint32 len;
    uint32 part_no, subpart_no;
    bool32 parent_exist = GS_FALSE;
    knl_update_info_t *ui = &cursor->update_info;
    index_part_t *index_part = NULL;
    btree_t *btree = NULL;
    uint16 *map;

    map = (uint16 *)cm_push(session->stack, GS_MAX_INDEX_COLUMNS * sizeof(uint16));

    if ((cursor->action == CURSOR_ACTION_UPDATE && !knl_check_ref_changed(cursor, cons, map)) ||
        !cons->cons_state.is_enable) {
        cm_pop(session->stack);
        return GS_SUCCESS;
    }

    if (cons->ref_entity == NULL) {
        cm_spin_lock(&cons->lock, NULL);
        if (cons->ref_entity == NULL) {
            if (knl_open_dc_by_id(session, cons->ref_uid, cons->ref_oid, &dc, GS_TRUE) != GS_SUCCESS) {
                cm_spin_unlock(&cons->lock);
                cm_pop(session->stack);
                return GS_ERROR;
            }

            cons->ref_entity = dc.handle;
        }
        cm_spin_unlock(&cons->lock);
    }

    ref_entity = (dc_entity_t *)cons->ref_entity;
    table = &ref_entity->table;

    for (i = 0; i < table->index_set.total_count; i++) {
        index = table->index_set.items[i];
        if (index->desc.id == cons->ref_ix) {
            break;
        }
    }

    if (index->desc.is_invalid) {
        cm_pop(session->stack);
        GS_THROW_ERROR(ERR_INDEX_NOT_STABLE, index->desc.name);
        return GS_ERROR;
    }

    knl_panic_log(i < table->index_set.count,
        "table's index count is incorrect, panic info: page %u-%u type %u "
        "table %s index %s current index_count %u record index_count %u",
        cursor->rowid.file, cursor->rowid.page, ((page_head_t *)cursor->page_buf)->type, table->desc.name,
        index->desc.name, i, table->index_set.count);
    if (IS_PART_INDEX(index)) {
        if (db_get_fk_part_no(session, cursor, index, cons->ref_entity, cons, &part_no) != GS_SUCCESS) {
            cm_pop(session->stack);
            return GS_ERROR;
        }

        if (part_no == GS_INVALID_ID32 || index->part_index->groups[part_no / PART_GROUP_SIZE] == NULL) {
            cm_pop(session->stack);
            GS_THROW_ERROR(ERR_CONSTRAINT_VIOLATED_NO_FOUND, "parent key not found");
            return GS_ERROR;
        }

        index_part = PART_GET_ENTITY(index->part_index, part_no);
        if (IS_PARENT_IDXPART(&index_part->desc)) {
            if (db_get_fk_subpart_no(session, cursor, index, cons->ref_entity, cons, part_no, &subpart_no) !=
                GS_SUCCESS) {
                cm_pop(session->stack);
                return GS_ERROR;
            }

            if (subpart_no == GS_INVALID_ID32 || index->part_index->groups[subpart_no / PART_GROUP_SIZE] == NULL) {
                cm_pop(session->stack);
                GS_THROW_ERROR(ERR_CONSTRAINT_VIOLATED_NO_FOUND, "parent key not found");
                return GS_ERROR;
            }
            index_part = PART_GET_SUBENTITY(index->part_index, index_part->subparts[subpart_no]);
        }

        if (index_part == NULL) {
            cm_pop(session->stack);
            GS_THROW_ERROR(ERR_CONSTRAINT_VIOLATED_NO_FOUND, "parent key not found");
            return GS_ERROR;
        }

        if (index_part->desc.is_invalid) {
            cm_pop(session->stack);
            GS_THROW_ERROR(ERR_INDEX_PART_UNUSABLE, index_part->desc.name, index->desc.name);
            return GS_ERROR;
        }
    }

    key = (char *)cm_push(session->stack, GS_KEY_BUF_SIZE);
    knl_init_key(INDEX_DESC(index), key, NULL);

    if (cursor->action == CURSOR_ACTION_INSERT) {
        for (i = 0; i < index->desc.column_count; i++) {
            column = dc_get_column(ref_entity, index->desc.columns[i]);
            col_id = cons->cols[i];
            data = CURSOR_COLUMN_DATA(cursor, col_id);
            len = CURSOR_COLUMN_SIZE(cursor, col_id);
            if (len == GS_NULL_VALUE_LEN) {
                cm_pop(session->stack);
                cm_pop(session->stack);
                return GS_SUCCESS;
            }
            knl_put_key_data(INDEX_DESC(index), key, column->datatype, data, len, i);
        }
    } else {
        knl_panic_log(cursor->action == CURSOR_ACTION_UPDATE,
            "current cursor's action is invalid, panic info: "
            "page %u-%u type %u table %s index %s",
            cursor->rowid.file, cursor->rowid.page, ((page_head_t *)cursor->page_buf)->type, table->desc.name,
            index->desc.name);
        for (i = 0; i < index->desc.column_count; i++) {
            column = dc_get_column(ref_entity, index->desc.columns[i]);
            col_id = cons->cols[i];
            if (map[i] == GS_INVALID_ID16) {
                data = CURSOR_COLUMN_DATA(cursor, col_id);
                len = CURSOR_COLUMN_SIZE(cursor, col_id);
            } else {
                uid = map[i];
                data = ui->data + ui->offsets[uid];
                len = ui->lens[uid];
            }

            if (len == GS_NULL_VALUE_LEN) {
                cm_pop(session->stack);
                cm_pop(session->stack);
                return GS_SUCCESS;
            }
            knl_put_key_data(INDEX_DESC(index), key, column->datatype, data, len, i);
        }
    }

    if (IS_PART_INDEX(index)) {
        btree = &index_part->btree;
    } else {
        btree = &index->btree;
    }

    if (btree->segment != NULL && index->desc.cr_mode == CR_PAGE) {
        if (pcrb_check_key_exist(session, btree, key, &parent_exist) != GS_SUCCESS) {
            cm_pop(session->stack);
            cm_pop(session->stack);
            return GS_ERROR;
        }
    } else if (btree->segment != NULL) {
        if (btree_check_key_exist(session, btree, key, &parent_exist) != GS_SUCCESS) {
            cm_pop(session->stack);
            cm_pop(session->stack);
            return GS_ERROR;
        }
    } else {
        parent_exist = GS_FALSE;
    }
    cm_pop(session->stack);
    cm_pop(session->stack);
    if (!parent_exist) {
        GS_THROW_ERROR(ERR_CONSTRAINT_VIOLATED_NO_FOUND, "parent key not found");
        return GS_ERROR;
    }

    return GS_SUCCESS;
}

static status_t knl_verify_check_cons(knl_session_t *session, knl_cursor_t *cursor, check_cons_t *check)
{
    status_t ret;
    cond_result_t cond_result;

    if (!check->cons_state.is_enable) {
        return GS_SUCCESS;
    }

    if (cursor->stmt == NULL || check->condition == NULL) {
        GS_LOG_RUN_WAR("[DC] could not decode check cond %s or stmt is null", T2S(&check->check_text));
        return GS_SUCCESS;
    }

    g_knl_callback.set_stmt_check(cursor->stmt, cursor, GS_TRUE);
    ret = g_knl_callback.match_cond_tree((void *)cursor->stmt, check->condition, &cond_result);
    g_knl_callback.set_stmt_check(cursor->stmt, NULL, GS_FALSE);
    if (ret != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (cond_result == COND_FALSE) {
        GS_THROW_ERROR(ERR_CONSTRAINT_VIOLATED_CHECK_FAILED);
        return GS_ERROR;
    }

    return GS_SUCCESS;
}

static status_t knl_delete_set_null(knl_session_t *session, knl_cursor_t *cursor, cons_dep_t *dep)
{
    dc_entity_t *entity = (dc_entity_t *)cursor->dc_entity;
    knl_column_t *dc_column = NULL;
    row_assist_t ra;
    int32 i, j;
    uint16 temp;
    uint16 col_id;
    bool32 is_csf = knl_is_table_csf(entity, cursor->part_loc.part_no);

    cursor->update_info.count = dep->col_count;
    cm_row_init(&ra, cursor->update_info.data, HEAP_MAX_ROW_SIZE, dep->col_count, is_csf);
    for (i = 0; i < dep->col_count; i++) {
        col_id = dep->cols[i];
        dc_column = dc_get_column(entity, col_id);
        if (dc_column->nullable != GS_TRUE) {
            GS_THROW_ERROR(ERR_COLUMN_NOT_NULL, dc_column->name);
            return GS_ERROR;
        }
        cursor->update_info.columns[i] = col_id;
        row_put_null(&ra);
    }
    row_end(&ra);
    // sort update_info from small to large
    for (i = 0; i < dep->col_count - 1; i++) {
        for (j = 0; j < dep->col_count - 1 - i; j++) {
            if (cursor->update_info.columns[j] > cursor->update_info.columns[j + 1]) {
                temp = cursor->update_info.columns[j];
                cursor->update_info.columns[j] = cursor->update_info.columns[j + 1];
                cursor->update_info.columns[j + 1] = temp;
            }
        }
    }

    cm_decode_row(cursor->update_info.data, cursor->update_info.offsets, cursor->update_info.lens, NULL);

    if (knl_internal_update(session, cursor) != GS_SUCCESS) {
        return GS_ERROR;
    }

    return GS_SUCCESS;
}

static status_t knl_fk_match_parent(void *handle, bool32 *matched)
{
    char *data = NULL;
    uint16 len;
    dep_condition_t *dep_cond = (dep_condition_t *)handle;
    cons_dep_t *dep = dep_cond->dep;
    int16 i;
    uint16 col_id;

    for (i = 0; i < dep->col_count - dep->ix_match_cols; i++) {
        col_id = dep->cols[dep->col_map[i + dep->ix_match_cols]];
        data = CURSOR_COLUMN_DATA(dep_cond->child_cursor, col_id);
        len = CURSOR_COLUMN_SIZE(dep_cond->child_cursor, col_id);
        if (len != dep_cond->lens[i] || memcmp(data, dep_cond->data[i], len) != 0) {
            *matched = GS_FALSE;
            return GS_SUCCESS;
        }
    }

    *matched = GS_TRUE;
    return GS_SUCCESS;
}

static void knl_init_child_cursor(knl_dictionary_t *dc, cons_dep_t *dep, knl_cursor_t *parent_cursor,
    index_t *parent_index, knl_cursor_t *cursor)
{
    dc_entity_t *entity = DC_ENTITY(dc);
    uint16 i;
    knl_column_t *column = NULL;
    char *data = NULL;
    uint16 len;
    uint16 col_id;
    dep_condition_t *dep_cond = NULL;

    if (dep->refactor == REF_DEL_NOT_ALLOWED) {
        cursor->action = CURSOR_ACTION_SELECT;
    } else if (dep->refactor == REF_DEL_CASCADE) {
        cursor->action = CURSOR_ACTION_DELETE;
    } else {
        cursor->action = CURSOR_ACTION_UPDATE;
    }

    dep_cond = (dep_condition_t *)cursor->stmt;
    cursor->vm_page = NULL;

    if (dep->scan_mode == DEP_SCAN_TABLE_FULL) {
        cursor->scan_mode = SCAN_MODE_TABLE_FULL;
    } else {
        cursor->scan_mode = SCAN_MODE_INDEX;
        cursor->index_slot = dep->idx_slot;
        cursor->index = DC_INDEX(dc, dep->idx_slot);
        knl_init_index_scan(cursor, GS_FALSE);

        for (i = 0; i < dep->ix_match_cols; i++) {
            col_id = parent_index->desc.columns[dep->col_map[i]];
            column = dc_get_column(entity, dep->cols[dep->col_map[i]]);
            data = CURSOR_COLUMN_DATA(parent_cursor, col_id);
            len = CURSOR_COLUMN_SIZE(parent_cursor, col_id);

            knl_set_scan_key(INDEX_DESC(cursor->index), &cursor->scan_range.l_key, column->datatype, data, len, i);
            knl_set_scan_key(INDEX_DESC(cursor->index), &cursor->scan_range.r_key, column->datatype, data, len, i);
        }

        for (i = dep->ix_match_cols; i < INDEX_DESC(cursor->index)->column_count; i++) {
            knl_set_key_flag(&cursor->scan_range.l_key, SCAN_KEY_LEFT_INFINITE, i);
            knl_set_key_flag(&cursor->scan_range.r_key, SCAN_KEY_RIGHT_INFINITE, i);
        }
    }

    dep_cond->dep = dep;
    dep_cond->child_cursor = cursor;

    for (i = 0; i < dep->col_count - dep->ix_match_cols; i++) {
        col_id = parent_index->desc.columns[dep->col_map[i + dep->ix_match_cols]];
        data = CURSOR_COLUMN_DATA(parent_cursor, col_id);
        len = CURSOR_COLUMN_SIZE(parent_cursor, col_id);
        dep_cond->data[i] = data;
        dep_cond->lens[i] = len;
    }
}

static status_t knl_verify_dep_by_row(knl_session_t *session, dep_condition_t *dep_cond, knl_cursor_t *parent_cursor,
    knl_dictionary_t *child_dc, bool32 *depended)
{
    dc_entity_t *entity;
    status_t status = GS_SUCCESS;
    knl_cursor_t *cursor = dep_cond->child_cursor;
    cons_dep_t *dep = dep_cond->dep;
    knl_handle_t sql_stmt = cursor->stmt;

    entity = (dc_entity_t *)child_dc->handle;
    session->wtid.is_locking = GS_TRUE;
    session->wtid.oid = entity->entry->id;
    session->wtid.uid = entity->entry->uid;

    if (dc_locked_by_self(session, entity->entry)) {
        if (lock_table_in_exclusive_mode(session, entity, entity->entry, LOCK_INF_WAIT) != GS_SUCCESS) {
            return GS_ERROR;
        }
    } else {
        if (lock_table_exclusive(session, entity, LOCK_INF_WAIT) != GS_SUCCESS) {
            return GS_ERROR;
        }
    }

    session->wtid.is_locking = GS_FALSE;
    *depended = GS_FALSE;
    for (;;) {
        cursor->stmt = dep_cond;
        if (knl_fetch(session, cursor) != GS_SUCCESS) {
            status = GS_ERROR;
            break;
        }

        if (cursor->eof) {
            break;
        }

        if (parent_cursor->action == CURSOR_ACTION_UPDATE || dep->refactor == REF_DEL_NOT_ALLOWED) {
            *depended = GS_TRUE;
            break;
        }

        cursor->stmt = sql_stmt;

        if (dep->refactor == REF_DEL_SET_NULL) {
            if (knl_delete_set_null(session, cursor, dep) != GS_SUCCESS) {
                status = GS_ERROR;
                break;
            }
        } else {
            if (knl_internal_delete(session, cursor) != GS_SUCCESS) {
                status = GS_ERROR;
                break;
            }
        }

        if (cursor->is_found) {
            if (knl_verify_children_dependency(session, cursor) != GS_SUCCESS) {
                status = GS_ERROR;
                break;
            }
        }
    }

    if (SCH_LOCKED_EXCLUSIVE(entity)) {
        lock_degrade_table_lock(session, entity);
    }
    cursor->stmt = sql_stmt;
    return status;
}

static status_t knl_verify_dep_by_key(knl_session_t *session, dep_condition_t *dep_cond, knl_cursor_t *parent_cursor,
    knl_dictionary_t *child_dc, bool32 *depended)
{
    index_t *index = NULL;
    status_t status = GS_ERROR;
    knl_cursor_t *cursor = dep_cond->child_cursor;
    cons_dep_t *dep = dep_cond->dep;
    knl_handle_t sql_stmt = cursor->stmt;

    dc_entity_t *entity = (dc_entity_t *)child_dc->handle;
    table_t *table = &entity->table;

    for (uint32 i = 0; i < table->index_set.count; i++) {
        index = table->index_set.items[i];
        if (index->desc.id == dep->idx_slot) {
            break;
        }
    }
    *depended = GS_FALSE;
    for (;;) {
        cursor->stmt = dep_cond;
        if (index->desc.cr_mode == CR_PAGE) {
            if (pcrb_fetch_depended(session, cursor) != GS_SUCCESS) {
                break;
            }
        } else {
            if (btree_fetch_depended(session, cursor) != GS_SUCCESS) {
                break;
            }
        }

        if (cursor->eof) {
            status = GS_SUCCESS;
            break;
        }

        if (parent_cursor->action == CURSOR_ACTION_UPDATE || dep->refactor == REF_DEL_NOT_ALLOWED) {
            *depended = GS_TRUE;
            status = GS_SUCCESS;
            break;
        }

        cursor->stmt = sql_stmt;

        if (dep->refactor == REF_DEL_SET_NULL) {
            if (knl_delete_set_null(session, cursor, dep) != GS_SUCCESS) {
                break;
            }
        } else {
            if (knl_internal_delete(session, cursor) != GS_SUCCESS) {
                break;
            }
        }

        if (cursor->is_found) {
            if (knl_verify_children_dependency(session, cursor) != GS_SUCCESS) {
                break;
            }
        }
    }
    cursor->stmt = sql_stmt;
    return status;
}

static status_t knl_verify_ref_entity(knl_session_t *session, dep_condition_t *dep_cond, knl_dictionary_t *child_dc,
    knl_cursor_t *parent_cursor, bool32 *depended)
{
    if (dep_cond->dep->scan_mode != DEP_SCAN_TABLE_FULL) {
        if (knl_verify_dep_by_key(session, dep_cond, parent_cursor, child_dc, depended) != GS_SUCCESS) {
            return GS_ERROR;
        }
    } else {
        if (knl_verify_dep_by_row(session, dep_cond, parent_cursor, child_dc, depended) != GS_SUCCESS) {
            return GS_ERROR;
        }
    }
    return GS_SUCCESS;
}

static status_t knl_verify_dep_part_table(knl_session_t *session, dep_condition_t *dep_cond, knl_dictionary_t *child_dc,
    knl_cursor_t *parent_cursor, bool32 *depended)
{
    uint32 lpart_no, rpart_no;
    uint32 lsubpart_no, rsubpart_no;
    table_part_t *compart = NULL;
    knl_cursor_t *cursor = dep_cond->child_cursor;

    if (dc_get_part_fk_range(session, parent_cursor, cursor, dep_cond->dep, &lpart_no, &rpart_no) != GS_SUCCESS) {
        return GS_ERROR;
    }

    /* no parititon match, the depended will be set GS_FALSE */
    if (lpart_no == GS_INVALID_ID32) {
        *depended = GS_FALSE;
        return GS_SUCCESS;
    }

    for (uint32 i = lpart_no; i <= rpart_no; i++) {
        cursor->part_loc.part_no = i;
        compart = TABLE_GET_PART(cursor->table, i);
        if (!IS_READY_PART(compart)) {
            continue;
        }

        if (!IS_PARENT_TABPART(&compart->desc)) {
            cursor->part_loc.subpart_no = GS_INVALID_ID32;
            if (knl_reopen_cursor(session, cursor, child_dc) != GS_SUCCESS) {
                return GS_ERROR;
            }

            if (knl_verify_ref_entity(session, dep_cond, child_dc, parent_cursor, depended) != GS_SUCCESS) {
                return GS_ERROR;
            }

            if (*depended) {
                return GS_SUCCESS;
            }

            continue;
        }

        if (dc_get_subpart_fk_range(session, parent_cursor, cursor, dep_cond->dep, i, &lsubpart_no, &rsubpart_no) !=
            GS_SUCCESS) {
            return GS_ERROR;
        }

        for (uint32 j = lsubpart_no; j <= rsubpart_no; j++) {
            cursor->part_loc.subpart_no = j;
            if (knl_reopen_cursor(session, cursor, child_dc) != GS_SUCCESS) {
                return GS_ERROR;
            }

            if (knl_verify_ref_entity(session, dep_cond, child_dc, parent_cursor, depended) != GS_SUCCESS) {
                return GS_ERROR;
            }

            if (*depended) {
                return GS_SUCCESS;
            }
        }
    }

    return GS_SUCCESS;
}

static status_t knl_verify_dep(knl_session_t *session, dep_condition_t *dep_cond, knl_dictionary_t *child_dc,
    knl_cursor_t *parent_cursor, bool32 *depended)
{
    knl_cursor_t *cursor = dep_cond->child_cursor;

    if (!IS_PART_TABLE(cursor->table)) {
        if (knl_verify_ref_entity(session, dep_cond, child_dc, parent_cursor, depended) != GS_SUCCESS) {
            return GS_ERROR;
        }
    } else {
        /* global index scan in part table is equal to normal table using index scan */
        if (dep_cond->dep->scan_mode != DEP_SCAN_TABLE_FULL && !IS_PART_INDEX(cursor->index)) {
            return knl_verify_ref_entity(session, dep_cond, child_dc, parent_cursor, depended);
        }

        if (knl_verify_dep_part_table(session, dep_cond, child_dc, parent_cursor, depended) != GS_SUCCESS) {
            return GS_ERROR;
        }
    }

    return GS_SUCCESS;
}

static status_t knl_verify_ref_dep(knl_session_t *session, knl_cursor_t *parent_cursor, index_t *parent_index,
    cons_dep_t *dep, bool32 *depended)
{
    dep_condition_t *dep_cond = NULL;
    knl_cursor_t *cursor = NULL;
    knl_dictionary_t child_dc;
    knl_match_cond_t org_match_cond = session->match_cond;
    status_t status;

    if (knl_open_dc_by_id(session, dep->uid, dep->oid, &child_dc, GS_TRUE) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (!dep->loaded || dep->chg_scn != child_dc.chg_scn) {
        dc_load_child_entity(session, dep, &child_dc);
    }

    CM_SAVE_STACK(session->stack);
    cursor = knl_push_cursor(session);

    cursor->stmt = cm_push(session->stack, sizeof(dep_condition_t));
    knl_init_child_cursor(&child_dc, dep, parent_cursor, parent_index, cursor);
    dep_cond = (dep_condition_t *)cursor->stmt;

    cursor->stmt = parent_cursor->stmt;
    if (knl_open_cursor(session, cursor, &child_dc) != GS_SUCCESS) {
        CM_RESTORE_STACK(session->stack);
        dc_close(&child_dc);
        return GS_ERROR;
    }

    if (cursor->action == CURSOR_ACTION_SELECT) {
        cursor->ssn = session->ssn + 1;
    }

    cursor->update_info.data = (char *)cm_push(session->stack, GS_MAX_ROW_SIZE);
    session->match_cond = knl_fk_match_parent;
    cursor->isolevel = (uint8)ISOLATION_CURR_COMMITTED;
    cursor->query_scn = DB_CURR_SCN(session);
    cursor->cc_cache_time = KNL_NOW(session);

    status = knl_verify_dep(session, dep_cond, &child_dc, parent_cursor, depended);

    knl_close_cursor(session, cursor);
    session->match_cond = org_match_cond;
    CM_RESTORE_STACK(session->stack);
    dc_close(&child_dc);
    return status;
}

bool32 knl_check_index_key_changed(knl_cursor_t *cursor, index_t *index, uint16 *map)
{
    uint32 i, col_id, uid;
    knl_update_info_t *ui = &cursor->update_info;

    for (i = 0; i < index->desc.column_count; i++) {
        col_id = index->desc.columns[i];

        if (map[i] == GS_INVALID_ID16) {
            continue;
        }

        uid = map[i];
        if (ui->lens[uid] != cursor->lens[col_id]) {
            return GS_TRUE;
        }

        if (ui->lens[uid] == GS_NULL_VALUE_LEN) {
            continue;
        }

        if (memcmp(ui->data + ui->offsets[uid], CURSOR_COLUMN_DATA(cursor, col_id), ui->lens[uid])) {
            return GS_TRUE;
        }
    }

    return GS_FALSE;
}

static status_t knl_verify_ref_depend(knl_session_t *session, knl_cursor_t *cursor, index_t *index, bool32 *depended)
{
    cons_dep_t *dep = NULL;
    bool32 has_null = GS_FALSE;
    uint16 col_id;
    uint32 i;

    *depended = GS_FALSE;
    for (i = 0; i < index->desc.column_count; i++) {
        col_id = index->desc.columns[i];
        if (CURSOR_COLUMN_SIZE(cursor, col_id) == GS_NULL_VALUE_LEN) {
            has_null = GS_TRUE;
            break;
        }
    }

    if (has_null) {
        *depended = GS_FALSE;
        return GS_SUCCESS;
    }

    dep = index->dep_set.first;
    while (dep != NULL) {
        if (!dep->cons_state.is_enable) {
            dep = dep->next;
            continue;
        }

        if (knl_verify_ref_dep(session, cursor, index, dep, depended)) {
            return GS_ERROR;
        }

        if (*depended) {
            return GS_SUCCESS;
        }

        dep = dep->next;
    }

    return GS_SUCCESS;
}

/*
 * kernel interface for ensure row is not referenced by child table row
 * @param handle pointer for kernel session
 * @note called when delete or update
 */
status_t knl_verify_children_dependency(knl_handle_t session, knl_cursor_t *cursor)
{
    table_t *table = &((dc_entity_t *)cursor->dc_entity)->table;
    index_t *index = NULL;
    bool32 depended = GS_FALSE;
    uint32 i;
    uint16 *map = NULL;
    knl_session_t *se = (knl_session_t *)session;

    if (!table->cons_set.referenced) {
        return GS_SUCCESS;
    }
    map = (uint16 *)cm_push(se->stack, GS_MAX_INDEX_COLUMNS * sizeof(uint16));

    for (i = 0; i < table->index_set.count; i++) {
        index = table->index_set.items[i];
        if (!index->desc.is_enforced || index->dep_set.count == 0) {
            continue;
        }

        if (cursor->action == CURSOR_ACTION_UPDATE && !knl_check_index_changed(se, cursor, index, map)) {
            continue;
        }

        if (knl_verify_ref_depend(se, cursor, index, &depended) != GS_SUCCESS) {
            cm_pop(se->stack);
            return GS_ERROR;
        }

        if (depended) {
            if (cursor->action == CURSOR_ACTION_DELETE || knl_check_index_key_changed(cursor, index, map)) {
                GS_THROW_ERROR(ERR_CONSTRAINT_VIOLATED_NO_FOUND, "child record found");
                cm_pop(se->stack);
                return GS_ERROR;
            }
        }
    }
    cm_pop(se->stack);
    return GS_SUCCESS;
}
/*
 * kernel interface for ensure constraint of check and foreign key(for child table)
 * @param handle pointer for kernel session
 * @note called when insert or update
 */
status_t knl_verify_ref_integrities(knl_handle_t session, knl_cursor_t *cursor)
{
    table_t *table = &((dc_entity_t *)cursor->dc_entity)->table;
    ref_cons_t *cons = NULL;

    if (table->cons_set.ref_count == 0) {
        return GS_SUCCESS;
    }

    if (table->index_set.count == 0) {
        /* if table has no index, row has not be decoded */
        cm_decode_row((char *)cursor->row, cursor->offsets, cursor->lens, NULL);
    }

    for (uint32 i = 0; i < table->cons_set.ref_count; i++) {
        cons = table->cons_set.ref_cons[i];
        if (knl_verify_ref_cons((knl_session_t *)session, cursor, cons) != GS_SUCCESS) {
            return GS_ERROR;
        }
    }

    return GS_SUCCESS;
}

static status_t knl_update_index_key(knl_handle_t session, knl_cursor_t *cursor)
{
    index_t *index = (index_t *)cursor->index;
    uint16 *map = NULL;
    knl_session_t *se = (knl_session_t *)session;
    bool32 need_update = GS_TRUE;

    map = (uint16 *)cm_push(se->stack, GS_MAX_INDEX_COLUMNS * sizeof(uint16));

    if (!knl_check_index_changed(se, cursor, index, map)) {
        cm_pop(se->stack);
        return GS_SUCCESS;
    }

    if (knl_check_index_operate_state(index, cursor, &need_update) != GS_SUCCESS) {
        cm_pop(se->stack);
        return GS_ERROR;
    }

    if (SECUREC_UNLIKELY(!need_update)) {
        cm_pop(se->stack);
        return GS_SUCCESS;
    }

    /* delete old value */
    if (knl_make_key(session, cursor, index, cursor->key) != GS_SUCCESS) {
        cm_pop(se->stack);
        return GS_ERROR;
    }

    if (index->acsor->do_delete(session, cursor) != GS_SUCCESS) {
        cm_pop(se->stack);
        return GS_ERROR;
    }

    /* insert new value */
    if (knl_make_update_key(session, cursor, index, cursor->key, &cursor->update_info, map) != GS_SUCCESS) {
        cm_pop(se->stack);
        return GS_ERROR;
    }

    if (index->acsor->do_insert(session, cursor) != GS_SUCCESS) {
        cm_pop(se->stack);
        return GS_ERROR;
    }

    cm_pop(se->stack);
    return GS_SUCCESS;
}

status_t knl_insert(knl_handle_t session, knl_cursor_t *cursor)
{
    if (cursor->vnc_column != NULL) {
        GS_THROW_ERROR(ERR_COLUMN_NOT_NULL, cursor->vnc_column);
        return GS_ERROR;
    }

    // part table , but table part is not ready (deleted or ...)
    if (IS_PART_TABLE(cursor->table) && !IS_READY_PART(cursor->table_part)) {
        GS_THROW_ERROR(ERR_PARTITION_NOT_READY, "table", "part_no");
        GS_LOG_RUN_WAR("the part_no %u of table %s is not ready!",  
            cursor->part_loc.part_no, ((table_t *)(cursor->table))->desc.name);
        return GS_ERROR;
    }

    if (knl_internal_insert(session, cursor) == GS_SUCCESS) {
        return GS_SUCCESS;
    }

    if (cursor->rowid_count == 0) {
        return GS_ERROR;
    }

    cm_reset_error();
    /* if batch insert failed, retry to insert as much as possible with single row insert */
    uint32 row_count = cursor->rowid_count;
    row_head_t *row_addr = cursor->row;
    for (uint32 i = 0; i < row_count; i++) {
        cursor->rowid_count = 1;
        if (knl_internal_insert(session, cursor) != GS_SUCCESS) {
            cursor->rowid_count = i;
            cursor->row = row_addr;
            return GS_ERROR;
        }
        cursor->row = (row_head_t *)((char *)cursor->row + cursor->row->size);
    }

    cursor->row = row_addr;
    return GS_SUCCESS;
}

static status_t knl_update_shadow_index(knl_session_t *session, knl_cursor_t *cursor, shadow_index_t *shadow_index,
    knl_cursor_action_t action)
{
    if (!shadow_index->is_valid) {
        return GS_SUCCESS;
    }

    if (shadow_index->part_loc.part_no != GS_INVALID_ID32) {
        if (shadow_index->part_loc.part_no != cursor->part_loc.part_no ||
            shadow_index->part_loc.subpart_no != cursor->part_loc.subpart_no) {
            return GS_SUCCESS;
        }

        cursor->index = SHADOW_INDEX_ENTITY(shadow_index);
        cursor->index_part = &shadow_index->index_part;
    } else {
        cursor->index = &shadow_index->index;
        if (IS_PART_INDEX(cursor->index)) {
            index_part_t *index_part = INDEX_GET_PART(cursor->index, cursor->part_loc.part_no);
            if (IS_PARENT_IDXPART(&index_part->desc)) {
                index_t *index = &shadow_index->index;
                index_part = PART_GET_SUBENTITY(index->part_index, index_part->subparts[cursor->part_loc.subpart_no]);
            }
            cursor->index_part = index_part;
        }
    }

    switch (action) {
        case CURSOR_ACTION_INSERT:
            return knl_insert_index_key(session, cursor);

        case CURSOR_ACTION_DELETE:
            return knl_delete_index_key(session, cursor);

        case CURSOR_ACTION_UPDATE:
            return knl_update_index_key(session, cursor);

        default:
            return GS_SUCCESS;
    }
}

static status_t knl_insert_single_appendix(knl_session_t *session, knl_cursor_t *cursor, dc_entity_t *entity)
{
    table_t *table = (table_t *)cursor->table;

    if (table->cons_set.check_count > 0) {
        for (uint32 i = 0; i < table->cons_set.check_count; i++) {
            if (knl_verify_check_cons(session, cursor, table->cons_set.check_cons[i]) != GS_SUCCESS) {
                return GS_ERROR;
            }
        }
    }

    if (table->shadow_index != NULL) {
        if (knl_update_shadow_index(session, cursor, table->shadow_index, CURSOR_ACTION_INSERT) != GS_SUCCESS) {
            return GS_ERROR;
        }
    }

    return GS_SUCCESS;
}

static inline bool32 knl_insert_index_batchable(knl_session_t *session, knl_cursor_t *cursor, index_t *index)
{
    return (index->desc.cr_mode == CR_PAGE || cursor->dc_type == DICT_TYPE_TEMP_TABLE_SESSION ||
        cursor->dc_type == DICT_TYPE_TEMP_TABLE_TRANS);
}

static status_t knl_batch_insert_indexes(knl_session_t *session, knl_cursor_t *cursor, bool32 expect_batch_insert)
{
    index_t *index = NULL;
    seg_stat_t temp_stat;
    btree_t *btree = NULL;
    idx_batch_insert insert_method = expect_batch_insert ? knl_batch_insert_index_keys : knl_insert_index_key;
    table_t *table = (table_t *)cursor->table;
    uint8 index_slot = cursor->index_slot;
    knl_handle_t org_index = cursor->index;

    for (uint32 i = 0; i < table->index_set.total_count; i++) {
        index = table->index_set.items[i];
        if (knl_insert_index_batchable(session, cursor, index) != expect_batch_insert) {
            continue;
        }

        cursor->index_slot = i;
        cursor->index = index;

        if (IS_PART_INDEX(cursor->index)) {
            knl_panic_log(cursor->part_loc.part_no != GS_INVALID_ID32,
                "the part_no record on cursor is invalid, "
                "panic info: table %s index %s",
                table->desc.name, ((index_t *)cursor->index)->desc.name);
            index_part_t *index_part = INDEX_GET_PART(cursor->index, cursor->part_loc.part_no);
            if (IS_PARENT_IDXPART(&index_part->desc)) {
                knl_panic_log(cursor->part_loc.subpart_no != GS_INVALID_ID32,
                    "the subpart_no record on cursor is "
                    "invalid, panic info: table %s index %s",
                    table->desc.name, ((index_t *)cursor->index)->desc.name);
                index_part = PART_GET_SUBENTITY(index->part_index, index_part->subparts[cursor->part_loc.subpart_no]);
            }
            cursor->index_part = index_part;
        }

        btree = CURSOR_BTREE(cursor);
#ifdef _STATISTICS
        SEG_STATS_INIT(session, &temp_stat);
#endif
        if (insert_method(session, cursor) != GS_SUCCESS) {
            knl_restore_cursor_index(cursor, org_index, index_slot);
            return GS_ERROR;
        }

#ifdef _STATISTICS
        SEG_STATS_RECORD(session, temp_stat, &btree->stat);
#endif
    }

    knl_restore_cursor_index(cursor, org_index, index_slot);
    return GS_SUCCESS;
}

static inline bool32 knl_need_insert_appendix(knl_cursor_t *cursor)
{
    table_t *table = (table_t *)cursor->table;

    if (table->desc.index_count > 0 || table->cons_set.check_count > 0 || table->shadow_index != NULL) {
        return GS_TRUE;
    }

    if (cursor->rowid_count > 0 && table->cons_set.ref_count > 0) {
        return GS_TRUE;
    }

    return GS_FALSE;
}

static status_t knl_insert_appendix(knl_session_t *session, knl_cursor_t *cursor)
{
    table_t *table = (table_t *)cursor->table;
    dc_entity_t *entity = (dc_entity_t *)cursor->dc_entity;

    if (SECUREC_LIKELY(cursor->rowid_count == 0)) {
        cm_decode_row((char *)cursor->row, cursor->offsets, cursor->lens, NULL);
        if (table->desc.index_count > 0) {
            if (knl_insert_indexes(session, cursor) != GS_SUCCESS) {
                return GS_ERROR;
            }
        }

        if (knl_insert_single_appendix(session, cursor, entity) != GS_SUCCESS) {
            return GS_ERROR;
        }
    } else {
        if (knl_batch_insert_indexes(session, cursor, GS_TRUE) != GS_SUCCESS) {
            return GS_ERROR;
        }

        row_head_t *row_addr = cursor->row;
        for (uint32 i = 0; i < cursor->rowid_count; i++) {
            cursor->rowid = cursor->rowid_array[i];
            cm_decode_row((char *)cursor->row, cursor->offsets, cursor->lens, NULL);
            if (knl_batch_insert_indexes(session, cursor, GS_FALSE) != GS_SUCCESS) {
                cursor->row = row_addr;
                return GS_ERROR;
            }

            if (knl_insert_single_appendix(session, cursor, entity) != GS_SUCCESS) {
                cursor->row = row_addr;
                return GS_ERROR;
            }

            if (knl_verify_ref_integrities(session, cursor) != GS_SUCCESS) {
                cursor->row = row_addr;
                return GS_ERROR;
            }
            cursor->row = (row_head_t *)((char *)cursor->row + cursor->row->size);
        }

        cursor->row = row_addr;
    }

    return GS_SUCCESS;
}

status_t knl_internal_insert(knl_handle_t session, knl_cursor_t *cursor)
{
    if (!cursor->is_valid) {
        GS_THROW_ERROR(ERR_INVALID_CURSOR);
        return GS_ERROR;
    }

    knl_session_t *se = (knl_session_t *)session;
    if (SECUREC_UNLIKELY(!cursor->logging)) {
#ifdef _REPLICATION
        if (se->kernel->lsnd_ctx.standby_num > 0) {
            GS_THROW_ERROR(ERR_OPERATIONS_NOT_ALLOW, "insert data in nologging mode when standby server is available");
            return GS_ERROR;
        }
#endif
        if (!se->rm->nolog_insert) {
            se->rm->nolog_insert = GS_TRUE;
            GS_LOG_RUN_WAR("The transcation(seg_id: %d, slot: %d, xnum: %d) is inserting data without logs.",
                se->rm->xid.xmap.seg_id, se->rm->xid.xmap.slot, se->rm->xid.xnum);
        }
    }

    seg_stat_t seg_stat;
    knl_savepoint_t save_point;
#ifdef _STATISTICS
    SEG_STATS_INIT(se, &seg_stat);
#endif
    knl_savepoint(session, &save_point);

    if (TABLE_ACCESSOR(cursor)->do_insert(session, cursor) != GS_SUCCESS) {
        int32 code = cm_get_error_code();
        if (code != ERR_SHRINK_EXTEND && !((table_t *)cursor->table)->desc.is_timescale) { // 时序表不会滚
            if (!cursor->logging) {
                knl_rollback(session, NULL);
            } else {
                knl_rollback(session, &save_point);
            }
        }

        return GS_ERROR;
    }

    dc_entity_t *entity = (dc_entity_t *)cursor->dc_entity;
    if (entity != NULL && entity->forbid_dml) {
        GS_THROW_ERROR(ERR_CONSTRAINT_VIOLATED);
        return GS_ERROR;
    }

    heap_t *heap = CURSOR_HEAP(cursor);
#ifdef _STATISTICS
    SEG_STATS_RECORD(se, seg_stat, &heap->stat);
#endif
    if (knl_need_insert_appendix(cursor)) {
        knl_handle_t org_index = cursor->index;
        uint8 org_index_slot = cursor->index_slot;
        if (knl_insert_appendix(session, cursor) != GS_SUCCESS) {
            if (!((table_t *)cursor->table)->desc.is_timescale) {  // 时序表不会滚
                knl_rollback(session, &save_point);
            }

            // 2025-1-16
            int32 err_code;
            const char *message = NULL;
            cm_get_error(&err_code, &message, NULL);
            if (cursor->is_insert_ignore && err_code == ERR_DUPLICATE_KEY) {
                cursor->is_insert_ignore = GS_FALSE;
                cm_reset_error();
            } else {
                cursor->is_insert_ignore = GS_FALSE;
                knl_restore_cursor_index(cursor, org_index, org_index_slot);
                return GS_ERROR;
            }
        }

        knl_restore_cursor_index(cursor, org_index, org_index_slot);
    }
#ifdef _STATISTICS
    if (entity != NULL && STATS_ENABLE_MONITOR_TABLE((knl_session_t *)session)) {
        stats_monitor_table_change(cursor);
    }
#endif

    if (SECUREC_UNLIKELY(cursor->rowid_count > 0)) {
        cursor->rowid_count = 0;
    }

    return GS_SUCCESS;
}

status_t knl_delete(knl_handle_t session, knl_cursor_t *cursor)
{
    return knl_internal_delete(session, cursor);
}

status_t knl_internal_delete(knl_handle_t handle, knl_cursor_t *cursor)
{
    knl_session_t *session = (knl_session_t *)handle;
    uint32 i;
    table_t *table;
    index_t *index = NULL;
    knl_savepoint_t savepoint;
    dc_entity_t *entity = (dc_entity_t *)cursor->dc_entity;
    index_set_t *index_set = NULL;
    seg_stat_t temp_stat;
    heap_t *heap = CURSOR_HEAP(cursor);
    btree_t *btree = NULL;

    table = (table_t *)cursor->table;
    index_set = &table->index_set;

    if (!cursor->is_valid) {
        GS_THROW_ERROR(ERR_INVALID_CURSOR);
        return GS_ERROR;
    }

#ifdef _STATISTICS
    SEG_STATS_INIT(session, &temp_stat);
#endif
    knl_savepoint(session, &savepoint);

    if (TABLE_ACCESSOR(cursor)->do_delete(session, cursor) != GS_SUCCESS) {
        knl_rollback(session, &savepoint);
        return GS_ERROR;
    }

#ifdef _STATISTICS
    SEG_STATS_RECORD(session, temp_stat, &heap->stat);
#endif
    cm_decode_row((char *)cursor->row, cursor->offsets, cursor->lens, NULL);

    if (SECUREC_UNLIKELY(!cursor->is_found)) {
        return GS_SUCCESS;
    }

    uint8 org_index_slot = cursor->index_slot;
    knl_handle_t org_index = cursor->index;

    for (i = 0; i < index_set->total_count; i++) {
        cursor->index_slot = i;
        index = index_set->items[i];
        cursor->index = index;

        if (IS_PART_INDEX(cursor->index)) {
            knl_panic_log(cursor->part_loc.part_no != GS_INVALID_ID32,
                "the part_no record on cursor is invalid, "
                "panic info: table %s index %s",
                table->desc.name, ((index_t *)cursor->index)->desc.name);
            index_part_t *index_part = INDEX_GET_PART(cursor->index, cursor->part_loc.part_no);
            if (IS_PARENT_IDXPART(&index_part->desc)) {
                knl_panic_log(cursor->part_loc.subpart_no != GS_INVALID_ID32,
                    "the subpart_no record on cursor is "
                    "invalid, panic info: table %s index %s",
                    table->desc.name, ((index_t *)cursor->index)->desc.name);
                index_part = PART_GET_SUBENTITY(index->part_index, index_part->subparts[cursor->part_loc.subpart_no]);
            }
            cursor->index_part = index_part;
        }
        btree = CURSOR_BTREE(cursor);
#ifdef _STATISTICS
        SEG_STATS_INIT(session, &temp_stat);
#endif

        if (knl_delete_index_key(session, cursor) != GS_SUCCESS) {
            knl_rollback(session, &savepoint);
            knl_restore_cursor_index(cursor, org_index, org_index_slot);
            return GS_ERROR;
        }
#ifdef _STATISTICS
        SEG_STATS_RECORD(session, temp_stat, &btree->stat);
#endif
    }

    if (entity != NULL && entity->forbid_dml) {
        knl_rollback(session, &savepoint);
        GS_THROW_ERROR(ERR_CONSTRAINT_VIOLATED);
        knl_restore_cursor_index(cursor, org_index, org_index_slot);
        return GS_ERROR;
    }

    if (SECUREC_UNLIKELY(table->shadow_index != NULL)) {
        if (knl_update_shadow_index(session, cursor, table->shadow_index, CURSOR_ACTION_DELETE) != GS_SUCCESS) {
            knl_rollback(session, &savepoint);
            knl_restore_cursor_index(cursor, org_index, org_index_slot);
            return GS_ERROR;
        }
    }

    knl_restore_cursor_index(cursor, org_index, org_index_slot);

#ifdef _STATISTICS
    if (entity != NULL && STATS_ENABLE_MONITOR_TABLE(session)) {
        stats_monitor_table_change(cursor);
    }
#endif

    return GS_SUCCESS;
}

status_t knl_update(knl_handle_t session, knl_cursor_t *cursor)
{
    if (cursor->vnc_column != NULL) {
        GS_THROW_ERROR(ERR_COLUMN_NOT_NULL, cursor->vnc_column);
        return GS_ERROR;
    }

    return knl_internal_update(session, cursor);
}

status_t knl_internal_update(knl_handle_t session, knl_cursor_t *cursor)
{
    uint32 i;
    table_t *table;
    index_t *index = NULL;
    knl_savepoint_t savepoint;
    dc_entity_t *entity = (dc_entity_t *)cursor->dc_entity;
    index_set_t *index_set = NULL;
    seg_stat_t temp_stat;
    heap_t *heap = CURSOR_HEAP(cursor);
    btree_t *btree = NULL;
    knl_session_t *se = (knl_session_t *)session;
    knl_part_locate_t new_part_loc;
    table = (table_t *)cursor->table;
    index_set = &table->index_set;

    if (!cursor->is_valid) {
        GS_THROW_ERROR(ERR_INVALID_CURSOR);
        return GS_ERROR;
    }

    knl_savepoint(session, &savepoint);
#ifdef _STATISTICS
    SEG_STATS_INIT(se, &temp_stat);
#endif
    /* check if it's need to do update overpart, if need, the new part no is stored in variable new_part_no */
    new_part_loc.part_no = GS_INVALID_ID32;
    new_part_loc.subpart_no = GS_INVALID_ID32;
    if (IS_PART_TABLE(table)) {
        if (part_prepare_crosspart_update(se, cursor, &new_part_loc) != GS_SUCCESS) {
            return GS_ERROR;
        }
    }

    if (part_check_update_crosspart(&new_part_loc, &cursor->part_loc)) {
        bool32 is_new_part_csf = knl_is_table_csf(entity, new_part_loc.part_no);
        bool32 is_old_part_csf = knl_is_table_csf(entity, cursor->part_loc.part_no);
        if (is_new_part_csf != is_old_part_csf) {
            knl_rollback(session, &savepoint);
            GS_THROW_ERROR(ERR_INVALID_OPERATION,
                ", cross partition update between different partition row types are forbidden");
            return GS_ERROR;
        }
        if (knl_crosspart_update(se, cursor, new_part_loc) != GS_SUCCESS) {
            knl_rollback(session, &savepoint);
            return GS_ERROR;
        }
        return GS_SUCCESS;
    }

    if (TABLE_ACCESSOR(cursor)->do_update(session, cursor) != GS_SUCCESS) {
        knl_rollback(session, &savepoint);
        return GS_ERROR;
    }

#ifdef _STATISTICS
    SEG_STATS_RECORD(se, temp_stat, &heap->stat);
#endif

    uint8 org_index_slot = cursor->index_slot;
    knl_handle_t org_index = cursor->index;

    for (i = 0; i < index_set->total_count; i++) {
        index = index_set->items[i];
        cursor->index_slot = i;
        cursor->index = index;

        if (IS_PART_INDEX(cursor->index)) {
            knl_panic_log(cursor->part_loc.part_no != GS_INVALID_ID32,
                "the part_no record on cursor is invalid, "
                "panic info: table %s index %s",
                table->desc.name, ((index_t *)cursor->index)->desc.name);
            index_part_t *index_part = INDEX_GET_PART(cursor->index, cursor->part_loc.part_no);
            if (IS_PARENT_IDXPART(&index_part->desc)) {
                knl_panic_log(cursor->part_loc.subpart_no != GS_INVALID_ID32,
                    "the subpart_no record on cursor is "
                    "invalid, panic info: table %s index %s",
                    table->desc.name, ((index_t *)cursor->index)->desc.name);
                index_part = PART_GET_SUBENTITY(index->part_index, index_part->subparts[cursor->part_loc.subpart_no]);
            }
            cursor->index_part = index_part;
        }

        btree = CURSOR_BTREE(cursor);
#ifdef _STATISTICS
        SEG_STATS_INIT(se, &temp_stat);
#endif

        if (knl_update_index_key(session, cursor) != GS_SUCCESS) {
            knl_rollback(session, &savepoint);
            knl_restore_cursor_index(cursor, org_index, org_index_slot);
            return GS_ERROR;
        }
#ifdef _STATISTICS
        SEG_STATS_RECORD(se, temp_stat, &btree->stat);
#endif
    }

    if (entity != NULL && entity->forbid_dml) {
        knl_rollback(session, &savepoint);
        GS_THROW_ERROR(ERR_CONSTRAINT_VIOLATED);
        knl_restore_cursor_index(cursor, org_index, org_index_slot);
        return GS_ERROR;
    }

    if (table->cons_set.ref_count > 0) {
        for (uint32 i = 0; i < table->cons_set.ref_count; i++) {
            if (knl_verify_ref_cons(se, cursor, table->cons_set.ref_cons[i]) != GS_SUCCESS) {
                knl_rollback(session, &savepoint);
                knl_restore_cursor_index(cursor, org_index, org_index_slot);
                return GS_ERROR;
            }
        }
    }

    if (table->cons_set.check_count > 0) {
        for (i = 0; i < table->cons_set.check_count; i++) {
            if (knl_verify_check_cons(se, cursor, table->cons_set.check_cons[i]) != GS_SUCCESS) {
                knl_rollback(session, &savepoint);
                knl_restore_cursor_index(cursor, org_index, org_index_slot);
                return GS_ERROR;
            }
        }
    }

    if (table->shadow_index != NULL) {
        if (knl_update_shadow_index(se, cursor, table->shadow_index, CURSOR_ACTION_UPDATE) != GS_SUCCESS) {
            knl_rollback(session, &savepoint);
            knl_restore_cursor_index(cursor, org_index, org_index_slot);
            return GS_ERROR;
        }
    }

    knl_restore_cursor_index(cursor, org_index, org_index_slot);

#ifdef _STATISTICS
    if (entity != NULL && STATS_ENABLE_MONITOR_TABLE(se)) {
        stats_monitor_table_change(cursor);
    }
#endif

    return GS_SUCCESS;
}

status_t knl_crosspart_update(knl_handle_t se, knl_cursor_t *cursor, knl_part_locate_t new_part_loc)
{
    row_head_t *old_row = NULL;
    row_head_t *new_row = NULL;
    knl_handle_t old_index_part = NULL;
    rowid_t old_rowid;
    knl_session_t *session = (knl_session_t *)se;

    knl_part_locate_t old_part_loc = cursor->part_loc;
    old_row = cursor->row;
    old_index_part = cursor->index_part;
    ROWID_COPY(old_rowid, cursor->rowid);
    CM_SAVE_STACK(session->stack);
    new_row = (row_head_t *)cm_push(session->stack, GS_MAX_ROW_SIZE);

    /* delete old row from the old part */
    if (knl_internal_delete(session, cursor) != GS_SUCCESS) {
        CM_RESTORE_STACK(session->stack);
        return GS_ERROR;
    }

    /* if row has been deleted by current stmt, we should return error, because we are updating row */
    if (SECUREC_UNLIKELY(!cursor->is_found)) {
        CM_RESTORE_STACK(session->stack);
        GS_THROW_ERROR(ERR_ROW_SELF_UPDATED);
        return GS_ERROR;
    }

    /* reorganize new row and copy lob data into new part */
    if (heap_prepare_update_overpart(session, cursor, new_row, new_part_loc) != GS_SUCCESS) {
        CM_RESTORE_STACK(session->stack);
        return GS_ERROR;
    }

    /* insert the new row into the new part */
    cursor->row = new_row;
    if (knl_internal_insert(session, cursor) != GS_SUCCESS) {
        ROWID_COPY(cursor->rowid, old_rowid);
        cursor->row = old_row;
        knl_set_table_part(cursor, old_part_loc);
        CM_RESTORE_STACK(session->stack);
        return GS_ERROR;
    }

    ROWID_COPY(cursor->rowid, old_rowid);
    cursor->row = old_row;
    knl_set_table_part(cursor, old_part_loc);
    cursor->index_part = old_index_part;
    CM_RESTORE_STACK(session->stack);
    return GS_SUCCESS;
}

status_t knl_fetch_by_rowid(knl_handle_t session, knl_cursor_t *cursor, bool32 *is_found)
{
    if (cursor->isolevel == ISOLATION_CURR_COMMITTED) {
        cursor->query_scn = DB_CURR_SCN((knl_session_t *)session);
        cursor->cc_cache_time = KNL_NOW((knl_session_t *)session);
    }

    if (TABLE_ACCESSOR(cursor)->do_fetch_by_rowid(session, cursor) != GS_SUCCESS) {
        return GS_ERROR;
    }

    *is_found = cursor->is_found;

    return GS_SUCCESS;
}

status_t knl_fetch(knl_handle_t session, knl_cursor_t *cursor)
{
    if (cursor->eof) {
        return GS_SUCCESS;
    }

    if (!cursor->is_valid) {
        GS_THROW_ERROR(ERR_INVALID_CURSOR);
        return GS_ERROR;
    }

    return cursor->fetch(session, cursor);
}

/*
 * kernel copy row
 * copy cursor row to dest cursor row
 * @note lob locator data would be re-generated and lob chunk data would be copied to
 * @param kernel session, src cursor, dest cursor
 */
status_t knl_copy_row(knl_handle_t handle, knl_cursor_t *src, knl_cursor_t *dest)
{
    knl_session_t *session = (knl_session_t *)handle;
    dc_entity_t *entity = (dc_entity_t *)src->dc_entity;
    dc_entity_t *mentity = (dc_entity_t *)dest->dc_entity;
    row_head_t *row = src->row;
    knl_column_t *column = NULL;
    row_assist_t ra;
    uint16 i;
    knl_put_row_column_t put_col_func = row->is_csf ? heap_put_csf_row_column : heap_put_bmp_row_column;

    lob_locator_t *locator = NULL;

    cm_row_init(&ra, (char *)dest->row, KNL_MAX_ROW_SIZE, ROW_COLUMN_COUNT(row), row->is_csf);

    for (i = 0; i < ROW_COLUMN_COUNT(row); i++) {
        column = dc_get_column(entity, i);

        if (!COLUMN_IS_LOB(column) || CURSOR_COLUMN_SIZE(src, i) == GS_NULL_VALUE_LEN) {
            put_col_func(row, src->offsets, src->lens, i, &ra);
            continue;
        }

        locator = (lob_locator_t *)CURSOR_COLUMN_DATA(src, i);

        if (knl_row_move_lob(session, dest, dc_get_column(mentity, i), locator, &ra) != GS_SUCCESS) {
            return GS_ERROR;
        }
    }
    row_end(&ra);

    return GS_SUCCESS;
}

/*
 * kernel lock row interface
 * lock the cursor row just fetched by cursor rowid
 * @note transaction wait maybe heap during locking, caller must set the correct
 * cursor action 'cause the lock behavior is different depending on cursor action.
 * @param session handle, kernel cursor, is_found(result)
 */
status_t knl_lock_row(knl_handle_t session, knl_cursor_t *cursor, bool32 *is_found)
{
    *is_found = GS_FALSE;

    if (cursor->eof) {
        return GS_SUCCESS;
    }

    if (!cursor->is_valid) {
        GS_THROW_ERROR(ERR_INVALID_CURSOR);
        return GS_ERROR;
    }

    if (cursor->action <= CURSOR_ACTION_SELECT) {
        GS_THROW_ERROR(ERR_INVALID_CURSOR);
        return GS_ERROR;
    }

    /* can't lock non-existent row */
    if (!cursor->is_found) {
        return GS_SUCCESS;
    }

    if (TABLE_ACCESSOR(cursor)->do_lock_row(session, cursor, &cursor->is_found) != GS_SUCCESS) {
        return GS_ERROR;
    }

    *is_found = cursor->is_found;
    return GS_SUCCESS;
}

knl_column_t *knl_get_column(knl_handle_t dc_entity, uint32 id)
{
    return dc_get_column((dc_entity_t *)dc_entity, id);
}

knl_table_desc_t *knl_get_table(knl_dictionary_t *dc)
{
    dc_entity_t *entity = DC_ENTITY(dc);
    return &entity->table.desc;
}

status_t knl_get_view_sub_sql(knl_handle_t session, knl_dictionary_t *dc, text_t *sql, uint32 *page_id)
{
    dc_entity_t *entity = DC_ENTITY(dc);
    knl_session_t *knl_session = (knl_session_t *)session;

    *page_id = GS_INVALID_ID32;

    if (entity->view.sub_sql.str != NULL) {
        *sql = entity->view.sub_sql;
        return GS_SUCCESS;
    }

    if (entity->view.sub_sql.len + 1 >= GS_LARGE_PAGE_SIZE) {
        return GS_ERROR;
    }

    knl_begin_session_wait(knl_session, LARGE_POOL_ALLOC, GS_FALSE);
    if (mpool_alloc_page_wait(knl_session->kernel->attr.large_pool, page_id, CM_MPOOL_ALLOC_WAIT_TIME) != GS_SUCCESS) {
        knl_end_session_wait(knl_session);
        return GS_ERROR;
    }
    knl_end_session_wait(knl_session);

    sql->len = entity->view.sub_sql.len;
    sql->str = mpool_page_addr(knl_session->kernel->attr.large_pool, *page_id);

    if (knl_read_lob(session, entity->view.lob, 0, sql->str, sql->len + 1, NULL) != GS_SUCCESS) {
        mpool_free_page(knl_session->kernel->attr.large_pool, *page_id);
        *page_id = GS_INVALID_ID32;
        return GS_ERROR;
    }

    sql->str[sql->len] = '\0';
    return GS_SUCCESS;
}

dynview_desc_t *knl_get_dynview(knl_dictionary_t *dc)
{
    dc_entity_t *entity = DC_ENTITY(dc);
    return entity->dview;
}

/*
 * Get the serial cached value from a table's dc_entity
 */
status_t knl_get_serial_cached_value(knl_handle_t session, knl_handle_t dc_entity, int64 *value)
{
    dc_entry_t *entry = NULL;
    dc_entity_t *entity = (dc_entity_t *)dc_entity;
    if (entity->has_serial_col != GS_TRUE) {
        GS_THROW_ERROR(ERR_NO_AUTO_INCREMENT_COLUMN);
        return GS_ERROR;
    }

    if (entity->type == DICT_TYPE_TEMP_TABLE_SESSION || entity->type == DICT_TYPE_TEMP_TABLE_TRANS) {
        knl_temp_cache_t *temp_table = NULL;

        if (knl_ensure_temp_cache(session, entity, &temp_table) != GS_SUCCESS) {
            return GS_ERROR;
        }

        if (temp_table->serial == 0) {
            temp_table->serial = entity->table.desc.serial_start;
        }
        *value = temp_table->serial;
        return GS_SUCCESS;
    }

    if (entity->table.heap.segment == NULL) {
        *value = entity->table.desc.serial_start;
        return GS_SUCCESS;
    }

    entry = entity->entry;
    cm_spin_lock(&entry->serial_lock, NULL);
    *value = HEAP_SEGMENT(entity->table.heap.entry, entity->table.heap.segment)->serial;
    cm_spin_unlock(&entry->serial_lock);

    return GS_SUCCESS;
}

status_t knl_get_serial_value(knl_handle_t handle, knl_handle_t dc_entity, int64 *value)
{
    uint32 residue = 1;
    knl_session_t *session = (knl_session_t *)handle;
    dc_entity_t *entity = (dc_entity_t *)dc_entity;
    dc_entry_t *entry = entity->entry;

    if (lock_table_shared(session, dc_entity, LOCK_INF_WAIT) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (entity->type == DICT_TYPE_TEMP_TABLE_SESSION || entity->type == DICT_TYPE_TEMP_TABLE_TRANS) {
        knl_temp_cache_t *temp_table = NULL;

        if (knl_ensure_temp_cache(session, entity, &temp_table) != GS_SUCCESS) {
            return GS_ERROR;
        }

        if (temp_table->serial == 0) {
            temp_table->serial = entity->table.desc.serial_start;
            if (temp_table->serial == 0) {
                temp_table->serial++;
            }
        }
        *value = temp_table->serial++;
        return GS_SUCCESS;
    }

    if (entity->table.heap.segment == NULL) {
        if (heap_create_entry(session, &entity->table.heap) != GS_SUCCESS) {
            return GS_ERROR;
        }
    }

    cm_spin_lock(&entry->serial_lock, NULL);

    if (entry->version != session->kernel->dc_ctx.version) {
        entry->serial_value = 0;
        entry->version = session->kernel->dc_ctx.version;
    }

    if (entry->serial_value == 0) {
        entry->serial_value = HEAP_SEGMENT(entity->table.heap.entry, entity->table.heap.segment)->serial;

        if (entry->serial_value == 0) {
            entry->serial_value = 1;
        }
    }

    if (entity->table.desc.serial_start == 0) {
        residue = 1;
    } else {
        residue = 0;
    }

    *value = entry->serial_value;
    if (*value == GS_INVALID_INT64) {
        cm_spin_unlock(&entry->serial_lock);
        return GS_ERROR;
    }

    if (*value >= GS_INVALID_INT64 - GS_SERIAL_CACHE_COUNT) {
        if (GS_INVALID_INT64 != HEAP_SEGMENT(entity->table.heap.entry, entity->table.heap.segment)->serial) {
            heap_update_serial(session, &entity->table.heap, GS_INVALID_INT64);
        }
    } else if(GS_SERIAL_CACHE_COUNT==1) {
        heap_update_serial(session, &entity->table.heap, entry->serial_value + GS_SERIAL_CACHE_COUNT);
    } else if ((entry->serial_value - entity->table.desc.serial_start) % GS_SERIAL_CACHE_COUNT == residue) {
        heap_update_serial(session, &entity->table.heap, entry->serial_value + GS_SERIAL_CACHE_COUNT);
    }

    entry->serial_value++;

    cm_spin_unlock(&entry->serial_lock);

    return GS_SUCCESS;
}

status_t knl_reset_serial_value(knl_handle_t handle, knl_handle_t dc_entity)
{
    knl_session_t *session = (knl_session_t *)handle;
    dc_entity_t *entity = (dc_entity_t *)dc_entity;
    dc_entry_t *entry = entity->entry;

    if (lock_table_shared(session, dc_entity, LOCK_INF_WAIT) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (entity->type == DICT_TYPE_TEMP_TABLE_SESSION || entity->type == DICT_TYPE_TEMP_TABLE_TRANS) {
        knl_temp_cache_t *temp_table = NULL;

        if (knl_ensure_temp_cache(session, entity, &temp_table) != GS_SUCCESS) {
            return GS_ERROR;
        }

        temp_table->serial = entity->table.desc.serial_start;
        return GS_SUCCESS;
    }

    cm_spin_lock(&entry->serial_lock, NULL);
    entry->serial_value = entity->table.desc.serial_start;
    if (entity->table.heap.segment != NULL) {
        heap_update_serial(session, &entity->table.heap, entity->table.desc.serial_start);
    }
    cm_spin_unlock(&entry->serial_lock);

    return GS_SUCCESS;
}

uint32 knl_get_column_count(knl_handle_t dc_entity)
{
    return ((dc_entity_t *)dc_entity)->column_count;
}

uint16 knl_get_column_id(knl_dictionary_t *dc, text_t *name)
{
    dc_entity_t *entity = DC_ENTITY(dc);
    knl_column_t *column = NULL;
    uint32 hash;
    uint16 index;
    char column_name[GS_NAME_BUFFER_SIZE];

    (void)cm_text2str(name, column_name, GS_NAME_BUFFER_SIZE);
    hash = cm_hash_string(column_name, entity->column_count);
    index = DC_GET_COLUMN_INDEX(entity, hash);

    while (index != GS_INVALID_ID16) {
        column = dc_get_column(entity, index);
        if (strcmp(column->name, column_name) == 0) {
            return index;
        }

        index = column->next;
    }

    return GS_INVALID_ID16;
}

uint32 knl_get_index_count(knl_handle_t dc_entity)
{
    dc_entity_t *entity = (dc_entity_t *)dc_entity;

    if (!(entity->type == DICT_TYPE_TABLE || entity->type == DICT_TYPE_TEMP_TABLE_SESSION ||
        entity->type == DICT_TYPE_TEMP_TABLE_TRANS || entity->type == DICT_TYPE_TABLE_NOLOGGING)) {
        return 0;
    }

    return entity->table.index_set.count;
}

knl_index_desc_t *knl_get_index(knl_handle_t dc_entity, uint32 index_id)
{
    dc_entity_t *entity = (dc_entity_t *)dc_entity;
    index_set_t *index_set;

    index_set = &entity->table.index_set;
    return &(index_set->items[index_id]->desc);
}

static status_t knl_internal_create_table(knl_session_t *session, knl_table_def_t *def, bool32 *is_existed)
{
    knl_constraint_def_t *cons = NULL;
    knl_reference_def_t *ref = NULL;
    dc_entry_t *entry = NULL;
    knl_dict_type_t obj_type;
    rd_create_table_t redo;
    table_t table;
    errno_t ret;
    bool32 has_logic = session->kernel->db.ctrl.core.lrep_mode == LOG_REPLICATION_ON;
    uint32 op_type = RD_CREATE_TABLE;

    *is_existed = GS_FALSE;
    if ((def->options & CREATE_IF_NOT_EXISTS) && dc_object_exists(session, &def->schema, &def->name, &obj_type)) {
        if (IS_TABLE_BY_TYPE(obj_type)) {
            *is_existed = GS_TRUE;
            GS_LOG_RUN_INF("table exists.");
            GS_THROW_ERROR(ERR_OBJECT_EXISTS, "table", def->name.str);
            return GS_ERROR;
        }
    }

    if (def->type == TABLE_TYPE_HEAP) {
        log_append_lrep_info(session, op_type, has_logic);
    }

    if (db_create_table(session, def, &table) != GS_SUCCESS) {
        session->rm->is_ddl_op = GS_FALSE;
        knl_rollback(session, NULL);
        if (def->options & CREATE_IF_NOT_EXISTS) {
            int32 err_code = cm_get_error_code();
            if (err_code == ERR_DUPLICATE_TABLE) {
                *is_existed = GS_TRUE;
                cm_reset_error();
                return GS_SUCCESS;
            }
        }
        return GS_ERROR;
    }

    redo.op_type = RD_CREATE_TABLE;
    redo.uid = table.desc.uid;
    redo.oid = table.desc.id;
    ret = strcpy_sp(redo.obj_name, GS_NAME_BUFFER_SIZE, table.desc.name);
    knl_securec_check(ret);

    log_put(session, RD_LOGIC_OPERATION, &redo, sizeof(rd_create_table_t),
        has_logic ? LOG_ENTRY_FLAG_WITH_LOGIC_OID : LOG_ENTRY_FLAG_NONE);

    if (has_logic) {
        session->rm->is_ddl_op = GS_FALSE;
    }

    // create table as select need lock table before dc is ready
    if (def->create_as_select) {
        dc_user_t *user = NULL;
        if (dc_open_user(session, &def->schema, &user) != GS_SUCCESS) {
            dc_free_broken_entry(session, table.desc.uid, table.desc.id);
            return GS_ERROR;
        }

        entry = DC_GET_ENTRY(user, table.desc.id);
        if (dc_try_lock_table_ux(session, entry) != GS_SUCCESS) {
            dc_free_broken_entry(session, table.desc.uid, table.desc.id);
            return GS_ERROR;
        }
    }

    knl_commit(session);

    dc_ready(session, table.desc.uid, table.desc.id);
    /* unlock parent tables of references constraints */
    for (uint32 i = 0; i < def->constraints.count; i++) {
        cons = (knl_constraint_def_t *)cm_galist_get(&def->constraints, i);
        if (cons->type != CONS_TYPE_REFERENCE) {
            continue;
        }

        ref = &cons->ref;

        if (ref->ref_dc.handle != NULL) {
            dc_invalidate(session, (dc_entity_t *)ref->ref_dc.handle);
            dc_close(&ref->ref_dc);
        }
    }
    return GS_SUCCESS;
}

status_t knl_create_table_as_select(knl_handle_t session, knl_handle_t stmt, knl_table_def_t *def)
{
    knl_session_t *se = (knl_session_t *)session;
    latch_t *ddl_latch = &se->kernel->db.ddl_latch;
    dc_user_t *user = NULL;
    status_t status;
    bool32 is_exist = GS_FALSE;

    if (knl_ddl_enabled(se, GS_FALSE) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (dc_open_user(se, &def->schema, &user) != GS_SUCCESS) {
        return GS_ERROR;
    }

    cm_latch_s(&user->user_latch, se->id, GS_FALSE, NULL);
    if (knl_ddl_latch_s(ddl_latch, session, NULL) != GS_SUCCESS) {
        cm_unlatch(&user->user_latch, NULL);
        return GS_ERROR;
    }

    if (knl_internal_create_table(se, def, &is_exist) != GS_SUCCESS) {
        unlock_tables_directly(se);
        cm_unlatch(ddl_latch, NULL);
        cm_unlatch(&user->user_latch, NULL);
        return GS_ERROR;
    }

    // is_exist is true, hasn't locked the table
    if (is_exist) {
        cm_unlatch(ddl_latch, NULL);
        cm_unlatch(&user->user_latch, NULL);
        return GS_SUCCESS;
    }

    status = g_knl_callback.import_rows(stmt, BATCH_COMMIT_COUNT);
    if (status != GS_SUCCESS) {
        knl_rollback(se, NULL);
        knl_drop_def_t drop_def = { { 0 } };
        drop_def.purge = GS_TRUE;
        drop_def.name = def->name;
        drop_def.owner = def->schema;
        if (knl_internal_drop_table(se, &drop_def) != GS_SUCCESS) {
            unlock_tables_directly(se);
            cm_unlatch(ddl_latch, NULL);
            cm_unlatch(&user->user_latch, NULL);
            return GS_ERROR;
        }
    }
    unlock_tables_directly(se);
    cm_unlatch(ddl_latch, NULL);
    cm_unlatch(&user->user_latch, NULL);
    return status;
}

status_t knl_create_table(knl_handle_t session, knl_table_def_t *def)
{
    knl_session_t *se = (knl_session_t *)session;
    latch_t *ddl_latch = &se->kernel->db.ddl_latch;
    dc_user_t *user = NULL;
    status_t status;
    bool32 is_existed = GS_FALSE;

    if (!se->is_begin_transaction) {    // 避免新增流计算任务重复任务名或目标表名冲突
        knl_commit(session); // 确保每次ddl操作前所有的事务都已提交，参考业界标准
    }

    if (knl_ddl_enabled(session, GS_FALSE) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (dc_open_user(se, &def->schema, &user) != GS_SUCCESS) {
        return GS_ERROR;
    }

    cm_latch_s(&user->user_latch, se->id, GS_FALSE, NULL);
    if (knl_ddl_latch_s(ddl_latch, session, NULL) != GS_SUCCESS) {
        cm_unlatch(&user->user_latch, NULL);
        return GS_ERROR;
    }

    status = knl_internal_create_table(session, def, &is_existed);
    unlock_tables_directly(session);
    cm_unlatch(ddl_latch, NULL);
    cm_unlatch(&user->user_latch, NULL);

    return status;
}

status_t knl_create_view(knl_handle_t session, knl_view_def_t *def)
{
    knl_session_t *se = (knl_session_t *)session;
    dc_user_t *user = NULL;

    if (knl_ddl_enabled(session, GS_FALSE) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (dc_open_user(se, &def->user, &user) != GS_SUCCESS) {
        return GS_ERROR;
    }

    cm_latch_s(&user->user_latch, se->id, GS_FALSE, NULL);
    if (db_create_view((knl_session_t *)session, def) != GS_SUCCESS) {
        cm_unlatch(&user->user_latch, NULL);
        return GS_ERROR;
    }

    cm_unlatch(&user->user_latch, NULL);
    return GS_SUCCESS;
}

status_t knl_create_or_replace_view(knl_handle_t session, knl_view_def_t *def)
{
    if (knl_ddl_enabled(session, GS_FALSE) != GS_SUCCESS) {
        return GS_ERROR;
    }
    return db_create_or_replace_view((knl_session_t *)session, def);
}

static inline void knl_set_new_space_type(knl_handle_t se, knl_space_def_t *def)
{
    knl_session_t *session = (knl_session_t *)se;
    if (cm_text_equal_ins(&def->name, &g_temp2_undo) && (DB_CORE_CTRL(session)->temp_undo_space == 0)) {
        def->extent_size = UNDO_EXTENT_SIZE;
        def->type = SPACE_TYPE_UNDO | SPACE_TYPE_TEMP | SPACE_TYPE_DEFAULT;
    } else if (cm_text_equal_ins(&def->name, &g_temp2) && (DB_CORE_CTRL(session)->temp_space == 0)) {
        def->type = SPACE_TYPE_TEMP | SPACE_TYPE_USERS | SPACE_TYPE_DEFAULT;
    } else if (cm_text_equal_ins(&def->name, &g_sysaux) && (DB_CORE_CTRL(session)->sysaux_space == 0)) {
        def->type = SPACE_TYPE_SYSAUX | SPACE_TYPE_DEFAULT;
    }
    return;
}

static void knl_save_core_space_type(knl_handle_t se, knl_space_def_t *def, latch_t *ddl_latch, uint32 space_id)
{
    knl_session_t *session = (knl_session_t *)se;
    if (def->type == (SPACE_TYPE_UNDO | SPACE_TYPE_TEMP | SPACE_TYPE_DEFAULT)) {
        undo_context_t *ctx = &session->kernel->undo_ctx;
        ctx->temp_space = spc_get_temp_undo(session);
        DB_CORE_CTRL(session)->temp_undo_space = space_id;
    } else if (def->type == (SPACE_TYPE_TEMP | SPACE_TYPE_USERS | SPACE_TYPE_DEFAULT)) {
        DB_CORE_CTRL(session)->temp_space = space_id;
    } else if (def->type == (SPACE_TYPE_SYSAUX | SPACE_TYPE_DEFAULT)) {
        DB_CORE_CTRL(session)->sysaux_space = space_id;
    } else {
        return;
    }

    if (db_save_core_ctrl(session) != GS_SUCCESS) {
        cm_unlatch(ddl_latch, NULL);
        CM_ABORT(0, "[SPACE] ABORT INFO: save core control space file failed when create space %s", T2S(&(def->name)));
    }

    return;
}
status_t knl_create_space(knl_handle_t session, knl_space_def_t *def)
{
    knl_session_t *se = (knl_session_t *)session;
    latch_t *ddl_latch = &se->kernel->db.ddl_latch;
    uint32 space_id;

    if (DB_IS_READONLY(se)) {
        GS_THROW_ERROR(ERR_CAPABILITY_NOT_SUPPORT, "operation on read only mode");
        return GS_ERROR;
    }

    if (def->in_memory == GS_TRUE) {
        GS_THROW_ERROR(ERR_CAPABILITY_NOT_SUPPORT, "create space all in memory");
        return GS_ERROR;
    }

    knl_set_new_space_type(session, def);

    if (knl_ddl_latch_x(ddl_latch, session, NULL) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (spc_create_space_precheck(se, def) != GS_SUCCESS) {
        cm_unlatch(ddl_latch, NULL);
        return GS_ERROR;
    }

    if (spc_create_space(se, def, &space_id) != GS_SUCCESS) {
        cm_unlatch(ddl_latch, NULL);
        return GS_ERROR;
    }

    if (def->type & SPACE_TYPE_DEFAULT) {
        knl_save_core_space_type(session, def, ddl_latch, space_id);
    }

    cm_unlatch(ddl_latch, NULL);

    return GS_SUCCESS;
}

status_t knl_alter_space(knl_handle_t session, knl_altspace_def_t *def)
{
    knl_session_t *se = (knl_session_t *)session;
    status_t status = GS_ERROR;
    uint32 space_id;
    latch_t *ddl_latch = &se->kernel->db.ddl_latch;

    if (DB_IS_READONLY(se)) {
        GS_THROW_ERROR(ERR_CAPABILITY_NOT_SUPPORT, "operation on read only mode");
        return GS_ERROR;
    }

    if (knl_ddl_latch_x(ddl_latch, session, NULL) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (GS_SUCCESS != spc_get_space_id(se, &def->name, &space_id)) {
        cm_unlatch(ddl_latch, NULL);
        return GS_ERROR;
    }

    space_t *space = KNL_GET_SPACE(se, space_id);

    switch (def->action) {
        case ALTSPACE_ADD_DATAFILE:
            status = spc_create_datafiles(se, space, def);
            break;
        case ALTSPACE_SET_AUTOEXTEND:
            status = spc_set_autoextend(se, space, &def->autoextend);
            break;
        case ALTSPACE_SET_AUTOOFFLINE:
            status = spc_set_autooffline(se, space, def->auto_offline);
            break;
        case ALTSPACE_DROP_DATAFILE:
            status = spc_drop_datafiles(se, space, &def->datafiles);
            break;
        case ALTSPACE_RENAME_SPACE:
            status = spc_rename_space(se, space, &def->rename_space);
            break;
        case ALTSPACE_OFFLINE_DATAFILE:
            status = spc_offline_datafiles(se, space, &def->datafiles);
            break;
        case ALTSPACE_RENAME_DATAFILE:
            status = spc_rename_datafiles(se, space, &def->datafiles, &def->rename_datafiles);
            break;
        case ALTSPACE_SET_AUTOPURGE:
            status = spc_set_autopurge(se, space, def->auto_purge);
            break;
        case ALTSPACE_SHRINK_SPACE:
            status = spc_shrink_space(se, space, &def->shrink);
            break;
        case ALTSPACE_PUNCH:
            status = spc_punch_hole(se, space, def->punch_size);
            break;
        default:
            status = GS_ERROR;
            break;
    };

    if (IS_SWAP_SPACE(space)) {
        se->temp_pool->get_swap_extents = 0;
    }

    space->allow_extend = GS_TRUE;
    cm_unlatch(ddl_latch, NULL);

    return status;
}

status_t knl_set_commit(knl_handle_t session, knl_commit_def_t *def)
{
    knl_session_t *se = (knl_session_t *)session;
    status_t status = GS_SUCCESS;

    switch (def->action) {
        case COMMIT_LOGGING:
            se->commit_batch = (bool8)def->batch;
            break;
        case COMMIT_WAIT:
            se->commit_nowait = (bool8)def->nowait;
            break;
        default:
            status = GS_ERROR;
            break;
    }

    return status;
}

void knl_set_lockwait_timeout(knl_handle_t session, knl_lockwait_def_t *def)
{
    knl_session_t *se = (knl_session_t *)session;
    se->lock_wait_timeout = def->lock_wait_timeout;
}
static status_t db_check_ddm_rule_by_obj(knl_session_t *session, uint32 uid, uint32 oid)
{
    knl_cursor_t *cursor = NULL;
    CM_SAVE_STACK(session->stack);
    cursor = knl_push_cursor(session);
    knl_scan_key_t *l_key = NULL;
    knl_scan_key_t *r_key = NULL;

    knl_open_sys_cursor(session, cursor, CURSOR_ACTION_SELECT, SYS_DDM_ID, IX_SYS_DDM_001_ID);
    knl_init_index_scan(cursor, GS_FALSE);
    l_key = &cursor->scan_range.l_key;
    knl_set_scan_key(INDEX_DESC(cursor->index), l_key, GS_TYPE_INTEGER, &uid, sizeof(uint32), IX_COL_SYS_DDM_001_UID);
    knl_set_scan_key(INDEX_DESC(cursor->index), l_key, GS_TYPE_INTEGER, &oid, sizeof(uint32), IX_COL_SYS_DDM_001_OID);
    knl_set_key_flag(l_key, SCAN_KEY_LEFT_INFINITE, IX_COL_SYS_DDM_001_COLID);
    r_key = &cursor->scan_range.r_key;
    knl_set_scan_key(INDEX_DESC(cursor->index), r_key, GS_TYPE_INTEGER, &uid, sizeof(uint32), IX_COL_SYS_DDM_001_UID);
    knl_set_scan_key(INDEX_DESC(cursor->index), r_key, GS_TYPE_INTEGER, &oid, sizeof(uint32), IX_COL_SYS_DDM_001_OID);
    knl_set_key_flag(r_key, SCAN_KEY_RIGHT_INFINITE, IX_COL_SYS_DDM_001_COLID);

    if (knl_fetch(session, cursor) != GS_SUCCESS) {
        CM_RESTORE_STACK(session->stack);
        return GS_ERROR;
    }
    if (cursor->eof == GS_FALSE) {
        CM_RESTORE_STACK(session->stack);
        GS_THROW_ERROR_EX(ERR_INVALID_OPERATION, ", the table has rule, please drop rule firstly.");
        return GS_ERROR;
    }
    CM_RESTORE_STACK(session->stack);
    return GS_SUCCESS;
}

status_t knl_drop_space(knl_handle_t session, knl_drop_space_def_t *def)
{
    knl_session_t *se = (knl_session_t *)session;
    space_t *space = NULL;
    uint32 space_id;
    status_t status;
    latch_t *ddl_latch = &se->kernel->db.ddl_latch;

    if (DB_IS_READONLY(se)) {
        GS_THROW_ERROR(ERR_CAPABILITY_NOT_SUPPORT, "operation on read only mode");
        return GS_ERROR;
    }

    if (se->kernel->db.status != DB_STATUS_OPEN) {
        GS_THROW_ERROR(ERR_DATABASE_NOT_OPEN, "drop tablespace");
        return GS_ERROR;
    }

    if (knl_ddl_latch_x(ddl_latch, session, NULL) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (spc_get_space_id(se, &def->obj_name, &space_id) != GS_SUCCESS) {
        cm_unlatch(ddl_latch, NULL);
        return GS_ERROR;
    }

    space = KNL_GET_SPACE(se, space_id);
    if (SPACE_IS_DEFAULT(space)) {
        GS_THROW_ERROR(ERR_INVALID_OPERATION, ",forbid to drop database system space");
        cm_unlatch(ddl_latch, NULL);
        return GS_ERROR;
    }

    if (!SPACE_IS_ONLINE(space)) {
        status = spc_drop_offlined_space(se, space, def->options);
    } else {
        status = spc_drop_online_space(se, space, def->options);
    }

    if (status == GS_SUCCESS) {
        status = spc_try_inactive_swap_encrypt(se);
    }
    cm_unlatch(ddl_latch, NULL);

    return status;
}

status_t knl_create_user(knl_handle_t session, knl_user_def_t *def)
{
    knl_session_t *se = (knl_session_t *)session;
    status_t status;
    latch_t *ddl_latch = &se->kernel->db.ddl_latch;

    if (knl_ddl_enabled(session, GS_FALSE) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (knl_ddl_latch_s(ddl_latch, session, NULL) != GS_SUCCESS) {
        return GS_ERROR;
    }
    status = user_create(se, def);
    cm_unlatch(ddl_latch, NULL);

    return status;
}

status_t knl_drop_user(knl_handle_t session, knl_drop_user_t *def)
{
    knl_session_t *se = (knl_session_t *)session;
    dc_user_t *user = NULL;
    status_t status;
    latch_t *ddl_latch = &se->kernel->db.ddl_latch;

    if (knl_ddl_enabled(session, GS_TRUE) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (dc_open_user(se, &def->owner, &user) != GS_SUCCESS) {
        return GS_ERROR;
    }
    cm_latch_x(&user->user_latch, se->id, NULL);
    if (knl_ddl_latch_s(ddl_latch, session, NULL) != GS_SUCCESS) {
        cm_unlatch(&user->user_latch, NULL);
        return GS_ERROR;
    }
    status = user_drop(se, def);
    cm_unlatch(ddl_latch, NULL);
    cm_unlatch(&user->user_latch, NULL);

    return status;
}
static status_t knl_refresh_sys_pwd(knl_session_t *session, knl_user_def_t *def)
{
    text_t owner;
    dc_user_t *user = NULL;
    cm_str2text(def->name, &owner);
    if (dc_open_user_direct(session, &owner, &user) != GS_SUCCESS) {
        return GS_ERROR;
    }
    if (cm_alter_config(session->kernel->attr.config, "_SYS_PASSWORD", user->desc.password, CONFIG_SCOPE_DISK,
        GS_TRUE) != GS_SUCCESS) {
        return GS_ERROR;
    }
    return GS_SUCCESS;
}
status_t knl_alter_user(knl_handle_t session, knl_user_def_t *def)
{
    knl_session_t *se = (knl_session_t *)session;
    status_t status = GS_SUCCESS;
    latch_t *ddl_latch = &se->kernel->db.ddl_latch;

    if (knl_ddl_enabled(session, GS_TRUE) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (knl_ddl_latch_s(ddl_latch, session, NULL) != GS_SUCCESS) {
        return GS_ERROR;
    }
    if (user_alter(se, def) != GS_SUCCESS) {
        cm_unlatch(ddl_latch, NULL);
        return GS_ERROR;
    }
    if (GS_BIT_TEST(def->mask, GS_GET_MASK(ALTER_USER_FIELD_PASSWORD))) {
        GS_LOG_RUN_WAR("user password of %s has been changed successfully", def->name);
        GS_LOG_ALARM(WARN_PASSWDCHANGE, "user : %s", def->name);
        if (cm_str_equal_ins(def->name, SYS_USER_NAME)) {
            status = knl_refresh_sys_pwd(session, def);
        }
    }
    cm_unlatch(ddl_latch, NULL);

    return status;
}

status_t knl_create_role(knl_handle_t session, knl_role_def_t *def)
{
    if (knl_ddl_enabled(session, GS_FALSE) != GS_SUCCESS) {
        return GS_ERROR;
    }
    return user_create_role((knl_session_t *)session, def);
}

status_t knl_drop_role(knl_handle_t session, knl_drop_def_t *def)
{
    if (knl_ddl_enabled(session, GS_TRUE) != GS_SUCCESS) {
        return GS_ERROR;
    }
    return user_drop_role((knl_session_t *)session, def);
}

status_t knl_create_tenant(knl_handle_t session, knl_tenant_def_t *def)
{
    knl_session_t *se = (knl_session_t *)session;
    latch_t *ddl_latch = &se->kernel->db.ddl_latch;
    uint32 id = se->id;
    status_t status;

    CM_MAGIC_CHECK(def, knl_tenant_def_t);

    if (knl_ddl_enabled(session, GS_FALSE) != GS_SUCCESS) {
        return GS_ERROR;
    }

    cm_latch_x(ddl_latch, id, NULL);
    status = tenant_create(se, def);
    cm_unlatch(ddl_latch, NULL);

    return status;
}

status_t knl_drop_tenant(knl_handle_t session, knl_drop_tenant_t *def)
{
    knl_session_t *se = (knl_session_t *)session;
    latch_t *ddl_latch = &se->kernel->db.ddl_latch;
    uint32 id = se->id;
    status_t status;

    CM_MAGIC_CHECK(def, knl_drop_tenant_t);

    if (knl_ddl_enabled(session, GS_FALSE) != GS_SUCCESS) {
        return GS_ERROR;
    }

    cm_latch_x(ddl_latch, id, NULL);
    status = tenant_drop(se, def);
    cm_unlatch(ddl_latch, NULL);

    return status;
}

status_t knl_create_sequence(knl_handle_t session, knl_sequence_def_t *def)
{
    knl_session_t *se = (knl_session_t *)session;
    dc_user_t *user = NULL;

    if (knl_ddl_enabled(session, GS_FALSE) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (dc_open_user(se, &def->user, &user) != GS_SUCCESS) {
        return GS_ERROR;
    }

    cm_latch_s(&user->user_latch, se->id, GS_FALSE, NULL);
    if (db_create_sequence((knl_session_t *)session, def) != GS_SUCCESS) {
        cm_unlatch(&user->user_latch, NULL);
        return GS_ERROR;
    }

    cm_unlatch(&user->user_latch, NULL);
    return GS_SUCCESS;
}

status_t knl_alter_sequence(knl_handle_t session, knl_sequence_def_t *def)
{
    if (knl_ddl_enabled(session, GS_TRUE) != GS_SUCCESS) {
        return GS_ERROR;
    }
    return db_alter_sequence((knl_session_t *)session, def);
}

status_t knl_alter_seq_nextval(knl_handle_t session, knl_sequence_def_t *def, int64 value)
{
    return db_alter_seq_nextval((knl_session_t *)session, def, value);
}

status_t knl_get_seq_def(knl_handle_t session, text_t *user, text_t *name, knl_sequence_def_t *def)
{
    return db_get_seq_def((knl_session_t *)session, user, name, def);
}

status_t knl_seq_nextval(knl_handle_t session, text_t *user, text_t *name, int64 *nextval)
{
    return db_next_seq_value((knl_session_t *)session, user, name, nextval);
}

status_t knl_get_nextval_for_cn(knl_handle_t session, text_t *user, text_t *name, int64 *value)
{
    return db_get_nextval_for_cn((knl_session_t *)session, user, name, value);
}

status_t knl_seq_multi_val(knl_handle_t session, knl_sequence_def_t *def, uint32 group_order, uint32 group_cnt,
    uint32 count)
{
    return db_multi_seq_value((knl_session_t *)session, def, group_order, group_cnt, count);
}

status_t knl_seq_currval(knl_handle_t session, text_t *user, text_t *name, int64 *nextval)
{
    return db_current_seq_value((knl_session_t *)session, user, name, nextval);
}

status_t knl_get_seq_dist_data(knl_handle_t session, text_t *user, text_t *name, binary_t **dist_data)
{
    return db_get_seq_dist_data((knl_session_t *)session, user, name, dist_data);
}

status_t knl_get_sequence_id(knl_handle_t session, text_t *user, text_t *name, uint32 *id)
{
    return db_get_sequence_id((knl_session_t *)session, user, name, id);
}

status_t knl_set_cn_seq_currval(knl_handle_t session, text_t *user, text_t *name, int64 nextval)
{
    return db_set_cn_seq_currval((knl_session_t *)session, user, name, nextval);
}

status_t knl_drop_sequence(knl_handle_t session, knl_drop_def_t *def)
{
    knl_dictionary_t dc;
    bool32 seq_exists = GS_FALSE;
    bool32 drop_if_exists = (def->options & DROP_IF_EXISTS);
    knl_session_t *se = (knl_session_t *)session;

    if (knl_ddl_enabled(session, GS_TRUE) != GS_SUCCESS) {
        int32 errcode = cm_get_error_code();
        GS_LOG_RUN_ERR("knl_ddl_enabled fail %d %s", errcode, g_error_desc[errcode]);
        return GS_ERROR;
    }

    if (GS_SUCCESS != dc_seq_open(se, &def->owner, &def->name, &dc)) {
        cm_reset_error_user(ERR_SEQ_NOT_EXIST, T2S(&def->owner), T2S_EX(&def->name), ERR_TYPE_SEQUENCE);
        if (drop_if_exists) {
            int32 code = cm_get_error_code();
            if (code == ERR_SEQ_NOT_EXIST) {
                cm_reset_error();
                return GS_SUCCESS;
            }
        }
        int32 errcode = cm_get_error_code();
        GS_LOG_RUN_ERR("dc_seq_open fail %d %s", errcode, g_error_desc[errcode]);
        return GS_ERROR;
    }

    if (db_drop_sequence(se, &dc, &seq_exists) != GS_SUCCESS) {
        int32 errcode = cm_get_error_code();
        GS_LOG_RUN_ERR("db_drop_sequence fail %d %s", errcode, g_error_desc[errcode]);
        dc_seq_close(&dc);
        return GS_ERROR;
    }

    dc_seq_close(&dc);

    if (!seq_exists && !drop_if_exists) {
        GS_THROW_ERROR(ERR_SEQ_NOT_EXIST, T2S(&def->owner), T2S_EX(&def->name));
        GS_LOG_RUN_ERR("fail seq_exists=%d drop_if_exists=%d", seq_exists, drop_if_exists);
        return GS_ERROR;
    }

    return GS_SUCCESS;
}

static bool32 knl_judge_index_exist(knl_handle_t session, knl_index_def_t *def, dc_entity_t *entity)
{
    table_t *table = &entity->table;
    index_t *index = NULL;

    for (uint32 i = 0; i < table->index_set.total_count; i++) {
        index = table->index_set.items[i];

        if (cm_text_str_equal(&def->name, index->desc.name)) {
            return GS_TRUE;
        }
    }

    return GS_FALSE;
}

status_t knl_create_index(knl_handle_t handle, knl_index_def_t *def)
{
    knl_dictionary_t dc;
    rd_table_t redo;
    knl_session_t *session = (knl_session_t *)handle;
    latch_t *ddl_latch = &session->kernel->db.ddl_latch;
    uint32 op_type = RD_CREATE_INDEX;

    knl_commit(session); // 确保每次ddl操作前所有的事务都已提交，参考业界标准

    if (knl_ddl_enabled(handle, GS_TRUE) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (dc_open(session, &def->user, &def->table, &dc) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (SYNONYM_EXIST(&dc)) {
        GS_THROW_ERROR(ERR_TABLE_OR_VIEW_NOT_EXIST, T2S(&def->user), T2S_EX(&def->table));
        dc_close(&dc);
        return GS_ERROR;
    }

    if (!DB_IS_MAINTENANCE(session) && dc.type == DICT_TYPE_TABLE_EXTERNAL) {
        GS_THROW_ERROR(ERR_OPERATIONS_NOT_SUPPORT, "create index", "external organized table");
        dc_close(&dc);
        return GS_ERROR;
    }

    if (dc.type != DICT_TYPE_TABLE && dc.type != DICT_TYPE_TEMP_TABLE_SESSION &&
        dc.type != DICT_TYPE_TEMP_TABLE_TRANS && dc.type != DICT_TYPE_TABLE_NOLOGGING) {
        GS_THROW_ERROR(ERR_OPERATIONS_NOT_SUPPORT, "create index", "view");
        dc_close(&dc);
        return GS_ERROR;
    }

    if (!DB_IS_MAINTENANCE(session) && !session->bootstrap && IS_SYS_DC(&dc)) {
        GS_THROW_ERROR(ERR_OPERATIONS_NOT_SUPPORT, "create index", "system table");
        dc_close(&dc);
        return GS_ERROR;
    }

    if (knl_ddl_latch_s(ddl_latch, handle, NULL) != GS_SUCCESS) {
        dc_close(&dc);
        return GS_ERROR;
    }

    // create index online is TRUE, should wait for DDL lock, i.e., nowait is FALSE
    uint32 timeout = def->online ? LOCK_INF_WAIT : session->kernel->attr.ddl_lock_timeout;
    if (lock_table_directly(session, &dc, timeout) != GS_SUCCESS) {
        cm_unlatch(ddl_latch, NULL);
        dc_close(&dc);
        return GS_ERROR;
    }

    if (lock_child_table_directly(session, dc.handle, !def->online) != GS_SUCCESS) {
        unlock_tables_directly(session);
        cm_unlatch(ddl_latch, NULL);
        dc_close(&dc);
        return GS_ERROR;
    }

    if (knl_judge_index_exist(handle, def, DC_ENTITY(&dc)) && (def->options & CREATE_IF_NOT_EXISTS)) {
        GS_THROW_ERROR(ERR_OBJECT_EXISTS, "index", def->name.str);
        unlock_tables_directly(session);
        cm_unlatch(ddl_latch, NULL);
        dc_close(&dc);
        return GS_ERROR;
    }

    bool32 has_logic = LOGIC_REP_DB_ENABLED(session) && LOGIC_REP_TABLE_ENABLED(session, DC_ENTITY(&dc));
    log_append_lrep_info(session, op_type, has_logic);

    if (def->online) {
        if (db_create_index_online(session, def, &dc) != GS_SUCCESS) {
            knl_rollback(handle, NULL);
            session->rm->is_ddl_op = GS_FALSE;
            unlock_tables_directly(session);
            cm_unlatch(ddl_latch, NULL);
            dc_close(&dc);
            return GS_ERROR;
        }
    } else {
        if (db_create_index(session, def, &dc, GS_FALSE, NULL) != GS_SUCCESS) {
            knl_rollback(handle, NULL);
            session->rm->is_ddl_op = GS_FALSE;
            unlock_tables_directly(session);
            cm_unlatch(ddl_latch, NULL);
            dc_close(&dc);
            return GS_ERROR;
        }
    }

    redo.op_type = RD_CREATE_INDEX;
    redo.uid = dc.uid;
    redo.oid = dc.oid;
    log_put(session, RD_LOGIC_OPERATION, &redo, sizeof(rd_table_t),
        has_logic ? LOG_ENTRY_FLAG_WITH_LOGIC_OID : LOG_ENTRY_FLAG_NONE);

    if (has_logic) {
        session->rm->is_ddl_op = GS_FALSE;
    }

    knl_commit(handle);
    dc_invalidate_children(session, (dc_entity_t *)dc.handle);
    dc_invalidate(session, (dc_entity_t *)dc.handle);
    unlock_tables_directly(session);
    cm_unlatch(ddl_latch, NULL);
    dc_close(&dc);

    if ((DB_IS_MAINTENANCE(session)) && IS_SYS_DC(&dc)) {
        if (knl_open_dc_by_id(handle, dc.uid, dc.oid, &dc, GS_TRUE) != GS_SUCCESS) {
            CM_ABORT(0, "[DB] ABORT INFO: failed to update dictionary cache, "
                "please check environment and restart instance");
        }
        dc_close(&dc);
    }

    return GS_SUCCESS;
}

static void knl_ddm_write_rd(knl_handle_t session, knl_dictionary_t *dc)
{
    rd_table_t rd_altable;
    rd_altable.op_type = RD_ALTER_TABLE;
    rd_altable.uid = dc->uid;
    rd_altable.oid = dc->oid;
    log_put(session, RD_LOGIC_OPERATION, &rd_altable, sizeof(rd_table_t), LOG_ENTRY_FLAG_NONE);
}

static status_t db_verify_write_sys_policy(knl_session_t *session, knl_dictionary_t *dc, policy_def_t *policy)
{
    table_t *table = DC_TABLE(dc);
    if (table->policy_set.plcy_count + 1 > GS_MAX_POLICIES) {
        GS_THROW_ERROR(ERR_TOO_MANY_OBJECTS, GS_MAX_POLICIES, "table's policies");
        return GS_ERROR;
    }

    if (dc->type != DICT_TYPE_TABLE || dc->is_sysnonym) {
        GS_THROW_ERROR(ERR_INVALID_OPERATION, ", please set rule on common table");
        return GS_ERROR;
    }
    dc_entity_t *entity = DC_ENTITY(dc);
    if (IS_SYS_TABLE(&entity->table) || IS_PART_TABLE(&entity->table)) {
        GS_THROW_ERROR(ERR_INVALID_OPERATION, ", please set rule on common table");
        return GS_ERROR;
    }

    /* get policy owner id */
    if (!knl_get_user_id(session, &policy->object_owner, &policy->object_owner_id)) {
        GS_THROW_ERROR(ERR_USER_NOT_EXIST, T2S(&policy->object_owner));
        return GS_ERROR;
    }

    if (policy->object_owner_id == DB_SYS_USER_ID) {
        GS_THROW_ERROR(ERR_INSUFFICIENT_PRIV);
        return GS_ERROR;
    }

    /* check if the policy name already exists */
    CM_SAVE_STACK(session->stack);
    knl_cursor_t *cursor = knl_push_cursor(session);
    knl_open_sys_cursor(session, cursor, CURSOR_ACTION_SELECT, SYS_POLICY_ID, IX_SYS_POLICY_001_ID);
    knl_init_index_scan(cursor, GS_TRUE);
    knl_set_scan_key(INDEX_DESC(cursor->index), &cursor->scan_range.l_key, GS_TYPE_INTEGER,
        (void *)&policy->object_owner_id, sizeof(uint32), IX_COL_SYS_POLICY_001_OBJ_SCHEMA_ID);
    knl_set_scan_key(INDEX_DESC(cursor->index), &cursor->scan_range.l_key, GS_TYPE_STRING,
        (void *)policy->object_name.str, (uint16)policy->object_name.len, IX_COL_SYS_POLICY_001_OBJ_NAME);
    knl_set_scan_key(INDEX_DESC(cursor->index), &cursor->scan_range.l_key, GS_TYPE_STRING,
        (void *)policy->policy_name.str, (uint16)policy->policy_name.len, IX_COL_SYS_POLICY_001_PNAME);

    if (knl_fetch(session, cursor) != GS_SUCCESS) {
        CM_RESTORE_STACK(session->stack);
        return GS_ERROR;
    }

    if (!cursor->eof) {
        CM_RESTORE_STACK(session->stack);
        GS_THROW_ERROR(ERR_DUPLICATE_NAME, "policy", T2S(&policy->policy_name));
        return GS_ERROR;
    }

    CM_RESTORE_STACK(session->stack);
    return GS_SUCCESS;
}

static status_t db_write_sys_policy(knl_session_t *session, policy_def_t *policy)
{
    row_assist_t row;
    table_t *table = NULL;
    status_t status;

    CM_SAVE_STACK(session->stack);
    knl_cursor_t *cursor = knl_push_cursor(session);
    uint32 max_size = session->kernel->attr.max_row_size;
    knl_open_sys_cursor(session, cursor, CURSOR_ACTION_INSERT, SYS_POLICY_ID, GS_INVALID_ID32);
    table = (table_t *)cursor->table;

    row_init(&row, (char *)cursor->row, max_size, table->desc.column_count);
    (void)row_put_uint32(&row, policy->object_owner_id);
    (void)row_put_text(&row, &policy->object_name);
    (void)row_put_text(&row, &policy->policy_name);
    (void)row_put_text(&row, &policy->function_owner);
    (void)row_put_text(&row, &policy->function);
    (void)row_put_uint32(&row, policy->stmt_types);
    (void)row_put_uint32(&row, policy->ptype);
    (void)row_put_uint32(&row, policy->check_option);
    (void)row_put_uint32(&row, policy->enable);
    (void)row_put_uint32(&row, policy->long_predicate);

    status = knl_internal_insert(session, cursor);
    CM_RESTORE_STACK(session->stack);
    return status;
}

status_t knl_write_sys_policy(knl_handle_t session, policy_def_t *plcy_def)
{
    knl_dictionary_t dc;
    knl_session_t *se = (knl_session_t *)session;
    latch_t *ddl_latch = &se->kernel->db.ddl_latch;
    uint32 id = se->id;

    if (knl_ddl_enabled(session, GS_TRUE) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (knl_open_dc_with_public(session, &plcy_def->object_owner, GS_TRUE, &plcy_def->object_name, &dc) != GS_SUCCESS) {
        GS_THROW_ERROR(ERR_TABLE_OR_VIEW_NOT_EXIST, T2S(&plcy_def->object_owner), T2S_EX(&plcy_def->object_name));
        return GS_ERROR;
    }

    cm_latch_s(ddl_latch, id, GS_FALSE, NULL);
    uint32 timeout = se->kernel->attr.ddl_lock_timeout;
    if (lock_table_directly(se, &dc, timeout) != GS_SUCCESS) {
        cm_unlatch(ddl_latch, NULL);
        knl_close_dc(&dc);
        return GS_ERROR;
    }

    if (db_verify_write_sys_policy(se, &dc, plcy_def) != GS_SUCCESS) {
        unlock_tables_directly(se);
        cm_unlatch(ddl_latch, NULL);
        knl_close_dc(&dc);
        return GS_ERROR;
    }

    if (db_write_sys_policy(se, plcy_def) != GS_SUCCESS) {
        unlock_tables_directly(se);
        cm_unlatch(ddl_latch, NULL);
        knl_close_dc(&dc);
        return GS_ERROR;
    }
    knl_ddm_write_rd(session, &dc);
    knl_commit(session);

    dc_invalidate(se, DC_ENTITY(&dc));
    unlock_tables_directly(se);
    cm_unlatch(ddl_latch, NULL);
    knl_close_dc(&dc);
    return GS_SUCCESS;
}

static status_t db_modify_sys_policy(knl_session_t *session, policy_def_t *policy, knl_cursor_action_t action)
{
    row_assist_t row;
    CM_SAVE_STACK(session->stack);
    knl_cursor_t *cursor = knl_push_cursor(session);

    knl_open_sys_cursor(session, cursor, action, SYS_POLICY_ID, IX_SYS_POLICY_001_ID);

    knl_init_index_scan(cursor, GS_TRUE);
    knl_set_scan_key(INDEX_DESC(cursor->index), &cursor->scan_range.l_key, GS_TYPE_INTEGER,
        (void *)&policy->object_owner_id, sizeof(uint32), IX_COL_SYS_POLICY_001_OBJ_SCHEMA_ID);
    knl_set_scan_key(INDEX_DESC(cursor->index), &cursor->scan_range.l_key, GS_TYPE_STRING,
        (void *)policy->object_name.str, (uint16)policy->object_name.len, IX_COL_SYS_POLICY_001_OBJ_NAME);
    knl_set_scan_key(INDEX_DESC(cursor->index), &cursor->scan_range.l_key, GS_TYPE_STRING,
        (void *)policy->policy_name.str, (uint16)policy->policy_name.len, IX_COL_SYS_POLICY_001_PNAME);

    if (knl_fetch(session, cursor) != GS_SUCCESS) {
        CM_RESTORE_STACK(session->stack);
        return GS_ERROR;
    }

    if (cursor->eof) {
        CM_RESTORE_STACK(session->stack);
        GS_THROW_ERROR(ERR_OBJECT_NOT_EXISTS, "policy", T2S(&policy->policy_name));
        return GS_ERROR;
    }

    if (action == CURSOR_ACTION_DELETE) {
        if (knl_internal_delete(session, cursor) != GS_SUCCESS) {
            CM_RESTORE_STACK(session->stack);
            return GS_ERROR;
        }
    } else if (action == CURSOR_ACTION_UPDATE) {
        row_init(&row, cursor->update_info.data, HEAP_MAX_ROW_SIZE, UPDATE_COLUMN_COUNT_ONE);
        (void)row_put_int32(&row, (int32)policy->enable);
        cursor->update_info.count = UPDATE_COLUMN_COUNT_ONE;
        cursor->update_info.columns[0] = SYS_POLICIES_COL_ENABLE;
        cm_decode_row(cursor->update_info.data, cursor->update_info.offsets, cursor->update_info.lens, NULL);
        if (knl_internal_update(session, cursor) != GS_SUCCESS) {
            CM_RESTORE_STACK(session->stack);
            return GS_ERROR;
        }
    }

    CM_RESTORE_STACK(session->stack);
    return GS_SUCCESS;
}

status_t knl_modify_sys_policy(knl_handle_t session, policy_def_t *plcy_def, knl_cursor_action_t action)
{
    knl_dictionary_t dc;
    knl_session_t *se = (knl_session_t *)session;
    latch_t *ddl_latch = &se->kernel->db.ddl_latch;
    uint32 id = se->id;

    /* get policy owner id */
    if (!knl_get_user_id(session, &plcy_def->object_owner, &plcy_def->object_owner_id)) {
        GS_THROW_ERROR(ERR_USER_NOT_EXIST, T2S(&plcy_def->object_owner));
        return GS_ERROR;
    }
    if (knl_ddl_enabled(session, GS_TRUE) != GS_SUCCESS) {
        return GS_ERROR;
    }
    if (knl_open_dc_with_public(session, &plcy_def->object_owner, GS_TRUE, &plcy_def->object_name, &dc) != GS_SUCCESS) {
        GS_THROW_ERROR(ERR_TABLE_OR_VIEW_NOT_EXIST, T2S(&plcy_def->object_owner), T2S_EX(&plcy_def->object_name));
        return GS_ERROR;
    }

    cm_latch_s(ddl_latch, id, GS_FALSE, NULL);
    uint32 timeout = se->kernel->attr.ddl_lock_timeout;
    if (lock_table_directly(se, &dc, timeout) != GS_SUCCESS) {
        cm_unlatch(ddl_latch, NULL);
        knl_close_dc(&dc);
        return GS_ERROR;
    }

    if (db_modify_sys_policy(se, plcy_def, action) != GS_SUCCESS) {
        unlock_tables_directly(se);
        cm_unlatch(ddl_latch, NULL);
        knl_close_dc(&dc);
        return GS_ERROR;
    }
    knl_ddm_write_rd(session, &dc);
    knl_commit(session);

    dc_invalidate(se, DC_ENTITY(&dc));
    unlock_tables_directly(se);
    cm_unlatch(ddl_latch, NULL);
    knl_close_dc(&dc);
    return GS_SUCCESS;
}

static void knl_alter_table_after_commit(knl_handle_t session, knl_dictionary_t *dc, knl_altable_def_t *def,
    trig_name_list_t *trig)
{
    knl_dictionary_t *ref_dc = NULL;
    dc_entity_t *entity = (dc_entity_t *)dc->handle;
    trig_set_t trig_set = entity->trig_set;

    switch (def->action) {
        case ALTABLE_ADD_CONSTRAINT:
            if (def->cons_def.new_cons.type == CONS_TYPE_REFERENCE) {
                ref_dc = &def->cons_def.new_cons.ref.ref_dc;
                if (ref_dc->handle != NULL) {
                    dc_invalidate((knl_session_t *)session, (dc_entity_t *)ref_dc->handle);
                    dc_close(ref_dc);
                }
            }
            break;
        case ALTABLE_ADD_COLUMN:
        case ALTABLE_RENAME_TABLE:
        case ALTABLE_RENAME_COLUMN:
        case ALTABLE_DROP_COLUMN:
            if (trig_set.trig_count > 0) {
                g_knl_callback.pl_free_trig_entity_by_tab(session, dc);
            }
            break;
        default:
            break;
    }
}

/*
 * kernel shrink space compact
 * Shrink compact the given table with table shared lock.
 * @note only support shrink compact heap segment
 * @param kernel session, dictionary
 */
static status_t knl_shrink_compact(knl_session_t *session, knl_dictionary_t *dc, heap_cmp_def_t def)
{
    table_part_t *table_part = NULL;
    knl_part_locate_t part_loc;

    if (def.timeout != 0) {
        GS_THROW_ERROR(ERR_INVALID_OPERATION, ",timeout only supported in shrink space");
        return GS_ERROR;
    }

    if (lock_table_shared_directly(session, dc) != GS_SUCCESS) {
        return GS_ERROR;
    }

    dc_entity_t *entity = DC_ENTITY(dc);
    if (entity->corrupted) {
        unlock_tables_directly(session);
        GS_THROW_ERROR(ERR_DC_CORRUPTED);
        return GS_ERROR;
    }

    table_t *table = DC_TABLE(dc);
    if (table->ashrink_stat != ASHRINK_END) {
        unlock_tables_directly(session);
        GS_THROW_ERROR(ERR_SHRINK_IN_PROGRESS_FMT, DC_ENTRY_USER_NAME(dc), DC_ENTRY_NAME(dc));
        return GS_ERROR;
    }

    if (IS_PART_TABLE(table)) {
        for (uint32 i = 0; i < table->part_table->desc.partcnt; i++) {
            table_part = TABLE_GET_PART(table, i);
            if (!IS_READY_PART(table_part)) {
                continue;
            }

            part_loc.part_no = i;
            if (heap_shrink_compart_compact(session, dc, part_loc, GS_FALSE, def) != GS_SUCCESS) {
                unlock_tables_directly(session);
                return GS_ERROR;
            }
        }
    } else {
        part_loc.part_no = 0;
        part_loc.subpart_no = GS_INVALID_ID32;
        if (heap_shrink_compact(session, dc, part_loc, GS_FALSE, def) != GS_SUCCESS) {
            unlock_tables_directly(session);
            return GS_ERROR;
        }
    }

    unlock_tables_directly(session);

    return GS_SUCCESS;
}

static void knl_ashrink_update_hwms(knl_session_t *session, knl_dictionary_t *dc, bool32 *valid_hwm)
{
    table_t *table = DC_TABLE(dc);
    knl_part_locate_t part_loc;

    part_loc.subpart_no = GS_INVALID_ID32;
    if (!IS_PART_TABLE(table)) {
        part_loc.part_no = 0;
        heap_ashrink_update_hwms(session, dc, part_loc, valid_hwm);
        return;
    }

    for (uint32 i = 0; i < table->part_table->desc.partcnt; i++) {
        table_part_t *table_part = TABLE_GET_PART(table, i);
        if (!IS_READY_PART(table_part)) {
            continue;
        }

        part_loc.part_no = i;
        bool32 valid = GS_FALSE;
        heap_ashrink_update_hwms(session, dc, part_loc, &valid);
        if (!(*valid_hwm)) {
            *valid_hwm = valid;
        }
    }

    return;
}

static status_t knl_internel_shrink_compact(knl_session_t *session, knl_dictionary_t *dc, heap_cmp_def_t def,
    bool32 *is_canceled)
{
    bool32 async_shrink = (bool32)(def.timeout != 0);
    bool32 shrink_hwm = !async_shrink;
    dc_entity_t *entity = DC_ENTITY(dc);
    table_t *table = DC_TABLE(dc);
    table_part_t *table_part = NULL;
    knl_part_locate_t part_loc;
    status_t status = GS_SUCCESS;

    lock_degrade_table_lock(session, entity);

    if (!IS_PART_TABLE(table)) {
        part_loc.part_no = 0;
        part_loc.subpart_no = GS_INVALID_ID32;
        status = heap_shrink_compact(session, dc, part_loc, shrink_hwm, def);
    } else {
        for (uint32 i = 0; i < table->part_table->desc.partcnt; i++) {
            if (async_shrink && (KNL_NOW(session) - def.end_time) / MICROSECS_PER_SECOND > 0) {
                GS_LOG_RUN_INF("async shrink timeout. uid %u oid %u name %s part_no %d.", dc->uid, dc->oid,
                    table->desc.name, i);
                break;
            }

            table_part = TABLE_GET_PART(table, i);
            if (!IS_READY_PART(table_part)) {
                continue;
            }

            part_loc.part_no = i;
            if (heap_shrink_compart_compact(session, dc, part_loc, shrink_hwm, def) != GS_SUCCESS) {
                status = GS_ERROR;
                break;
            }
        }
    }

    if (status != GS_SUCCESS) {
        if (cm_get_error_code() != ERR_OPERATION_CANCELED) {
            return GS_ERROR;
        }
        cm_reset_error();
        session->canceled = GS_FALSE;
        *is_canceled = GS_TRUE;
    }

    return lock_upgrade_table_lock(session, entity);
}

/*
 * kernel shrink space
 * Same like the shrink space compact, but we shrink the
 * hwm of all the heap segments
 * @note only support shrink heap segment
 * @param kernel session, dictionary
 */
static status_t knl_shrink_space(knl_session_t *session, knl_dictionary_t *dc, heap_cmp_def_t def, bool32 *retry,
    bool32 *is_canceled)
{
    bool32 async_shrink = (bool32)(def.timeout != 0);
    uint32 tlock_time = session->kernel->attr.ddl_lock_timeout;

    def.end_time = KNL_NOW(session) + (date_t)def.timeout * MICROSECS_PER_SECOND;
    tlock_time = async_shrink ? MAX(tlock_time, def.timeout) : tlock_time;

    if (lock_table_directly(session, dc, tlock_time) != GS_SUCCESS) {
        return GS_ERROR;
    }

    dc_entity_t *entity = DC_ENTITY(dc);
    if (entity->corrupted) {
        unlock_tables_directly(session);
        GS_THROW_ERROR(ERR_DC_CORRUPTED);
        return GS_ERROR;
    }

    table_t *table = &entity->table;
    if (table->ashrink_stat != ASHRINK_END) {
        GS_LOG_RUN_INF("last shrink not finish,reset table async shrink status.uid %u oid %u name %s", dc->uid, dc->oid,
            table->desc.name);
        *retry = GS_TRUE;
        dc_invalidate(session, entity);
        unlock_tables_directly(session);
        return GS_SUCCESS;
    }

    table->ashrink_stat = async_shrink ? ASHRINK_COMPACT : ASHRINK_END;

    if (knl_internel_shrink_compact(session, dc, def, is_canceled) != GS_SUCCESS) {
        GS_LOG_RUN_WAR("table async shrink compact failed.uid %u oid %u name %s", dc->uid, dc->oid, table->desc.name);
        table->ashrink_stat = ASHRINK_END;
        unlock_tables_directly(session);
        return GS_ERROR;
    }

    if (!async_shrink) {
        status_t status = heap_shrink_spaces(session, dc, GS_FALSE);
        dc_invalidate(session, entity);
        unlock_tables_directly(session);
        return status;
    }

    bool32 valid_hwm = GS_FALSE;
    knl_ashrink_update_hwms(session, dc, &valid_hwm);

    if (!valid_hwm) {
        GS_LOG_RUN_INF("table async shrink compact zero rows.uid %u oid %u name %s", dc->uid, dc->oid,
            table->desc.name);
        dc_invalidate(session, entity);
        unlock_tables_directly(session);
        return GS_SUCCESS;
    }

    table->ashrink_stat = ASHRINK_WAIT_SHRINK;
    if (ashrink_add(session, dc, DB_CURR_SCN(session)) != GS_SUCCESS) {
        GS_LOG_RUN_WAR("push table to async shrink list failed.uid %u oid %u name %s", dc->uid, dc->oid,
            table->desc.name);
        dc_invalidate(session, entity);
        unlock_tables_directly(session);
        return GS_ERROR;
    }

    unlock_tables_directly(session);
    return GS_SUCCESS;
}

static status_t knl_check_shrinkable(knl_handle_t session, knl_dictionary_t *dc, knl_altable_def_t *def)
{
    if (IS_SYS_DC(dc)) {
        GS_THROW_ERROR(ERR_OPERATIONS_NOT_SUPPORT, "shrink table", "view or system table");
        return GS_ERROR;
    }

    if (SYNONYM_EXIST(dc)) {
        GS_THROW_ERROR(ERR_TABLE_OR_VIEW_NOT_EXIST, T2S(&def->user), T2S_EX(&def->name));
        return GS_ERROR;
    }

    if (dc->type == DICT_TYPE_TABLE_EXTERNAL) {
        GS_THROW_ERROR(ERR_OPERATIONS_NOT_SUPPORT, "shrink table", "external organized table");
        return GS_ERROR;
    }

    if (dc->type < DICT_TYPE_TABLE || dc->type > DICT_TYPE_TABLE_NOLOGGING) {
        GS_THROW_ERROR(ERR_OPERATIONS_NOT_SUPPORT, "shrink table", "view or system table");
        return GS_ERROR;
    }

    if (dc->type == DICT_TYPE_TEMP_TABLE_SESSION || dc->type == DICT_TYPE_TEMP_TABLE_TRANS) {
        GS_THROW_ERROR(ERR_OPERATIONS_NOT_SUPPORT, "shrink table", "temp table");
        return GS_ERROR;
    }

    return GS_SUCCESS;
}

/*
 * shrink table space online
 * @param kernel session, alter table definition
 */
status_t knl_alter_table_shrink(knl_handle_t session, knl_altable_def_t *def)
{
    knl_dictionary_t dc;
    status_t status = GS_SUCCESS;
    knl_session_t *se = (knl_session_t *)session;
    bool32 is_canceled = GS_FALSE;

    if (knl_ddl_enabled(session, GS_TRUE) != GS_SUCCESS) {
        return GS_ERROR;
    }

    heap_cmp_def_t shrink_def;
    uint32 shrink_opt = def->table_def.shrink_opt;
    shrink_def.percent = def->table_def.shrink_percent;
    shrink_def.timeout = def->table_def.shrink_timeout;

    if (shrink_opt & SHRINK_CASCADE) {
        GS_THROW_ERROR(ERR_CAPABILITY_NOT_SUPPORT, "shrink cascade");
        return GS_ERROR;
    }

    for (;;) {
        if (dc_open(se, &def->user, &def->name, &dc) != GS_SUCCESS) {
            return GS_ERROR;
        }

        if (knl_check_shrinkable(session, &dc, def) != GS_SUCCESS) {
            dc_close(&dc);
            return GS_ERROR;
        }

        if (knl_ddl_latch_s(&se->kernel->db.ddl_latch, session, NULL) != GS_SUCCESS) {
            dc_close(&dc);
            return GS_ERROR;
        }

        table_t *table = DC_TABLE(&dc);
        if (db_check_table_nologging_attr(table) != GS_SUCCESS) {
            cm_unlatch(&se->kernel->db.ddl_latch, NULL);
            dc_close(&dc);
            return GS_ERROR;
        }

        bool32 retry = GS_FALSE;
        if (shrink_opt & SHRINK_COMPACT) {
            status = knl_shrink_compact(se, &dc, shrink_def);
        } else {
            status = knl_shrink_space(se, &dc, shrink_def, &retry, &is_canceled);
        }

        cm_unlatch(&se->kernel->db.ddl_latch, NULL);
        dc_close(&dc);

        if (!retry) {
            break;
        }
    }

    if (is_canceled) {
        se->canceled = GS_TRUE;
    }

    if (status == GS_SUCCESS) {
        se->stat.table_alters++;
    }

    return status;
}

static status_t knl_altable_check_table_type(knl_session_t *session, knl_altable_def_t *def, knl_dictionary_t *dc)
{
    if (SYNONYM_EXIST(dc)) {
        GS_THROW_ERROR(ERR_TABLE_OR_VIEW_NOT_EXIST, T2S(&def->user), T2S_EX(&def->name));
        return GS_ERROR;
    }

    if (dc->type == DICT_TYPE_TABLE_EXTERNAL) {
        GS_THROW_ERROR(ERR_OPERATIONS_NOT_SUPPORT, "alter table", "external organized table");
        return GS_ERROR;
    }

    if (dc->type < DICT_TYPE_TABLE || dc->type > DICT_TYPE_TABLE_NOLOGGING) {
        GS_THROW_ERROR(ERR_OPERATIONS_NOT_SUPPORT, "alter table", "view or system table");
        return GS_ERROR;
    }

    if (!DB_IS_MAINTENANCE(session) && IS_SYS_DC(dc)) {
        GS_THROW_ERROR(ERR_OPERATIONS_NOT_SUPPORT, "alter table", "system table");
        return GS_ERROR;
    }

    return GS_SUCCESS;
}

static bool32 knl_altable_logicrep_enabled(knl_session_t *session, knl_altable_def_t *def, knl_dictionary_t *dc)
{
    if (def->action == ALTABLE_ADD_COLUMN || def->action == ALTABLE_MODIFY_COLUMN ||
        def->action == ALTABLE_RENAME_COLUMN || def->action == ALTABLE_DROP_COLUMN) {
        return LOGIC_REP_DB_ENABLED(session) && LOGIC_REP_TABLE_ENABLED(session, DC_ENTITY(dc));
    }
    return GS_FALSE;
}

static void knl_altable_write_logical(knl_session_t *session, knl_altable_def_t *def, knl_dictionary_t *dc)
{
    rd_table_t rd_altable;
    rd_rename_table_t rd_rename;

    bool32 has_logic = knl_altable_logicrep_enabled(session, def, dc);

    if (def->action == ALTABLE_RENAME_TABLE) {
        rd_rename.op_type = RD_RENAME_TABLE;
        rd_rename.uid = dc->uid;
        rd_rename.oid = dc->oid;
        (void)cm_text2str(&def->table_def.new_name, rd_rename.new_name, GS_NAME_BUFFER_SIZE);
        log_put(session, RD_LOGIC_OPERATION, &rd_rename, sizeof(rd_rename_table_t), LOG_ENTRY_FLAG_NONE);
    } else {
        rd_altable.op_type = RD_ALTER_TABLE;
        rd_altable.uid = dc->uid;
        rd_altable.oid = dc->oid;
        log_put(session, RD_LOGIC_OPERATION, &rd_altable, sizeof(rd_table_t),
            has_logic ? LOG_ENTRY_FLAG_WITH_LOGIC_OID : LOG_ENTRY_FLAG_NONE);
        if (has_logic) {
            log_append_data(session, (void *)(&def->action), sizeof(uint32));
        }
    }
}

static status_t knl_altable_with_action(knl_handle_t se, knl_handle_t stmt, knl_altable_def_t *def,
    knl_dictionary_t *dc, trig_name_list_t *trig)
{
    status_t status;

    switch (def->action) {
        case ALTABLE_ADD_COLUMN:
            status = db_altable_add_column(se, dc, stmt, def);
            break;

        case ALTABLE_MODIFY_COLUMN:
            status = db_altable_modify_column(se, dc, stmt, def);
            break;

        case ALTABLE_RENAME_COLUMN:
            status = db_altable_rename_column(se, dc, def);
            break;

        case ALTABLE_DROP_COLUMN:
            status = db_altable_drop_column(se, dc, def);
            break;

        case ALTABLE_ADD_CONSTRAINT:
            status = db_altable_add_cons(se, dc, def);
            break;

        case ALTABLE_DROP_CONSTRAINT:
            status = db_altable_drop_cons(se, dc, def);
            break;

        case ALTABLE_MODIFY_CONSTRAINT:
            status = GS_ERROR;
            GS_THROW_ERROR(ERR_INVALID_OPERATION, ",unsupported alter table operation");
            break;

        case ALTABLE_RENAME_CONSTRAINT:
            status = db_altable_rename_constraint(se, dc, def);
            break;

        case ALTABLE_TABLE_PCTFREE:
            status = db_altable_pctfree(se, dc, def);
            break;

        case ALTABLE_TABLE_INITRANS:
            status = db_altable_initrans(se, dc, def);
            break;

        case ALTABLE_MODIFY_STORAGE:
            status = db_altable_storage(se, dc, def);
            break;

        case ALTABLE_MODIFY_PART_INITRANS:
            status = db_altable_part_initrans(se, dc, &def->part_def);
            break;

        case ALTABLE_MODIFY_PART_STORAGE:
            status = db_altable_part_storage(se, dc, def);
            break;

        case ALTABLE_RENAME_TABLE:
            status = db_altable_rename_table(se, dc, def, trig);
            break;

        case ALTABLE_APPENDONLY:
            status = db_altable_appendonly(se, dc, def);
            break;

        case ALTABLE_DROP_PARTITION:
            status = db_altable_drop_part(se, dc, def, GS_FALSE);
            break;

        case ALTABLE_DROP_SUBPARTITION:
            status = db_altable_drop_subpartition(se, dc, def, GS_FALSE);
            break;

        case ALTABLE_TRUNCATE_PARTITION:
            status = db_altable_truncate_part(se, dc, &def->part_def);
            break;

        case ALTABLE_TRUNCATE_SUBPARTITION:
            status = db_altable_truncate_subpart(se, dc, &def->part_def);
            break;

        case ALTABLE_ADD_PARTITION:
            status = db_altable_add_part(se, dc, def);
            break;

        case ALTABLE_ADD_SUBPARTITION:
            status = db_altable_add_subpartition(se, dc, def);
            break;

        case ALTABLE_SPLIT_PARTITION:
            status = db_altable_split_part(se, dc, def);
            break;

        case ALTABLE_SPLIT_SUBPARTITION:
            status = db_altable_split_subpart(se, dc, def);
            break;

        case ALTABLE_AUTO_INCREMENT:
            status = db_altable_auto_increment(se, dc, def->table_def.serial_start);
            break;

        case ALTABLE_ENABLE_ALL_TRIG:
            status = db_altable_set_all_trig_status(se, dc, GS_TRUE);
            break;

        case ALTABLE_DISABLE_ALL_TRIG:
            status = db_altable_set_all_trig_status(se, dc, GS_FALSE);
            break;

        case ALTABLE_ENABLE_NOLOGGING:
            status = db_altable_enable_nologging(se, dc);
            break;

        case ALTABLE_DISABLE_NOLOGGING:
            status = db_altable_disable_nologging(se, dc);
            break;

        case ALTABLE_ENABLE_PART_NOLOGGING:
            status = db_altable_enable_part_nologging(se, dc, def);
            break;

        case ALTABLE_DISABLE_PART_NOLOGGING:
            status = db_altable_disable_part_nologging(se, dc, def);
            break;

        case ALTABLE_ENABLE_SUBPART_NOLOGGING:
            status = db_altable_enable_subpart_nologging(se, dc, def);
            break;

        case ALTABLE_DISABLE_SUBPART_NOLOGGING:
            status = db_altable_disable_subpart_nologging(se, dc, def);
            break;

        case ALTABLE_COALESCE_PARTITION:
            status = db_altable_coalesce_partition(se, dc, def);
            break;

        case ALTABLE_COALESCE_SUBPARTITION:
            status = db_altable_coalesce_subpartition(se, dc, def);
            break;

        case ALTABLE_APPLY_CONSTRAINT:
            status = db_altable_apply_constraint(se, dc, def);
            break;

        case ALTABLE_SET_INTERVAL_PART:
            status = db_altable_set_interval_part(se, dc, &def->part_def);
            break;

        case ALTABLE_ADD_LOGICAL_LOG:
            status = db_altable_add_logical_log(se, dc, def);
            break;

        case ALTABLE_DROP_LOGICAL_LOG:
            status = db_altable_drop_logical_log(se, dc, def);
            break;

        case ALTABLE_ENABLE_ROW_MOVE:
        case ALTABLE_DISABLE_ROW_MOVE:
        default:
            status = GS_ERROR;
            GS_THROW_ERROR(ERR_INVALID_OPERATION, ",unsupported alter table operation");
            break;
    }

    return status;
}

/*
 * perform alter table, move the knl_ddl_enabled to the outside,
 * @attention: SQL layer should not call this interface.
 * @param kernel session, knl_handle_t stmt, knl_altable_def_t * def
 */
status_t knl_perform_alter_table(knl_handle_t session, knl_handle_t stmt, knl_altable_def_t *def)
{
    knl_dictionary_t dc;
    status_t status;
    trig_name_list_t trig;
    errno_t ret;
    knl_session_t *se = (knl_session_t *)session;

    ret = memset_sp(&dc, sizeof(knl_dictionary_t), 0, sizeof(knl_dictionary_t));
    knl_securec_check(ret);
    if (dc_open(se, &def->user, &def->name, &dc) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (dc.type == DICT_TYPE_TABLE_EXTERNAL) {
        GS_THROW_ERROR(ERR_OPERATIONS_NOT_SUPPORT, "alter table", "external organzied table");
        dc_close(&dc);
        return GS_ERROR;
    }

    if (knl_altable_check_table_type(se, def, &dc) != GS_SUCCESS) {
        dc_close(&dc);
        return GS_ERROR;
    }

    uint32 timeout = se->kernel->attr.ddl_lock_timeout;
    if (lock_table_directly(se, &dc, timeout) != GS_SUCCESS) {
        dc_close(&dc);
        return GS_ERROR;
    }

    if (lock_parent_table_directly(se, dc.handle, GS_TRUE) != GS_SUCCESS) {
        unlock_tables_directly(se);
        dc_close(&dc);
        return GS_ERROR;
    }

    if (lock_child_table_directly(se, dc.handle, GS_TRUE) != GS_SUCCESS) {
        unlock_tables_directly(se);
        dc_close(&dc);
        return GS_ERROR;
    }

    if (!IS_ALTABLE_NOLOGGING_ACTION(def->action)) {
        table_t *table = DC_TABLE(&dc);
        if (db_check_table_nologging_attr(table) != GS_SUCCESS) {
            unlock_tables_directly(session);
            dc_close(&dc);
            return GS_ERROR;
        }
    }

    bool32 has_logic = LOGIC_REP_DB_ENABLED(se) && LOGIC_REP_TABLE_ENABLED(se, DC_ENTITY(&dc));

    status = knl_altable_with_action(se, stmt, def, &dc, &trig);

    if (status == GS_SUCCESS) {
        knl_altable_write_logical(se, def, &dc);

        if (has_logic) {
            se->rm->is_ddl_op = GS_FALSE;
        }

        SYNC_POINT(session, "SP_B1_ALTER_TABLE");
        knl_commit(session);
        if (db_garbage_segment_handle(se, dc.uid, dc.oid, GS_FALSE) != GS_SUCCESS) {
            cm_spin_lock(&se->kernel->rmon_ctx.mark_mutex, NULL);
            se->kernel->rmon_ctx.delay_clean_segments = GS_TRUE;
            cm_spin_unlock(&se->kernel->rmon_ctx.mark_mutex);
            GS_LOG_RUN_ERR("failed to handle garbage segment");
        }

        if (!DB_IS_MAINTENANCE(se)) {
            knl_alter_table_after_commit(session, &dc, def, &trig);
        }

        se->stat.table_alters++;
    } else {
        if (has_logic) {
            se->rm->is_ddl_op = GS_FALSE;
        }
        knl_rollback(session, NULL);
    }

    dc_invalidate_children(se, (dc_entity_t *)dc.handle);
    dc_invalidate_parents(se, (dc_entity_t *)dc.handle);
    dc_invalidate(se, (dc_entity_t *)dc.handle);
    unlock_tables_directly(se);
    dc_close(&dc);

    if ((DB_IS_MAINTENANCE(se)) && IS_SYS_DC(&dc)) {
        if (knl_open_dc_by_id(session, dc.uid, dc.oid, &dc, GS_TRUE) != GS_SUCCESS) {
            CM_ABORT(0, "[DB] ABORT INFO: failed to update dictionary cache,"
                "please check environment and restart instance");
        }
        dc_close(&dc);
    }

    return status;
}

status_t knl_alter_table(knl_handle_t session, knl_handle_t stmt, knl_altable_def_t *def)
{
    knl_session_t *se = (knl_session_t *)session;
    status_t status;
    latch_t *ddl_latch = &se->kernel->db.ddl_latch;

    knl_commit(session); // 确保每次ddl操作前所有的事务都已提交，参考业界标准

    if (knl_ddl_enabled(session, GS_TRUE) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (knl_ddl_latch_s(ddl_latch, session, NULL) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (def->action == ALTABLE_MODIFY_LOB) {
        status = db_altable_modify_lob((knl_session_t *)session, def);
        cm_unlatch(ddl_latch, NULL);
        return status;
    }

    status = knl_perform_alter_table(session, stmt, def);
    cm_unlatch(ddl_latch, NULL);

    return status;
}

status_t knl_open_dc_by_index(knl_handle_t se, text_t *owner, text_t *table, text_t *idx_name, knl_dictionary_t *dc)
{
    knl_session_t *session = (knl_session_t *)se;
    uint32 uid;
    knl_index_desc_t desc;
    index_t *index = NULL;

    if (table == NULL) {
        if (!dc_get_user_id(session, owner, &uid)) {
            GS_THROW_ERROR(ERR_INDEX_NOT_EXIST, T2S(owner), T2S_EX(idx_name));
            return GS_ERROR;
        }

        if (db_fetch_index_desc(session, uid, idx_name, &desc) != GS_SUCCESS) {
            return GS_ERROR;
        }

        if (knl_open_dc_by_id(session, desc.uid, desc.table_id, dc, GS_TRUE) != GS_SUCCESS) {
            return GS_ERROR;
        }
    } else {
        if (knl_open_dc(session, owner, table, dc) != GS_SUCCESS) {
            return GS_ERROR;
        }
    }

    index = dc_find_index_by_name(DC_ENTITY(dc), idx_name);
    if (index == NULL) {
        dc_close(dc);
        GS_THROW_ERROR(ERR_INDEX_NOT_EXIST, T2S(owner), T2S_EX(idx_name));
        return GS_ERROR;
    }

    return GS_SUCCESS;
}

bool32 knl_find_dc_by_tmpidx(knl_handle_t se, text_t *owner, text_t *idx_name)
{
    knl_session_t *session = (knl_session_t *)se;
    knl_temp_dc_t *temp_dc = session->temp_dc;
    dc_entry_t *entry = NULL;
    index_t *index = NULL;

    if (temp_dc == NULL) {
        return GS_FALSE;
    }

    for (uint32 i = 0; i < session->temp_table_capacity; i++) {
        entry = (dc_entry_t *)temp_dc->entries[i];
        if (entry == NULL) {
            continue;
        }

        index = dc_find_index_by_name(entry->entity, idx_name);

        if (index != NULL) {
            return GS_TRUE;
        }
    }

    return GS_FALSE;
}

status_t knl_alter_index_coalesce(knl_handle_t session, knl_alindex_def_t *def)
{
    knl_dictionary_t dc;
    status_t status = GS_SUCCESS;
    bool32 lock_inuse = GS_FALSE;
    index_t *index = NULL;
    knl_session_t *se = (knl_session_t *)session;
    latch_t *ddl_latch = &se->kernel->db.ddl_latch;

    if (knl_ddl_enabled(session, GS_TRUE) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (knl_ddl_latch_s(ddl_latch, session, NULL) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (knl_open_dc_by_index(se, &def->user, NULL, &def->name, &dc) != GS_SUCCESS) {
        cm_unlatch(ddl_latch, NULL);
        return GS_ERROR;
    }

    if (!lock_table_without_xact(se, dc.handle, &lock_inuse)) {
        dc_close(&dc);
        cm_unlatch(ddl_latch, NULL);
        return GS_ERROR;
    }

    index = dc_find_index_by_name(DC_ENTITY(&dc), &def->name);
    if (index == NULL) {
        unlock_table_without_xact(se, dc.handle, lock_inuse);
        dc_close(&dc);
        GS_THROW_ERROR(ERR_INDEX_NOT_EXIST, T2S(&def->user), T2S_EX(&def->name));
        cm_unlatch(ddl_latch, NULL);
        return GS_ERROR;
    }

    if (def->type == ALINDEX_TYPE_MODIFY_PART) {
        status = db_alter_part_index_coalesce(se, &dc, def, index);
    } else if (def->type == ALINDEX_TYPE_MODIFY_SUBPART) {
        status = db_alter_subpart_index_coalesce(se, &dc, def, index);
    } else if (def->type == ALINDEX_TYPE_COALESCE) {
        status = db_alter_index_coalesce(se, &dc, index);
    }

    unlock_table_without_xact(se, dc.handle, lock_inuse);
    dc_close(&dc);
    cm_unlatch(ddl_latch, NULL);

    return status;
}

uint32 knl_get_index_vcol_count(knl_index_desc_t *desc)
{
    uint32 vcol_count = 0;

    if (!desc->is_func) {
        return 0;
    }

    for (uint32 i = 0; i < desc->column_count; i++) {
        if (desc->columns[i] >= DC_VIRTUAL_COL_START) {
            vcol_count++;
        }
    }

    return vcol_count;
}

void knl_get_index_name(knl_index_desc_t *desc, char *name, uint32 max_len)
{
    errno_t ret = strncpy_s(name, max_len, desc->name, strlen(desc->name));
    knl_securec_check(ret);
}

status_t knl_alter_index_rename(knl_handle_t session, knl_alt_index_prop_t *def, knl_dictionary_t *dc,
    index_t *old_index)
{
    knl_session_t *se = (knl_session_t *)session;
    knl_cursor_t *cursor = NULL;
    char old_name[GS_NAME_BUFFER_SIZE];
    char new_name[GS_NAME_BUFFER_SIZE];
    bool32 is_found = GS_FALSE;
    errno_t ret;

    CM_SAVE_STACK(se->stack);
    cursor = knl_push_cursor(se);

    if (db_fetch_sysindex_row(se, cursor, dc->uid, &def->new_name, CURSOR_ACTION_SELECT, &is_found) != GS_SUCCESS) {
        CM_RESTORE_STACK(se->stack);
        return GS_ERROR;
    }
    CM_RESTORE_STACK(se->stack);

    if (is_found) {
        GS_THROW_ERROR(ERR_OBJECT_EXISTS, "index", T2S(&def->new_name));
        return GS_ERROR;
    }

    knl_get_index_name(&old_index->desc, old_name, GS_NAME_BUFFER_SIZE);

    ret = strncpy_s(new_name, GS_NAME_BUFFER_SIZE, def->new_name.str, def->new_name.len);
    knl_securec_check(ret);

    cm_str2text_safe(new_name, (uint32)strlen(new_name), &def->new_name);

    if (db_update_index_name(se, old_index->desc.uid, old_name, &def->new_name) != GS_SUCCESS) {
        int32 err_code = cm_get_error_code();

        if (err_code == ERR_DUPLICATE_KEY) {
            cm_reset_error();
            GS_THROW_ERROR(ERR_OBJECT_EXISTS, "index", T2S(&def->new_name));
        }

        return GS_ERROR;
    }

    return GS_SUCCESS;
}

status_t knl_alter_index(knl_handle_t session, knl_alindex_def_t *def)
{
    knl_dictionary_t dc;
    status_t status;
    rd_table_t redo;
    index_t *index = NULL;
    knl_session_t *se = (knl_session_t *)session;
    latch_t *ddl_latch = &se->kernel->db.ddl_latch;
    uint32 op_type = RD_ALTER_INDEX;

    if (knl_ddl_enabled(session, GS_TRUE) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (knl_ddl_latch_s(ddl_latch, session, NULL) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (knl_open_dc_by_index(se, &def->user, NULL, &def->name, &dc) != GS_SUCCESS) {
        cm_unlatch(ddl_latch, NULL);
        return GS_ERROR;
    }

    table_t *table = DC_TABLE(&dc);
    if (db_check_table_nologging_attr(table) != GS_SUCCESS) {
        dc_close(&dc);
        cm_unlatch(ddl_latch, NULL);
        return GS_ERROR;
    }

    if (!DB_IS_MAINTENANCE(se) && IS_SYS_DC(&dc)) {
        GS_THROW_ERROR(ERR_OPERATIONS_NOT_SUPPORT, "alter index", "system table");
        dc_close(&dc);
        cm_unlatch(ddl_latch, NULL);
        return GS_ERROR;
    }

    bool32 timeout_default = GS_TRUE;
    if (def->type == ALINDEX_TYPE_REBUILD || def->type == ALINDEX_TYPE_REBUILD_PART) {
        timeout_default = !def->rebuild.is_online;
    }

    uint32 timeout = timeout_default ? se->kernel->attr.ddl_lock_timeout : LOCK_INF_WAIT;
    if (lock_table_directly(se, &dc, timeout) != GS_SUCCESS) {
        dc_close(&dc);
        cm_unlatch(ddl_latch, NULL);
        return GS_ERROR;
    }

    if (lock_child_table_directly(se, dc.handle, timeout_default) != GS_SUCCESS) {
        unlock_tables_directly(se);
        dc_close(&dc);
        cm_unlatch(ddl_latch, NULL);
        return GS_ERROR;
    }

    index = dc_find_index_by_name(DC_ENTITY(&dc), &def->name);
    if (index == NULL) {
        unlock_tables_directly(se);
        dc_close(&dc);
        cm_unlatch(ddl_latch, NULL);
        GS_THROW_ERROR(ERR_INDEX_NOT_EXIST, T2S(&def->user), T2S_EX(&def->name));
        return GS_ERROR;
    }

    bool32 has_logic = LOGIC_REP_DB_ENABLED(se) && LOGIC_REP_TABLE_ENABLED(se, DC_ENTITY(&dc));
    log_append_lrep_altindex(se, op_type, has_logic, (uint32 *)&def->type, index->desc.name);

    switch (def->type) {
        case ALINDEX_TYPE_REBUILD:
            status = db_alter_index_rebuild(se, def, &dc, index);
            break;

        case ALINDEX_TYPE_REBUILD_PART:
        case ALINDEX_TYPE_REBUILD_SUBPART:
            if (def->rebuild.specified_parts > 1 || def->rebuild.parallelism) {
                status = db_alter_index_rebuild(se, def, &dc, index);
            } else {
                status = db_alter_index_rebuild_part(se, def, &dc, index);
            }
            break;

        case ALINDEX_TYPE_RENAME:
            status = knl_alter_index_rename(session, &def->idx_def, &dc, index);
            break;

        case ALINDEX_TYPE_UNUSABLE:
            status = db_alter_index_unusable(session, index);
            break;

        case ALINDEX_TYPE_INITRANS:
            status = db_alter_index_initrans(se, def, index);
            break;

        case ALINDEX_TYPE_MODIFY_PART:
            status = db_alter_index_partition(se, def, &dc, index);
            break;
        case ALINDEX_TYPE_MODIFY_SUBPART:
            status = db_alter_index_subpartition(se, def, &dc, index);
            break;
        default:
            GS_THROW_ERROR(ERR_CAPABILITY_NOT_SUPPORT, "alter index not in type rebuild or rebuild_part");
            status = GS_ERROR;
    }

    /* alter index will reload dc entity, so memory of index may be reused by other table, we
     * should use uid, oid of dc instead of index->desc.uid while writing logic log.
     */
    if (status == GS_SUCCESS) {
        redo.op_type = RD_ALTER_INDEX;
        redo.uid = dc.uid;
        redo.oid = dc.oid;
        log_put(se, RD_LOGIC_OPERATION, &redo, sizeof(rd_table_t),
            has_logic ? LOG_ENTRY_FLAG_WITH_LOGIC_OID : LOG_ENTRY_FLAG_NONE);

        if (has_logic) {
            se->rm->is_ddl_op = GS_FALSE;
        }

        if (DB_IS_MAINTENANCE(se) && IS_CORE_SYS_TABLE(dc.uid, dc.oid)) {
            if (db_save_core_ctrl(se) != GS_SUCCESS) {
                GS_LOG_RUN_ERR("[DB] failed to save core control file");
                dc_close(&dc);
                cm_unlatch(ddl_latch, NULL);
                return GS_ERROR;
            }
        }
        knl_commit(session);
    } else {
        if (has_logic) {
            se->rm->is_ddl_op = GS_FALSE;
        }
        knl_rollback(session, NULL);
    }

    dc_invalidate_children(se, (dc_entity_t *)dc.handle);
    if (SCH_LOCKED_EXCLUSIVE(dc.handle)) {
        dc_invalidate(se, (dc_entity_t *)dc.handle);
    }

    if (status == GS_SUCCESS) {
        db_update_index_clean_option(session, def, index->desc);
    }

    if (db_garbage_segment_handle(se, dc.uid, dc.oid, GS_FALSE) != GS_SUCCESS) {
        cm_spin_lock(&se->kernel->rmon_ctx.mark_mutex, NULL);
        se->kernel->rmon_ctx.delay_clean_segments = GS_TRUE;
        cm_spin_unlock(&se->kernel->rmon_ctx.mark_mutex);
        GS_LOG_RUN_ERR("[DB] failed to handle garbage segment");
    }
    dc_close(&dc);
    unlock_tables_directly(se);
    if (DB_IS_MAINTENANCE(se) && IS_CORE_SYS_TABLE(dc.uid, dc.oid)) {
        if (dc_load_core_table(se, dc.oid) != GS_SUCCESS) {
            CM_ABORT(0, "[DB] ABORT INFO: failed to update core system dictionary cache,\
            please check environment and restart instance");
        }
    } else if (DB_IS_MAINTENANCE(se) && IS_SYS_DC(&dc)) {
        if (knl_open_dc_by_id(session, dc.uid, dc.oid, &dc, GS_TRUE) != GS_SUCCESS) {
            CM_ABORT(0, "[DB] ABORT INFO: failed to update dictionary cache,\
            please check environment and restart instance");
        }
        dc_close(&dc);
    }
    cm_unlatch(ddl_latch, NULL);

    return status;
}

status_t knl_purge(knl_handle_t session, knl_purge_def_t *def)
{
    knl_session_t *se = (knl_session_t *)session;
    status_t status;
    latch_t *ddl_latch = &se->kernel->db.ddl_latch;

    if (knl_ddl_enabled(session, GS_TRUE) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (knl_ddl_latch_s(ddl_latch, session, NULL) != GS_SUCCESS) {
        return GS_ERROR;
    }
    status = db_purge(se, def);
    cm_unlatch(ddl_latch, NULL);

    return status;
}

static void set_drop_table_def(drop_table_def_t *def_drop, bool32 is_referenced, const char *name, knl_drop_def_t *def)
{
    def_drop->is_referenced = is_referenced;
    errno_t ret = strncpy_s(def_drop->name, GS_NAME_BUFFER_SIZE, name, GS_NAME_BUFFER_SIZE - 1);
    knl_securec_check(ret);
    def_drop->options = def->options;
    def_drop->purge = def->purge;
}

status_t knl_internal_drop_table(knl_handle_t session, knl_drop_def_t *def)
{
    knl_dictionary_t dc;
    status_t status;
    bool32 is_referenced = GS_FALSE;
    bool32 is_drop = GS_FALSE;
    knl_session_t *se = (knl_session_t *)session;
    uint32 op_type = RD_DROP_TABLE;
    core_ctrl_t *core = &se->kernel->db.ctrl.core;

    if (knl_ddl_enabled(session, GS_TRUE) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (dc_open(se, &def->owner, &def->name, &dc) != GS_SUCCESS) {
        if (!(def->options & DROP_IF_EXISTS)) {
            return GS_ERROR;
        }

        int32 err_code = cm_get_error_code();

        if (err_code == ERR_TABLE_OR_VIEW_NOT_EXIST) {
            cm_reset_error();
            return GS_SUCCESS;
        }

        return GS_ERROR;
    }

    table_t *table = DC_TABLE(&dc);
    if (db_check_table_nologging_attr(table) != GS_SUCCESS) {
        dc_close(&dc);
        return GS_ERROR;
    }

    if (SYNONYM_EXIST(&dc)) {
        GS_THROW_ERROR(ERR_TABLE_OR_VIEW_NOT_EXIST, T2S(&def->owner), T2S_EX(&def->name));
        dc_close(&dc);
        return GS_ERROR;
    }
    if (def->temp) {
        if (!(dc.type == DICT_TYPE_TEMP_TABLE_TRANS || dc.type == DICT_TYPE_TEMP_TABLE_SESSION)) {
            GS_THROW_ERROR(ERR_OPERATIONS_NOT_SUPPORT, "drop temporary table",
                "common table or system table only for temp");
            dc_close(&dc);
            return GS_ERROR;
        }
    }
    if (dc.type < DICT_TYPE_TABLE || dc.type > DICT_TYPE_TABLE_EXTERNAL) {
        GS_THROW_ERROR(ERR_OPERATIONS_NOT_SUPPORT, "drop table", "temp table or system table");
        dc_close(&dc);
        return GS_ERROR;
    }

    if ((IS_SYS_DC(&dc) && !DB_IS_MAINTENANCE(se)) || IS_CORE_SYS_TABLE(dc.uid, dc.oid)) {
        dc_close(&dc);
        GS_THROW_ERROR(ERR_OPERATIONS_NOT_SUPPORT, "drop table", "system table");
        return GS_ERROR;
    }

    uint32 timeout = se->kernel->attr.ddl_lock_timeout;
    if (lock_table_directly(se, &dc, timeout) != GS_SUCCESS) {
        dc_close(&dc);
        return GS_ERROR;
    }

    if (lock_parent_table_directly(se, dc.handle, GS_TRUE) != GS_SUCCESS) {
        unlock_tables_directly(se);
        dc_close(&dc);
        return GS_ERROR;
    }
    // if table has ddm policy, please drop policy first
    if (db_check_ddm_rule_by_obj(se, dc.uid, dc.oid) != GS_SUCCESS) {
        unlock_tables_directly(se);
        dc_close(&dc);
        return GS_ERROR;
    }

    table = DC_TABLE(&dc);
    is_referenced = db_table_is_referenced(se, table, GS_FALSE);
    if (is_referenced) {
        if (def->options & DROP_CASCADE_CONS) {
            if (db_drop_cascade_cons(se, dc.uid, dc.oid) != GS_SUCCESS) {
                unlock_tables_directly(se);
                dc_close(&dc);
                return GS_ERROR;
            }
        } else {
            unlock_tables_directly(se);
            dc_close(&dc);
            GS_THROW_ERROR(ERR_TABLE_IS_REFERENCED);
            return GS_ERROR;
        }
    }
    // if (db_altable_drop_logical_log(se, &dc, NULL) != GS_SUCCESS) {
    //     unlock_tables_directly(se);
    //     dc_close(&dc);
    //     GS_THROW_ERROR(ERR_DROP_LOGICAL_LOG);
    //     return GS_ERROR;
    // }

    is_drop = (dc.type != DICT_TYPE_TABLE || table->desc.space_id == SYS_SPACE_ID ||
        table->desc.space_id == core->sysaux_space || def->purge || !se->kernel->attr.recyclebin);
    bool32 has_logic = LOGIC_REP_DB_ENABLED(se) && LOGIC_REP_TABLE_ENABLED(se, DC_ENTITY(&dc));
    drop_table_def_t def_drop;
    set_drop_table_def(&def_drop, is_referenced, (char *)table->desc.name, def);
    log_append_lrep_table(se, op_type, has_logic, &def_drop);

    if (is_drop) {
        status = db_drop_table(se, &dc);
    } else {
        status = rb_drop_table(se, &dc);
    }

    if (status != GS_SUCCESS) {
        knl_rollback(session, NULL);
        unlock_tables_directly(se);
        se->rm->is_ddl_op = GS_FALSE;
        dc_close(&dc);
        return GS_ERROR;
    }

    if (has_logic) {
        se->rm->is_ddl_op = GS_FALSE;
    }

    if (is_referenced) {
        dc_invalidate_children(se, (dc_entity_t *)dc.handle);
    }

    unlock_tables_directly(se);
    if (is_drop) {
        dc_free_entry(se, DC_ENTRY(&dc));
    }
    dc_close(&dc);

    return status;
}

status_t knl_drop_table(knl_handle_t session, knl_drop_def_t *def)
{
    knl_session_t *se = (knl_session_t *)session;
    status_t status;
    latch_t *ddl_latch = &se->kernel->db.ddl_latch;
    dc_user_t *user = NULL;

    if (dc_open_user(se, &def->owner, &user) != GS_SUCCESS) {
        return GS_ERROR;
    }

    cm_latch_s(&user->user_latch, se->id, GS_FALSE, NULL);
    if (knl_ddl_latch_s(ddl_latch, session, NULL) != GS_SUCCESS) {
        cm_unlatch(&user->user_latch, NULL);
        return GS_ERROR;
    }

    status = knl_internal_drop_table(session, def);
    cm_unlatch(ddl_latch, NULL);
    cm_unlatch(&user->user_latch, NULL);
    return status;
}

status_t knl_drop_index(knl_handle_t session, knl_drop_def_t *def)
{
    knl_dictionary_t dc;
    table_t *table = NULL;
    rd_table_t redo;
    index_t *index = NULL;
    knl_session_t *se = (knl_session_t *)session;
    text_t *table_name = NULL;
    latch_t *ddl_latch = &se->kernel->db.ddl_latch;
    uint32 op_type = RD_DROP_INDEX;

    if (knl_ddl_enabled(session, GS_TRUE) != GS_SUCCESS) {
        return GS_ERROR;
    }

    table_name = def->ex_name.len > 0 ? &def->ex_name : NULL;

    if (knl_open_dc_by_index(se, &def->owner, table_name, &def->name, &dc) != GS_SUCCESS) {
        if (!(def->options & DROP_IF_EXISTS)) {
            return GS_ERROR;
        }

        int32 err_code = cm_get_error_code();

        if (err_code == ERR_OBJECT_NOT_EXISTS || err_code == ERR_INDEX_NOT_EXIST) {
            cm_reset_error();
            return GS_SUCCESS;
        }

        return GS_ERROR;
    }

    table = DC_TABLE(&dc);
    if (db_check_table_nologging_attr(table) != GS_SUCCESS) {
        dc_close(&dc);
        return GS_ERROR;
    }

    if ((!DB_IS_MAINTENANCE(se) && IS_SYS_TABLE(table)) || IS_CORE_SYS_TABLE(table->desc.uid, table->desc.id)) {
        GS_THROW_ERROR(ERR_OPERATIONS_NOT_SUPPORT, "drop index", "system table");
        dc_close(&dc);
        return GS_ERROR;
    }

    if (knl_ddl_latch_s(ddl_latch, session, NULL) != GS_SUCCESS) {
        dc_close(&dc);
        return GS_ERROR;
    }

    uint32 timeout = se->kernel->attr.ddl_lock_timeout;
    if (lock_table_directly(se, &dc, timeout) != GS_SUCCESS) {
        dc_close(&dc);
        cm_unlatch(ddl_latch, NULL);
        return GS_ERROR;
    }

    if (lock_child_table_directly(se, dc.handle, GS_TRUE) != GS_SUCCESS) {
        unlock_tables_directly(se);
        dc_close(&dc);
        cm_unlatch(ddl_latch, NULL);
        return GS_ERROR;
    }

    index = dc_find_index_by_name(DC_ENTITY(&dc), &def->name);

    if (index == NULL) {
        unlock_tables_directly(se);
        dc_close(&dc);
        cm_unlatch(ddl_latch, NULL);
        if (def->options & DROP_IF_EXISTS) {
            return GS_SUCCESS;
        }
        GS_THROW_ERROR(ERR_INDEX_NOT_EXIST, T2S(&def->owner), T2S_EX(&def->name));
        return GS_ERROR;
    }

    if (index->desc.is_enforced) {
        unlock_tables_directly(se);
        dc_close(&dc);
        cm_unlatch(ddl_latch, NULL);
        GS_THROW_ERROR(ERR_INDEX_ENFORCEMENT);
        return GS_ERROR;
    }

    bool32 has_logic = LOGIC_REP_DB_ENABLED(se) && LOGIC_REP_TABLE_ENABLED(se, DC_ENTITY(&dc));
    log_append_lrep_index(se, op_type, has_logic, (char *)index->desc.name);

    if (db_drop_index(se, index, &dc) != GS_SUCCESS) {
        se->rm->is_ddl_op = GS_FALSE;
        knl_rollback(se, NULL);
        unlock_tables_directly(se);
        dc_close(&dc);
        cm_unlatch(ddl_latch, NULL);
        return GS_ERROR;
    }

    redo.op_type = RD_DROP_INDEX;
    redo.uid = dc.uid;
    redo.oid = dc.oid;
    log_put(se, RD_LOGIC_OPERATION, &redo, sizeof(rd_table_t),
        has_logic ? LOG_ENTRY_FLAG_WITH_LOGIC_OID : LOG_ENTRY_FLAG_NONE);

    if (has_logic) {
        se->rm->is_ddl_op = GS_FALSE;
    }

    SYNC_POINT(session, "SP_B1_DROP_INDEX");
    knl_commit(session);
    if (db_garbage_segment_handle(se, dc.uid, dc.oid, GS_FALSE) != GS_SUCCESS) {
        cm_spin_lock(&se->kernel->rmon_ctx.mark_mutex, NULL);
        se->kernel->rmon_ctx.delay_clean_segments = GS_TRUE;
        cm_spin_unlock(&se->kernel->rmon_ctx.mark_mutex);
        GS_LOG_RUN_ERR("failed to handle garbage segment");
    }

    dc_invalidate_children(se, DC_ENTITY(&dc));
    dc_invalidate(se, DC_ENTITY(&dc));
    dc_close(&dc);
    unlock_tables_directly(se);

    if ((DB_IS_MAINTENANCE(se)) && IS_SYS_DC(&dc)) {
        if (knl_open_dc_by_id(session, dc.uid, dc.oid, &dc, GS_TRUE) != GS_SUCCESS) {
            CM_ABORT(0, "[DB] ABORT INFO: failed to update dictionary cache,"
                "please check environment and restart instance");
        }
        dc_close(&dc);
    }
    cm_unlatch(ddl_latch, NULL);

    return GS_SUCCESS;
}

status_t knl_drop_view(knl_handle_t session, knl_drop_def_t *def)
{
    knl_dictionary_t dc;
    status_t status;
    knl_dict_type_t obj_type;
    knl_session_t *se = (knl_session_t *)session;

    if (knl_ddl_enabled(session, GS_TRUE) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if ((def->options & DROP_IF_EXISTS) && !dc_object_exists(se, &def->owner, &def->name, &obj_type)) {
        return GS_SUCCESS;
    }

    if (dc_open(se, &def->owner, &def->name, &dc) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (dc.type == DICT_TYPE_DYNAMIC_VIEW || dc.type == DICT_TYPE_GLOBAL_DYNAMIC_VIEW) {
        GS_THROW_ERROR(ERR_OPERATIONS_NOT_SUPPORT, "drop view", "dynamic view");
        dc_close(&dc);
        return GS_ERROR;
    }

    if (SYNONYM_EXIST(&dc) || dc.type != DICT_TYPE_VIEW) {
        GS_THROW_ERROR(ERR_TABLE_OR_VIEW_NOT_EXIST, T2S(&def->owner), T2S_EX(&def->name));
        dc_close(&dc);
        return GS_ERROR;
    }

    status = db_drop_view(se, &dc);
    dc_close(&dc);

    return status;
}

static status_t knl_check_truncate_table(knl_session_t *session, knl_trunc_def_t *def, knl_dictionary_t dc)
{
    if (SYNONYM_EXIST(&dc)) {
        GS_THROW_ERROR(ERR_TABLE_OR_VIEW_NOT_EXIST, T2S(&def->owner), T2S_EX(&def->name));
        return GS_ERROR;
    }

    if (dc.type == DICT_TYPE_TABLE_EXTERNAL) {
        GS_THROW_ERROR(ERR_OPERATIONS_NOT_SUPPORT, "truncate table", "external organized table");
        return GS_ERROR;
    }

    if (dc.type < DICT_TYPE_TABLE || dc.type > DICT_TYPE_TABLE_NOLOGGING) {
        GS_THROW_ERROR(ERR_OPERATIONS_NOT_SUPPORT, "truncate table", "view or system table");
        return GS_ERROR;
    }

    if (IS_SYS_DC(&dc)) {
        if (dc.oid == SYS_AUDIT_ID || SYS_STATS_TABLE_ENABLE_TRUNCATE(dc, session)) {
            return GS_SUCCESS;
        }

        GS_THROW_ERROR(ERR_OPERATIONS_NOT_SUPPORT, "truncate table", "system table");
        return GS_ERROR;
    }

    return GS_SUCCESS;
}

status_t knl_truncate_table(knl_handle_t session, knl_trunc_def_t *def)
{
    knl_dictionary_t dc;
    status_t status = GS_ERROR;
    rd_table_t redo;
    knl_session_t *se = (knl_session_t *)session;
    bool32 is_changed = GS_FALSE;
    bool32 is_not_rcyclebin = GS_FALSE;

    if (knl_ddl_enabled(session, GS_FALSE) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (GS_SUCCESS != dc_open(se, &def->owner, &def->name, &dc)) {
        return GS_ERROR;
    }

    table_t *table = DC_TABLE(&dc);
    if (db_check_table_nologging_attr(table) != GS_SUCCESS) {
        dc_close(&dc);
        return GS_ERROR;
    }

    if (knl_check_truncate_table(se, def, dc) != GS_SUCCESS) {
        dc_close(&dc);
        return GS_ERROR;
    }

    uint32 timeout = se->kernel->attr.ddl_lock_timeout;
    if (GS_SUCCESS != lock_table_directly(se, &dc, timeout)) {
        dc_close(&dc);
        return GS_ERROR;
    }

    table = DC_TABLE(&dc);
    if (db_table_is_referenced(se, table, GS_TRUE)) {
        unlock_tables_directly(se);
        dc_close(&dc);
        GS_THROW_ERROR(ERR_TABLE_IS_REFERENCED);
        return GS_ERROR;
    }

    /* reset serial value */
    if (knl_reset_serial_value(session, dc.handle) != GS_SUCCESS) {
        unlock_tables_directly(se);
        dc_close(&dc);
        return GS_ERROR;
    }

    if (dc.type == DICT_TYPE_TABLE || dc.type == DICT_TYPE_TABLE_NOLOGGING) {
        if (!db_table_has_segment(se, &dc)) {
            unlock_tables_directly(se);
            dc_close(&dc);
            return GS_SUCCESS;
        }
    }

    is_not_rcyclebin = dc.type != DICT_TYPE_TABLE || table->desc.space_id == SYS_SPACE_ID ||
        def->option != TRUNC_RECYCLE_STORAGE || !se->kernel->attr.recyclebin || IS_SYS_STATS_TABLE(dc.uid, dc.oid);

    if (is_not_rcyclebin) {
        // when the state(is_invalid) of global index is changed, the flag is_changed will be set to GS_TRUE
        status = db_truncate_table_prepare(se, &dc, def->option & TRUNC_REUSE_STORAGE, &is_changed);
    } else {
        status = rb_truncate_table(se, &dc);
    }

    if (status == GS_SUCCESS) {
        redo.op_type = RD_ALTER_TABLE;
        redo.uid = dc.uid;
        redo.oid = dc.oid;
        if (IS_LOGGING_TABLE_BY_TYPE(dc.type)) {
            log_put(se, RD_LOGIC_OPERATION, &redo, sizeof(rd_table_t), LOG_ENTRY_FLAG_NONE);
        }

        SYNC_POINT(session, "SP_B1_TRUNCATE_TABLE");
        knl_commit(session);
        if (db_garbage_segment_handle(se, dc.uid, dc.oid, GS_FALSE) != GS_SUCCESS) {
            GS_LOG_RUN_ERR("failed to handle garbage segment");
            is_changed = GS_TRUE; // if garbage segment has not been cleaned, must load latest dc;
            db_force_truncate_table(se, &dc, def->option & TRUNC_REUSE_STORAGE, is_not_rcyclebin);
            cm_spin_lock(&se->kernel->rmon_ctx.mark_mutex, NULL);
            se->kernel->rmon_ctx.delay_clean_segments = GS_TRUE;
            cm_spin_unlock(&se->kernel->rmon_ctx.mark_mutex);
        }
        SYNC_POINT(session, "SP_B2_TRUNCATE_TABLE");
    } else {
        knl_rollback(session, NULL);
    }

    // it means that the state(is_invalid) of global index is changed when is_changed is true,
    // then we need invalidate dc
    if (is_not_rcyclebin && !is_changed && table->ashrink_stat == ASHRINK_END) {
        db_update_seg_scn(se, &dc);
    } else {
        dc_invalidate(se, DC_ENTITY(&dc));
    }

    dc_close(&dc);
    unlock_tables_directly(se);

    return status;
}

status_t knl_flashback_table(knl_handle_t session, knl_flashback_def_t *def)
{
    knl_session_t *se = (knl_session_t *)session;
    status_t status;
    latch_t *ddl_latch = &se->kernel->db.ddl_latch;

    if (knl_ddl_enabled(session, GS_TRUE) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (knl_ddl_latch_s(ddl_latch, session, NULL) != GS_SUCCESS) {
        return GS_ERROR;
    }

    status = fb_flashback(se, def);
    cm_unlatch(ddl_latch, NULL);

    return status;
}

void knl_savepoint(knl_handle_t handle, knl_savepoint_t *savepoint)
{
    knl_session_t *session = (knl_session_t *)handle;
    knl_rm_t *rm = session->rm;

    if (rm->txn == NULL) {
        *savepoint = g_init_savepoint;
    } else {
        savepoint->urid = rm->undo_page_info.undo_rid;
        savepoint->noredo_urid = rm->noredo_undo_page_info.undo_rid;
        savepoint->xid = rm->xid.value;
        savepoint->key_lock = rm->key_lock_group;
        savepoint->row_lock = rm->row_lock_group;
        savepoint->sch_lock = rm->sch_lock_group;
        savepoint->alck_lock = rm->alck_lock_group;
        savepoint->lob_items = rm->lob_items;
    }
    savepoint->lsn = session->curr_lsn;
    savepoint->name[0] = '\0';
}

status_t knl_set_savepoint(knl_handle_t handle, text_t *name)
{
    knl_session_t *session = (knl_session_t *)handle;
    knl_rm_t *rm = session->rm;
    knl_savepoint_t *savepoint = NULL;
    uint8 i, j;

    if (DB_IS_READONLY(session)) {
        GS_THROW_ERROR(ERR_CAPABILITY_NOT_SUPPORT, "operation on read only mode");
        return GS_ERROR;
    }

    if (name->len >= GS_MAX_NAME_LEN) {
        GS_THROW_ERROR(ERR_NAME_TOO_LONG, "savepoint", name->len, GS_MAX_NAME_LEN - 1);
        return GS_ERROR;
    }

    for (i = 0; i < rm->svpt_count; i++) {
        if (cm_text_str_equal_ins(name, rm->save_points[i].name)) {
            break;
        }
    }

    // remove the savepoint with same name.
    if (i < rm->svpt_count) {
        for (j = i; j < rm->svpt_count - 1; j++) {
            rm->save_points[j] = rm->save_points[j + 1];
        }
        rm->svpt_count--;
    } else {
        if (rm->svpt_count == GS_MAX_SAVEPOINTS) {
            GS_THROW_ERROR(ERR_TOO_MANY_SAVEPOINTS);
            return GS_ERROR;
        }
    }

    savepoint = &rm->save_points[rm->svpt_count];
    knl_savepoint(session, savepoint);
    (void)cm_text2str(name, savepoint->name, GS_MAX_NAME_LEN);
    rm->svpt_count++;

    return GS_SUCCESS;
}

status_t knl_release_savepoint(knl_handle_t handle, text_t *name)
{
    knl_session_t *session = (knl_session_t *)handle;
    knl_rm_t *rm = session->rm;
    uint8 i;

    for (i = 0; i < rm->svpt_count; i++) {
        if (cm_text_str_equal_ins(name, rm->save_points[i].name)) {
            break;
        }
    }

    if (i == rm->svpt_count) {
        GS_THROW_ERROR(ERR_SAVEPOINT_NOT_EXIST, T2S(name));
        return GS_ERROR;
    }

    rm->svpt_count = i;

    return GS_SUCCESS;
}

status_t knl_rollback_savepoint(knl_handle_t handle, text_t *name)
{
    knl_session_t *session = (knl_session_t *)handle;
    knl_rm_t *rm = session->rm;
    knl_savepoint_t *savepoint = NULL;
    uint8 i;

    for (i = 0; i < rm->svpt_count; i++) {
        if (cm_text_str_equal_ins(name, rm->save_points[i].name)) {
            savepoint = &rm->save_points[i];
            break;
        }
    }

    if (i == rm->svpt_count) {
        GS_THROW_ERROR(ERR_SAVEPOINT_NOT_EXIST, T2S(name));
        return GS_ERROR;
    }

    knl_rollback(session, savepoint);

    return GS_SUCCESS;
}

status_t knl_alloc_swap_extent(knl_handle_t se, page_id_t *extent)
{
    knl_session_t *session = (knl_session_t *)se;
    space_t *swap_space = SPACE_GET(DB_CORE_CTRL(session)->swap_space);

    if (GS_SUCCESS != spc_alloc_swap_extent(session, swap_space, extent)) {
        GS_THROW_ERROR(ERR_ALLOC_TEMP_EXTENT);
        return GS_ERROR;
    }

    knl_panic_log(!IS_INVALID_PAGID(*extent), "alloc swap extent from swap space error, page id %u-%u.", extent->file,
        extent->page);
    knl_panic_log(IS_SWAP_SPACE(SPACE_GET(DATAFILE_GET(extent->file)->space_id)),
        "alloc swap extent from swap space error, page id %u-%u.", extent->file, extent->page);

    session->stat.temp_allocs++;
    return GS_SUCCESS;
}

void knl_release_swap_extent(knl_handle_t se, page_id_t extent)
{
    knl_session_t *session = (knl_session_t *)se;
    space_t *swap_space = SPACE_GET(DB_CORE_CTRL(session)->swap_space);

    knl_panic_log(!IS_INVALID_PAGID(extent), "alloc swap extent from swap space error, page id %u-%u.", extent.file,
        extent.page);
    // verify swap space by space_id. because space->ctrl maybe freed in knl_close_temp_tables
    knl_panic_log((DATAFILE_GET(extent.file)->space_id) == session->kernel->db.ctrl.core.swap_space,
        "release swap extent error, page id %u-%u is below to space %u.", extent.file, extent.page,
        DATAFILE_GET(extent.file)->space_id);

    spc_free_temp_extent(session, swap_space, extent);
    return;
}

static inline void swap_free_cipher_buf(knl_session_t *session, char *data_buf)
{
    if (!session->thread_shared) {
        cm_pop(session->stack);
    } else {
        free(data_buf);
    }
}

status_t knl_read_swap_data(knl_handle_t se, page_id_t extent, uint32 cipher_len, char *data, uint32 size)
{
    knl_session_t *session = (knl_session_t *)se;
    datafile_t *df = &session->kernel->db.datafiles[extent.file];
    int32 *handle = &session->datafiles[extent.file];
    int64 offset = (int64)extent.page * DEFAULT_PAGE_SIZE;
    space_t *swap_space = SPACE_GET(DB_CORE_CTRL(session)->swap_space);
    char *data_buf = data;
    uint32 data_size = size;
    bool8 is_encrypt = cipher_len > 0 ? GS_TRUE : GS_FALSE;

    if (is_encrypt) {
        uint32 extent_size = swap_space->ctrl->extent_size * DEFAULT_PAGE_SIZE;
        if (!session->thread_shared) {
            data_buf = (char *)cm_push(session->stack, extent_size);
        } else {
            data_buf = (char *)malloc(extent_size);
        }
        if (data_buf == NULL) {
            GS_THROW_ERROR(ERR_ALLOC_MEMORY, (uint64)(extent_size), "read swap data");
            return GS_ERROR;
        }
        data_size = extent_size;
    }

    if (spc_read_datafile(session, df, handle, offset, data_buf, data_size) != GS_SUCCESS) {
        spc_close_datafile(df, handle);
        GS_THROW_ERROR(ERR_READ_FILE, errno);
        GS_LOG_RUN_ERR("[SPACE] failed to open datafile %s", df->ctrl->name);
        if (is_encrypt) {
            swap_free_cipher_buf(session, data_buf);
        }
        return GS_ERROR;
    }

    if (is_encrypt) {
        if (cm_kmc_decrypt(GS_KMC_KERNEL_DOMAIN, data_buf, cipher_len, data, &data_size) != GS_SUCCESS) {
            GS_LOG_RUN_ERR("swap data decrypt failed");
            swap_free_cipher_buf(session, data_buf);
            return GS_ERROR;
        }
        knl_panic_log(data_size == size,
            "the data_size is incorrect, panic info: "
            "page %u-%u data_size %u swap_data size %u",
            extent.file, extent.page, data_size, size);
        swap_free_cipher_buf(session, data_buf);
    }

    return GS_SUCCESS;
}

status_t knl_write_swap_data(knl_handle_t se, page_id_t extent, const char *data, uint32 size, uint32 *cipher_len)
{
    knl_session_t *session = (knl_session_t *)se;
    datafile_t *df = &session->kernel->db.datafiles[extent.file];
    int32 *handle = &session->datafiles[extent.file];
    int64 offset = (int64)extent.page * DEFAULT_PAGE_SIZE;
    space_t *swap_space = SPACE_GET(DB_CORE_CTRL(session)->swap_space);
    encrypt_context_t *encrypt_ctx = &session->kernel->encrypt_ctx;
    bool8 is_encrypt = encrypt_ctx->swap_encrypt_flg;
    char *cipher_buf = NULL;

    *cipher_len = 0;
    if (is_encrypt) {
        uint32 extent_size = swap_space->ctrl->extent_size * DEFAULT_PAGE_SIZE;
        *cipher_len = extent_size;
        if (!session->thread_shared) {
            cipher_buf = (char *)cm_push(session->stack, extent_size);
        } else {
            cipher_buf = (char *)malloc(extent_size);
        }
        if (cipher_buf == NULL) {
            GS_THROW_ERROR(ERR_ALLOC_MEMORY, (uint64)(extent_size), "write swap data");
            return GS_ERROR;
        }
        if (cm_kmc_encrypt(GS_KMC_KERNEL_DOMAIN, encrypt_ctx->swap_encrypt_version, data, size, cipher_buf,
            cipher_len) != GS_SUCCESS) {
            GS_LOG_RUN_ERR("swap data encrypt failed");
            swap_free_cipher_buf(session, cipher_buf);
            return GS_ERROR;
        }
        knl_panic_log(*cipher_len - size <= encrypt_ctx->swap_cipher_reserve_size,
            "Encrypted length of data is "
            "invalid, panic info: page %u-%u cipher_len %u size %u swap_cipher_reserve_size %u",
            extent.file, extent.page, *cipher_len, size, encrypt_ctx->swap_cipher_reserve_size);
        knl_panic_log(*cipher_len <= extent_size,
            "Encrypted length of data is more than extent_size, panic info: "
            "page %u-%u cipher_len %u extent_size %u",
            extent.file, extent.page, *cipher_len, extent_size);
        data = cipher_buf;
        size = extent_size;
    }

    if (spc_write_datafile(session, df, handle, offset, data, size) != GS_SUCCESS) {
        spc_close_datafile(df, handle);
        if (is_encrypt) {
            swap_free_cipher_buf(session, cipher_buf);
        }
        GS_THROW_ERROR(ERR_WRITE_FILE, errno);
        GS_LOG_RUN_ERR("[SPACE] failed to write datafile %s", df->ctrl->name);
        return GS_ERROR;
    }

    if (is_encrypt) {
        swap_free_cipher_buf(session, cipher_buf);
    }
    return GS_SUCCESS;
}

uint32 knl_get_swap_extents(knl_handle_t se)
{
    datafile_t *df = NULL;
    knl_session_t *session = (knl_session_t *)se;
    space_t *swap_space = SPACE_GET(DB_CORE_CTRL(session)->swap_space);
    uint32 total_extents = 0;
    int64 df_size = 0;
    uint32 id;

    CM_POINTER2(session, swap_space);

    cm_spin_lock(&swap_space->lock, NULL);

    for (id = 0; id < swap_space->ctrl->file_hwm; id++) {
        if (GS_INVALID_ID32 == swap_space->ctrl->files[id]) {
            continue;
        }

        df = DATAFILE_GET(swap_space->ctrl->files[id]);
        if (DATAFILE_IS_AUTO_EXTEND(df)) {
            df_size = df->ctrl->auto_extend_maxsize;
        } else {
            df_size = df->ctrl->size;
        }
        // size is less than 2^14, file_hwm <= 8, so total_extents is less than uint32
        total_extents += (uint32)(df_size / DEFAULT_PAGE_SIZE / swap_space->ctrl->extent_size);
    }

    cm_spin_unlock(&swap_space->lock);
    return total_extents;
}

status_t knl_alter_database(knl_handle_t session, knl_alterdb_def_t *def)
{
    knl_session_t *se = (knl_session_t *)session;
    status_t status = GS_SUCCESS;
    latch_t *ddl_latch = &se->kernel->db.ddl_latch;
#ifdef _BACKUP
    bak_context_t *ctx = &se->kernel->backup_ctx;

    if (!BAK_NOT_WORK(ctx) && !HIGH_PRIO_ACT(def->action)) {
        GS_THROW_ERROR(ERR_FORBID_ALTER_DATABASE);
        return GS_ERROR;
    }
#endif

    switch (def->action) {
        case STARTUP_DATABASE_MOUNT:
            if (se->kernel->db.status >= DB_STATUS_MOUNT) {
                GS_THROW_ERROR(ERR_DATABASE_ALREADY_MOUNT);
                return GS_ERROR;
            }
            status = db_mount(se);
            if (status != GS_SUCCESS) {
                GS_LOG_RUN_ERR("failed to alter database MOUNT");
            }
            break;

        case STARTUP_DATABASE_OPEN:
            if (se->kernel->db.status > DB_STATUS_MOUNT) {
                GS_THROW_ERROR(ERR_DATABASE_ALREADY_OPEN);
                return GS_ERROR;
            }

            if (se->kernel->db.status < DB_STATUS_MOUNT) {
                status = db_mount(se);
            }

            if (status != GS_SUCCESS) {
                GS_LOG_RUN_ERR("failed to alter database MOUNT");
            } else {
                // 20241029    吴锦锋   针对损坏文件无法打开的问题，增加忽略配置 
                if (se->kernel->attr.ignore_corrupted_logs)
                {
                    def->open_options.ignore_logs = GS_TRUE;
                    def->open_options.ignore_systime = GS_TRUE;

                    // def->open_options.resetlogs = GS_TRUE;
                }

                status = db_open(se, &def->open_options);
                if (status != GS_SUCCESS) {
                    GS_LOG_RUN_ERR("failed to alter database OPEN");
                }
            }
            break;

        case DATABASE_ARCHIVELOG:
            if (se->kernel->db.status != DB_STATUS_MOUNT) {
                GS_THROW_ERROR(ERR_DATABASE_NOT_MOUNT, "set archivelog");
                return GS_ERROR;
            }
            status = db_alter_archivelog(se, ARCHIVE_LOG_ON);
            break;

        case DATABASE_NOARCHIVELOG:
            if (se->kernel->db.status != DB_STATUS_MOUNT) {
                GS_THROW_ERROR(ERR_DATABASE_NOT_MOUNT, "set noarchivelog");
                return GS_ERROR;
            }
            status = db_alter_archivelog(se, ARCHIVE_LOG_OFF);
            break;

        case ADD_LOGFILE:
            if (DB_IS_READONLY(se)) {
                GS_THROW_ERROR(ERR_CAPABILITY_NOT_SUPPORT, "operation on read only mode");
                return GS_ERROR;
            }

            if (se->kernel->db.status != DB_STATUS_OPEN) {
                GS_THROW_ERROR(ERR_DATABASE_NOT_OPEN, "add logfile");
                return GS_ERROR;
            }

            if (knl_ddl_latch_x(ddl_latch, session, NULL) != GS_SUCCESS) {
                return GS_ERROR;
            }
            status = db_alter_add_logfile(se, def);
            cm_unlatch(ddl_latch, NULL);
            break;

        case DROP_LOGFILE:
            if (DB_IS_READONLY(se)) {
                GS_THROW_ERROR(ERR_CAPABILITY_NOT_SUPPORT, "operation on read only mode");
                return GS_ERROR;
            }

            if (se->kernel->db.status != DB_STATUS_OPEN) {
                GS_THROW_ERROR(ERR_DATABASE_NOT_OPEN, "drop logfile");
                return GS_ERROR;
            }

            if (knl_ddl_latch_x(ddl_latch, session, NULL) != GS_SUCCESS) {
                return GS_ERROR;
            }
            status = db_alter_drop_logfile(se, def);
            cm_unlatch(ddl_latch, NULL);
            break;
        case ARCHIVE_LOGFILE:
            status = db_alter_archive_logfile(se, def);
            break;

        case MAXIMIZE_STANDBY_DB:
            status = db_alter_protection_mode(se, def);
            break;
#ifdef _REPLICATION
        case SWITCHOVER_STANDBY:
            status = db_alter_switchover(se, def);
            break;

        case FAILOVER_STANDBY:
            status = db_alter_failover(se, def);
            break;
#endif
        case CONVERT_TO_STANDBY:
            status = db_alter_convert_to_standby(se, def);
            break;

        case CONVERT_TO_READ_ONLY:
            if (knl_ddl_latch_x(ddl_latch, session, NULL) != GS_SUCCESS) {
                return GS_ERROR;
            }

            status = db_alter_convert_to_readonly(se);
            cm_unlatch(ddl_latch, NULL);
            break;

        case CONVERT_TO_READ_WRITE:
            if (knl_ddl_latch_x(ddl_latch, session, NULL) != GS_SUCCESS) {
                return GS_ERROR;
            }

            status = db_alter_convert_to_readwrite(se);
            cm_unlatch(ddl_latch, NULL);
            break;

        case START_STANDBY:
            status = GS_ERROR;
            break;

        case ALTER_DATAFILE:
            if (knl_ddl_latch_s(ddl_latch, session, NULL) != GS_SUCCESS) {
                return GS_ERROR;
            }
            status = db_alter_datafile(se, &def->datafile);
            cm_unlatch(ddl_latch, NULL);
            break;

        case DELETE_ARCHIVELOG:
            // to delete archivelog
            if (se->kernel->db.status < DB_STATUS_OPEN) {
                GS_THROW_ERROR(ERR_DATABASE_NOT_OPEN, "delete archivelog");
                return GS_ERROR;
            }

            if (!se->kernel->arch_ctx.is_archive) {
                GS_THROW_ERROR(ERR_OPERATIONS_NOT_ALLOW, "delete archivelog on noarchivelog mode");
                return GS_ERROR;
            }

            status = db_alter_delete_archivelog(se, def);
            break;

        case DELETE_BACKUPSET:
            status = db_alter_delete_backupset(se, def);
            break;

        case ENABLE_LOGIC_REPLICATION:
            status = db_alter_logicrep(se, LOG_REPLICATION_ON);
            break;

        case DISABLE_LOGIC_REPLICATION:
            status = db_alter_logicrep(se, LOG_REPLICATION_OFF);
            break;

        case DATABASE_CLEAR_LOGFILE:
            status = db_alter_clear_logfile(se, def->clear_logfile_id);
            break;

        case ALTER_CHARSET:
            status = db_alter_charset(se, def->charset_id);
            break;

        case REBUILD_TABLESPACE:
            status = db_alter_rebuild_space(se, &def->rebuild_spc.space_name);
            break;

        case CANCEL_UPGRADE:
            status = db_alter_cancel_upgrade(se);
            break;

        case UPDATE_MASTER_SERVER_KEY:
            cm_spin_lock(&se->kernel->encrypt_ctx.lock, NULL);
            status = db_alter_update_server_masterkey(se);
            cm_spin_unlock(&se->kernel->encrypt_ctx.lock);
            break;
        case UPDATE_MASTER_KERNEL_KEY:
            cm_spin_lock(&se->kernel->encrypt_ctx.lock, NULL);
            status = db_alter_update_kernel_masterkey(se);
            cm_spin_unlock(&se->kernel->encrypt_ctx.lock);
            break;
        case UPDATE_MASTER_ALL_KEY:
            cm_spin_lock(&se->kernel->encrypt_ctx.lock, NULL);
            status = db_alter_update_masterkey(se);
            cm_spin_unlock(&se->kernel->encrypt_ctx.lock);
            break;

        default:
            GS_THROW_ERROR(ERR_INVALID_DATABASE_DEF, "the input is not support");
            return GS_ERROR;
    }
    return status;
}

void knl_get_system_name(knl_handle_t session, constraint_type_t type, char *name, uint32 name_len)
{
    static const char *cons_name_prefix[MAX_CONS_TYPE_COUNT] = { "PK", "UQ", "REF", "CHK" };
    const char *prefix = cons_name_prefix[type];
    knl_instance_t *kernel = ((knl_session_t *)session)->kernel;

    uint32 id = cm_atomic32_inc(&kernel->seq_name);
    int32 ret = sprintf_s(name, name_len, "_%s_SYS_%d_%d", prefix, kernel->db.ctrl.core.open_count, id);
    knl_securec_check_ss(ret);
}

status_t knl_switch_log(knl_handle_t session)
{
    CM_POINTER(session);
    knl_session_t *se = (knl_session_t *)session;
    database_t *db = &se->kernel->db;

    if (DB_IS_READONLY(se)) {
        GS_THROW_ERROR(ERR_CAPABILITY_NOT_SUPPORT, "operation on read only mode");
        return GS_ERROR;
    }

    if (db->status != DB_STATUS_OPEN) {
        GS_THROW_ERROR(ERR_DATABASE_NOT_OPEN, "set param");
        return GS_ERROR;
    }

    if (log_switch_logfile(se, GS_INVALID_FILEID, GS_INVALID_ASN, NULL) != GS_SUCCESS) {
        return GS_ERROR;
    }

    return GS_SUCCESS;
}

status_t knl_checkpoint(knl_handle_t handle, ckpt_type_t type)
{
    knl_session_t *session = (knl_session_t *)handle;
    database_t *db = &session->kernel->db;

    if (db->status != DB_STATUS_OPEN) {
        GS_THROW_ERROR(ERR_DATABASE_NOT_OPEN, "operation");
        return GS_ERROR;
    }

    if (type == CKPT_TYPE_GLOBAL) {
        GS_THROW_ERROR(ERR_CAPABILITY_NOT_SUPPORT, "global checkpoint");
        return GS_ERROR;
    } else {
        ckpt_trigger(session, GS_TRUE, CKPT_TRIGGER_FULL);
        return GS_SUCCESS;
    }
}

typedef status_t (*knl_dump_page_func)(knl_session_t *session, page_head_t *head, cm_dump_t *dump);
typedef struct st_knl_dump_page_obj {
    page_type_t type;             // page type
    knl_dump_page_func dump_func; // page dump function
} knl_dump_page_obj_t;

static knl_dump_page_obj_t g_knl_dump_page_func_list[] = { { PAGE_TYPE_FREE_PAGE, NULL },
                                                           { PAGE_TYPE_SPACE_HEAD, space_head_dump },
                                                           { PAGE_TYPE_HEAP_HEAD, map_segment_dump },
                                                           { PAGE_TYPE_HEAP_MAP, map_dump_page },
                                                           { PAGE_TYPE_HEAP_DATA, heap_dump_page },
                                                           { PAGE_TYPE_UNDO_HEAD, undo_segment_dump },
                                                           { PAGE_TYPE_TXN, txn_dump_page },
                                                           { PAGE_TYPE_UNDO, undo_dump_page },
                                                           { PAGE_TYPE_BTREE_HEAD, btree_segment_dump },
                                                           { PAGE_TYPE_BTREE_NODE, btree_dump_page },
                                                           { PAGE_TYPE_LOB_HEAD, lob_segment_dump },
                                                           { PAGE_TYPE_LOB_DATA, lob_dump_page },
                                                           { PAGE_TYPE_TEMP_HEAP, NULL },
                                                           { PAGE_TYPE_TEMP_INDEX, NULL },
                                                           { PAGE_TYPE_FILE_HEAD, NULL },
                                                           { PAGE_TYPE_CTRL, NULL },
                                                           { PAGE_TYPE_PCRH_DATA, pcrh_dump_page },
                                                           { PAGE_TYPE_PCRB_NODE, pcrb_dump_page },
                                                           { PAGE_TYPE_DF_MAP_HEAD, df_dump_map_head_page },
                                                           { PAGE_TYPE_DF_MAP_DATA, df_dump_map_data_page } };

#define KNL_PAGE_DUMP_COUNT (uint32)(sizeof(g_knl_dump_page_func_list) / sizeof(knl_dump_page_obj_t))

static inline knl_dump_page_func knl_get_page_dump_func(page_type_t type)
{
    for (uint32 i = 0; i < KNL_PAGE_DUMP_COUNT; i++) {
        if (g_knl_dump_page_func_list[i].type == type) {
            return g_knl_dump_page_func_list[i].dump_func;
        }
    }
    return NULL;
}

static status_t knl_dump_page_head(knl_session_t *session, page_head_t *head, cm_dump_t *dump)
{
    dump->offset = 0;

    // the max number of data files is smaller than 1023, file is uint16, page is uint32, is not larger than uint32
    cm_dump(dump, "\ninformation of page %u-%u\n", (uint32)AS_PAGID_PTR(head->id)->file,
        (uint32)AS_PAGID_PTR(head->id)->page);
    cm_dump(dump, "\tlsn: %llu", head->lsn);
    cm_dump(dump, "\tpcn: %u", head->pcn);
    cm_dump(dump, "\tsize: %u", PAGE_SIZE(*head));
    cm_dump(dump, "\ttype: %s", page_type(head->type));
    cm_dump(dump, "\tnext_ext: %u-%u\n", (uint32)AS_PAGID_PTR(head->next_ext)->file,
        (uint32)AS_PAGID_PTR(head->next_ext)->page);
    CM_DUMP_WRITE_FILE(dump);
    return GS_SUCCESS;
}

static status_t knl_internal_dump_page(knl_session_t *session, const char *file_name, page_head_t *page_head,
    cm_dump_t *dump)
{
    knl_dump_page_func dump_func = knl_get_page_dump_func(page_head->type);

    if (dump_func == NULL) {
        GS_THROW_ERROR(ERR_INVALID_PAGE_TYPE);
        return GS_ERROR;
    }

    if (cm_file_exist(file_name)) {
        GS_THROW_ERROR(ERR_FILE_ALREADY_EXIST, file_name, "failed to dump page");
        return GS_ERROR;
    }

    if (cm_create_file(file_name, O_RDWR | O_BINARY | O_SYNC, &dump->handle) != GS_SUCCESS) {
        GS_THROW_ERROR(ERR_CREATE_FILE, file_name, errno);
        return GS_ERROR;
    }

    if (knl_dump_page_head(session, page_head, dump) != GS_SUCCESS) {
        cm_close_file(dump->handle);
        return GS_ERROR;
    }

    status_t status = dump_func(session, page_head, dump);
    cm_close_file(dump->handle);

    return status;
}

status_t knl_dump_ctrl_page(knl_handle_t handle, knl_alter_sys_def_t *def)
{
    knl_session_t *session = (knl_session_t *)handle;
    char file_name[GS_MAX_FILE_NAME_LEN];
    database_ctrl_t *page = NULL;

    // default size 1024
    cm_dump_t dump = {
        .handle = GS_INVALID_HANDLE,
        .buf_size = PAGE_DUMP_SIZE
    };

    if (DB_IS_READONLY(session)) {
        GS_THROW_ERROR(ERR_CAPABILITY_NOT_SUPPORT, "operation on read only mode");
        return GS_ERROR;
    }

    uint32 ret = memset_sp(file_name, GS_MAX_FILE_NAME_LEN, 0, GS_MAX_FILE_NAME_LEN);
    knl_securec_check(ret);

    if (CM_IS_EMPTY(&def->out_file)) {
        ret = snprintf_s(file_name, GS_MAX_FILE_NAME_LEN, GS_MAX_FILE_NAME_LEN - 1, "%s/trc/ctrl_page.trc",
            session->kernel->home);
        knl_securec_check_ss(ret);
    } else {
        if (def->out_file.len >= GS_MAX_FILE_NAME_LEN) {
            GS_THROW_ERROR(ERR_INVALID_FILE_NAME, T2S(&def->out_file), (uint32)GS_MAX_FILE_NAME_LEN);
            return GS_ERROR;
        }

        ret = memcpy_sp(file_name, GS_MAX_FILE_NAME_LEN, def->out_file.str, def->out_file.len);
        knl_securec_check(ret);
    }

    page = (database_ctrl_t *)&session->kernel->db.ctrl;
    if (page == NULL) {
        return GS_SUCCESS;
    }

    if (cm_file_exist(file_name)) {
        GS_THROW_ERROR(ERR_FILE_ALREADY_EXIST, file_name, "failed to dump ctrlfile");
        return GS_ERROR;
    } else {
        if (cm_create_file(file_name, O_RDWR | O_BINARY | O_SYNC | O_TRUNC, &dump.handle) != GS_SUCCESS) {
            return GS_ERROR;
        }
    }

    dump.buf = (char *)cm_push(session->stack, dump.buf_size);
    if (knl_dump_page_head(session, (page_head_t *)page->pages, &dump) != GS_SUCCESS) {
        cm_close_file(dump.handle);
        cm_pop(session->stack);
        return GS_ERROR;
    }

    if (dump_ctrl_page(page, &dump) != GS_SUCCESS) {
        cm_close_file(dump.handle);
        cm_pop(session->stack);
        return GS_ERROR;
    }

    if (dump_rebuild_ctrl_statement(page, &dump) != GS_SUCCESS) {
        cm_close_file(dump.handle);
        cm_pop(session->stack);
        return GS_ERROR;
    }

    cm_close_file(dump.handle);
    cm_pop(session->stack);
    return GS_SUCCESS;
}

status_t knl_dump_page(knl_handle_t handle, knl_alter_sys_def_t *def)
{
    knl_session_t *session = (knl_session_t *)handle;
    char file_name[GS_MAX_FILE_NAME_LEN];
    page_head_t *page = NULL;
    bool32 has_err = GS_FALSE;

    if (DB_IS_READONLY(session)) {
        GS_THROW_ERROR(ERR_CAPABILITY_NOT_SUPPORT, "operation on read only mode");
        return GS_ERROR;
    }

    if (session->kernel->db.status != DB_STATUS_OPEN) {
        GS_THROW_ERROR(ERR_DATABASE_NOT_OPEN, "operation dump page");
        return GS_ERROR;
    }

    uint32 ret = memset_sp(file_name, GS_MAX_FILE_NAME_LEN, 0, GS_MAX_FILE_NAME_LEN);
    knl_securec_check(ret);

    if (CM_IS_EMPTY(&def->out_file)) {
        ret = snprintf_s(file_name, GS_MAX_FILE_NAME_LEN, GS_MAX_FILE_NAME_LEN - 1, "%s/trc/%d_%d.trc",
            session->kernel->home, def->page_id.file, def->page_id.page);
        knl_securec_check_ss(ret);
    } else {
        if (def->out_file.len >= GS_MAX_FILE_NAME_LEN) {
            GS_THROW_ERROR(ERR_INVALID_FILE_NAME, T2S(&def->out_file), (uint32)GS_MAX_FILE_NAME_LEN);
            return GS_ERROR;
        }

        ret = memcpy_sp(file_name, GS_MAX_FILE_NAME_LEN, def->out_file.str, def->out_file.len);
        knl_securec_check(ret);
    }

    if (!spc_validate_page_id(session, def->page_id)) {
        GS_THROW_ERROR(ERR_INVALID_PAGE_ID, "");
        return GS_ERROR;
    }

    if (buf_read_page(session, def->page_id, LATCH_MODE_S, ENTER_PAGE_NORMAL) != GS_SUCCESS) {
        return GS_ERROR;
    }

    // default size 1024
    cm_dump_t dump = {
        .handle = GS_INVALID_HANDLE,
        .buf_size = PAGE_DUMP_SIZE
    };
    dump.buf = (char *)cm_push(session->stack, dump.buf_size);

    if (session->curr_page != NULL) {
        page = (page_head_t *)CURR_PAGE;
        if (knl_internal_dump_page(session, file_name, page, &dump) != GS_SUCCESS) {
            has_err = GS_TRUE;
        }
        buf_leave_page(session, GS_FALSE);
    }

    cm_pop(session->stack);

    return (has_err ? GS_ERROR : GS_SUCCESS);
}

status_t knl_dump_dc(knl_handle_t handle, knl_alter_sys_def_t *def)
{
    knl_session_t *session = (knl_session_t *)handle;
    dc_dump_info_t info;
    status_t status = GS_SUCCESS;

    if (DB_IS_READONLY(session)) {
        GS_THROW_ERROR(ERR_CAPABILITY_NOT_SUPPORT, "operation on read only mode");
        return GS_ERROR;
    }

    if (session->kernel->db.status != DB_STATUS_OPEN) {
        GS_THROW_ERROR(ERR_DATABASE_NOT_OPEN, "operation dump catalog");
        return GS_ERROR;
    }

    cm_dump_t dump;
    dump.handle = GS_INVALID_HANDLE;
    dump.buf_size = PAGE_DUMP_SIZE;
    dump.buf = (char *)cm_push(session->stack, dump.buf_size);
    info = def->dump_info;

    switch (info.dump_type) {
        case DC_DUMP_TABLE:
            status = dc_dump_table(session, &dump, info);
            break;
        case DC_DUMP_USER:
            status = dc_dump_user(session, &dump, info);
            break;
        default:
            break;
    }
    cm_pop(session->stack);
    return status;
}

static status_t knl_get_table_by_pageid(knl_session_t *session, page_head_t *page, uint32 *uid, uint32 *tabid)
{
    bool32 belong = GS_FALSE;
    heap_page_t *heap_page = NULL;
    lob_segment_t *lob_segment = NULL;
    heap_segment_t *heap_segment = NULL;
    btree_segment_t *btree_segment = NULL;

    switch (page->type) {
        case PAGE_TYPE_HEAP_HEAD:
            heap_segment = (heap_segment_t *)((char *)page + sizeof(page_head_t));
            *uid = heap_segment->uid;
            *tabid = heap_segment->oid;
            return GS_SUCCESS;

        case PAGE_TYPE_HEAP_MAP:
            GS_THROW_ERROR(ERR_CAPABILITY_NOT_SUPPORT, "get table info from map page");
            return GS_ERROR;

        case PAGE_TYPE_HEAP_DATA:
        case PAGE_TYPE_PCRH_DATA:
            heap_page = (heap_page_t *)page;
            *uid = heap_page->uid;
            *tabid = heap_page->oid;

            if (heap_check_page_belong_table(session, heap_page, *uid, *tabid, &belong) != GS_SUCCESS) {
                return GS_ERROR;
            }

            if (!belong) {
                GS_THROW_ERROR(ERR_PAGE_NOT_BELONG_TABLE, page_type(heap_page->head.type));
                return GS_ERROR;
            }

            return GS_SUCCESS;

        case PAGE_TYPE_BTREE_HEAD:
            btree_segment = (btree_segment_t *)((char *)page + CM_ALIGN8(sizeof(btree_page_t)));
            *uid = btree_segment->uid;
            *tabid = btree_segment->table_id;
            return GS_SUCCESS;

        case PAGE_TYPE_BTREE_NODE:
            return btree_get_table_by_page(session, page, uid, tabid);

        case PAGE_TYPE_PCRB_NODE:
            return pcrb_get_table_by_page(session, page, uid, tabid);

        case PAGE_TYPE_LOB_HEAD:
            lob_segment = (lob_segment_t *)((char *)page + sizeof(page_head_t));
            *uid = lob_segment->uid;
            *tabid = lob_segment->table_id;
            return GS_SUCCESS;

        case PAGE_TYPE_LOB_DATA:
            return lob_get_table_by_page(session, page, uid, tabid);

        default:
            GS_THROW_ERROR(ERR_PAGE_NOT_BELONG_TABLE, page_type(page->type));
            return GS_ERROR;
    }
}

static status_t knl_fetch_table_name(knl_session_t *session, uint32 uid, uint32 table_id, text_t *table_name)
{
    knl_table_desc_t desc;

    CM_SAVE_STACK(session->stack);
    knl_cursor_t *cursor = knl_push_cursor(session);
    knl_open_sys_cursor(session, cursor, CURSOR_ACTION_SELECT, SYS_TABLE_ID, IX_SYS_TABLE_002_ID);
    knl_init_index_scan(cursor, GS_TRUE);
    knl_set_scan_key(INDEX_DESC(cursor->index), &cursor->scan_range.l_key, GS_TYPE_INTEGER, &uid, sizeof(uint32),
        IX_COL_SYS_TABLE_002_USER_ID);
    knl_set_scan_key(INDEX_DESC(cursor->index), &cursor->scan_range.l_key, GS_TYPE_INTEGER, &table_id, sizeof(uint32),
        IX_COL_SYS_TABLE_002_ID);

    if (knl_fetch(session, cursor)) {
        CM_RESTORE_STACK(session->stack);
        return GS_ERROR;
    }

    if (cursor->eof) {
        GS_THROW_ERROR(ERR_OBJECT_NOT_EXISTS, "table", "");
        CM_RESTORE_STACK(session->stack);
        return GS_ERROR;
    }

    dc_convert_table_desc(cursor, &desc);
    dc_user_t *user = NULL;
    if (dc_open_user_by_id(session, desc.uid, &user) != GS_SUCCESS) {
        CM_RESTORE_STACK(session->stack);
        return GS_ERROR;
    }

    errno_t ret = sprintf_s(table_name->str, table_name->len, "%s.%s", user->desc.name, desc.name);
    knl_securec_check_ss(ret);
    table_name->len = ret;
    CM_RESTORE_STACK(session->stack);
    return GS_SUCCESS;
}

status_t knl_get_table_name(knl_handle_t se, uint32 fileid, uint32 pageid, text_t *table_name)
{
    uint32 uid, tabid;
    page_id_t page_id;
    knl_session_t *session = (knl_session_t *)se;

    page_id.file = fileid;
    page_id.page = pageid;
    if (session->kernel->db.status != DB_STATUS_OPEN) {
        GS_THROW_ERROR(ERR_DATABASE_NOT_OPEN, "operation dump table");
        return GS_ERROR;
    }

    if (!spc_validate_page_id(session, page_id)) {
        GS_THROW_ERROR(ERR_INVALID_PAGE_ID, "");
        return GS_ERROR;
    }

    CM_SAVE_STACK(session->stack);
    page_head_t *page = (page_head_t *)cm_push(session->stack, DEFAULT_PAGE_SIZE);
    buf_enter_page(session, page_id, LATCH_MODE_S, ENTER_PAGE_NORMAL);
    errno_t ret = memcpy_sp(page, DEFAULT_PAGE_SIZE, CURR_PAGE, DEFAULT_PAGE_SIZE);
    knl_securec_check(ret);
    buf_leave_page(session, GS_FALSE);

    if (knl_get_table_by_pageid(session, page, &uid, &tabid) != GS_SUCCESS) {
        CM_RESTORE_STACK(session->stack);
        return GS_ERROR;
    }

    CM_RESTORE_STACK(session->stack);

    if (knl_fetch_table_name(session, uid, tabid, table_name) != GS_SUCCESS) {
        return GS_ERROR;
    }

    return GS_SUCCESS;
}

status_t knl_backup(knl_handle_t session, knl_backup_t *param)
{
    knl_session_t *se = (knl_session_t *)session;

    CM_POINTER(session);
    if (!cm_spin_try_lock(&se->kernel->lock)) {
        GS_THROW_ERROR(ERR_BACKUP_RESTORE, "backup", "because database is starting");
        return GS_ERROR;
    }

    if (se->kernel->db.status != DB_STATUS_OPEN && se->kernel->db.status != DB_STATUS_MOUNT) {
        GS_THROW_ERROR(ERR_INVALID_OPERATION, ",not mount/open mode, can not backup");
        cm_spin_unlock(&se->kernel->lock);
        return GS_ERROR;
    }

    if (se->kernel->db.status == DB_STATUS_MOUNT && param->type == BACKUP_MODE_INCREMENTAL) {
        GS_THROW_ERROR(ERR_INVALID_OPERATION, ",can not make incremental backup in mount mode");
        cm_spin_unlock(&se->kernel->lock);
        return GS_ERROR;
    }
#ifdef _REPLICATION
    if ((!DB_IS_PRIMARY(&se->kernel->db) && !DB_IS_PHYSICAL_STANDBY(&se->kernel->db)) ||
        (DB_IS_RAFT_ENABLED(se->kernel) && !DB_IS_PRIMARY(&se->kernel->db) && param->type != BACKUP_MODE_FULL)) {
        GS_THROW_ERROR(ERR_INVALID_OPERATION, ", can not do backup on current database role");
        cm_spin_unlock(&se->kernel->lock);
        return GS_ERROR;
    }
#else
    if ((!DB_IS_PRIMARY(&se->kernel->db) && !DB_IS_PHYSICAL_STANDBY(&se->kernel->db)) ||
        (!DB_IS_PRIMARY(&se->kernel->db) && param->type != BACKUP_MODE_FULL)) {
        GS_THROW_ERROR(ERR_INVALID_OPERATION, ", can not do backup on current database role");
        cm_spin_unlock(&se->kernel->lock);
        return GS_ERROR;
    }
#endif
    cm_spin_unlock(&se->kernel->lock);

#ifdef _BUCKUP
    if (bak_backup_database(se, param) != GS_SUCCESS) {
        return GS_ERROR;
    }
#endif

    return GS_SUCCESS;
}

status_t knl_restore(knl_handle_t session, knl_restore_t *param)
{
    knl_session_t *se = (knl_session_t *)session;
#ifdef _BACKUP
    bak_context_t *ctx = &se->kernel->backup_ctx;
#endif
    status_t status;

    CM_POINTER(session);
    if (param->type == RESTORE_BLOCK_RECOVER || param->file_type == RESTORE_DATAFILE) {
        if (se->kernel->db.status != DB_STATUS_MOUNT) {
            GS_THROW_ERROR(ERR_INVALID_OPERATION, ",not mount mode, can not recover block or recover file from backup");
            return GS_ERROR;
        }
    } else {
        if (se->kernel->db.status != DB_STATUS_NOMOUNT) {
            GS_THROW_ERROR(ERR_INVALID_OPERATION, ",not nomount mode, can not restore");
            return GS_ERROR;
        }
    }

#ifdef _BACKUP
    if (param->file_type == RESTORE_ALL && ctx->bak.restored) {
        GS_THROW_ERROR(ERR_INVALID_OPERATION, ", restore database has been performed, "
            "please restart and restore again");
        return GS_ERROR;
    }

    if (rst_check_backupset_path(param) != GS_SUCCESS) {
        return GS_ERROR;
    }
#endif

    if (param->file_type == RESTORE_DATAFILE) {
        return abr_restore_file_recover((knl_session_t *)session, param);
    }

#ifdef _BUCKUP
    status = rst_restore_database((knl_session_t *)session, param);
#endif
    if (param->file_type == RESTORE_ALL) {
#ifdef _BUCKUP
        ctx->bak.restored = GS_TRUE;
#endif
        se->kernel->db.ctrl.core.dbid = dbc_generate_dbid(se);
        db_set_ctrl_restored(se, GS_TRUE);
    }

#ifdef _BACKUP
    return status;
#else
    return GS_SUCCESS;
#endif
}

static status_t knl_recover_precheck(knl_session_t *se, knl_recover_t *param, knl_scn_t *max_recover_scn)
{
    if (param->action == RECOVER_UNTIL_CANCEL) {
        if (se->kernel->db.status != DB_STATUS_MOUNT) {
            GS_THROW_ERROR(ERR_DATABASE_NOT_MOUNT, " recover database until cancle ");
            return GS_ERROR;
        }
#ifdef _REPLICATION
        if (DB_IS_RAFT_ENABLED(se->kernel)) {
            GS_THROW_ERROR(ERR_INVALID_OPERATION, ", can not recover database until cancle when database in raft mode");
            return GS_ERROR;
        }
#endif
    } else {
        if (se->kernel->db.status != DB_STATUS_NOMOUNT) {
            GS_THROW_ERROR(ERR_DATABASE_ALREADY_MOUNT);
            return GS_ERROR;
        }

        if (db_mount_ctrl(se) != GS_SUCCESS) {
            return GS_ERROR;
        }

        if (param->action == RECOVER_UNTIL_TIME) {
            if (param->time.tv_sec < se->kernel->db.ctrl.core.init_time) {
                GS_THROW_ERROR(ERR_RECOVER_TIME_INVALID);
                return GS_ERROR;
            }
            *max_recover_scn = KNL_TIME_TO_SCN(&param->time, DB_INIT_TIME(se));
        }

        if (param->action == RECOVER_UNTIL_SCN) {
            *max_recover_scn = param->scn;
        }

        if ((*max_recover_scn) < (uint64)se->kernel->db.ctrl.core.scn) {
            GS_THROW_ERROR(ERR_RECOVER_TIME_INVALID);
            return GS_ERROR;
        }

        if (db_mount(se) != GS_SUCCESS) {
            return GS_ERROR;
        }

        if (DB_IS_PRIMARY(&se->kernel->db) &&
            (param->action == RECOVER_UNTIL_SCN || param->action == RECOVER_UNTIL_TIME)) {
            if (log_prepare_for_pitr(se) != GS_SUCCESS) {
                return GS_ERROR;
            }
        }
    }

    return GS_SUCCESS;
}

status_t knl_recover(knl_handle_t session, knl_recover_t *param)
{
    knl_session_t *se = (knl_session_t *)session;
    knl_scn_t max_recover_scn = GS_INVALID_ID64;

    CM_POINTER(session);

    se->kernel->db.recover_for_restore = GS_TRUE;
    if (knl_recover_precheck(se, param, &max_recover_scn) != GS_SUCCESS) {
        se->kernel->db.recover_for_restore = GS_FALSE;
        return GS_ERROR;
    }

    se->kernel->rcy_ctx.action = param->action;
    if (db_recover(se, max_recover_scn) != GS_SUCCESS) {
        se->kernel->rcy_ctx.action = RECOVER_NORMAL;
        se->kernel->db.recover_for_restore = GS_FALSE;
        return GS_ERROR;
    }

    se->kernel->db.recover_for_restore = GS_FALSE;
    return GS_SUCCESS;
}
#ifdef _REPLICATION
status_t knl_build(knl_handle_t session, knl_build_def_t *param)
{
    knl_session_t *se = (knl_session_t *)session;

    CM_POINTER(session);

    if (db_build_baseline(se, param) != GS_SUCCESS) {
        return GS_ERROR;
    }
    return GS_SUCCESS;
}

status_t knl_stop_build(knl_handle_t session)
{
    knl_session_t *se = (knl_session_t *)session;
    bak_context_t *backup_ctx = &se->kernel->backup_ctx;
    bak_t *bak = &backup_ctx->bak;

    if (DB_IS_CASCADED_PHYSICAL_STANDBY(&se->kernel->db)) {
        GS_THROW_ERROR(ERR_INVALID_OPERATION, ", can not stop build on cascaded standby");
        return GS_ERROR;
    }

    if (!PRIMARY_IS_BUILDING(backup_ctx)) {
        return GS_SUCCESS;
    }

    bak->build_stopped = GS_TRUE;
    bak->failed = GS_TRUE;
    do {
        if (se->killed) {
            bak->build_stopped = GS_FALSE;
            bak->failed = GS_FALSE;
            GS_THROW_ERROR(ERR_OPERATION_KILLED);
            return GS_ERROR;
        }
        cm_sleep(200);
    } while (PRIMARY_IS_BUILDING(backup_ctx));

    bak->build_stopped = GS_FALSE;
    bak->failed = GS_FALSE;
    return GS_SUCCESS;
}
#endif

status_t knl_validate(knl_handle_t session, knl_validate_t *param)
{
    knl_session_t *se = (knl_session_t *)session;

    if (param->validate_type == VALIDATE_DATAFILE_PAGE) {
        return buf_validate_corrupted_page(se, param);
    } else {
#ifdef _BACKUP
        return bak_validate_backupset(se, param);
#else
        return GS_SUCCESS;
#endif
    }
}

status_t knl_lock_tables(knl_handle_t session, lock_tables_def_t *def)
{
    knl_dictionary_t dc;
    lock_table_t *table = NULL;
    galist_t *tables = &def->tables;
    knl_session_t *se = (knl_session_t *)session;
    status_t status = GS_ERROR;
    uint32 wait_time = def->wait_time;
    schema_lock_t *lock = NULL;
    dc_entity_t *entity = NULL;

    if (DB_IS_READONLY(se)) {
        GS_THROW_ERROR(ERR_CAPABILITY_NOT_SUPPORT, "operation on read only mode");
        return GS_ERROR;
    }

    for (uint32 i = 0; i < tables->count; i++) {
        table = (lock_table_t *)cm_galist_get(tables, i);

        if (dc_open(se, &table->schema, &table->name, &dc) != GS_SUCCESS) {
            return GS_ERROR;
        }

        if (SYNONYM_EXIST(&dc)) {
            dc_close(&dc);
            GS_THROW_ERROR(ERR_TABLE_OR_VIEW_NOT_EXIST, T2S(&table->schema), T2S_EX(&table->name));
            return GS_ERROR;
        }

        if (dc.type == DICT_TYPE_DYNAMIC_VIEW || dc.type == DICT_TYPE_VIEW ||
            dc.type == DICT_TYPE_GLOBAL_DYNAMIC_VIEW) {
            dc_close(&dc);
            GS_THROW_ERROR(ERR_INVALID_OPERATION, ",not support lock view");
            return GS_ERROR;
        }

        if (dc.type == DICT_TYPE_TEMP_TABLE_TRANS || dc.type == DICT_TYPE_TEMP_TABLE_SESSION) {
            dc_close(&dc);
            return GS_SUCCESS;
        }

        entity = (dc_entity_t *)dc.handle;
        lock = entity->entry->sch_lock;

        switch (def->lock_mode) {
            case LOCK_MODE_SHARE:
                se->wtid.is_locking = GS_TRUE;
                se->wtid.oid = entity->entry->id;
                se->wtid.uid = entity->entry->uid;
                status = lock_table_shared(se, dc.handle, wait_time);
                break;

            case LOCK_MODE_EXCLUSIVE:
                se->wtid.is_locking = GS_TRUE;
                se->wtid.oid = entity->entry->id;
                se->wtid.uid = entity->entry->uid;
                status = lock_table_exclusive(se, dc.handle, wait_time);
                break;
        }

        if (status != GS_SUCCESS) {
            break;
        }

        cm_spin_lock(&entity->entry->sch_lock_mutex, &se->stat_sch_lock);
        SCH_LOCK_EXPLICIT(se, lock);
        cm_spin_unlock(&entity->entry->sch_lock_mutex);
    }
    se->wtid.is_locking = GS_FALSE;
    dc_close(&dc);
    return status;
}

status_t knl_load_sys_dc(knl_handle_t session, knl_alter_sys_def_t *def)
{
    text_t user;
    text_t name;
    knl_dictionary_t dc;

    cm_str2text(def->param, &user);
    cm_str2text(def->value, &name);

    if (knl_ddl_enabled(session, GS_TRUE) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (!cm_text_str_equal(&user, "SYS")) {
        GS_THROW_ERROR(ERR_OPERATIONS_NOT_SUPPORT, "load sys dictionary", "non-sys user");
        return GS_ERROR;
    }

    if (dc_open((knl_session_t *)session, &user, &name, &dc) != GS_SUCCESS) {
        return GS_ERROR;
    }

    return GS_SUCCESS;
}

status_t knl_init_entry(knl_handle_t session, knl_alter_sys_def_t *def)
{
    knl_session_t *se = (knl_session_t *)session;

    if (knl_ddl_enabled(session, GS_FALSE) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (se->kernel->db.open_status != DB_OPEN_STATUS_UPGRADE) {
        GS_THROW_ERROR(ERR_OPERATIONS_NOT_SUPPORT, "of initializing entry for upgrade mode", "non-upgrade mode");
        return GS_ERROR;
    }

    if (knl_internal_repair_catalog(session) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (dc_init_all_entry_for_upgrade(se) != GS_SUCCESS) {
        return GS_ERROR;
    }

    db_update_sysdata_version(session);

    if (db_callback_function(session) != GS_SUCCESS) {
        return GS_ERROR;
    }

    se->kernel->db.open_status = DB_OPEN_STATUS_UPGRADE_PHASE_2;
    GS_LOG_RUN_INF("[UPGRADE] all entry have been initialized successfully");

    return GS_SUCCESS;
}

xact_status_t knl_xact_status(knl_handle_t session)
{
    knl_session_t *se = (knl_session_t *)session;
    txn_t *txn = NULL;

    if (se->rm == NULL) {
        return XACT_END;
    }

    txn = se->rm->txn;
    if (txn == NULL) {
        return XACT_END;
    }

    return (xact_status_t)txn->status;
}

status_t knl_flush_buffer(knl_handle_t session, knl_alter_sys_def_t *def)
{
    knl_session_t *se = (knl_session_t *)session;
    buf_context_t *ctx = &se->kernel->buf_ctx;
    buf_set_t *set = NULL;
    uint32 total = 0;

    CM_POINTER(session);

    for (uint32 i = 0; i < ctx->buf_set_count; i++) {
        set = &ctx->buf_set[i];
        total += buf_expire_cache(se, set);
    }

    GS_LOG_RUN_INF("recycled (%d) buffer ctrls.", total);
    return GS_SUCCESS;
}

void knl_free_temp_cache_memory(knl_temp_cache_t *temp_table)
{
    if (temp_table->memory != NULL) {
        mctx_destroy(temp_table->memory);
        temp_table->memory = NULL;
    }

    temp_table->cbo_stats = NULL;
}

knl_handle_t knl_get_temp_cache(knl_handle_t session, uint32 uid, uint32 oid)
{
    knl_temp_cache_t *temp_table_ptr = NULL;
    knl_session_t *se = (knl_session_t *)session;
    uint32 i;
    for (i = 0; i < se->temp_table_count; i++) {
        temp_table_ptr = &se->temp_table_cache[i];
        if (temp_table_ptr->user_id == uid && temp_table_ptr->table_id == oid) {
            break;
        }
    }
    if (i >= se->temp_table_count) {
        return NULL;
    }
    return temp_table_ptr;
}

status_t knl_put_temp_cache(knl_handle_t session, knl_handle_t dc_entity)
{
    knl_session_t *se = (knl_session_t *)session;
    dc_entity_t *entity = (dc_entity_t *)dc_entity;
    table_t *table = &entity->table;
    knl_temp_cache_t *temp_table_ptr = NULL;
    uint32 i;
    errno_t errcode;
    index_t *index = NULL;

    for (i = 0; i < se->temp_table_count; i++) {
        if ((se->temp_table_cache[i].user_id == table->desc.uid) &&
            (se->temp_table_cache[i].table_id == table->desc.id)) {
            GS_THROW_ERROR(ERR_OBJECT_ID_EXISTS, "table id in temp table cache", table->desc.id);
            return GS_ERROR;
        }
    }

    for (i = 0; i < se->temp_table_count; i++) {
        temp_table_ptr = &se->temp_table_cache[i];
        if (temp_table_ptr->table_id == GS_INVALID_ID32) {
            break;
        }

        if (!knl_temp_object_isvalid_by_id(se, temp_table_ptr->user_id, temp_table_ptr->table_id,
            temp_table_ptr->org_scn)) {
            GS_LOG_RUN_WAR("free and reuse outdated temp cache for table (%d:%d), cached scn (%lld)",
                temp_table_ptr->user_id, temp_table_ptr->table_id, temp_table_ptr->org_scn);
            knl_free_temp_vm(session, temp_table_ptr);
            break;
        }
    }

    if (i >= se->temp_table_capacity) {
        GS_THROW_ERROR(ERR_TOO_MANY_OBJECTS, se->temp_table_capacity, "items in temp table cache");
        return GS_ERROR;
    }

    if (i >= se->temp_table_count) {
        se->temp_table_count++;
    }

    temp_table_ptr = &se->temp_table_cache[i];
    temp_table_ptr->table_type = entity->type;
    temp_table_ptr->org_scn = table->desc.org_scn;
    temp_table_ptr->seg_scn = se->temp_version++;
    temp_table_ptr->chg_scn = table->desc.chg_scn;
    temp_table_ptr->user_id = table->desc.uid;
    temp_table_ptr->table_id = table->desc.id;
    temp_table_ptr->table_segid = GS_INVALID_ID32;
    temp_table_ptr->index_segid = GS_INVALID_ID32;
    temp_table_ptr->rows = 0;
    temp_table_ptr->serial = 0;
    temp_table_ptr->cbo_stats = NULL;
    temp_table_ptr->rmid = se->rmid;
    temp_table_ptr->hold_rmid = GS_INVALID_ID32;

    if (temp_table_ptr->memory != NULL) {
        knl_free_temp_cache_memory(temp_table_ptr);
    }

    errcode = memset_sp(&temp_table_ptr->index_root, sizeof(temp_table_ptr->index_root), 0xFF,
        sizeof(temp_table_ptr->index_root));
    knl_securec_check(errcode);

    for (i = 0; i < table->index_set.total_count; i++) {
        index = table->index_set.items[i];
        temp_table_ptr->index_root[index->desc.id].org_scn = index->desc.org_scn;
    }

    return GS_SUCCESS;
}

void knl_free_temp_vm(knl_handle_t session, knl_handle_t temp_table)
{
    knl_session_t *se = (knl_session_t *)session;
    knl_temp_cache_t *cache = (knl_temp_cache_t *)temp_table;

    if (cache->table_segid != GS_INVALID_ID32) {
        temp_drop_segment(se->temp_mtrl, cache->table_segid);
        cache->table_segid = GS_INVALID_ID32;
    }

    if (cache->index_segid != GS_INVALID_ID32) {
        temp_drop_segment(se->temp_mtrl, cache->index_segid);
        cache->index_segid = GS_INVALID_ID32;
    }

    cache->table_id = GS_INVALID_ID32;

    knl_free_temp_cache_memory(cache);

    (void)g_knl_callback.invalidate_temp_cursor(session, cache);
}

bool32 knl_is_temp_table_empty(knl_handle_t session, uint32 uid, uint32 oid)
{
    temp_heap_page_t *page = NULL;
    vm_page_t *vm_page = NULL;
    mtrl_segment_t *segment = NULL;
    knl_temp_cache_t *temp_table = NULL;
    knl_session_t *se = (knl_session_t *)session;
    uint32 vmid;

    temp_table = knl_get_temp_cache(session, uid, oid);
    if (temp_table == NULL || temp_table->table_segid == GS_INVALID_ID32) {
        return GS_TRUE;
    }

    segment = se->temp_mtrl->segments[temp_table->table_segid];
    if (segment->vm_list.count > 1) {
        return GS_FALSE;
    }

    vmid = segment->vm_list.last;
    if (buf_enter_temp_page_nolock(se, vmid) != GS_SUCCESS) {
        GS_LOG_RUN_ERR("Fail to open vm (%d) when check temp table.", vmid);
        return GS_FALSE;
    }

    vm_page = buf_curr_temp_page(se);
    page = (temp_heap_page_t *)vm_page->data;
    if (page->dirs == 0) {
        buf_leave_temp_page_nolock(se, GS_FALSE);
        return GS_TRUE;
    }

    buf_leave_temp_page_nolock(se, GS_FALSE);
    return GS_FALSE;
}

status_t knl_ensure_temp_cache(knl_handle_t session, knl_handle_t dc_entity, knl_temp_cache_t **temp_table_ret)
{
    knl_session_t *se = (knl_session_t *)session;
    dc_entity_t *entity = (dc_entity_t *)dc_entity;
    table_t *table = &entity->table;
    knl_temp_cache_t *temp_table;

    temp_table = knl_get_temp_cache(session, table->desc.uid, table->desc.id);

    if (temp_table != NULL) {
        if (temp_table->org_scn != table->desc.org_scn) {
            GS_LOG_RUN_WAR("Found invalid temp cache for table (%d:%d), dc scn(%lld), cached scn (%lld)",
                table->desc.uid, table->desc.oid, table->desc.org_scn, temp_table->org_scn);

            knl_free_temp_vm(session, temp_table);
            temp_table = NULL;
        } else {
            if (temp_table->mem_chg_scn != table->desc.chg_scn) {
                knl_free_temp_cache_memory(temp_table);
            }
        }
    }

    if (temp_table == NULL) {
        if (knl_put_temp_cache(session, dc_entity) != GS_SUCCESS) {
            GS_THROW_ERROR(ERR_TOO_MANY_OBJECTS, se->temp_table_capacity, "temp tables opened");
            *temp_table_ret = NULL;
            return GS_ERROR;
        }

        temp_table = knl_get_temp_cache(session, table->desc.uid, table->desc.id);
        knl_panic_log(temp_table->org_scn == table->desc.org_scn,
            "temp_table's org_scn is not equal to table, panic info: temp_table org_scn %llu table %s org_scn %llu",
            temp_table->org_scn, entity->table.desc.name, table->desc.org_scn);

        if (temp_heap_create_segment(se, temp_table) != GS_SUCCESS) {
            knl_free_temp_vm(session, temp_table);
            *temp_table_ret = NULL;
            return GS_ERROR;
        }
    }
    /* one for temp heap, one for temp index */
    knl_panic_log(temp_table->table_segid < se->temp_table_capacity * 2,
        "temp_table's table_segid is invalid, panic info: table %s table_segid %u temp_table_capacity %u",
        entity->table.desc.name, temp_table->table_segid, se->temp_table_capacity);

    *temp_table_ret = temp_table;
    if (entity->stat_exists) {
        entity->stats_version++;
    }
    return GS_SUCCESS;
}

status_t knl_ensure_temp_index(knl_handle_t session, knl_cursor_t *cursor, knl_dictionary_t *dc,
    knl_temp_cache_t *temp_table)
{
    table_t *table = DC_TABLE(dc);
    index_t *index = NULL;
    temp_btree_segment_t *root_seg = NULL;
    knl_session_t *se = (knl_session_t *)session;

    if (temp_table->chg_scn == table->desc.chg_scn) {
        return GS_SUCCESS;
    }

    for (uint32 i = 0; i < table->index_set.count; i++) {
        index = table->index_set.items[i];
        root_seg = &temp_table->index_root[index->desc.id];

        if (root_seg->org_scn == index->desc.org_scn) {
            continue;
        }

        // index has been dropped and recreated
        if (cursor->action != CURSOR_ACTION_INSERT && cursor->scan_mode == SCAN_MODE_INDEX &&
            cursor->index_slot == index->desc.slot) {
            dc_user_t *user = NULL;

            if (dc_open_user_by_id(se, index->desc.uid, &user) != GS_SUCCESS) {
                return GS_ERROR;
            }

            GS_THROW_ERROR(ERR_INDEX_NOT_EXIST, user->desc.name, index->desc.name);
            return GS_ERROR;
        }

        root_seg->root_vmid = GS_INVALID_ID32;
    }

    return GS_SUCCESS;
}

/* get the temp table cache and attach it with one rm */
static status_t knl_attach_temp_cache(knl_session_t *session, knl_cursor_t *cursor, dc_entity_t *entity,
    knl_temp_cache_t **temp_table_ret)
{
    knl_temp_cache_t *temp_table = NULL;

    if (knl_ensure_temp_cache(session, entity, &temp_table) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (cursor->action > CURSOR_ACTION_SELECT) {
        if (temp_table->hold_rmid != GS_INVALID_ID32 && temp_table->hold_rmid != session->rmid) {
            GS_THROW_ERROR(ERR_TEMP_TABLE_HOLD, entity->entry->user->desc.name, entity->table.desc.name);
            return GS_ERROR;
        }

        if (temp_table->hold_rmid == GS_INVALID_ID32) {
            temp_table->hold_rmid = session->rmid;
        }
    }

    *temp_table_ret = temp_table;
    return GS_SUCCESS;
}

status_t knl_open_temp_cursor(knl_handle_t session, knl_cursor_t *cursor, knl_dictionary_t *dc)
{
    knl_session_t *se = (knl_session_t *)session;
    knl_temp_cache_t *temp_table = NULL;
    dc_entity_t *entity = DC_ENTITY(dc);

    if (knl_attach_temp_cache(session, cursor, entity, &temp_table) != GS_SUCCESS) {
        return GS_ERROR;
    }

    /* 2 means one for temp heap, one for temp index */
    knl_panic_log(temp_table->table_segid < se->temp_table_capacity * 2,
        "temp_table's table_segid is invalid, "
        "panic info: page %u-%u type %u table %s index %s table_segid %u",
        cursor->rowid.file, cursor->rowid.page, ((page_head_t *)cursor->page_buf)->type, entity->table.desc.name,
        ((index_t *)cursor->index)->desc.name, temp_table->table_segid);
    mtrl_segment_t *segment = se->temp_mtrl->segments[temp_table->table_segid];
    cursor->rowid.vmid = segment->vm_list.first;
    cursor->rowid.vm_slot = GS_INVALID_ID16;
    cursor->temp_cache = temp_table;
    cursor->ssn = se->ssn;
    cursor->index = NULL;
    cursor->rowid_no = 0;
    cursor->key_loc.is_initialized = GS_FALSE;

    knl_panic_log(segment->vm_list.count > 0,
        "the count of vm page list is incorrect, panic info: "
        "page %u-%u type %u table %s index %s vm_list count %u",
        cursor->rowid.file, cursor->rowid.page, ((page_head_t *)cursor->page_buf)->type, entity->table.desc.name,
        ((index_t *)cursor->index)->desc.name, segment->vm_list.count);

    if (knl_ensure_temp_index((knl_session_t *)session, cursor, dc, temp_table) != GS_SUCCESS) {
        knl_free_temp_vm(session, temp_table);
        temp_table = NULL;
        return GS_ERROR;
    }

    if (cursor->action == CURSOR_ACTION_INSERT) {
        cursor->rowid_count = 0;
        cursor->row_offset = 0;
        return GS_SUCCESS;
    }

    if (cursor->scan_mode == SCAN_MODE_INDEX) {
        index_t *index = DC_INDEX(dc, cursor->index_slot);
        cursor->index = index;
        cursor->fetch = index->acsor->do_fetch;

        temp_btree_segment_t *root_seg = &temp_table->index_root[((index_t *)cursor->index)->desc.id];
        if (root_seg->root_vmid == GS_INVALID_ID32) {
            if (GS_SUCCESS != temp_btree_create_segment(se, (index_t *)cursor->index, temp_table)) {
                GS_THROW_ERROR(ERR_VM, "fail to create temp_btree_create_segment in knl_open_temp_cursor");
                return GS_ERROR;
            }
        }

        ((index_t *)cursor->index)->desc.entry.vmid = 0;
        ((index_t *)cursor->index)->temp_btree = NULL;
        /* one for heap, one for index */
        knl_panic_log(temp_table->index_segid < se->temp_table_capacity * 2,
            "temp_table's index_segid is invalid, panic info: page %u-%u type %u table %s index %s index_segid %u",
            cursor->rowid.file, cursor->rowid.page, ((page_head_t *)cursor->page_buf)->type, entity->table.desc.name,
            ((index_t *)cursor->index)->desc.name, temp_table->index_segid);
    } else if (cursor->scan_mode == SCAN_MODE_TABLE_FULL) {
        cursor->fetch = TABLE_ACCESSOR(cursor)->do_fetch;
        return GS_SUCCESS;
    } else if (cursor->scan_mode == SCAN_MODE_ROWID) {
        cursor->fetch = TABLE_ACCESSOR(cursor)->do_rowid_fetch;
        return GS_SUCCESS;
    }

    return GS_SUCCESS;
}

status_t knl_exec_grant_privs(knl_handle_t session, knl_grant_def_t *def)
{
    if (knl_ddl_enabled(session, GS_TRUE) != GS_SUCCESS) {
        return GS_ERROR;
    }
    return db_exec_grant_privs((knl_session_t *)session, def);
}

status_t knl_exec_revoke_privs(knl_handle_t session, knl_revoke_def_t *def)
{
    if (knl_ddl_enabled(session, GS_TRUE) != GS_SUCCESS) {
        return GS_ERROR;
    }
    return db_exec_revoke_privs((knl_session_t *)session, def);
}
void knl_close_temp_tables(knl_handle_t session, knl_dict_type_t type)
{
    uint32 i;
    knl_temp_cache_t *temp_table_ptr = NULL;
    knl_session_t *se = (knl_session_t *)session;

    for (i = 0; i < se->temp_table_count; i++) {
        temp_table_ptr = &se->temp_table_cache[i];
        if (temp_table_ptr->table_id != GS_INVALID_ID32) {
            if (temp_table_ptr->table_segid != GS_INVALID_ID32 && type >= temp_table_ptr->table_type &&
                (temp_table_ptr->hold_rmid == GS_INVALID_ID32 ||
                (temp_table_ptr->hold_rmid != GS_INVALID_ID32 && temp_table_ptr->hold_rmid == se->rmid))) {
                knl_free_temp_vm(session, temp_table_ptr);
            }
        }
    }
    if (type == DICT_TYPE_TEMP_TABLE_SESSION) {
        temp_mtrl_release_context(se);
        se->temp_table_count = 0;
    }
}

status_t knl_init_temp_dc(knl_handle_t session)
{
    knl_session_t *sess = (knl_session_t *)session;
    dc_context_t *dc_ctx = &sess->kernel->dc_ctx;
    memory_context_t *context = NULL;
    errno_t ret;

    if (dc_create_memory_context(dc_ctx, &context) != GS_SUCCESS) {
        return GS_ERROR;
    }

    knl_temp_dc_t *temp_dc = NULL;
    if (dc_alloc_mem(dc_ctx, context, sizeof(knl_temp_dc_t), (void **)&temp_dc) != GS_SUCCESS) {
        return GS_ERROR;
    }

    ret = memset_sp(temp_dc, sizeof(knl_temp_dc_t), 0, sizeof(knl_temp_dc_t));
    knl_securec_check(ret);

    temp_dc->entries = sess->temp_dc_entries;
    ret = memset_sp(temp_dc->entries, sizeof(void *) * sess->temp_table_capacity, 0,
        sizeof(void *) * sess->temp_table_capacity);
    knl_securec_check(ret);

    temp_dc->ctx = (void *)context;
    sess->temp_dc = temp_dc;

    return GS_SUCCESS;
}
void knl_release_temp_dc(knl_handle_t session)
{
    knl_session_t *sess = (knl_session_t *)session;
    knl_temp_dc_t *temp_dc = sess->temp_dc;
    if (temp_dc != NULL) {
        for (uint32 i = 0; i < sess->temp_table_capacity; i++) {
            dc_entry_t *entry = (dc_entry_t *)temp_dc->entries[i];
            if (entry != NULL) {
                mctx_destroy(entry->entity->memory);
            }
        }

        memory_context_t *ctx = (memory_context_t *)(temp_dc->ctx);
        mctx_destroy(ctx);
        sess->temp_dc = NULL;
    }
}

status_t knl_get_lob_recycle_pages(knl_handle_t se, page_id_t entry, uint32 *extents, uint32 *pages, uint32 *page_size)
{
    knl_session_t *session = (knl_session_t *)se;
    page_head_t *head = NULL;
    lob_segment_t *lob_segment = NULL;
    datafile_t *datafile = NULL;
    space_t *space = NULL;

    if (!spc_validate_page_id(session, entry)) {
        /* treat it as empty table */
        *extents = 0;
        *pages = 0;
        *page_size = DEFAULT_PAGE_SIZE;
        return GS_SUCCESS;
    }

    if (buf_read_page(session, entry, LATCH_MODE_S, ENTER_PAGE_NORMAL) != GS_SUCCESS) {
        return GS_ERROR;
    }

    head = (page_head_t *)session->curr_page;

    if (head->type != PAGE_TYPE_LOB_HEAD) {
        buf_leave_page(session, GS_FALSE);
        GS_THROW_ERROR(ERR_INVALID_SEGMENT_ENTRY);
        return GS_ERROR;
    }

    lob_segment = (lob_segment_t *)(session->curr_page + sizeof(page_head_t));
    *pages = lob_segment->free_list.count;
    buf_leave_page(session, GS_FALSE);

    datafile = DATAFILE_GET(entry.file);
    space = SPACE_GET(datafile->space_id);
    *page_size = DEFAULT_PAGE_SIZE;
    *extents = (*pages + space->ctrl->extent_size - 1) / space->ctrl->extent_size;

    return GS_SUCCESS;
}

static status_t knl_find_ltt_slot(knl_session_t *session, uint32 *tmp_id)
{
    uint32 id = 0;
    for (id = 0; id < session->temp_table_capacity; id++) {
        dc_entry_t *entry = (dc_entry_t *)session->temp_dc->entries[id];
        if (entry == NULL) {
            break;
        }
    }

    if (id >= session->temp_table_capacity) {
        GS_THROW_ERROR(ERR_TOO_MANY_OBJECTS, session->temp_table_capacity, "local temporary tables");
        return GS_ERROR;
    }

    *tmp_id = id;
    return GS_SUCCESS;
}

status_t knl_create_ltt(knl_handle_t session, knl_table_def_t *def, bool32 *is_existed)
{
    memory_context_t *ctx = NULL;
    dc_entry_t *entry = NULL;
    dc_entity_t *entity = NULL;
    knl_session_t *sess = (knl_session_t *)session;
    dc_context_t *dc_ctx = &sess->kernel->dc_ctx;
    errno_t ret;
    dc_user_t *user = NULL;
    *is_existed = GS_FALSE;

    if (knl_ddl_enabled(session, GS_FALSE) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (dc_open_user(sess, &def->schema, &user) != GS_SUCCESS) {
        return GS_ERROR;
    }

    knl_dictionary_t dc;

    if (dc_find_ltt(sess, user, &def->name, &dc, is_existed) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (*is_existed) {
        if (def->options & CREATE_IF_NOT_EXISTS) {
            return GS_SUCCESS;
        }
        GS_THROW_ERROR(ERR_OBJECT_EXISTS, user->desc.name, T2S(&def->name));
        return GS_ERROR;
    }

    uint32 tmp_id = 0;
    if (knl_find_ltt_slot(sess, &tmp_id) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (dc_create_memory_context(dc_ctx, &ctx) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (dc_alloc_mem(dc_ctx, ctx, sizeof(dc_entity_t), (void **)&entity) != GS_SUCCESS) {
        mctx_destroy(ctx);
        return GS_ERROR;
    }

    ret = memset_sp(entity, sizeof(dc_entity_t), 0, sizeof(dc_entity_t));
    knl_securec_check(ret);
    entity->memory = ctx;
    knl_table_desc_t *desc = &entity->table.desc;
    if (db_init_table_desc(sess, desc, def) != GS_SUCCESS) {
        mctx_destroy(ctx);
        return GS_ERROR;
    }

    if (dc_create_ltt_entry(sess, ctx, user, desc, tmp_id, &entry) != GS_SUCCESS) {
        mctx_destroy(ctx);
        return GS_ERROR;
    }

    entity->type = entry->type;
    entity->entry = entry;
    entry->entity = entity;
    entity->valid = GS_TRUE;

    if (db_create_ltt(sess, def, entity) != GS_SUCCESS) {
        mctx_destroy(ctx);
        return GS_ERROR;
    }

    entry->ready = GS_TRUE;
    sess->temp_dc->entries[tmp_id] = entity->entry;

    return GS_SUCCESS;
}

status_t knl_drop_ltt(knl_handle_t session, knl_drop_def_t *def)
{
    knl_dictionary_t dc;
    dc_user_t *user = NULL;
    bool32 found = GS_FALSE;
    knl_session_t *se = (knl_session_t *)session;

    if (knl_ddl_enabled(session, GS_FALSE) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (dc_open_user(se, &def->owner, &user) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (dc_find_ltt(se, user, &def->name, &dc, &found) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (!found) {
        if (def->options & DROP_IF_EXISTS) {
            return GS_SUCCESS;
        }
        GS_THROW_ERROR(ERR_TABLE_OR_VIEW_NOT_EXIST, T2S(&def->owner), T2S_EX(&def->name));
        return GS_ERROR;
    }

    cm_latch_x(&se->ltt_latch, se->id, NULL);
    (void)db_drop_ltt(se, &dc);
    cm_unlatch(&se->ltt_latch, NULL);
    return GS_SUCCESS;
}

status_t knl_create_ltt_index(knl_handle_t session, knl_index_def_t *def)
{
    knl_dictionary_t dc;
    knl_session_t *se = (knl_session_t *)session;

    if (knl_ddl_enabled(session, GS_FALSE) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (dc_open(se, &def->user, &def->table, &dc) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (knl_judge_index_exist(session, def, DC_ENTITY(&dc))) {
        dc_close(&dc);

        if (def->options & CREATE_IF_NOT_EXISTS) {
            return GS_SUCCESS;
        }
        GS_THROW_ERROR(ERR_OBJECT_EXISTS, "index", T2S(&def->name));
        return GS_ERROR;
    }

    if (db_create_ltt_index(se, def, &dc, GS_TRUE) != GS_SUCCESS) {
        dc_close(&dc);
        return GS_ERROR;
    }

    dc_close(&dc);
    return GS_SUCCESS;
}

status_t knl_drop_ltt_index(knl_handle_t session, knl_drop_def_t *def)
{
    knl_dictionary_t dc;

    if (knl_ddl_enabled(session, GS_FALSE) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (def->ex_owner.str == NULL || def->ex_name.str == NULL) {
        GS_THROW_ERROR_EX(ERR_SQL_SYNTAX_ERROR, "drop index needs on clause for local temporary table");
        return GS_ERROR;
    }

    if (!cm_text_equal(&def->owner, &def->ex_owner)) {
        GS_THROW_ERROR_EX(ERR_SQL_SYNTAX_ERROR, "the owner of index and table need to be the same");
        return GS_ERROR;
    }

    if (dc_open((knl_session_t *)session, &def->ex_owner, &def->ex_name, &dc) != GS_SUCCESS) {
        return GS_ERROR;
    }

    table_t *table = DC_TABLE(&dc);
    index_t *index = NULL;
    for (uint32 i = 0; i < table->index_set.count; i++) {
        index_t *ptr = table->index_set.items[i];
        if (cm_text_str_equal(&def->name, ptr->desc.name)) {
            index = ptr;
            break;
        }
    }

    if (index == NULL) {
        dc_close(&dc);
        GS_THROW_ERROR(ERR_INDEX_NOT_EXIST, T2S(&def->owner), T2S_EX(&def->name));
        return GS_ERROR;
    }

    knl_temp_cache_t *temp_cache = knl_get_temp_cache(session, dc.uid, dc.oid);
    if (temp_cache != NULL) {
        dc_entity_t *entity = DC_ENTITY(&dc);
        knl_table_desc_t *desc = &entity->table.desc;

        knl_panic_log(temp_cache->org_scn == desc->org_scn,
            "the temp_cache's org_scn is not equal to table, "
            "panic info: table %s index %s temp_cache org_scn %llu table org_scn %llu",
            desc->name, index->desc.name, temp_cache->org_scn, desc->org_scn);

        if (temp_cache->index_segid != GS_INVALID_ID32) {
            temp_cache->index_root[index->desc.id].root_vmid = GS_INVALID_ID32;
        }

        temp_cache->index_root[index->desc.id].org_scn = GS_INVALID_ID64;
    }

    uint32 slot = index->desc.slot;
    index_t *end_index = table->index_set.items[table->index_set.count - 1];
    table->index_set.items[slot] = end_index;
    end_index->desc.slot = slot;
    table->index_set.items[table->index_set.count - 1] = NULL;
    table->index_set.count--;
    table->index_set.total_count--;

    dc_close(&dc);
    return GS_SUCCESS;
}

void knl_set_logbuf_stack(knl_handle_t kernel, uint32 sid, char *plog_buf, cm_stack_t *stack)
{
    knl_instance_t *ctx = (knl_instance_t *)kernel;
    knl_session_t *session = ctx->sessions[sid];
    session->stack = stack;
    session->log_buf = plog_buf;
}

void knl_try_begin_session_wait(knl_handle_t se, wait_event_t event, bool32 immediate)
{
    knl_session_t *session = (knl_session_t *)se;
    if (session->is_waiting && session->wait.event == event) {
        return;
    }
    knl_begin_session_wait(se, event, immediate);
}

void knl_try_end_session_wait(knl_handle_t se, wait_event_t event)
{
    knl_session_t *session = (knl_session_t *)se;
    if (!(session->is_waiting && session->wait.event == event)) {
        return;
    }
    knl_end_session_wait(se);
}

void knl_begin_session_wait(knl_handle_t se, wait_event_t event, bool32 immediate)
{
    knl_session_t *session = (knl_session_t *)se;

    if (session->is_waiting && event == session->wait.event) {
        return;
    }

    session->wait.event = event;
    session->wait.usecs = 0;
    session->wait.pre_spin_usecs = cm_total_spin_usecs();
    session->is_waiting = GS_TRUE;
    session->wait.begin_time = session->kernel->attr.timer->now;
    session->wait.immediate = immediate;

    if (!immediate || !session->kernel->attr.enable_timed_stat) {
        return;
    }

    // (void)cm_gettimeofday(&session->wait.begin_tv);
}

void knl_end_session_wait(knl_handle_t se)
{
    knl_session_t *session = (knl_session_t *)se;
    // timeval_t tv_end;

    if (!session->is_waiting) {
        return;
    }

    if (session->wait.immediate && session->kernel->attr.enable_timed_stat) {
        // (void)cm_gettimeofday(&tv_end);
        // session->wait.usecs = TIMEVAL_DIFF_US(&session->wait.begin_tv, &tv_end);
        session->wait.usecs = KNL_NOW(session) - session->wait.begin_time;
    } else {
        session->wait.usecs = cm_total_spin_usecs() - session->wait.pre_spin_usecs;
    }

    if (session->wait.usecs > 0) {
        session->stat.wait_time[session->wait.event] += session->wait.usecs;
    }
    session->stat.wait_count[session->wait.event]++;

    session->is_waiting = GS_FALSE;
}

status_t knl_begin_itl_waits(knl_handle_t se, uint32 *itl_waits)
{
    knl_session_t *session = (knl_session_t *)se;

    if (session->itl_dead_locked) {
        GS_THROW_ERROR(ERR_DEAD_LOCK, "itl", session->id);
        return GS_ERROR;
    }

    if (session->lock_dead_locked) {
        GS_THROW_ERROR(ERR_DEAD_LOCK, "lock", session->id);
        return GS_ERROR;
    }

    if (session->dead_locked) {
        GS_THROW_ERROR(ERR_DEAD_LOCK, "transaction", session->id);
        return GS_ERROR;
    }

    if (session->canceled) {
        GS_THROW_ERROR(ERR_OPERATION_CANCELED);
        return GS_ERROR;
    }

    if (session->killed) {
        GS_THROW_ERROR(ERR_OPERATION_KILLED);
        return GS_ERROR;
    }

    if (!session->is_waiting) {
        *itl_waits = *itl_waits + 1;
    }

    (session, ENQ_TX_ITL, GS_TRUE);
    cm_spin_sleep_and_stat2(1);
    return GS_SUCCESS;
}

void knl_end_itl_waits(knl_handle_t se)
{
    knl_session_t *session = (knl_session_t *)se;

    knl_try_end_session_wait(session, ENQ_TX_ITL);
    session->wpid = INVALID_PAGID;
    session->itl_dead_locked = GS_FALSE;
    session->dead_locked = GS_FALSE;
    session->lock_dead_locked = GS_FALSE;
}

bool32 knl_db_is_primary(knl_handle_t session)
{
    CM_POINTER(session);
    knl_session_t *se = (knl_session_t *)session;
    database_t *db = &se->kernel->db;

    return DB_IS_PRIMARY(db);
}

bool32 knl_db_is_physical_standby(knl_handle_t session)
{
    CM_POINTER(session);
    knl_session_t *se = (knl_session_t *)session;
    database_t *db = &se->kernel->db;

    return DB_IS_PHYSICAL_STANDBY(db);
}

bool32 knl_db_is_cascaded_standby(knl_handle_t session)
{
    CM_POINTER(session);
    knl_session_t *se = (knl_session_t *)session;
    database_t *db = &se->kernel->db;

    return DB_IS_CASCADED_PHYSICAL_STANDBY(db);
}

#ifdef DB_DEBUG_VERSION
status_t knl_add_syncpoint(knl_handle_t session, syncpoint_def_t *def)
{
    knl_session_t *se = (knl_session_t *)session;

    CM_POINTER2(session, def);

    if (DB_IS_READONLY(se)) {
        GS_THROW_ERROR(ERR_CAPABILITY_NOT_SUPPORT, "operation on read only mode");
        return GS_ERROR;
    }

    return sp_add_syncpoint(se, def);
}

status_t knl_reset_syncpoint(knl_handle_t session)
{
    knl_session_t *se = (knl_session_t *)session;

    CM_POINTER(session);

    if (DB_IS_READONLY(se)) {
        GS_THROW_ERROR(ERR_CAPABILITY_NOT_SUPPORT, "operation on read only mode");
        return GS_ERROR;
    }

    return sp_reset_syncpoint(se);
}

status_t knl_exec_syncpoint(knl_handle_t session, const char *syncpoint_name)
{
    CM_POINTER2(session, syncpoint_name);

    return sp_exec_syncpoint(session, syncpoint_name);
}

void knl_clear_syncpoint_action(knl_handle_t session)
{
    CM_POINTER(session);

    sp_clear_syncpoint_action(session);
}

#endif /* DB_DEBUG_VERSION */

status_t knl_analyze_table_dynamic(knl_handle_t session, knl_analyze_tab_def_t *def)
{
    knl_session_t *se = (knl_session_t *)session;

    if (knl_ddl_enabled(session, GS_FALSE) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (def->part_no != GS_INVALID_ID32) {
        return db_analyze_table_part(se, def, GS_TRUE);
    } else {
        return db_analyze_table(se, def, GS_TRUE);
    }
}

status_t knl_analyze_table(knl_handle_t session, knl_analyze_tab_def_t *def)
{
    knl_session_t *se = (knl_session_t *)session;

    if (knl_ddl_enabled(session, GS_FALSE) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (def->part_name.len > 0) {
        return db_analyze_table_part(se, def, GS_FALSE);
    } else {
        return db_analyze_table(se, def, GS_FALSE);
    }
}

status_t knl_analyze_index_dynamic(knl_handle_t session, knl_analyze_index_def_t *def)
{
    knl_session_t *se = (knl_session_t *)session;

    if (knl_ddl_enabled(session, GS_FALSE) != GS_SUCCESS) {
        return GS_ERROR;
    }
#ifdef _STATISTICS
    return db_analyze_index(se, def, GS_TRUE);
#else
    return GS_SUCCESS;
#endif
}

status_t knl_write_sysddm(knl_handle_t *session, knl_ddm_def_t *def)
{
    knl_session_t *se = (knl_session_t *)session;
    knl_dictionary_t dc;

    if (knl_ddl_enabled(session, GS_FALSE) != GS_SUCCESS) {
        return GS_ERROR;
    }
    if (knl_open_dc_by_id(session, def->uid, def->oid, &dc, GS_FALSE) != GS_SUCCESS) {
        return GS_ERROR;
    }

    uint32 timeout = se->kernel->attr.ddl_lock_timeout;
    if (lock_table_directly(se, &dc, timeout) != GS_SUCCESS) {
        dc_close(&dc);
        return GS_ERROR;
    }
    if (db_write_sysddm(se, def) != GS_SUCCESS) {
        unlock_tables_directly(se);
        dc_close(&dc);
        return GS_ERROR;
    }
    knl_ddm_write_rd(session, &dc);
    knl_commit(session);
    dc_invalidate(se, (dc_entity_t *)dc.handle);
    unlock_tables_directly(se);
    dc_close(&dc);
    return GS_SUCCESS;
}
status_t knl_check_ddm_rule(knl_handle_t *session, text_t ownname, text_t tabname, text_t rulename)
{
    knl_dictionary_t dc;
    knl_session_t *se = (knl_session_t *)session;
    knl_ddm_def_t def;
    errno_t ret = memset_sp(&def, sizeof(knl_ddm_def_t), 0, sizeof(knl_ddm_def_t));
    knl_securec_check(ret);
    if (dc_open(se, &ownname, &tabname, &dc) != GS_SUCCESS) {
        return GS_ERROR;
    }
    def.uid = dc.uid;
    def.oid = dc.oid;
    dc_close(&dc);
    if (cm_text2str(&rulename, def.rulename, GS_NAME_BUFFER_SIZE) != GS_SUCCESS) {
        return GS_ERROR;
    }
    return db_check_rule_exists_by_name(se, &def);
}

status_t knl_drop_ddm_rule_by_name(knl_handle_t *session, text_t ownname, text_t tabname, text_t rulename)
{
    knl_session_t *se = (knl_session_t *)session;
    knl_dictionary_t dc;
    knl_ddm_def_t def;
    errno_t ret = memset_sp(&def, sizeof(knl_ddm_def_t), 0, sizeof(knl_ddm_def_t));
    knl_securec_check(ret);

    if (knl_ddl_enabled(session, GS_FALSE) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (dc_open(se, &ownname, &tabname, &dc) != GS_SUCCESS) {
        return GS_ERROR;
    }

    uint32 timeout = se->kernel->attr.ddl_lock_timeout;
    if (lock_table_directly(se, &dc, timeout) != GS_SUCCESS) {
        dc_close(&dc);
        return GS_ERROR;
    }
    def.uid = dc.uid;
    def.oid = dc.oid;
    (void)cm_text2str(&rulename, def.rulename, GS_NAME_BUFFER_SIZE);
    if (db_check_rule_exists_by_name(se, &def) == GS_ERROR) {
        unlock_tables_directly(se);
        dc_close(&dc);
        return GS_ERROR;
    }
    if (db_drop_ddm_rule_by_name(se, &def) != GS_SUCCESS) {
        unlock_tables_directly(se);
        dc_close(&dc);
        return GS_ERROR;
    }
    knl_ddm_write_rd(session, &dc);
    knl_commit(session);
    dc_invalidate(se, (dc_entity_t *)dc.handle);
    unlock_tables_directly(se);
    dc_close(&dc);
    return GS_SUCCESS;
}

status_t knl_analyze_index(knl_handle_t session, knl_analyze_index_def_t *def)
{
    knl_session_t *se = (knl_session_t *)session;

    if (knl_ddl_enabled(session, GS_FALSE) != GS_SUCCESS) {
        return GS_ERROR;
    }
#ifdef _STATISTICS
    return db_analyze_index(se, def, GS_FALSE);
#else
    return GS_SUCCESS;
#endif
}

/*
 * knl_analyze_schema
 *
 * This procedure gathers statistics for all objects in a schema.
 */
status_t knl_analyze_schema(knl_handle_t session, knl_analyze_schema_def_t *def)
{
    if (knl_ddl_enabled(session, GS_FALSE) != GS_SUCCESS) {
        return GS_ERROR;
    }

    return db_analyze_schema((knl_session_t *)session, def);
}

/*
 * knl_delete_table_stats
 *
 * This function is used to delete stat info of table.
 */
status_t knl_delete_table_stats(knl_handle_t session, text_t *own_name, text_t *tab_name, text_t *part_name)
{
    knl_session_t *se = (knl_session_t *)session;

    if (DB_IS_READONLY(se)) {
        GS_THROW_ERROR(ERR_CAPABILITY_NOT_SUPPORT, "operation on read only mode");
        return GS_ERROR;
    }
#ifdef _STATISTICS
    return db_delete_table_stats(se, own_name, tab_name, part_name);
#else
    return GS_SUCCESS;
#endif
}

/*
 * knl_delete_schema_stats
 *
 * This function is used to delete stat info of a schema.
 */
status_t knl_delete_schema_stats(knl_handle_t session, text_t *schema_name)
{
    return db_delete_schema_stats((knl_session_t *)session, schema_name);
}

char *knl_get_db_name(knl_handle_t session)
{
    CM_POINTER(session);
    knl_session_t *se = (knl_session_t *)session;
    database_t *db = &se->kernel->db;

    return db->ctrl.core.name;
}

db_status_t knl_get_db_status(knl_handle_t session)
{
    CM_POINTER(session);
    knl_session_t *se = (knl_session_t *)session;
    database_t *db = &se->kernel->db;

    return db->status;
}
db_open_status_t knl_get_db_open_status(knl_handle_t session)
{
    CM_POINTER(session);
    knl_session_t *se = (knl_session_t *)session;
    database_t *db = &se->kernel->db;

    return db->open_status;
}

uint64 knl_txn_buffer_size(uint32 page_size, uint32 segment_count)
{
    uint32 txn_per_page = (page_size - PAGE_HEAD_SIZE - PAGE_TAIL_SIZE) / sizeof(txn_t);

    /* txn undo page of one undo segment is UNDO_MAX_TXN_PAGE * SIZE_K(8) / page_size */
    uint64 capacity = (uint64)segment_count * (UNDO_MAX_TXN_PAGE * SIZE_K(8) / page_size) * txn_per_page;

    return capacity * sizeof(tx_item_t);
}

status_t knl_get_segment_size_by_cursor(knl_handle_t se, knl_cursor_t *knl_cur, uint32 *extents, uint32 *pages,
    uint32 *page_size)
{
    page_id_t entry;
    knl_session_t *session = (knl_session_t *)se;
    heap_segment_t *segment = (heap_segment_t *)CURSOR_HEAP(knl_cur)->segment;
    if (segment != NULL) {
        entry = segment->extents.first;
        if (knl_get_segment_size(session, entry, extents, pages, page_size) != GS_SUCCESS) {
            return GS_ERROR;
        }
    } else {
        *extents = 0;
        *pages = 0;
        *page_size = 0;
    }
    return GS_SUCCESS;
}

status_t knl_get_segment_size(knl_handle_t se, page_id_t entry, uint32 *extents, uint32 *pages, uint32 *page_size)
{
    knl_session_t *session = (knl_session_t *)se;
    btree_segment_t *btree_segment = NULL;
    heap_segment_t *heap_segment = NULL;
    lob_segment_t *lob_segment = NULL;

    if (!spc_validate_page_id(session, entry)) {
        /* treat it as empty table */
        *extents = 0;
        *pages = 0;
        *page_size = DEFAULT_PAGE_SIZE;

        return GS_SUCCESS;
    }

    if (buf_read_page(session, entry, LATCH_MODE_S, ENTER_PAGE_NORMAL) != GS_SUCCESS) {
        return GS_ERROR;
    }

    page_head_t *head = (page_head_t *)session->curr_page;
    datafile_t *datafile = DATAFILE_GET(entry.file);
    space_t *space = SPACE_GET(datafile->space_id);
    switch (head->type) {
        case PAGE_TYPE_HEAP_HEAD:
            heap_segment = (heap_segment_t *)(session->curr_page + sizeof(page_head_t));
            *extents = heap_segment->extents.count + heap_segment->free_extents.count;
            *pages = heap_get_all_page_count(space, heap_segment);
            break;

        case PAGE_TYPE_BTREE_HEAD:
            btree_segment = (btree_segment_t *)(session->curr_page + sizeof(btree_page_t));
            *extents = btree_segment->extents.count;
            *pages = btree_get_segment_page_count(space, btree_segment);
            break;

        case PAGE_TYPE_LOB_HEAD:
            lob_segment = (lob_segment_t *)(session->curr_page + sizeof(page_head_t));
            *extents = lob_segment->extents.count;
            *pages = spc_pages_by_ext_cnt(space, *extents, head->type);
            break;

        default:
            buf_leave_page(session, GS_FALSE);
            GS_THROW_ERROR(ERR_INVALID_SEGMENT_ENTRY);
            return GS_ERROR;
    }

    buf_leave_page(session, GS_FALSE);
    *page_size = DEFAULT_PAGE_SIZE;
    return GS_SUCCESS;
}

/*
 * get first free extent from given page id.
 * this is the last extent in current datafile when is_last equals true.
 */
status_t knl_get_free_extent(knl_handle_t se, uint32 file_id, page_id_t start, uint32 *extent, uint64 *page_count,
    bool32 *is_last)
{
    knl_session_t *session = (knl_session_t *)se;
    datafile_t *df = DATAFILE_GET(file_id);

    return df_get_free_extent(session, df, start, extent, page_count, is_last);
}

void knl_calc_seg_size(seg_size_type_t type, uint32 pages, uint32 page_size, uint32 extents, int64 *result)
{
    switch (type) {
        case SEG_BYTES:
            *result = (int64)pages * (int64)page_size;
            break;
        case SEG_PAGES:
            *result = (int64)pages;
            break;
        default:
            *result = (int64)extents;
            break;
    }
}

status_t knl_get_partitioned_lobsize(knl_handle_t session, knl_dictionary_t *dc, seg_size_type_t type, int32 col_id,
    int64 *result)
{
    lob_t *lob = NULL;
    knl_column_t *column = NULL;
    dc_entity_t *entity = DC_ENTITY(dc);
    table_t *table = &entity->table;

    if (col_id >= (int32)table->desc.column_count) {
        GS_THROW_ERROR(ERR_INVALID_PARAMETER, "column id");
        return GS_ERROR;
    }

    if (table->part_table == NULL) {
        // not partition, return error
        GS_THROW_ERROR(ERR_INVALID_PART_TYPE, "table", ",can not calc table-size.");
        return GS_ERROR;
    }

    *result = 0;
    for (uint32 i = 0; i < table->desc.column_count; i++) {
        column = dc_get_column(entity, i);
        GS_CONTINUE_IFTRUE(!COLUMN_IS_LOB(column));

        lob = (lob_t *)column->lob;
        GS_CONTINUE_IFTRUE((col_id != -1) && (col_id != lob->desc.column_id));

        if (part_get_lob_segment_size((knl_session_t *)session, dc, lob, type, result) != GS_SUCCESS) {
            return GS_ERROR;
        }

        GS_BREAK_IF_TRUE(col_id != -1);
    }
    return GS_SUCCESS;
}

status_t knl_get_partitioned_tabsize(knl_handle_t session, knl_dictionary_t *dc, seg_size_type_t type, int64 *result)
{
    table_t *table = DC_TABLE(dc);
    part_table_t *part_table = table->part_table;
    knl_session_t *se = (knl_session_t *)session;

    if (table->part_table == NULL) {
        // not partition, return error
        GS_THROW_ERROR(ERR_INVALID_PART_TYPE, "table", ",can not calc table-size.");
        return GS_ERROR;
    }

    *result = 0;
    int64 part_size = 0;
    table_part_t *table_part = NULL;
    for (uint32 i = 0; i < part_table->desc.partcnt; ++i) {
        table_part = PART_GET_ENTITY(part_table, i);
        if (!IS_READY_PART(table_part)) {
            continue;
        }

        part_size = 0;
        if (part_get_heap_segment_size(se, dc, table_part, type, &part_size) != GS_SUCCESS) {
            return GS_ERROR;
        }

        *result += part_size;
    }

    return GS_SUCCESS;
}

status_t knl_get_table_size(knl_handle_t session, knl_dictionary_t *dc, seg_size_type_t type, int64 *result)
{
    table_t *table = DC_TABLE(dc);
    page_id_t entry;
    uint32 pages, page_size, extents;
    if (table->part_table != NULL) {
        return knl_get_partitioned_tabsize(session, dc, type, result);
    }

    *result = 0;
    if (table->heap.segment == NULL) {
        return GS_SUCCESS;
    }
    entry = HEAP_SEGMENT(table->heap.entry, table->heap.segment)->extents.first;
    if (knl_get_segment_size(session, entry, &extents, &pages, &page_size) != GS_SUCCESS) {
        return GS_ERROR;
    }
    (void)knl_calc_seg_size(type, pages, page_size, extents, result);
    return GS_SUCCESS;
}

status_t knl_get_table_partsize(knl_handle_t session, knl_dictionary_t *dc, seg_size_type_t type, text_t *part_name,
    int64 *result)
{
    table_t *table = DC_TABLE(dc);
    part_table_t *part_table = table->part_table;
    knl_session_t *se = (knl_session_t *)session;
    table_part_t *compart = NULL;
    *result = 0;

    if (table->part_table == NULL) {
        GS_THROW_ERROR(ERR_INVALID_PART_TYPE, "table", ",can not calc table-part size.");
        return GS_ERROR;
    }

    if (!part_table_find_by_name(part_table, part_name, &compart)) {
        GS_THROW_ERROR(ERR_PARTITION_NOT_EXIST, "table", T2S(part_name));
        return GS_ERROR;
    }

    return part_get_heap_segment_size(se, dc, compart, type, result);
}

status_t knl_get_partitioned_indsize(knl_handle_t session, knl_dictionary_t *dc, seg_size_type_t type,
    text_t *index_name, int64 *result)
{
    index_t *index = NULL;
    uint32 start_slot, end_slot;
    table_t *table = DC_TABLE(dc);
    knl_session_t *se = (knl_session_t *)session;

    if (table->part_table == NULL) {
        // not partition, return error
        GS_THROW_ERROR(ERR_INVALID_PART_TYPE, "table", ",can not calc index-size.");
        return GS_ERROR;
    }

    if (index_name == NULL) {
        start_slot = 0;
        end_slot = table->index_set.count;
    } else {
        index = dc_find_index_by_name(DC_ENTITY(dc), index_name);
        if (index == NULL) {
            text_t user_name;

            if (knl_get_user_name(session, dc->uid, &user_name) != GS_SUCCESS) {
                return GS_ERROR;
            }

            GS_THROW_ERROR(ERR_INDEX_NOT_EXIST, T2S(&user_name), T2S_EX(index_name));
            return GS_ERROR;
        }

        start_slot = index->desc.slot;
        end_slot = start_slot + 1;
    }

    *result = 0;
    index_part_t *index_part = NULL;
    table_part_t *table_part = NULL;
    for (uint32 i = start_slot; i < end_slot; i++) {
        index = table->index_set.items[i];
        if (!IS_PART_INDEX(index)) {
            continue;
        }

        for (uint32 j = 0; j < index->part_index->desc.partcnt; j++) {
            int64 part_size = 0;
            index_part = INDEX_GET_PART(index, j);
            table_part = TABLE_GET_PART(table, j);
            if (!IS_READY_PART(table_part) || index_part == NULL) {
                continue;
            }

            if (part_get_btree_segment_size(se, index, index_part, type, &part_size) != GS_SUCCESS) {
                return GS_ERROR;
            }

            *result += part_size;
        }

        GS_BREAK_IF_TRUE(index_name != NULL);
    }

    return GS_SUCCESS;
}

status_t knl_create_synonym(knl_handle_t session, knl_synonym_def_t *def)
{
    knl_dictionary_t dc;
    bool32 is_found = GS_FALSE;
    knl_session_t *knl_session = (knl_session_t *)session;
    dc_user_t *user = NULL;
    errno_t ret;

    if (knl_ddl_enabled(session, GS_FALSE) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (dc_open_user(knl_session, &def->owner, &user) != GS_SUCCESS) {
        return GS_ERROR;
    }

    cm_latch_s(&user->user_latch, knl_session->id, GS_FALSE, NULL);

    if (DB_NOT_READY(knl_session)) {
        GS_THROW_ERROR(ERR_NO_DB_ACTIVE);
        cm_unlatch(&user->user_latch, NULL);
        return GS_ERROR;
    }

    /* direct to open dc, no need to open "public" use again */
    ret = memset_sp(&dc, sizeof(knl_dictionary_t), 0, sizeof(knl_dictionary_t));
    knl_securec_check(ret);
    if (knl_open_dc_if_exists(knl_session, &def->owner, &def->name, &dc, &is_found) != GS_SUCCESS) {
        cm_unlatch(&user->user_latch, NULL);
        return GS_ERROR;
    }

    if (SYNONYM_OBJECT_EXIST(&dc)) {
        /* first close the link table */
        dc_close(&dc);
    }

    if (SYNONYM_EXIST(&dc)) {
        if (SYNONYM_IS_REPLACE & def->flags) {
            /* second close the synonym entry, synonym entry do not have the entity */
            if (db_drop_synonym(knl_session, &dc) != GS_SUCCESS) {
                cm_unlatch(&user->user_latch, NULL);
                return GS_ERROR;
            }
        } else {
            GS_THROW_ERROR(ERR_OBJECT_EXISTS, T2S(&def->owner), T2S_EX(&def->name));
            cm_unlatch(&user->user_latch, NULL);
            return GS_ERROR;
        }
    }

    if (db_create_synonym(knl_session, def) != GS_SUCCESS) {
        cm_unlatch(&user->user_latch, NULL);
        return GS_ERROR;
    }

    cm_unlatch(&user->user_latch, NULL);
    return GS_SUCCESS;
}

status_t knl_drop_synonym(knl_handle_t session, knl_drop_def_t *def)
{
    bool32 is_found = GS_FALSE;
    knl_dictionary_t dc;
    knl_session_t *knl_session = (knl_session_t *)session;
    errno_t ret;

    CM_POINTER2(session, def);

    if (knl_ddl_enabled(session, GS_TRUE) != GS_SUCCESS) {
        return GS_ERROR;
    }

    ret = memset_sp(&dc, sizeof(knl_dictionary_t), 0, sizeof(knl_dictionary_t));
    knl_securec_check(ret);

    if (knl_open_dc_if_exists(session, &def->owner, &def->name, &dc, &is_found) != GS_SUCCESS) {
        if (!dc.is_sysnonym) {
            return GS_ERROR;
        }
    }

    if (SYNONYM_OBJECT_EXIST(&dc)) {
        dc_close(&dc);
    }

    if (SYNONYM_NOT_EXIST(&dc)) {
        if (def->options & DROP_IF_EXISTS) {
            return GS_SUCCESS;
        }
        GS_THROW_ERROR(ERR_SYNONYM_NOT_EXIST, T2S(&def->owner), T2S_EX(&def->name));
        return GS_ERROR;
    }

    return db_drop_synonym(knl_session, &dc);
}

status_t knl_delete_dependency(knl_handle_t session, uint32 uid, int64 oid, uint32 tid)
{
    knl_session_t *knl_session = (knl_session_t *)session;

    return db_delete_dependency(knl_session, uid, oid, tid);
}

status_t knl_update_trig_table_flag(knl_handle_t session, knl_table_desc_t *desc, bool32 has_trig)
{
    knl_session_t *knl_session = (knl_session_t *)session;
    return db_update_table_trig_flag(knl_session, desc, has_trig);
}

status_t knl_insert_dependency(knl_handle_t *session, object_address_t *depender, object_address_t *ref_obj,
    uint32 order)
{
    knl_cursor_t *cursor = NULL;
    knl_session_t *knl_session = (knl_session_t *)session;

    CM_SAVE_STACK(knl_session->stack);

    cursor = knl_push_cursor(knl_session);

    if (GS_SUCCESS != db_write_sysdep(knl_session, cursor, depender, ref_obj, order)) {
        CM_RESTORE_STACK(knl_session->stack);
        return GS_ERROR;
    }

    CM_RESTORE_STACK(knl_session->stack);
    return GS_SUCCESS;
}

void knl_get_link_name(knl_dictionary_t *dc, text_t *user, text_t *objname)
{
    dc_entry_t *entry = NULL;
    synonym_link_t *synonym_link = NULL;

    user->str = NULL;
    user->len = 0;
    objname->str = NULL;
    objname->len = 0;

    if (dc->is_sysnonym && dc->syn_handle) {
        entry = (dc_entry_t *)dc->syn_handle;
        if (entry->appendix && entry->appendix->synonym_link) {
            synonym_link = entry->appendix->synonym_link;
            cm_str2text(synonym_link->user, user);
            cm_str2text(synonym_link->name, objname);
        }
    }
}

status_t knl_get_space_size(knl_handle_t se, uint32 space_id, int32 *page_size, knl_handle_t info)
{
    knl_session_t *session = (knl_session_t *)se;
    knl_space_info_t *spc_info = (knl_space_info_t *)info;
    datafile_t *df = NULL;
    int64 normal_pages, normal_size, compress_pages, compress_size;
    uint32 id;

    normal_pages = 0;
    normal_size = 0;
    compress_pages = 0;
    compress_size = 0;

    if (space_id >= GS_MAX_SPACES) {
        GS_THROW_ERROR(ERR_TOO_MANY_OBJECTS, GS_MAX_SPACES, "tablespace");
        return GS_ERROR;
    }

    space_t *space = SPACE_GET(space_id);
    if (!space->ctrl->used) {
        spc_unlock_space(space);
        GS_THROW_ERROR(ERR_OBJECT_ID_NOT_EXIST, "tablespace", space_id);
        return GS_ERROR;
    }

    if (!SPACE_IS_ONLINE(space) || space->head == NULL) {
        spc_unlock_space(space);
        *page_size = DEFAULT_PAGE_SIZE;
        spc_info->total = 0;
        spc_info->used = 0;
        spc_info->normal_total = 0;
        spc_info->normal_used = 0;
        spc_info->compress_total = 0;
        spc_info->compress_used = 0;
        return GS_SUCCESS;
    }

    if (!spc_view_try_lock_space(session, space, "get space size failed")) {
        return GS_ERROR;
    }

    for (uint32 i = 0; i < space->ctrl->file_hwm; i++) {
        id = space->ctrl->files[i];
        if (GS_INVALID_ID32 == id) {
            continue;
        }
        df = DATAFILE_GET(space->ctrl->files[i]);
        if (!DATAFILE_IS_COMPRESS(df)) {
            normal_pages += spc_get_df_used_pages(session, space, i);
            normal_size += DATAFILE_GET(id)->ctrl->size;
        } else {
            compress_pages += spc_get_df_used_pages(session, space, i);
            compress_size += DATAFILE_GET(id)->ctrl->size;
        }
    }

    if (!SPACE_IS_BITMAPMANAGED(space)) {
        normal_pages -= SPACE_HEAD_RESIDENT(space)->free_extents.count * space->ctrl->extent_size;
        normal_pages -= spc_get_punch_extents(session, space) * space->ctrl->extent_size;
    }

    spc_unlock_space(space);

    *page_size = DEFAULT_PAGE_SIZE;
    spc_info->normal_total = normal_size;
    spc_info->normal_used = normal_pages * DEFAULT_PAGE_SIZE;
    spc_info->compress_total = compress_size;
    spc_info->compress_used = compress_pages * DEFAULT_PAGE_SIZE;
    spc_info->used = spc_info->normal_used + spc_info->compress_used;
    spc_info->total = spc_info->normal_total + spc_info->compress_total;
    return GS_SUCCESS;
}

status_t knl_get_space_name(knl_handle_t session, uint32 space_id, text_t *space_name)
{
    return spc_get_space_name((knl_session_t *)session, space_id, space_name);
}

status_t knl_comment_on(knl_handle_t session, knl_comment_def_t *def)
{
    knl_session_t *knl_session = (knl_session_t *)session;
    CM_POINTER2(session, def);

    if (knl_ddl_enabled(session, GS_TRUE) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (GS_SUCCESS != db_comment_on(knl_session, def)) {
        return GS_ERROR;
    }

    knl_commit(session);
    return GS_SUCCESS;
}

/*
 * privilege kernel API
 */
bool32 knl_check_sys_priv_by_name(knl_handle_t session, text_t *user, uint32 priv_id)
{
    return dc_check_sys_priv_by_name((knl_session_t *)session, user, priv_id);
}

bool32 knl_check_sys_priv_by_uid(knl_handle_t session, uint32 uid, uint32 priv_id)
{
    return dc_check_sys_priv_by_uid((knl_session_t *)session, uid, priv_id);
}

bool32 knl_check_dir_priv_by_uid(knl_handle_t session, uint32 uid, uint32 priv_id)
{
    return dc_check_dir_priv_by_uid((knl_session_t *)session, uid, priv_id);
}

bool32 knl_check_obj_priv_by_name(knl_handle_t session, text_t *curr_user, text_t *obj_user, text_t *obj_name,
    object_type_t objtype, uint32 priv_id)
{
    return dc_check_obj_priv_by_name((knl_session_t *)session, curr_user, obj_user, obj_name, objtype, priv_id);
}

bool32 knl_check_user_priv_by_name(knl_handle_t session, text_t *curr_user, text_t *obj_user, uint32 priv_id)
{
    return dc_check_user_priv_by_name((knl_session_t *)session, curr_user, obj_user, priv_id);
}

bool32 knl_check_obj_priv_with_option(knl_handle_t session, text_t *curr_user, text_t *obj_user, text_t *obj_name,
    object_type_t objtype, uint32 priv_id)
{
    return dc_check_obj_priv_with_option((knl_session_t *)session, curr_user, obj_user, obj_name, objtype, priv_id);
}

bool32 knl_check_allobjprivs_with_option(knl_handle_t session, text_t *curr_user, text_t *obj_user, text_t *obj_name,
    object_type_t objtype)
{
    return dc_check_allobjprivs_with_option((knl_session_t *)session, curr_user, obj_user, obj_name, objtype);
}

bool32 knl_sys_priv_with_option(knl_handle_t session, text_t *user, uint32 priv_id)
{
    return dc_sys_priv_with_option((knl_session_t *)session, user, priv_id);
}

bool32 knl_grant_role_with_option(knl_handle_t session, text_t *user, text_t *role, bool32 with_option)
{
    return dc_grant_role_with_option((knl_session_t *)session, user, role, with_option);
}

status_t knl_create_profile(knl_handle_t session, knl_profile_def_t *def)
{
    status_t status = GS_SUCCESS;
    knl_session_t *ptr_session = (knl_session_t *)session;
    profile_t *profile = NULL;
    bucket_t *bucket = NULL;

    if (knl_ddl_enabled(session, GS_TRUE) != GS_SUCCESS) {
        return GS_ERROR;
    }

    bucket = profile_get_bucket(ptr_session, &def->name);
    cm_latch_x(&bucket->latch, ptr_session->id, NULL);
    bool32 is_exists = profile_find_by_name(ptr_session, &def->name, bucket, &profile);

    if (is_exists == GS_TRUE) {
        if (def->is_replace) {
            status = profile_drop(ptr_session, (knl_drop_def_t *)def, profile);
        } else {
            GS_THROW_ERROR(ERR_OBJECT_EXISTS, "profile", T2S(&def->name));
            status = GS_ERROR;
        }
    }

    if (status != GS_SUCCESS) {
        cm_unlatch(&bucket->latch, NULL);
        return status;
    }

    if (profile_alloc_and_insert_bucket(ptr_session, def, bucket, &profile) != GS_SUCCESS) {
        cm_unlatch(&bucket->latch, NULL);
        return GS_ERROR;
    }

    status = profile_create(ptr_session, profile);
    cm_unlatch(&bucket->latch, NULL);

    return status;
}

status_t knl_drop_profile(knl_handle_t session, knl_drop_def_t *def)
{
    status_t status = GS_SUCCESS;
    knl_session_t *ptr_session = (knl_session_t *)session;
    profile_t *profile = NULL;
    bucket_t *bucket = NULL;

    if (knl_ddl_enabled(session, GS_TRUE) != GS_SUCCESS) {
        return GS_ERROR;
    }

    bucket = profile_get_bucket(ptr_session, &def->name);
    cm_latch_x(&bucket->latch, ptr_session->id, NULL);
    bool32 is_exists = profile_find_by_name(ptr_session, &def->name, bucket, &profile);

    if (is_exists == GS_FALSE) {
        cm_unlatch(&bucket->latch, NULL);
        GS_THROW_ERROR(ERR_PROFILE_NOT_EXIST, T2S(&def->name));
        return GS_ERROR;
    }

    status = profile_drop(ptr_session, def, profile);
    cm_unlatch(&bucket->latch, NULL);

    return status;
}

status_t knl_alter_profile(knl_handle_t session, knl_profile_def_t *def)
{
    if (knl_ddl_enabled(session, GS_TRUE) != GS_SUCCESS) {
        return GS_ERROR;
    }
    return profile_alter((knl_session_t *)session, def);
}

status_t knl_create_directory(knl_handle_t session, knl_directory_def_t *def)
{
    if (knl_ddl_enabled(session, GS_TRUE) != GS_SUCCESS) {
        return GS_ERROR;
    }

    return db_create_directory((knl_session_t *)session, def);
}

status_t knl_rebuild_ctrlfile(knl_handle_t session, knl_rebuild_ctrlfile_def_t *def)
{
    return ctrl_rebuild_ctrl_files((knl_session_t *)session, def);
}

status_t knl_drop_directory(knl_handle_t session, knl_drop_def_t *def)
{
    if (knl_ddl_enabled(session, GS_TRUE) != GS_SUCCESS) {
        return GS_ERROR;
    }

    return db_drop_directory((knl_session_t *)session, def);
}


status_t knl_check_user_lock(knl_handle_t session, text_t *user)
{
    if (!KNL_IS_DB_OPEN_NORMAL(session)) {
        return GS_SUCCESS;
    }

    return dc_check_user_lock((knl_session_t *)session, user);
}

status_t knl_check_user_lock_timed(knl_handle_t session, text_t *user, bool32 *p_lock_unlock)
{
    if (!KNL_IS_DB_OPEN_NORMAL(session)) {
        return GS_SUCCESS;
    }

    return dc_check_user_lock_timed((knl_session_t *)session, user, p_lock_unlock);
}

status_t knl_check_user_expire(knl_handle_t session, text_t *user, char *message, uint32 message_len)
{
    if (!KNL_IS_DB_OPEN_NORMAL(session)) {
        return GS_SUCCESS;
    }

    return dc_check_user_expire((knl_session_t *)session, user, message, message_len);
}

status_t knl_process_failed_login(knl_handle_t session, text_t *user, uint32 *p_lock_unlock)
{
    if (!KNL_IS_DB_OPEN_NORMAL(session)) {
        return GS_SUCCESS;
    }

    return dc_process_failed_login((knl_session_t *)session, user, p_lock_unlock);
}

/* the following 3 functions were intended to replaced knl_get_page_size() */
status_t knl_update_serial_value(knl_handle_t session, knl_handle_t dc_entity, int64 value)
{
    dc_entity_t *entity = (dc_entity_t *)dc_entity;
    dc_entry_t *entry = entity->entry;
    knl_session_t *se = (knl_session_t *)session;

    if (entity->type == DICT_TYPE_TEMP_TABLE_SESSION || entity->type == DICT_TYPE_TEMP_TABLE_TRANS) {
        knl_temp_cache_t *temp_table = NULL;

        if (knl_ensure_temp_cache(session, entity, &temp_table) != GS_SUCCESS) {
            return GS_ERROR;
        }

        if (temp_table->serial == 0) {
            temp_table->serial = entity->table.desc.serial_start;
            if (temp_table->serial == 0) {
                temp_table->serial++;
            }
        }

        if (value < temp_table->serial) {
            return GS_SUCCESS;
        }

        temp_table->serial = (value == GS_INVALID_INT64) ? value : (value + 1);

        return GS_SUCCESS;
    }

    if (entity->table.heap.segment == NULL) {
        if (heap_create_entry(se, &entity->table.heap) != GS_SUCCESS) {
            return GS_ERROR;
        }
    }

    if (entry->serial_value == 0) {
        entry->serial_value = HEAP_SEGMENT(entity->table.heap.entry, entity->table.heap.segment)->serial;

        if (entry->serial_value == 0) {
            entry->serial_value = 1;
        }
    }

    if (value < entry->serial_value) {
        return GS_SUCCESS;
    }

    cm_spin_lock(&entry->serial_lock, NULL);
    if (value < entry->serial_value) {
        cm_spin_unlock(&entry->serial_lock);
        return GS_SUCCESS;
    }

    if (value >= GS_INVALID_INT64 - GS_SERIAL_CACHE_COUNT) {
        if (GS_INVALID_INT64 != HEAP_SEGMENT(entity->table.heap.entry, entity->table.heap.segment)->serial) {
            heap_update_serial(se, &entity->table.heap, GS_INVALID_INT64);
        }
    } else if (value >= HEAP_SEGMENT(entity->table.heap.entry, entity->table.heap.segment)->serial) {
        heap_update_serial(se, &entity->table.heap, DC_CACHED_SERIAL_VALUE(value, entity->table.desc.serial_start));
    }

    entry->serial_value = (value == GS_INVALID_INT64) ? value : (value + 1);

    cm_spin_unlock(&entry->serial_lock);

    return GS_SUCCESS;
}

uint32 knl_get_update_info_size(knl_handle_t handle)
{
    uint32 ui_size;
    knl_attr_t *attr = (knl_attr_t *)handle;

    uint32 column_size = attr->max_column_count * sizeof(uint16);
    uint32 offsets_size = attr->max_column_count * sizeof(uint16);
    uint32 lens_size = attr->max_column_count * sizeof(uint16);
    uint32 data_size = attr->page_size;

    ui_size = column_size + offsets_size + lens_size + data_size;

    return ui_size;
}

void knl_bind_update_info(knl_handle_t handle, char *buf)
{
    knl_session_t *session = (knl_session_t *)handle;

    uint32 buf_size = session->kernel->attr.max_column_count;

    session->update_info.columns = (uint16 *)buf;
    session->update_info.offsets = session->update_info.columns + buf_size;
    session->update_info.lens = session->update_info.offsets + buf_size;
    session->update_info.data = (char *)(session->update_info.lens + buf_size);
}

status_t knl_get_page_size(knl_handle_t session, uint32 *page_size)
{
    *page_size = (((knl_session_t *)session)->kernel->attr.page_size);
    return GS_SUCCESS;
}

knl_column_t *knl_find_column(text_t *col_name, knl_dictionary_t *dc)
{
    uint16 col_id;
    knl_column_t *column = NULL;

    col_id = knl_get_column_id(dc, col_name);
    if (col_id == GS_INVALID_ID16) {
        return NULL;
    }

    column = knl_get_column(dc->handle, col_id);
    if (KNL_COLUMN_IS_DELETED(column)) {
        return NULL;
    }

    return column;
}

void knl_get_sync_info(knl_handle_t session, knl_handle_t sync_info)
{
    knl_session_t *se = (knl_session_t *)session;
    ha_sync_info_t *ha_sync_info = (ha_sync_info_t *)sync_info;
#ifdef _REPLICATION
    lsnd_get_sync_info(se, ha_sync_info);
#endif
}

uint32 knl_get_dbwrite_file_id(knl_handle_t session)
{
    knl_instance_t *kernel = (knl_instance_t *)((knl_session_t *)session)->kernel;

    database_t *db = &kernel->db;

    return db->ctrl.core.dw_file_id;
}

uint32 knl_get_dbwrite_end(knl_handle_t session)
{
    knl_instance_t *kernel = (knl_instance_t *)((knl_session_t *)session)->kernel;

    database_t *db = &kernel->db;

    return db->ctrl.core.dw_end;
}

bool32 knl_batch_insert_enabled(knl_handle_t session, knl_dictionary_t *dc, uint8 trig_disable)
{
    dc_entity_t *entity = (dc_entity_t *)dc->handle;
    knl_session_t *se = (knl_session_t *)session;

    if (!trig_disable && entity->trig_set.trig_count > 0) {
        return GS_FALSE;
    }

    if (LOGIC_REP_TABLE_ENABLED((knl_session_t *)session, entity)) {
        return GS_FALSE;
    }

    if (entity->table.desc.cr_mode == CR_PAGE) {
        return GS_TRUE;
    }

    if (!se->kernel->attr.temptable_support_batch) {
        return GS_FALSE;
    }

    return (dc->type == DICT_TYPE_TEMP_TABLE_SESSION || dc->type == DICT_TYPE_TEMP_TABLE_TRANS);
}

static bool32 knl_check_idxes_columns_duplicate(knl_index_def_t *idx1, knl_index_def_t *idx2)
{
    if (idx1->columns.count != idx2->columns.count) {
        return GS_FALSE;
    }

    knl_index_col_def_t *index_col1 = NULL;
    knl_index_col_def_t *index_col2 = NULL;
    for (uint32 i = 0; i < idx1->columns.count; i++) {
        index_col1 = (knl_index_col_def_t *)cm_galist_get(&idx1->columns, i);
        index_col2 = (knl_index_col_def_t *)cm_galist_get(&idx2->columns, i);
        if (!cm_text_equal(&index_col1->name, &index_col2->name)) {
            return GS_FALSE;
        }
    }

    return GS_TRUE;
}

static status_t knl_create_indexes_check_def(knl_session_t *session, knl_indexes_def_t *def)
{
    if (def->index_count > GS_MAX_INDEX_COUNT_PERSQL) {
        GS_THROW_ERROR(ERR_OPERATIONS_NOT_ALLOW, "create more than eight indexes in one SQL statement");
        return GS_ERROR;
    }

    text_t *table_name = &def->indexes_def[0].table;
    text_t *user_name = &def->indexes_def[0].user;
    uint32 parallelism = def->indexes_def[0].parallelism;
    for (uint32 i = def->index_count - 1; i > 0; i--) {
        if (!cm_text_equal_ins(table_name, &def->indexes_def[i].table) ||
            !cm_text_equal_ins(user_name, &def->indexes_def[i].user)) {
            GS_THROW_ERROR(ERR_OPERATIONS_NOT_ALLOW, "create indexcluster for different tables in one SQL statement");
            return GS_ERROR;
        }

        if (parallelism != def->indexes_def[i].parallelism) {
            GS_THROW_ERROR(ERR_OPERATIONS_NOT_ALLOW, "create indexes with different parallelism in one SQL statement");
            return GS_ERROR;
        }
    }

    bool32 is_parted = def->indexes_def[0].parted;
    for (uint32 i = 0; i < def->index_count; i++) {
        if (def->indexes_def[i].parted != is_parted) {
            GS_THROW_ERROR(ERR_OPERATIONS_NOT_ALLOW, "create different type indexes in create indexcluster statement");
            return GS_ERROR;
        }

        if (def->indexes_def[i].is_func) {
            GS_THROW_ERROR(ERR_OPERATIONS_NOT_ALLOW, "create function indexes in create indexcluster SQL statement");
            return GS_ERROR;
        }

        if (def->indexes_def[i].parallelism == 0) {
            GS_THROW_ERROR(ERR_OPERATIONS_NOT_ALLOW, "create indexes without specify parallelism value");
            return GS_ERROR;
        }


        for (uint32 j = 0; j < def->index_count; j++) {
            if (i == j) {
                continue;
            }

            if (cm_text_equal_ins(&def->indexes_def[i].name, &def->indexes_def[j].name) &&
                cm_text_equal_ins(&def->indexes_def[i].user, &def->indexes_def[j].user)) {
                GS_THROW_ERROR(ERR_OPERATIONS_NOT_ALLOW, "create duplicate indexes for the same table");
                return GS_ERROR;
            }

            if (knl_check_idxes_columns_duplicate(&def->indexes_def[i], &def->indexes_def[j])) {
                GS_THROW_ERROR(ERR_OPERATIONS_NOT_ALLOW, "create different indexes with the same index columns");
                return GS_ERROR;
            }
        }
    }

    return GS_SUCCESS;
}

static status_t knl_create_indexes_check_dc(knl_session_t *session, knl_indexes_def_t *def, knl_dictionary_t *dc)
{
    text_t *table_name = &def->indexes_def[0].table;
    text_t *user_name = &def->indexes_def[0].user;

    if (SYNONYM_EXIST(dc)) {
        GS_THROW_ERROR(ERR_TABLE_OR_VIEW_NOT_EXIST, T2S(user_name), T2S_EX(table_name));
        return GS_ERROR;
    }

    if (IS_SYS_DC(dc)) {
        GS_THROW_ERROR(ERR_OPERATIONS_NOT_SUPPORT, "create indexes", "system table");
        return GS_ERROR;
    }

    if (dc->type == DICT_TYPE_TABLE_EXTERNAL) {
        GS_THROW_ERROR(ERR_OPERATIONS_NOT_SUPPORT, "create indexes", "external organized table");
        return GS_ERROR;
    }

    if (dc->type == DICT_TYPE_TEMP_TABLE_SESSION || dc->type == DICT_TYPE_TEMP_TABLE_TRANS) {
        GS_THROW_ERROR(ERR_OPERATIONS_NOT_SUPPORT, "create indexes", "temporary table");
        return GS_ERROR;
    }

    if (dc->type != DICT_TYPE_TABLE && dc->type != DICT_TYPE_TEMP_TABLE_SESSION &&
        dc->type != DICT_TYPE_TEMP_TABLE_TRANS && dc->type != DICT_TYPE_TABLE_NOLOGGING) {
        GS_THROW_ERROR(ERR_OPERATIONS_NOT_SUPPORT, "create indexes", "view");
        return GS_ERROR;
    }

    return GS_SUCCESS;
}

static void knl_create_indexes_release_lock(knl_session_t *session)
{
    latch_t *ddl_latch = &session->kernel->db.ddl_latch;
    unlock_tables_directly(session);
    cm_unlatch(ddl_latch, NULL);
}

static status_t knl_create_indexes_lock_resource(knl_session_t *session, knl_indexes_def_t *def, knl_dictionary_t *dc)
{
    if (knl_create_indexes_check_dc(session, def, dc) != GS_SUCCESS) {
        return GS_ERROR;
    }

    latch_t *ddl_latch = &session->kernel->db.ddl_latch;
    if (knl_ddl_latch_s(ddl_latch, session, NULL) != GS_SUCCESS) {
        return GS_ERROR;
    }

    uint32 timeout = session->kernel->attr.ddl_lock_timeout;
    if (lock_table_directly(session, dc, timeout) != GS_SUCCESS) {
        cm_unlatch(ddl_latch, NULL);
        return GS_ERROR;
    }

    if (lock_child_table_directly(session, dc->handle, GS_TRUE) != GS_SUCCESS) {
        knl_create_indexes_release_lock(session);
        return GS_ERROR;
    }

    for (uint32 i = 0; i < def->index_count; i++) {
        if (knl_judge_index_exist(session, &def->indexes_def[i], DC_ENTITY(dc))) {
            knl_create_indexes_release_lock(session);
            GS_THROW_ERROR(ERR_OBJECT_EXISTS, "index", T2S(&def->indexes_def[i].name));
            return GS_ERROR;
        }
    }

    table_t *table = DC_TABLE(dc);
    if (table->index_set.total_count + def->index_count > GS_MAX_TABLE_INDEXES) {
        knl_create_indexes_release_lock(session);
        GS_THROW_ERROR(ERR_TOO_MANY_INDEXES, T2S(&def->indexes_def[0].user), T2S_EX(&def->indexes_def[0].table));
        return GS_ERROR;
    }

    if (!IS_PART_TABLE(table)) {
        knl_create_indexes_release_lock(session);
        GS_THROW_ERROR(ERR_OPERATIONS_NOT_ALLOW, "create indexes on non-partitioned table");
        return GS_ERROR;
    }

    return GS_SUCCESS;
}

status_t knl_create_indexes(knl_handle_t se, knl_indexes_def_t *def)
{
    knl_session_t *session = (knl_session_t *)se;

    if (!KNL_IS_DB_OPEN_NORMAL(session)) {
        GS_THROW_ERROR(ERR_OPERATIONS_NOT_ALLOW, "create indexes in abnormally open mode");
        return GS_ERROR;
    }

    if (knl_create_indexes_check_def(session, def) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (knl_ddl_enabled(session, GS_TRUE) != GS_SUCCESS) {
        return GS_ERROR;
    }

    knl_dictionary_t dc;
    text_t *table_name = &def->indexes_def[0].table;
    text_t *user_name = &def->indexes_def[0].user;
    if (dc_open(session, user_name, table_name, &dc) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (knl_create_indexes_lock_resource(session, def, &dc) != GS_SUCCESS) {
        dc_close(&dc);
        return GS_ERROR;
    }

    bool32 has_logic = LOGIC_REP_DB_ENABLED(session) && LOGIC_REP_TABLE_ENABLED(session, DC_ENTITY(&dc));
    log_append_lrep_info(session, RD_CREATE_INDEXES, has_logic);
    if (db_create_indexes(session, def, &dc) != GS_SUCCESS) {
        knl_rollback(session, NULL);
        session->rm->is_ddl_op = GS_FALSE;
        knl_create_indexes_release_lock(session);
        dc_close(&dc);
        return GS_ERROR;
    }

    rd_table_t redo;
    redo.op_type = RD_CREATE_INDEXES;
    redo.uid = dc.uid;
    redo.oid = dc.oid;
    log_put(session, RD_LOGIC_OPERATION, &redo, sizeof(rd_table_t),
        has_logic ? LOG_ENTRY_FLAG_WITH_LOGIC_OID : LOG_ENTRY_FLAG_NONE);
    if (has_logic) {
        session->rm->is_ddl_op = GS_FALSE;
    }

    knl_commit(session);
    dc_invalidate_children(session, (dc_entity_t *)dc.handle);
    dc_invalidate(session, (dc_entity_t *)dc.handle);
    knl_create_indexes_release_lock(session);
    dc_close(&dc);

    return GS_SUCCESS;
}

#ifdef Z_SHARDING

routing_info_t *knl_get_table_routing_info(knl_handle_t dc_entity)
{
    return &((dc_entity_t *)dc_entity)->table.routing_info;
}

memory_context_t *knl_get_dc_memory_context(knl_handle_t dc_entity)
{
    return ((dc_entity_t *)dc_entity)->memory;
}

void knl_get_low_arch(knl_handle_t session, uint32 *rst_id, uint32 *asn)
{
    knl_session_t *se = (knl_session_t *)session;
    CM_POINTER(se);

    log_get_curr_rstid_asn(se, rst_id, asn);
}

void knl_get_high_arch(knl_handle_t session, uint32 *rst_id, uint32 *asn)
{
    knl_session_t *se = (knl_session_t *)session;
    CM_POINTER(se);

    arch_get_last_rstid_asn(se, rst_id, asn);
}

char *knl_get_arch_dest_type(knl_handle_t session, uint32 id, knl_handle_t attr, bool32 *is_primary)
{
    knl_session_t *se = (knl_session_t *)session;
    arch_attr_t *arch_attr = (arch_attr_t *)attr;
    CM_POINTER3(se, arch_attr, is_primary);

    return arch_get_dest_type(se, id, arch_attr, is_primary);
}

void knl_get_arch_dest_path(knl_handle_t session, uint32 id, knl_handle_t attr, char *path, uint32 path_size)
{
    knl_session_t *se = (knl_session_t *)session;
    arch_attr_t *arch_attr = (arch_attr_t *)attr;
    CM_POINTER3(se, arch_attr, path);

    arch_get_dest_path(se, id, arch_attr, path, path_size);
}

char *knl_get_arch_sync_status(knl_handle_t session, uint32 id, knl_handle_t attr, knl_handle_t dest_sync)
{
    knl_session_t *se = (knl_session_t *)session;
    arch_attr_t *arch_attr = (arch_attr_t *)attr;
    arch_dest_sync_t *sync = (arch_dest_sync_t *)dest_sync;
    CM_POINTER3(se, arch_attr, sync);

    return arch_get_sync_status(se, id, arch_attr, sync);
}

char *knl_get_arch_sync(knl_handle_t dest_sync)
{
    arch_dest_sync_t *sync = (arch_dest_sync_t *)dest_sync;
    CM_POINTER(sync);

    return arch_get_dest_sync(sync);
}

/*
 * get the name of datafile according to the file number specified by user.
 *
 * @Note
 * the output argument will return the name to the datafile_t directly,
 * if the caller(SQL engine) wants to store the name, it should allocate memory by itself
 */
status_t knl_get_dfname_by_number(knl_handle_t session, int32 filenumber, char **filename)
{
    return spc_get_datafile_name_bynumber((knl_session_t *)session, filenumber, filename);
}

status_t knl_regist_trigger(knl_handle_t session, text_t *user, text_t *table, void *entry)
{
    dc_entry_t *dc_entry;
    knl_dictionary_t dc;

    dc_entry = knl_get_dc_entry(session, user, table, &dc);
    if (dc_entry == NULL) {
        GS_THROW_ERROR(ERR_TABLE_OR_VIEW_NOT_EXIST, T2S(user), T2S_EX(table));
        return GS_ERROR;
    }

    if (dc_is_reserved_entry(dc_entry->uid, dc_entry->id)) {
        GS_THROW_ERROR(ERR_OPERATIONS_NOT_SUPPORT, "register trigger", "system table");
        return GS_ERROR;
    }

    return dc_add_trigger((knl_session_t *)session, &dc, dc_entry, entry);
}

status_t knl_regist_trigger_2(knl_handle_t session, knl_dictionary_t *dc, void *entry)
{
    if (IS_SYS_DC(dc)) {
        GS_THROW_ERROR(ERR_OPERATIONS_NOT_SUPPORT, "register trigger", "system table");
        return GS_ERROR;
    }

    knl_session_t *se = (knl_session_t *)session;
    return dc_add_trigger(se, dc, DC_ENTITY(dc)->entry, entry);
}

bool32 knl_has_update_default_col(knl_handle_t handle)
{
    dc_entity_t *dc_entity = (dc_entity_t *)handle;
    return dc_entity->has_udef_col;
}

bool32 knl_failover_triggered_pending(knl_handle_t knl_handle)
{
    knl_instance_t *kernel = (knl_instance_t *)knl_handle;
    switch_ctrl_t *ctrl = &kernel->switch_ctrl;
#ifdef _REPLICATION
    if (!DB_IS_RAFT_ENABLED(kernel)) {
        return (ctrl->request == SWITCH_REQ_FAILOVER_PROMOTE || ctrl->request == SWITCH_REQ_FORCE_FAILOVER_PROMOTE);
    } else {
        return (ctrl->request == SWITCH_REQ_RAFT_PROMOTE || ctrl->request == SWITCH_REQ_RAFT_PROMOTE_PENDING);
    }
#else
    return (ctrl->request == SWITCH_REQ_FAILOVER_PROMOTE || ctrl->request == SWITCH_REQ_FORCE_FAILOVER_PROMOTE);
#endif
}

bool32 knl_failover_triggered(knl_handle_t knl_handle)
{
    knl_instance_t *kernel = (knl_instance_t *)knl_handle;
    switch_ctrl_t *ctrl = &kernel->switch_ctrl;
#ifdef _REPLICATION
    if (!DB_IS_RAFT_ENABLED(kernel)) {
        return (ctrl->request == SWITCH_REQ_FAILOVER_PROMOTE ||
            (ctrl->request == SWITCH_REQ_FORCE_FAILOVER_PROMOTE && kernel->lrcv_ctx.session == NULL));
    } else {
        return (ctrl->request == SWITCH_REQ_RAFT_PROMOTE);
    }
#else
    return (ctrl->request == SWITCH_REQ_RAFT_PROMOTE);
#endif
}

bool32 knl_switchover_triggered(knl_handle_t knl_handle)
{
    knl_instance_t *kernel = (knl_instance_t *)knl_handle;
    switch_ctrl_t *ctrl = &kernel->switch_ctrl;
#ifdef _REPLICATION
    if (!DB_IS_RAFT_ENABLED(kernel)) {
        return (ctrl->request == SWITCH_REQ_DEMOTE || ctrl->request == SWITCH_REQ_PROMOTE);
    } else {
        return GS_FALSE;
    }
#else
    return (ctrl->request == SWITCH_REQ_DEMOTE || ctrl->request == SWITCH_REQ_PROMOTE);
#endif
}

bool32 knl_open_mode_triggered(knl_handle_t knl_handle)
{
    knl_instance_t *kernel = (knl_instance_t *)knl_handle;
    switch_ctrl_t *ctrl = &kernel->switch_ctrl;
#ifdef _REPLICATION
    if (!DB_IS_RAFT_ENABLED(kernel)) {
        return (ctrl->request == SWITCH_REQ_READONLY || ctrl->request == SWITCH_REQ_CANCEL_UPGRADE);
    } else {
        return GS_FALSE;
    }
#else
    return (ctrl->request == SWITCH_REQ_READONLY || ctrl->request == SWITCH_REQ_CANCEL_UPGRADE);
#endif
}

status_t knl_tx_enabled(knl_handle_t session)
{
    knl_session_t *se = (knl_session_t *)session;

    if (DB_NOT_READY(se)) {
        GS_THROW_ERROR(ERR_NO_DB_ACTIVE);
        return GS_ERROR;
    }

    if (DB_IS_READONLY(se)) {
        GS_THROW_ERROR(ERR_CAPABILITY_NOT_SUPPORT, "operation on read only mode");
        return GS_ERROR;
    }
    return GS_SUCCESS;
}

status_t knl_ddl_enabled(knl_handle_t session, bool32 forbid_in_rollback)
{
    knl_session_t *se = (knl_session_t *)session;

    if (DB_IS_READONLY(se)) {
        GS_THROW_ERROR(ERR_CAPABILITY_NOT_SUPPORT, "operation on read only mode");
        return GS_ERROR;
    }

    if (!DB_IS_PRIMARY(&se->kernel->db)) {
        GS_THROW_ERROR(ERR_DATABASE_ROLE, "operation", "not in primary mode");
        return GS_ERROR;
    }

    if (DB_IS_UPGRADE(se)) {
        return GS_SUCCESS;
    }

    if (se->bootstrap) {
        return GS_SUCCESS;
    }

    if (DB_STATUS(se) != DB_STATUS_OPEN) {
        GS_THROW_ERROR(ERR_DATABASE_NOT_AVAILABLE);
        return GS_ERROR;
    }

    if (!forbid_in_rollback) {
        return GS_SUCCESS;
    }

    if (se->kernel->dc_ctx.completed) {
        return GS_SUCCESS;
    }

    if (DB_IN_BG_ROLLBACK(se)) {
        GS_THROW_ERROR(ERR_DATABASE_IS_ROLLING_BACK);
        return GS_ERROR;
    } else {
        GS_THROW_ERROR(ERR_DATABASE_NOT_AVAILABLE);
        return GS_ERROR;
    }
}

status_t knl_convert_path_format(text_t *src, char *dst, uint32 dst_size, const char *home)
{
    uint32 len;
    uint32 home_len = (uint32)strlen(home);
    bool32 in_home = GS_FALSE;
    errno_t ret;
    cm_trim_text(src);

    if (CM_TEXT_FIRST(src) == '?') {
        CM_REMOVE_FIRST(src);
        len = home_len + src->len;
        in_home = GS_TRUE;
    } else {
        len = src->len;
    }

    if (len > GS_MAX_FILE_NAME_LEN) {
        GS_THROW_ERROR(ERR_NAME_TOO_LONG, "datafile or logfile", len, GS_MAX_FILE_NAME_LEN);
        return GS_ERROR;
    }

    if (in_home) {
        ret = memcpy_s(dst, dst_size, home, home_len);
        knl_securec_check(ret);
        if (src->len > 0) {
            ret = memcpy_s(dst + home_len, dst_size - home_len, src->str, src->len);
            knl_securec_check(ret);
        }
    } else {
        if (src->len > 0) {
            ret = memcpy_s(dst, dst_size, src->str, src->len);
            knl_securec_check(ret);
        }
    }

    dst[len] = '\0';
    return GS_SUCCESS;
}

status_t knl_get_convert_params(const char *item_name, char *value, file_convert_t *file_convert, const char *home)
{
    text_t text;
    text_t left;
    text_t right;
    uint32 i;
    char comma = ',';

    if (strlen(value) == 0) {
        file_convert->is_convert = GS_FALSE;
        return GS_SUCCESS;
    }

    file_convert->is_convert = GS_TRUE;
    cm_str2text(value, &text);

    /* two max_file_convert_num, one is for primary, one is for standby
     * other number like 2 or 1 is for calculate odd-even
     * The primary path of the mapping relationship is odd,
     * and the standby path of the mapping relationship is even
     */
    for (i = 0; i < GS_MAX_FILE_CONVERT_NUM * 2; i++) {
        cm_split_text(&text, comma, '\0', &left, &right);
        if (i % 2 == 0) {
            if (CM_TEXT_FIRST(&left) == '?') {
                GS_LOG_RUN_ERR("? can only be used for the local path, not for the peer path in %s", item_name);
                GS_THROW_ERROR(ERR_INVALID_PARAMETER, item_name);
                return GS_ERROR;
            }
            if (left.len > GS_MAX_FILE_NAME_LEN) {
                GS_THROW_ERROR(ERR_NAME_TOO_LONG, "datafile or logfile", left.len, GS_MAX_FILE_NAME_LEN);
                return GS_ERROR;
            }
            (void)cm_text2str(&left, file_convert->convert_list[i / 2].primry_path, GS_FILE_NAME_BUFFER_SIZE);
        } else {
            if (knl_convert_path_format(&left, file_convert->convert_list[i / 2].standby_path, GS_FILE_NAME_BUFFER_SIZE,
                home) != GS_SUCCESS) {
                return GS_ERROR;
            }
        }
        text = right;
        if (text.len == 0) {
            if (i % 2 == 1) {
                file_convert->count = (i + 1) / 2;
                return GS_SUCCESS;
            } else {
                GS_THROW_ERROR(ERR_INVALID_PARAMETER, item_name);
                return GS_ERROR;
            }
        }
    }
    GS_THROW_ERROR(ERR_TOO_MANY_OBJECTS, GS_MAX_FILE_CONVERT_NUM, "path number in %s", item_name);
    return GS_ERROR;
}

status_t knl_create_interval_part(knl_handle_t session, knl_dictionary_t *dc, uint32 part_no, part_key_t *part_key)
{
    knl_session_t *se = (knl_session_t *)session;
    dc_entity_t *dc_entity = DC_ENTITY(dc);
    table_t *table = DC_TABLE(dc);

    // check whether dc is corrupted or not, if corrupted, could not create interval partition
    if (dc_entity->corrupted) {
        GS_THROW_ERROR(ERR_DC_CORRUPTED);
        GS_LOG_RUN_ERR("dc for table %s is corrupted ", table->desc.name);
        return GS_ERROR;
    }

    if (part_no == GS_INVALID_ID32) {
        GS_THROW_ERROR(ERR_INVALID_PART_KEY, "inserted partition key does not map to any partition");
        return GS_ERROR;
    }

    /* check physical part is created or not */
    if (is_interval_part_created(se, dc, part_no)) {
        return GS_SUCCESS;
    }

    if (db_create_interval_part(se, dc, part_no, part_key) != GS_SUCCESS) {
        return GS_ERROR;
    }

    return GS_SUCCESS;
}

/*
 * kernel get parallel schedule interface
 * We divide the heap segment during the given worker number(which maybe adjust inner).
 * @note in this interface, we hold no table lock
 * @param kernel session, dictionary, partition no, worker number, parallel range(output)
 */
status_t knl_get_paral_schedule(knl_handle_t handle, knl_dictionary_t *dc, knl_part_locate_t part_loc, uint32 workers,
    knl_paral_range_t *range)
{
    knl_session_t *session = (knl_session_t *)handle;
    dc_entity_t *entity = DC_ENTITY(dc);
    table_t *table = NULL;
    table_part_t *table_part = NULL;
    heap_t *heap = NULL;
    knl_scn_t org_scn;

    if (knl_check_dc(handle, dc) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (dc->type != DICT_TYPE_TABLE && dc->type != DICT_TYPE_TABLE_NOLOGGING) {
        range->workers = 0;
        return GS_SUCCESS;
    }

    table = &entity->table;

    if (IS_PART_TABLE(table)) {
        knl_panic_log(part_loc.part_no < table->part_table->desc.partcnt,
            "the part_no is not smaller than part count, panic info: part_no %u part count %u table %s",
            part_loc.part_no, table->part_table->desc.partcnt, table->desc.name);
        table_part = TABLE_GET_PART(table, part_loc.part_no);
        if (!IS_READY_PART(table_part)) {
            range->workers = 0;
            return GS_SUCCESS;
        }

        if (knl_is_parent_part((knl_handle_t)entity, part_loc.part_no)) {
            knl_panic_log(part_loc.subpart_no != GS_INVALID_ID32, "the subpart_no is invalid, panic info: table %s",
                table->desc.name);
            table_part_t *subpart = PART_GET_SUBENTITY(table->part_table, table_part->subparts[part_loc.subpart_no]);
            if (subpart == NULL) {
                range->workers = 0;
                return GS_SUCCESS;
            }

            heap = &subpart->heap;
            org_scn = subpart->desc.org_scn;
        } else {
            heap = &table_part->heap;
            org_scn = table_part->desc.org_scn;
        }
    } else {
        heap = &table->heap;
        org_scn = table->desc.org_scn;
    }

    heap_get_paral_schedule(session, heap, org_scn, workers, range);

    return GS_SUCCESS;
}

status_t knl_check_sessions_per_user(knl_handle_t session, text_t *username, uint32 count)
{
    knl_session_t *sess = session;
    dc_user_t *user = NULL;
    uint64 limit;

    if (!sess->kernel->attr.enable_resource_limit) {
        return GS_SUCCESS;
    }

    if (dc_open_user_direct(sess, username, &user) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (GS_SUCCESS != profile_get_param_limit(sess, user->desc.profile_id, SESSIONS_PER_USER, &limit)) {
        return GS_ERROR;
    }

    if (PARAM_UNLIMITED == limit) {
        return GS_SUCCESS;
    }

    if (count >= limit) {
        GS_THROW_ERROR(ERR_EXCEED_SESSIONS_PER_USER, limit);
        return GS_ERROR;
    }

    return GS_SUCCESS;
}

/*
 * knl_insert_dependency_list
 * This function is used to insert dependency list to dependency$.
 */
status_t knl_insert_dependency_list(knl_handle_t session, object_address_t *depender, galist_t *referenced_list)
{
    knl_session_t *knl_session = (knl_session_t *)session;

    return db_write_sysdep_list(knl_session, depender, referenced_list);
}

#ifdef _STATISTICS
/*
 * knl_purge_stats
 * This function is used to purge the stats before given time.
 */
status_t knl_purge_stats(knl_handle_t session, int64 max_analyze_time)
{
    knl_session_t *knl_session = (knl_session_t *)session;

    if (knl_ddl_enabled(session, GS_FALSE) != GS_SUCCESS) {
        return GS_ERROR;
    }

    return stats_purge_stats_by_time(knl_session, max_analyze_time);
}
#endif

bool32 knl_is_lob_table(knl_dictionary_t *dc)
{
    return ((dc_entity_t *)dc->handle)->contain_lob;
}

status_t knl_reconstruct_lob_row(knl_handle_t session, knl_handle_t entity, knl_cursor_t *cursor, uint32 *scan_id,
    uint32 col_id)
{
    knl_column_t *column = NULL;
    errno_t ret;
    char *copy_row_start = NULL;
    char *copy_row_dest = NULL;
    char *copy_row_src = NULL;
    uint32 len;
    lob_locator_t *locator = NULL;
    text_t lob;
    uint32 id = *scan_id;
    bool32 is_csf = cursor->row->is_csf;

    CM_SAVE_STACK(((knl_session_t *)session)->stack);
    lob.str = cm_push(((knl_session_t *)session)->stack, GS_LOB_LOCATOR_BUF_SIZE);
    ret = memset_sp(lob.str, GS_LOB_LOCATOR_BUF_SIZE, 0, GS_LOB_LOCATOR_BUF_SIZE);
    knl_securec_check(ret);

    while (id < col_id) {
        column = knl_get_column(entity, id);

        if (!COLUMN_IS_LOB(column)) {
            id++;
            continue;
        }

        if (CURSOR_COLUMN_SIZE(cursor, id) == GS_NULL_VALUE_LEN) {
            id++;
            continue;
        }

        copy_row_start = heap_get_col_start(cursor->row, cursor->offsets, cursor->lens, id);
        locator = (lob_locator_t *)CURSOR_COLUMN_DATA(cursor, id);

        if (!locator->head.is_outline) {
            if (CURSOR_COLUMN_SIZE(cursor, id) <= KNL_LOB_LOCATOR_SIZE) {
                cursor->lob_inline_num--;
                id++;
                continue;
            }

            ret = memcpy_sp(lob.str, locator->head.size, (char *)locator->data, locator->head.size);
            knl_securec_check(ret);

            lob.len = locator->head.size;
            locator = knl_lob_col_new_start(is_csf, locator, lob.len);
            locator->head.size = 0;
            locator->first = INVALID_PAGID;
            locator->last = INVALID_PAGID;

            if (knl_write_lob(session, cursor, (char *)locator, column, GS_TRUE, &lob) != GS_SUCCESS) {
                CM_RESTORE_STACK(((knl_session_t *)session)->stack);
                return GS_ERROR;
            }

            copy_row_dest = copy_row_start + knl_lob_outline_size(is_csf);
            copy_row_src = copy_row_start + knl_lob_inline_size(is_csf, lob.len, GS_TRUE);
            len = cursor->row->size - cursor->offsets[id] - knl_lob_inline_size(is_csf, lob.len, GS_FALSE);

            if (len > 0) {
                ret = memmove_s(copy_row_dest, len, copy_row_src, len);
                knl_securec_check(ret);
            }

            cursor->row->size -= (uint16)(knl_lob_inline_size(is_csf, lob.len, GS_TRUE) - knl_lob_outline_size(is_csf));
            heap_write_col_size(is_csf, copy_row_start, KNL_LOB_LOCATOR_SIZE);
            cursor->lob_inline_num--;
            id++;
            break;
        }

        id++;
    }

    *scan_id = id;
    CM_RESTORE_STACK(((knl_session_t *)session)->stack);
    return GS_SUCCESS;
}

status_t knl_reconstruct_lob_update_info(knl_handle_t session, knl_dictionary_t *dc, knl_cursor_t *cursor,
    uint32 col_id)
{
    char *copy_row_start = NULL;
    char *copy_row_dest = NULL;
    char *copy_row_src = NULL;
    text_t lob;
    knl_update_info_t *ui = &cursor->update_info;
    bool32 is_csf = ((row_head_t *)ui->data)->is_csf;

    CM_SAVE_STACK(((knl_session_t *)session)->stack);
    lob.str = cm_push(((knl_session_t *)session)->stack, GS_LOB_LOCATOR_BUF_SIZE);
    errno_t ret = memset_sp(lob.str, GS_LOB_LOCATOR_BUF_SIZE, 0, GS_LOB_LOCATOR_BUF_SIZE);
    knl_securec_check(ret);

    for (uint32 i = 0; i < ui->count; i++) {
        if (i > col_id) {
            break;
        }

        uint32 col = ui->columns[i];
        knl_column_t *column = knl_get_column(dc->handle, col);
        if (!COLUMN_IS_LOB(column)) {
            continue;
        }

        if (CURSOR_UPDATE_COLUMN_SIZE(cursor, i) == GS_NULL_VALUE_LEN) {
            continue;
        }

        row_head_t *row = (row_head_t *)ui->data;
        copy_row_start = heap_get_col_start((row_head_t *)ui->data, ui->offsets, ui->lens, i);
        lob_locator_t *locator = (lob_locator_t *)((char *)ui->data + ui->offsets[i]);

        if (!locator->head.is_outline) {
            if (CURSOR_UPDATE_COLUMN_SIZE(cursor, i) <= KNL_LOB_LOCATOR_SIZE) {
                cursor->lob_inline_num--;
                continue;
            }

            ret = memcpy_sp(lob.str, locator->head.size, (char *)locator->data, locator->head.size);
            knl_securec_check(ret);
            lob.len = locator->head.size;
            locator = knl_lob_col_new_start(is_csf, locator, lob.len);
            locator->head.size = 0;
            locator->first = INVALID_PAGID;
            locator->last = INVALID_PAGID;

            if (knl_write_lob(session, cursor, (char *)locator, column, GS_TRUE, &lob) != GS_SUCCESS) {
                CM_RESTORE_STACK(((knl_session_t *)session)->stack);
                return GS_ERROR;
            }

            copy_row_dest = copy_row_start + knl_lob_outline_size(is_csf);
            copy_row_src = copy_row_start + knl_lob_inline_size(is_csf, lob.len, GS_TRUE);
            uint32 len = row->size - ui->offsets[i] - knl_lob_inline_size(is_csf, lob.len, GS_FALSE);

            if (len > 0) {
                ret = memmove_s(copy_row_dest, len, copy_row_src, len);
                knl_securec_check(ret);
            }

            row->size -= (uint16)(knl_lob_inline_size(is_csf, lob.len, GS_TRUE) - knl_lob_outline_size(is_csf));
            heap_write_col_size(is_csf, copy_row_start, KNL_LOB_LOCATOR_SIZE);
            cursor->lob_inline_num--;
            break;
        }
    }

    CM_RESTORE_STACK(((knl_session_t *)session)->stack);
    return GS_SUCCESS;
}

/*
 * knl_submit_job
 * This procedure submits a new job.
 */
status_t knl_submit_job(knl_handle_t session, knl_job_def_t *def)
{
    return db_write_sysjob((knl_session_t *)session, def);
}

/*
 * knl_update_job
 * This procedure update a job.
 */
status_t knl_update_job(knl_handle_t session, text_t *user, knl_job_node_t *job, bool32 should_exist)
{
    return db_update_sysjob((knl_session_t *)session, user, job, should_exist);
}

/*
 * knl_delete_job
 * This procedure delete a job.
 */
status_t knl_delete_job(knl_handle_t session, text_t *user, const int64 jobno, bool32 should_exist)
{
    return db_delete_sysjob((knl_session_t *)session, user, jobno, should_exist);
}

/* implementation for resource manager */
status_t knl_create_control_group(knl_handle_t session, knl_rsrc_group_t *group)
{
    if (knl_ddl_enabled(session, GS_TRUE) != GS_SUCCESS) {
        return GS_ERROR;
    }
    return db_create_control_group((knl_session_t *)session, group);
}

status_t knl_delete_control_group(knl_handle_t session, text_t *group_name)
{
    if (knl_ddl_enabled(session, GS_TRUE) != GS_SUCCESS) {
        return GS_ERROR;
    }
    return db_delete_control_group((knl_session_t *)session, group_name);
}

status_t knl_update_control_group(knl_handle_t session, knl_rsrc_group_t *group)
{
    if (knl_ddl_enabled(session, GS_TRUE) != GS_SUCCESS) {
        return GS_ERROR;
    }
    return db_update_control_group((knl_session_t *)session, group);
}

status_t knl_create_rsrc_plan(knl_handle_t session, knl_rsrc_plan_t *plan)
{
    if (knl_ddl_enabled(session, GS_TRUE) != GS_SUCCESS) {
        return GS_ERROR;
    }
    return db_create_rsrc_plan((knl_session_t *)session, plan);
}

status_t knl_delete_rsrc_plan(knl_handle_t session, text_t *plan_name)
{
    if (knl_ddl_enabled(session, GS_TRUE) != GS_SUCCESS) {
        return GS_ERROR;
    }
    return db_delete_rsrc_plan((knl_session_t *)session, plan_name);
}

status_t knl_update_rsrc_plan(knl_handle_t session, knl_rsrc_plan_t *plan)
{
    if (knl_ddl_enabled(session, GS_TRUE) != GS_SUCCESS) {
        return GS_ERROR;
    }
    return db_update_rsrc_plan((knl_session_t *)session, plan);
}

status_t knl_create_rsrc_plan_rule(knl_handle_t session, knl_rsrc_plan_rule_def_t *def)
{
    if (knl_ddl_enabled(session, GS_TRUE) != GS_SUCCESS) {
        return GS_ERROR;
    }
    return db_create_rsrc_plan_rule((knl_session_t *)session, def);
}

status_t knl_delete_rsrc_plan_rule(knl_handle_t session, text_t *plan_name, text_t *group_name)
{
    if (knl_ddl_enabled(session, GS_TRUE) != GS_SUCCESS) {
        return GS_ERROR;
    }
    return db_delete_rsrc_plan_rule((knl_session_t *)session, plan_name, group_name);
}

status_t knl_update_rsrc_plan_rule(knl_handle_t session, knl_rsrc_plan_rule_def_t *def)
{
    if (knl_ddl_enabled(session, GS_TRUE) != GS_SUCCESS) {
        return GS_ERROR;
    }
    return db_update_rsrc_plan_rule((knl_session_t *)session, def);
}

status_t knl_set_cgroup_mapping(knl_handle_t session, knl_rsrc_group_mapping_t *mapping)
{
    if (knl_ddl_enabled(session, GS_TRUE) != GS_SUCCESS) {
        return GS_ERROR;
    }
    return db_set_cgroup_mapping((knl_session_t *)session, mapping);
}

status_t knl_alter_sql_map(knl_handle_t session, knl_sql_map_t *sql_map)
{
    if (knl_ddl_enabled(session, GS_TRUE) != GS_SUCCESS) {
        return GS_ERROR;
    }
    return db_alter_sql_sysmap((knl_session_t *)session, sql_map);
}

status_t knl_drop_sql_map(knl_handle_t session, knl_sql_map_t *sql_map)
{
    bool8 is_exist = GS_FALSE;

    if (knl_ddl_enabled(session, GS_TRUE) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (db_delete_sql_sysmap((knl_session_t *)session, sql_map, &is_exist) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (!is_exist && (sql_map->options & DROP_IF_EXISTS) == 0) {
        GS_THROW_ERROR(ERR_SQL_MAP_NOT_EXIST);
        return GS_ERROR;
    }
    return GS_SUCCESS;
}

status_t knl_refresh_sql_map_hash(knl_handle_t session, knl_cursor_t *cursor, uint32 hash_value)
{
    uint32 old_hash_value = *(uint32 *)CURSOR_COLUMN_DATA(cursor, SYS_SQL_MAP_COL_SRC_HASHCODE);
    if (old_hash_value != hash_value) {
        if (db_update_sql_map_hash((knl_session_t *)session, cursor, hash_value) != GS_SUCCESS) {
            return GS_ERROR;
        }
    }
    return GS_SUCCESS;
}

static void estimate_segment_rows(uint32 *pages, uint32 *rows, knl_session_t *session, table_t *table,
    table_part_t *part)
{
    if (pages != NULL) {
        *pages = 0;
    }
    if (rows != NULL) {
        *rows = 0;
    }

    uint32 tmp_pages;
    uint32 pctfree;
    knl_scn_t org_scn;
    space_t *space = NULL;
    heap_t *heap = NULL;
    heap_segment_t *seg = NULL;
    page_head_t *head = NULL;

    if (part == NULL) {
        space = SPACE_GET(table->desc.space_id);
        pctfree = table->desc.pctfree;
        heap = &table->heap;
        org_scn = table->desc.org_scn;
    } else {
        space = SPACE_GET(part->desc.space_id);
        pctfree = part->desc.pctfree;
        heap = &part->heap;
        org_scn = part->desc.org_scn;
    }

    if (!SPACE_IS_ONLINE(space) || !space->ctrl->used) {
        return;
    }
    if (IS_INVALID_PAGID(heap->entry)) {
        return;
    }

    buf_enter_page(session, heap->entry, LATCH_MODE_S, ENTER_PAGE_RESIDENT);
    head = (page_head_t *)CURR_PAGE;
    seg = HEAP_SEG_HEAD;
    if (head->type != PAGE_TYPE_HEAP_HEAD || seg->org_scn != org_scn) {
        buf_leave_page(session, GS_FALSE);
        return;
    }

    if (IS_INVALID_PAGID(seg->data_last)) {
        tmp_pages = 0;
    } else {
        if (seg->extents.count == 1) {
            tmp_pages = seg->data_last.page - seg->data_first.page + 1;
        } else {
            tmp_pages = seg->extents.count * space->ctrl->extent_size;
            tmp_pages = tmp_pages - seg->map_count[0] - seg->map_count[1] - seg->map_count[2]; // map pages 0 - 2
            tmp_pages--;                                                                       // segment page
        }
    }
    buf_leave_page(session, GS_FALSE);

    if (pages != NULL) {
        *pages = tmp_pages;
    }

    if (rows != NULL) {
        // pctfree is a ratio num
        *rows = ((uint64)tmp_pages *
            (space->ctrl->block_size - sizeof(heap_page_t) - heap->cipher_reserve_size - PAGE_TAIL_SIZE) *
            (100 - pctfree)) /
            (table->desc.estimate_len * 100);
    }
}

static void estimate_all_subpart_rows(uint32 *pages, uint32 *rows, knl_handle_t sess, table_t *table,
    table_part_t *part)
{
    if (pages != NULL) {
        *pages = 0;
    }
    if (rows != NULL) {
        *rows = 0;
    }

    knl_session_t *session = (knl_session_t *)sess;
    table_part_t *subpart = NULL;
    for (uint32 i = 0; i < part->desc.subpart_cnt; i++) {
        subpart = PART_GET_SUBENTITY(table->part_table, part->subparts[i]);
        if (subpart == NULL) {
            continue;
        }
        uint32 tmp_pages, tmp_rows;
        estimate_segment_rows(&tmp_pages, &tmp_rows, session, table, (table_part_t *)subpart);
        if (pages != NULL) {
            *pages += tmp_pages;
        }
        if (rows != NULL) {
            *rows += tmp_rows;
        }
    }
}

static void estimate_all_part_rows(uint32 *pages, uint32 *rows, knl_handle_t sess, table_t *table)
{
    if (pages != NULL) {
        *pages = 0;
    }
    if (rows != NULL) {
        *rows = 0;
    }

    knl_session_t *session = (knl_session_t *)sess;
    for (uint32 i = 0; i < table->part_table->desc.partcnt; i++) {
        table_part_t *part = TABLE_GET_PART(table, i);
        if (!IS_READY_PART(part)) {
            continue;
        }
        uint32 tmp_pages, tmp_rows;
        if (IS_PARENT_TABPART(&part->desc)) {
            estimate_all_subpart_rows(&tmp_pages, &tmp_rows, sess, table, part);
        } else {
            estimate_segment_rows(&tmp_pages, &tmp_rows, session, table, part);
        }

        if (pages != NULL) {
            *pages += tmp_pages;
        }
        if (rows != NULL) {
            *rows += tmp_rows;
        }
    }
}

static void estimate_temp_table_rows(knl_session_t *session, table_t *table, uint32 *pages, uint32 *rows)
{
    space_t *space = SPACE_GET(table->desc.space_id);

    if (!SPACE_IS_ONLINE(space) || !space->ctrl->used) {
        return;
    }

    knl_temp_cache_t *temp_cache = knl_get_temp_cache(session, table->desc.uid, table->desc.id);

    if (temp_cache == NULL) {
        return;
    }

    mtrl_segment_t *segment = session->temp_mtrl->segments[temp_cache->table_segid];
    if (segment->vm_list.count == 0) {
        return;
    }

    uint64 total_size = TEMP_ESTIMATE_TOTAL_ROW_SIZE(segment, table);
    if (rows != NULL) {
        *rows = (uint32)(total_size * TEMP_ESTIMATE_ROW_SIZE_RATIO / table->desc.estimate_len);
    }
    if (pages != NULL) {
        *pages = segment->vm_list.count;
    }
}

void knl_estimate_table_rows(uint32 *pages, uint32 *rows, knl_handle_t sess, knl_handle_t entity, uint32 part_no)
{
    table_t *table = &((dc_entity_t *)entity)->table;
    table_part_t *part = NULL;

    if (knl_get_db_status(sess) != DB_STATUS_OPEN) {
        if (pages != NULL) {
            *pages = 0;
        }
        if (rows != NULL) {
            *rows = 0;
        }
        return;
    }

    if (IS_PART_TABLE(table)) {
        if (part_no == GS_INVALID_ID32) {
            estimate_all_part_rows(pages, rows, sess, table);
            return;
        }
        part = TABLE_GET_PART(table, part_no);

        if (IS_READY_PART(part) && IS_PARENT_TABPART(&part->desc)) {
            estimate_all_subpart_rows(pages, rows, sess, table, part);
            return;
        }
    }

    if (TABLE_IS_TEMP(table->desc.type)) {
        estimate_temp_table_rows((knl_session_t *)sess, table, pages, rows);
    } else {
        estimate_segment_rows(pages, rows, (knl_session_t *)sess, table, part);
    }
}

void knl_estimate_subtable_rows(uint32 *pages, uint32 *rows, knl_handle_t sess, knl_handle_t entity, uint32 part_no,
    uint32 subpart_no)
{
    table_t *table = &((dc_entity_t *)entity)->table;
    table_part_t *part = NULL;

    if (knl_get_db_status(sess) != DB_STATUS_OPEN) {
        if (pages != NULL) {
            *pages = 0;
        }
        if (rows != NULL) {
            *rows = 0;
        }
        return;
    }

    if (!IS_PART_TABLE(table)) {
        estimate_segment_rows(pages, rows, (knl_session_t *)sess, table, NULL);
        return;
    }

    if (part_no == GS_INVALID_ID32) {
        estimate_all_part_rows(pages, rows, sess, table);
        return;
    }

    part = TABLE_GET_PART(table, part_no);

    if (part == NULL || !IS_PARENT_TABPART(&part->desc)) {
        estimate_segment_rows(pages, rows, (knl_session_t *)sess, table, part);
        return;
    }

    if (subpart_no == GS_INVALID_ID32) {
        estimate_all_subpart_rows(pages, rows, sess, table, part);
        return;
    }

    table_part_t *subpart = PART_GET_SUBENTITY(table->part_table, part->subparts[subpart_no]);
    if (subpart == NULL) {
        estimate_all_subpart_rows(pages, rows, sess, table, part);
        return;
    }

    estimate_segment_rows(pages, rows, (knl_session_t *)sess, table, (table_part_t *)subpart);
    return;
}

void knl_inc_dc_ver(knl_handle_t kernel)
{
    ((knl_instance_t *)kernel)->dc_ctx.version++;
}

/**
 * recycle lob pages for sql engine
 * @note getting locator from cursor->row
 * @param kernel session, kernel cursor
 */
status_t knl_recycle_lob_insert_pages(knl_handle_t session, knl_cursor_t *cursor)
{
    knl_session_t *se = (knl_session_t *)session;
    dc_entity_t *entity = (dc_entity_t *)cursor->dc_entity;
    knl_column_t *column = NULL;
    lob_locator_t *locator = NULL;

    for (uint32 i = 0; i < entity->column_count; i++) {
        column = dc_get_column(entity, i);

        if (KNL_COLUMN_IS_DELETED(column)) {
            continue;
        }

        if (!COLUMN_IS_LOB(column)) {
            continue;
        }

        if (CURSOR_COLUMN_SIZE(cursor, i) == GS_NULL_VALUE_LEN) {
            continue;
        }

        locator = (lob_locator_t *)CURSOR_COLUMN_DATA(cursor, i);

        if (!locator->head.is_outline) {
            continue;
        }

        if (lob_recycle_pages(se, cursor, (lob_t *)column->lob, locator) != GS_SUCCESS) {
            return GS_ERROR;
        }
    }

    return GS_SUCCESS;
}

/**
 * recycle lob pages for sql engine
 * @note getting locator from  cursor->update_info
 * @param kernel session, kernel cursor
 */
status_t knl_recycle_lob_update_pages(knl_handle_t session, knl_cursor_t *cursor)
{
    knl_session_t *se = (knl_session_t *)session;
    dc_entity_t *entity = (dc_entity_t *)cursor->dc_entity;
    knl_column_t *column = NULL;
    lob_locator_t *locator = NULL;
    knl_update_info_t *ui = &cursor->update_info;
    uint16 col_id;

    for (uint32 i = 0; i < ui->count; i++) {
        col_id = cursor->update_info.columns[i];
        column = dc_get_column(entity, col_id);

        if (KNL_COLUMN_IS_DELETED(column)) {
            continue;
        }

        if (!COLUMN_IS_LOB(column)) {
            continue;
        }

        if (ui->lens[i] == GS_NULL_VALUE_LEN) {
            continue;
        }

        locator = (lob_locator_t *)((char *)ui->data + ui->offsets[i]);

        if (!locator->head.is_outline) {
            continue;
        }

        if (lob_recycle_pages(se, cursor, (lob_t *)column->lob, locator) != GS_SUCCESS) {
            return GS_ERROR;
        }
    }

    return GS_SUCCESS;
}

status_t knl_recycle_lob_column_pages(knl_handle_t session, knl_cursor_t *cursor, knl_column_t *column, char *lob)
{
    knl_session_t *se = (knl_session_t *)session;
    lob_locator_t *locator = (lob_locator_t *)lob;
    if (KNL_COLUMN_IS_DELETED(column) || !locator->head.is_outline) {
        return GS_SUCCESS;
    }
    return lob_recycle_pages(se, cursor, (lob_t *)column->lob, (lob_locator_t *)locator);
}

status_t knl_delete_syssyn_by_name(knl_handle_t knl_session, uint32 uid, const char *syn_name)
{
    knl_session_t *session = (knl_session_t *)knl_session;
    knl_cursor_t *cursor = NULL;
    dc_user_t *user = NULL;

    if (dc_open_user_by_id(session, uid, &user) != GS_SUCCESS) {
        return GS_ERROR;
    }

    CM_SAVE_STACK(session->stack);

    cursor = knl_push_cursor(session);
    knl_open_sys_cursor(session, cursor, CURSOR_ACTION_DELETE, SYS_SYN_ID, IX_SYS_SYNONYM001_ID);
    knl_init_index_scan(cursor, GS_TRUE);
    knl_set_scan_key(INDEX_DESC(cursor->index), &cursor->scan_range.l_key, GS_TYPE_INTEGER, (void *)&uid,
        sizeof(uint32), IX_COL_SYS_SYNONYM001_USER);
    knl_set_scan_key(INDEX_DESC(cursor->index), &cursor->scan_range.l_key, GS_TYPE_STRING, (void *)syn_name,
        (uint16)strlen(syn_name), IX_COL_SYS_SYNONYM001_SYNONYM_NAME);

    if (GS_SUCCESS != knl_fetch(session, cursor)) {
        CM_RESTORE_STACK(session->stack);
        return GS_ERROR;
    }

    if (cursor->eof) {
        CM_RESTORE_STACK(session->stack);
        GS_THROW_ERROR(ERR_SYNONYM_NOT_EXIST, user->desc.name, syn_name);
        return GS_ERROR;
    }

    if (GS_SUCCESS != knl_internal_delete(session, cursor)) {
        CM_RESTORE_STACK(session->stack);
        return GS_ERROR;
    }

    CM_RESTORE_STACK(session->stack);

    return GS_SUCCESS;
}

status_t knl_check_and_load_synonym(knl_handle_t knl_session, text_t *user, text_t *name, knl_synonym_t *result,
    bool32 *exists)
{
    knl_session_t *session = (knl_session_t *)knl_session;
    uint32 uid, syn_uid;
    text_t syn_name, table_owner, table_name;
    *exists = GS_FALSE;

    if (knl_ddl_enabled(session, GS_FALSE) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (!knl_get_user_id(session, user, &uid)) {
        GS_THROW_ERROR(ERR_USER_NOT_EXIST, T2S(user));
        return GS_ERROR;
    }

    CM_SAVE_STACK(session->stack);

    knl_cursor_t *cursor = knl_push_cursor(session);

    knl_open_sys_cursor(session, cursor, CURSOR_ACTION_SELECT, SYS_SYN_ID, IX_SYS_SYNONYM001_ID);
    knl_init_index_scan(cursor, GS_TRUE);
    knl_set_scan_key(INDEX_DESC(cursor->index), &cursor->scan_range.l_key, GS_TYPE_INTEGER, (void *)&uid,
        sizeof(uint32), IX_COL_SYS_SYNONYM001_USER);
    knl_set_scan_key(INDEX_DESC(cursor->index), &cursor->scan_range.l_key, GS_TYPE_STRING, name->str, name->len,
        IX_COL_SYS_SYNONYM001_SYNONYM_NAME);

    for (;;) {
        if (knl_fetch(session, cursor) != GS_SUCCESS) {
            CM_RESTORE_STACK(session->stack);
            return GS_ERROR;
        }

        if (cursor->eof) {
            break;
        }
        // get synonym uid and name
        syn_uid = *(uint32 *)CURSOR_COLUMN_DATA(cursor, SYS_SYN_USER);
        syn_name.len = (uint32)CURSOR_COLUMN_SIZE(cursor, SYS_SYN_SYNONYM_NAME);
        syn_name.str = (char *)CURSOR_COLUMN_DATA(cursor, SYS_SYN_SYNONYM_NAME);

        if (uid == syn_uid && cm_text_equal(&syn_name, name)) {
            // get synonym info
            result->uid = *(uint32 *)CURSOR_COLUMN_DATA(cursor, SYS_SYN_USER);
            result->id = *(uint32 *)CURSOR_COLUMN_DATA(cursor, SYS_SYN_OBJID);
            result->chg_scn = *(int64 *)CURSOR_COLUMN_DATA(cursor, SYS_SYN_CHG_SCN);
            result->org_scn = *(int64 *)CURSOR_COLUMN_DATA(cursor, SYS_SYN_ORG_SCN);
            result->type = *(uint32 *)CURSOR_COLUMN_DATA(cursor, SYS_SYN_TYPE);

            table_owner.len = (uint32)CURSOR_COLUMN_SIZE(cursor, SYS_SYN_TABLE_OWNER);
            table_owner.str = (char *)CURSOR_COLUMN_DATA(cursor, SYS_SYN_TABLE_OWNER);
            table_name.len = (uint32)CURSOR_COLUMN_SIZE(cursor, SYS_SYN_TABLE_NAME);
            table_name.str = (char *)CURSOR_COLUMN_DATA(cursor, SYS_SYN_TABLE_NAME);

            cm_text2str(&syn_name, result->name, GS_NAME_BUFFER_SIZE);
            cm_text2str(&table_owner, result->table_owner, GS_NAME_BUFFER_SIZE);
            cm_text2str(&table_name, result->table_name, GS_NAME_BUFFER_SIZE);
            *exists = GS_TRUE;
            break;
        }
    }

    CM_RESTORE_STACK(session->stack);
    return GS_SUCCESS;
}

status_t knl_pl_create_synonym(knl_handle_t knl_session, knl_synonym_def_t *def, const int64 syn_id)
{
    knl_session_t *session = (knl_session_t *)knl_session;
    knl_synonym_t synonym;
    knl_cursor_t *cursor = NULL;
    object_address_t depender, referer;
    dc_user_t *user = NULL;
    errno_t err;

    if (knl_ddl_enabled(session, GS_FALSE) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (DB_NOT_READY(session)) {
        GS_THROW_ERROR(ERR_NO_DB_ACTIVE);
        return GS_ERROR;
    }

    if (db_init_synonmy_desc(session, &synonym, def) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (dc_open_user_by_id(session, synonym.uid, &user) != GS_SUCCESS) {
        return GS_ERROR;
    }

    // for creating table bug fix: cursor->row is null
    CM_SAVE_STACK(session->stack);

    cursor = knl_push_cursor(session);

    cursor->row = (row_head_t *)cursor->buf;

    // init the function synonym id as ref->oid
    synonym.id = (uint32)syn_id;

    if (db_write_syssyn(session, cursor, &synonym) != GS_SUCCESS) {
        CM_RESTORE_STACK(session->stack);
        return GS_ERROR;
    }

    // insert into sys.dependency$
    depender.uid = synonym.uid;
    depender.oid = synonym.id;
    depender.tid = OBJ_TYPE_PL_SYNONYM;
    depender.scn = synonym.chg_scn;
    err = strncpy_s(depender.name, GS_NAME_BUFFER_SIZE, def->name.str, def->name.len);
    knl_securec_check(err);
    referer.uid = def->ref_uid;
    referer.oid = def->ref_oid;
    referer.tid = knl_get_object_type(def->ref_dc_type);
    referer.scn = def->ref_chg_scn;
    err = strncpy_s(referer.name, GS_NAME_BUFFER_SIZE, def->table_name.str, def->table_name.len);
    knl_securec_check(err);
    if (db_write_sysdep(session, cursor, &depender, &referer, 0) != GS_SUCCESS) {
        CM_RESTORE_STACK(session->stack);
        return GS_ERROR;
    }

    CM_RESTORE_STACK(session->stack);
    return GS_SUCCESS;
}

status_t knl_insert_ddl_loginfo(knl_handle_t knl_session, knl_dist_ddl_loginfo_t *info)
{
    row_assist_t ra;
    table_t *table = NULL;
    knl_cursor_t *cursor = NULL;
    knl_session_t *session = (knl_session_t *)knl_session;
    knl_column_t *lob_column = NULL;

    CM_SAVE_STACK(session->stack);

    if (sql_push_knl_cursor(session, &cursor) != GS_SUCCESS) {
        GS_THROW_ERROR(ERR_STACK_OVERFLOW);
        CM_RESTORE_STACK(session->stack);
        return GS_ERROR;
    }
    knl_open_sys_cursor(session, cursor, CURSOR_ACTION_INSERT, SYS_DIST_DDL_LOGINFO, GS_INVALID_ID32);
    table = (table_t *)cursor->table;
    lob_column = knl_get_column(cursor->dc_entity, DIST_DDL_LOGINFO_COL_DDL);
    row_init(&ra, (char *)cursor->row, HEAP_MAX_ROW_SIZE, table->desc.column_count);
    (void)row_put_text(&ra, &info->dist_ddl_id);
    (void)row_put_int32(&ra, (int32)info->rec.group_id);
    (void)row_put_int32(&ra, (int32)info->rec.datanode_id);
    if (knl_row_put_lob(session, cursor, lob_column, &info->rec.ddl_info, &ra) != GS_SUCCESS) {
        CM_RESTORE_STACK(session->stack);
        return GS_ERROR;
    }
    (void)row_put_timestamp(&ra, info->rec.create_time);
    (void)row_put_timestamp(&ra, info->rec.expired_time);
    (void)row_put_int32(&ra, (int32)info->rec.retry_times);
    (void)row_put_int32(&ra, (int32)info->rec.status);

    if (GS_SUCCESS != knl_internal_insert(session, cursor)) {
        CM_RESTORE_STACK(session->stack);
        return GS_ERROR;
    }

    CM_RESTORE_STACK(session->stack);
    return GS_SUCCESS;
}

bool32 knl_is_dist_ddl(knl_handle_t knl_session)
{
    return (((knl_session_t *)knl_session)->dist_ddl_id != NULL);
}

void knl_set_ddl_id(knl_handle_t knl_session, text_t *id)
{
    ((knl_session_t *)knl_session)->dist_ddl_id = id;
}

void knl_clean_before_commit(knl_handle_t knl_session)
{
    knl_session_t *session = (knl_session_t *)knl_session;
    if (session->dist_ddl_id != NULL) {
        (void)knl_delete_ddl_loginfo(knl_session, session->dist_ddl_id);
    }

    session->dist_ddl_id = NULL;
}

status_t knl_clean_ddl_loginfo(knl_handle_t knl_session, text_t *ddl_id, uint32 *rows)
{
    knl_cursor_t *cursor = NULL;
    knl_session_t *session = (knl_session_t *)knl_session;

    *rows = 0;
    CM_SAVE_STACK(session->stack);
    cursor = knl_push_cursor(session);

    knl_open_sys_cursor(session, cursor, CURSOR_ACTION_DELETE, SYS_DIST_DDL_LOGINFO, IX_DIST_DDL_LOGINFO_001_ID);
    knl_init_index_scan(cursor, GS_TRUE);
    knl_set_scan_key(INDEX_DESC(cursor->index), &cursor->scan_range.l_key, GS_TYPE_STRING, ddl_id->str, ddl_id->len, 0);

    if (knl_fetch(session, cursor) != GS_SUCCESS) {
        GS_LOG_DEBUG_ERR("delete ddl loginfo :%s fetch failed", T2S(ddl_id));
        CM_RESTORE_STACK(session->stack);
        return GS_ERROR;
    }

    if (!cursor->eof) {
        if (GS_SUCCESS != knl_internal_delete(session, cursor)) {
            CM_RESTORE_STACK(session->stack);
            return GS_ERROR;
        }
        *rows = 1;
    }

    CM_RESTORE_STACK(session->stack);

    return GS_SUCCESS;
}

status_t knl_delete_ddl_loginfo(knl_handle_t knl_session, text_t *ddl_id)
{
    uint32 rows = 0;

    return knl_clean_ddl_loginfo(knl_session, ddl_id, &rows);
}

status_t knl_query_ddl_loginfo(knl_handle_t knl_session, text_t *ddl_id, text_t *ddl_info, uint32 *used_encrypt)
{
    knl_cursor_t *cursor = NULL;
    knl_session_t *session = (knl_session_t *)knl_session;
    lob_locator_t *src_lob = NULL;

    cursor = knl_push_cursor(session);
    knl_open_sys_cursor(session, cursor, CURSOR_ACTION_SELECT, SYS_DIST_DDL_LOGINFO, IX_DIST_DDL_LOGINFO_001_ID);
    knl_init_index_scan(cursor, GS_TRUE);
    knl_set_scan_key(INDEX_DESC(cursor->index), &cursor->scan_range.l_key, GS_TYPE_STRING, ddl_id->str, ddl_id->len, 0);
    if (knl_fetch(session, cursor) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (!cursor->eof) {
        src_lob = (lob_locator_t *)CURSOR_COLUMN_DATA(cursor, DIST_DDL_LOGINFO_COL_DDL);
        ddl_info->len = knl_lob_size(src_lob);
        ddl_info->str = (char *)cm_push(session->stack, (ddl_info->len + 1));
        if (ddl_info->str == NULL) {
            GS_THROW_ERROR(ERR_STACK_OVERFLOW);
            return GS_ERROR;
        }

        *used_encrypt = *(uint32 *)CURSOR_COLUMN_DATA(cursor, DIST_DDL_LOGINFO_COL_RETRY_TIMES);
        if (knl_read_lob(session, src_lob, 0, ddl_info->str, ddl_info->len, NULL) != GS_SUCCESS) {
            return GS_ERROR;
        }

        ddl_info->str[ddl_info->len] = '\0';
        return GS_SUCCESS;
    }

    GS_THROW_ERROR(ERR_TF_QUERY_DDL_INFO_FAILED);
    return GS_ERROR;
}

status_t knl_convert_xa_xid(xa_xid_t *src, knl_xa_xid_t *dst)
{
    errno_t ret;

    if (src->gtrid_len == 0) {
        GS_THROW_ERROR_EX(ERR_XA_INVALID_XID, "gtrid len: 0");
        return GS_ERROR;
    }

    dst->gtrid_len = src->gtrid_len;
    ret = memcpy_sp(dst->gtrid, GS_MAX_XA_BASE16_GTRID_LEN, src->data, (uint32)src->gtrid_len);
    knl_securec_check(ret);

    dst->bqual_len = src->bqual_len;
    ret = memcpy_sp(dst->bqual, GS_MAX_XA_BASE16_BQUAL_LEN, src->data + src->gtrid_len, (uint32)src->bqual_len);
    knl_securec_check(ret);

    dst->fmt_id = src->fmt_id;
    return GS_SUCCESS;
}

bool32 knl_xa_xid_equal(knl_xa_xid_t *xid1, knl_xa_xid_t *xid2)
{
    text_t xid_text1;
    text_t xid_text2;

    if (xid1->fmt_id != xid2->fmt_id) {
        return GS_FALSE;
    }

    cm_str2text_safe(xid1->gtrid, xid1->gtrid_len, &xid_text1);
    cm_str2text_safe(xid2->gtrid, xid2->gtrid_len, &xid_text2);

    if (!cm_text_equal(&xid_text1, &xid_text2)) {
        return GS_FALSE;
    }

    cm_str2text_safe(xid1->bqual, xid1->bqual_len, &xid_text1);
    cm_str2text_safe(xid2->bqual, xid2->bqual_len, &xid_text2);

    if (!cm_text_equal(&xid_text1, &xid_text2)) {
        return GS_FALSE;
    }

    return GS_TRUE;
}

uint32 knl_get_bucket_by_variant(variant_t *data, uint32 part_cnt)
{
    if (data->type == GS_TYPE_NUMBER || data->type == GS_TYPE_DECIMAL) {
        dec4_t d4;
        (void)cm_dec_8_to_4(&d4, &data->v_dec);
        data->v_bin.bytes = (uint8 *)&d4;
        data->v_bin.size = cm_dec4_stor_sz(&d4);

        return part_get_bucket_by_variant(data, part_cnt);
    } else {
        return part_get_bucket_by_variant(data, part_cnt);
    }
}

status_t knl_open_external_cursor(knl_handle_t session, knl_cursor_t *cursor, knl_dictionary_t *dc)
{
    int32 ret;
    uint32 mode;
    bool32 is_found = GS_FALSE;
    uint32 uid = ((knl_session_t *)session)->uid;
    char dest_name[GS_FILE_NAME_BUFFER_SIZE] = { 0 };
    char path_name[GS_MAX_PATH_BUFFER_SIZE] = { 0 };
    table_t *table = (table_t *)cursor->table;
    knl_ext_desc_t *external_desc = table->desc.external_desc;

    cursor->fetch = TABLE_ACCESSOR(cursor)->do_fetch;
    if (db_fetch_directory_path(session, external_desc->directory, path_name, GS_MAX_PATH_BUFFER_SIZE, &is_found) !=
        GS_SUCCESS) {
        return GS_ERROR;
    }

    if (!is_found) {
        GS_THROW_ERROR(ERR_OBJECT_NOT_EXISTS, "directory", external_desc->directory);
        return GS_ERROR;
    }

    /* check if has read priv on the directory */
    if (!db_check_dirpriv_by_uid(session, external_desc->directory, uid, GS_PRIV_DIRE_READ)) {
        GS_THROW_ERROR(ERR_INSUFFICIENT_PRIV);
        return GS_ERROR;
    }

    ret = snprintf_s(dest_name, GS_FILE_NAME_BUFFER_SIZE, GS_FILE_NAME_BUFFER_SIZE - 1, "%s/%s", path_name,
        external_desc->location);
    knl_securec_check_ss(ret);

    if (!cm_file_exist(dest_name)) {
        GS_THROW_ERROR(ERR_FILE_NOT_EXIST, "external", external_desc->location);
        return GS_ERROR;
    }

    knl_panic_log(external_desc->external_type == LOADER,
        "external type is abnormal, panic info: page %u-%u type %u table %s", cursor->rowid.file, cursor->rowid.page,
        ((page_head_t *)cursor->page_buf)->type, table->desc.name);
    mode = O_BINARY | O_SYNC | O_RDONLY;
    /* file cursor->fd is closed in external_heap_fetch_by_page */
    if (cm_open_file(dest_name, mode, &cursor->fd) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (cm_seek_file(cursor->fd, 0, SEEK_SET) != 0) {
        cm_close_file(cursor->fd);
        cursor->fd = -1;
        return GS_ERROR;
    }

    cursor->text.len = 0;
    MAXIMIZE_ROWID(cursor->rowid);

    return GS_SUCCESS;
}

void knl_destroy_se_alcks(knl_handle_t session)
{
    lock_destroy_se_alcks((knl_session_t *)session);
}

static status_t knl_prepare_check_dc(knl_session_t *session, knl_dictionary_t *dc)
{
    dc_entity_t *entity = DC_ENTITY(dc);

    if (entity == NULL) {
        GS_THROW_ERROR(ERR_DC_INVALIDATED);
        return GS_ERROR;
    }
    dc_entry_t *entry = entity->entry;

    if (entity->corrupted) {
        GS_THROW_ERROR(ERR_DC_CORRUPTED);
        return GS_ERROR;
    }

    if (!IS_LOGGING_TABLE_BY_TYPE(dc->type)) {
        if (entry && entry->need_empty_entry && KNL_IS_DATABASE_OPEN(session)) {
            GS_THROW_ERROR(ERR_DC_INVALIDATED);
            return GS_ERROR;
        }
    }

    return GS_SUCCESS;
}

static status_t knl_check_dc_sync(knl_session_t *session, knl_dictionary_t *dc)
{
    text_t orig_user;
    text_t name;
    knl_dictionary_t new_dc;
    bool32 is_found = GS_FALSE;
    dc_entity_t *entity = DC_ENTITY(dc);
    dc_entry_t *sync_entry = (dc_entry_t *)dc->syn_handle;

    if (dc->syn_orig_uid != sync_entry->uid) {
        if (knl_get_user_name(session, dc->syn_orig_uid, &orig_user) != GS_SUCCESS) {
            cm_reset_error();
            GS_THROW_ERROR(ERR_INVALID_OPERATION, ", please check user or schema");
            return GS_ERROR;
        }

        cm_str2text(sync_entry->name, &name);
        if (knl_open_dc_if_exists(session, &orig_user, &name, &new_dc, &is_found) != GS_SUCCESS) {
            return GS_ERROR;
        }

        if (is_found) {
            knl_close_dc(&new_dc);
            GS_THROW_ERROR(ERR_DC_INVALIDATED);
            return GS_ERROR;
        }
    }

    if ((dc->syn_chg_scn == sync_entry->chg_scn) && (dc->chg_scn == entity->entry->chg_scn) && entity->valid &&
        !entity->entry->recycled) {
        return dc_check_stats_version(dc, entity);
    }

    GS_THROW_ERROR(ERR_DC_INVALIDATED);
    return GS_ERROR;
}

status_t knl_check_dc(knl_handle_t handle, knl_dictionary_t *dc)
{
    knl_session_t *session = (knl_session_t *)handle;
    dc_entity_t *entity = DC_ENTITY(dc);

    if (knl_prepare_check_dc(session, dc) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (SYNONYM_EXIST(dc)) {
        return knl_check_dc_sync(session, dc);
    } else {
        if (dc->type == DICT_TYPE_TEMP_TABLE_SESSION && IS_LTT_BY_NAME(entity->table.desc.name)) {
            knl_session_t *curr = (knl_session_t *)knl_get_curr_sess();
            uint32 tab_id = entity->table.desc.id;

            dc_entry_t *entry = entity->entry;
            if (entry == NULL || tab_id < GS_LTT_ID_OFFSET ||
                tab_id >= (GS_LTT_ID_OFFSET + curr->temp_table_capacity)) {
                GS_THROW_ERROR(ERR_DC_INVALIDATED);
                return GS_ERROR;
            }

            dc_entry_t *sess_entry = (dc_entry_t *)curr->temp_dc->entries[tab_id - GS_LTT_ID_OFFSET];
            if (entry == sess_entry && dc->org_scn == sess_entry->org_scn) {
                return GS_SUCCESS;
            }
        } else if ((dc->chg_scn == entity->entry->chg_scn) && entity->valid && !entity->entry->recycled) {
            return dc_check_stats_version(dc, entity);
        }
    }

    GS_THROW_ERROR(ERR_DC_INVALIDATED);
    return GS_ERROR;
}

status_t knl_set_table_stats(knl_handle_t session, knl_table_set_stats_t *tab_stats)
{
    knl_session_t *se = (knl_session_t *)session;
    knl_dictionary_t dc;
    part_table_t *part_table = NULL;
    table_t *table = NULL;
    table_part_t *table_part = NULL;
    status_t status;

    if (knl_open_dc(session, &tab_stats->owner, &tab_stats->name, &dc) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (IS_TEMP_TABLE_BY_DC(&dc)) {
        dc_close(&dc);
        GS_THROW_ERROR(ERR_OPERATIONS_NOT_SUPPORT, "set statistics", "temp table");
        return GS_ERROR;
    }

    if (lock_table_shared_directly(se, &dc) != GS_SUCCESS) {
        dc_close(&dc);
        return GS_ERROR;
    }

    table = DC_TABLE(&dc);

    if (tab_stats->is_single_part) {
        if (!IS_PART_TABLE(table)) {
            unlock_tables_directly(se);
            dc_close(&dc);
            GS_THROW_ERROR(ERR_OPERATIONS_NOT_SUPPORT, "set partition statistics", table->desc.name);
            return GS_ERROR;
        }

        part_table = table->part_table;

        if (!part_table_find_by_name(part_table, &tab_stats->part_name, &table_part)) {
            unlock_tables_directly(se);
            dc_close(&dc);
            GS_THROW_ERROR(ERR_PARTITION_NOT_EXIST, "table", T2S(&tab_stats->part_name));
            return GS_ERROR;
        }
    }

#ifdef _STATISTICS
    status = stats_set_tables(se, &dc, tab_stats, table_part);
    if (status == GS_SUCCESS) {
        knl_commit(session);
    } else {
        knl_rollback(session, NULL);
    }
#endif

    unlock_tables_directly(se);
    dc_close(&dc);
    return status;
}

status_t knl_set_columns_stats(knl_handle_t session, knl_column_set_stats_t *col_stats)
{
    knl_session_t *se = (knl_session_t *)session;
    knl_dictionary_t dc;
    part_table_t *part_table = NULL;
    table_t *table = NULL;
    table_part_t *table_part = NULL;
    knl_column_t *column = NULL;
    status_t status;

    if (knl_open_dc(session, &col_stats->owner, &col_stats->tabname, &dc) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (IS_TEMP_TABLE_BY_DC(&dc)) {
        dc_close(&dc);
        GS_THROW_ERROR(ERR_OPERATIONS_NOT_SUPPORT, "set statistics", "temp table");
        return GS_ERROR;
    }

    if (lock_table_shared_directly(se, &dc) != GS_SUCCESS) {
        dc_close(&dc);
        return GS_ERROR;
    }

    table = DC_TABLE(&dc);
    column = knl_find_column(&col_stats->colname, &dc);

    if (column == NULL) {
        unlock_tables_directly(se);
        dc_close(&dc);
        GS_THROW_ERROR(ERR_COLUMN_NOT_EXIST, T2S(&col_stats->tabname), T2S_EX(&col_stats->colname));
        return GS_ERROR;
    }

    if (col_stats->is_single_part) {
        if (!IS_PART_TABLE(table)) {
            unlock_tables_directly(se);
            dc_close(&dc);
            GS_THROW_ERROR(ERR_OPERATIONS_NOT_SUPPORT, "set partition statistics", table->desc.name);
            return GS_ERROR;
        }

        part_table = table->part_table;

        if (!part_table_find_by_name(part_table, &col_stats->part_name, &table_part)) {
            unlock_tables_directly(se);
            dc_close(&dc);
            GS_THROW_ERROR(ERR_PARTITION_NOT_EXIST, "table", T2S(&col_stats->part_name));
            return GS_ERROR;
        }
    }

#ifdef _STATISTICS
    status = stats_set_column(se, &dc, col_stats, table_part, column);
    if (status == GS_SUCCESS) {
        knl_commit(session);
    } else {
        knl_rollback(session, NULL);
    }
#endif

    unlock_tables_directly(se);
    dc_close(&dc);
    return status;
}

static status_t knl_ckeck_index_status(knl_dictionary_t *dc, knl_index_set_stats_t *ind_stats, index_t *index,
    index_part_t **idx_part)
{
    if (index == NULL) {
        GS_THROW_ERROR(ERR_INDEX_NOT_EXIST, T2S(&ind_stats->owner), T2S_EX(&ind_stats->name));
        return GS_ERROR;
    }

    if (index->desc.is_invalid) {
        GS_THROW_ERROR(ERR_INDEX_NOT_STABLE, T2S_EX(&ind_stats->name));
        return GS_ERROR;
    }

    if (ind_stats->is_single_part) {
        table_t *table = DC_TABLE(dc);
        if (!IS_PART_INDEX(index)) {
            GS_THROW_ERROR(ERR_OPERATIONS_NOT_SUPPORT, "set partition statistics", table->desc.name);
            return GS_ERROR;
        }

        part_index_t *part_idx = index->part_index;
        if (!part_index_find_by_name(part_idx, &ind_stats->part_name, idx_part)) {
            GS_THROW_ERROR(ERR_PARTITION_NOT_EXIST, "table", T2S(&ind_stats->part_name));
            return GS_ERROR;
        }

        if ((*idx_part)->desc.is_invalid) {
            GS_THROW_ERROR(ERR_INDEX_PART_UNUSABLE, T2S(&ind_stats->part_name), T2S_EX(&ind_stats->name));
            return GS_ERROR;
        }
    }
    return GS_SUCCESS;
}

status_t knl_set_index_stats(knl_handle_t session, knl_index_set_stats_t *ind_stats)
{
    knl_session_t *se = (knl_session_t *)session;
    knl_dictionary_t dc;
    index_part_t *idx_part = NULL;
#ifndef _STATISTICS
    status_t status = GS_SUCCESS;
#endif

    if (knl_open_dc_by_index(se, &ind_stats->owner, NULL, &ind_stats->name, &dc) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (IS_TEMP_TABLE_BY_DC(&dc)) {
        dc_close(&dc);
        GS_THROW_ERROR(ERR_OPERATIONS_NOT_SUPPORT, "set statistics", "temp table");
        return GS_ERROR;
    }

    if (lock_table_shared_directly(se, &dc) != GS_SUCCESS) {
        dc_close(&dc);
        return GS_ERROR;
    }

    index_t *idx = dc_find_index_by_name(DC_ENTITY(&dc), &ind_stats->name);
    if (knl_ckeck_index_status(&dc, ind_stats, idx, &idx_part) != GS_SUCCESS) {
        unlock_tables_directly(se);
        dc_close(&dc);
        return GS_ERROR;
    }

#ifdef _STATISTICS
    status_t status = stats_set_index(se, &dc, ind_stats, idx_part, idx);
    if (status == GS_SUCCESS) {
        knl_commit(session);
    } else {
        knl_rollback(session, NULL);
    }
#endif

    unlock_tables_directly(se);
    dc_close(&dc);
    return status;
}

status_t knl_lock_table_stats(knl_handle_t session, knl_dictionary_t *dc)
{
    knl_session_t *se = (knl_session_t *)session;
    dc_entity_t *entity = DC_ENTITY(dc);
#ifndef _STATISTICS
    status_t status = GS_SUCCESS;
#endif

    cm_latch_x(&entity->cbo_latch, se->id, NULL);

#ifdef _STATISTICS
    if (entity->stats_locked) {
        GS_THROW_ERROR(ERR_OPERATIONS_NOT_SUPPORT, "lock table statistics", "locked table");
        cm_unlatch(&entity->cbo_latch, NULL);
        return GS_ERROR;
    }

    status_t status = stats_set_analyze_time(session, dc, GS_TRUE);
    if (status == GS_SUCCESS) {
        knl_commit(session);
        entity->stats_locked = GS_TRUE;
    } else {
        knl_rollback(session, NULL);
    }
#endif

    cm_unlatch(&entity->cbo_latch, NULL);
    return status;
}

status_t knl_unlock_table_stats(knl_handle_t session, knl_dictionary_t *dc)
{
    knl_session_t *se = (knl_session_t *)session;
    dc_entity_t *entity = DC_ENTITY(dc);
    status_t status = GS_SUCCESS;

    cm_latch_x(&entity->cbo_latch, se->id, NULL);

    if (!entity->stats_locked) {
        GS_THROW_ERROR(ERR_OPERATIONS_NOT_SUPPORT, "unlock table statistics", "non-locked table");
        cm_unlatch(&entity->cbo_latch, NULL);
        return GS_SUCCESS;
    }
#ifdef GS_SUCCESS
    status_t status = stats_set_analyze_time(session, dc, GS_FALSE);
    if (status == GS_SUCCESS) {
        knl_commit(session);
        entity->stats_locked = GS_FALSE;
    } else {
        knl_rollback(session, NULL);
    }
#endif

    cm_unlatch(&entity->cbo_latch, NULL);
    return status;
}

status_t knl_check_undo_space(knl_session_t *session, uint32 space_id)
{
    space_t *undo_space = SPACE_GET(space_id);
    undo_context_t *ctx = &session->kernel->undo_ctx;
    space_t *old_undo_space = SPACE_GET(ctx->space->ctrl->id);
    datafile_t *df = NULL;
    uint32 id;
    uint64 total_size = 0;

    if (undo_space->ctrl->type != SPACE_TYPE_UNDO) {
        GS_THROW_ERROR(ERR_CAPABILITY_NOT_SUPPORT, "switch UNDO tablespace using non-undo tablespace");
        return GS_ERROR;
    }

    if (space_id == old_undo_space->ctrl->id) {
        GS_THROW_ERROR(ERR_INVALID_OPERATION, ",switch the same undo tablespace");
        return GS_ERROR;
    }

    if (!SPACE_IS_ONLINE(undo_space)) {
        GS_THROW_ERROR(ERR_SPACE_OFFLINE, undo_space->ctrl->name, "can not be switched");
        return GS_ERROR;
    }

    for (uint32 i = 0; i < undo_space->ctrl->file_hwm; i++) {
        id = undo_space->ctrl->files[i];
        if (GS_INVALID_ID32 == id) {
            continue;
        }
        df = DATAFILE_GET(id);

        /* calculate space max size by maxsize with autoextend on or size with autoextend off of each datafile */
        if (DATAFILE_IS_AUTO_EXTEND(df)) {
            total_size += (uint64)df->ctrl->auto_extend_maxsize;
        } else {
            total_size += (uint64)df->ctrl->size;
        }
    }

    if (total_size <= UNDO_SEGMENT_COUNT * UNDO_DEF_TXN_PAGE * DEFAULT_PAGE_SIZE) {
        GS_THROW_ERROR(ERR_INVALID_OPERATION, ", new undo tablespace size too small");
        return GS_ERROR;
    }

    return GS_SUCCESS;
}

status_t knl_alter_switch_undo_space(knl_handle_t se, text_t *spc_name)
{
    knl_session_t *session = (knl_session_t *)se;
    core_ctrl_t *core_ctrl = DB_CORE_CTRL(session);

    if (!DB_IS_PRIMARY(&session->kernel->db)) {
        GS_THROW_ERROR(ERR_DATABASE_ROLE, "operation", "not in primary mode");
        return GS_ERROR;
    }

    if (!DB_IS_RESTRICT(session)) {
        GS_THROW_ERROR(ERR_INVALID_OPERATION, ",operation only supported in restrict mode");
        return GS_ERROR;
    }

    if (core_ctrl->undo_segments_extended) {
        GS_THROW_ERROR(ERR_INVALID_OPERATION, ",operation not supported after undo segments extend");
        return GS_ERROR;
    }

    if (undo_check_active_transaction(session)) {
        GS_THROW_ERROR(ERR_TXN_IN_PROGRESS, "end all transaction before action");
        return GS_ERROR;
    }

    uint32 space_id;
    if (spc_get_space_id(session, spc_name, &space_id) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (knl_check_undo_space(session, space_id) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (undo_switch_space(session, space_id) != GS_SUCCESS) {
        return GS_ERROR;
    }

    return GS_SUCCESS;
}

status_t knl_get_consis_hash_buckets(knl_handle_t handle, knl_consis_hash_strategy_t *strategy, bool32 *is_found)
{
    lob_locator_t *lob = NULL;
    status_t status;
    knl_session_t *session = (knl_session_t *)handle;

    CM_SAVE_STACK(session->stack);
    do {
        knl_cursor_t *cursor = knl_push_cursor(session);
        status = db_query_consis_hash_strategy(session, &strategy->slice_cnt, &strategy->group_cnt, cursor, is_found);
        GS_BREAK_IF_ERROR(status);
        if (*is_found) {
            lob = (lob_locator_t *)CURSOR_COLUMN_DATA(cursor, SYS_CONSIS_HASH_STRATEGY_COL_BUCKETS);
            status = knl_read_lob(session, lob, 0, strategy->buckets.bytes, BUCKETDATALEN, NULL);
            GS_BREAK_IF_ERROR(status);
        }
    } while (0);

    CM_RESTORE_STACK(session->stack);
    return GS_SUCCESS;
}

static status_t knl_btree_corruption_scan(knl_session_t *session, btree_t *btree, knl_corrupt_info_t *info)
{
    btree_page_t *page = NULL;
    page_head_t *head = NULL;
    space_t *space = NULL;
    page_id_t next_ext;
    uint32 extent_size = 0;
    uint32 extents = 1;

    if (!IS_INVALID_PAGID(btree->entry)) {
        space = SPACE_GET(btree->segment->space_id);
        // No.0 extents can not degrade, here should calc, because the page have not been read
        extent_size = spc_get_ext_size(space, 0);
        buf_enter_page(session, btree->entry, LATCH_MODE_S, ENTER_PAGE_NORMAL);
        head = (page_head_t *)CURR_PAGE;
        next_ext = AS_PAGID(head->next_ext);
        uint32 extent_count = btree->segment->extents.count;
        page_id_t last_pagid = (btree->segment->ufp_count > 0) ? btree->segment->ufp_first : btree->segment->ufp_extent;
        page_id_t curr_pagid = btree->segment->extents.first;
        buf_leave_page(session, GS_FALSE);

        for (;;) {
            if (IS_INVALID_PAGID(curr_pagid) || IS_SAME_PAGID(last_pagid, curr_pagid)) {
                break;
            }

            if (knl_check_session_status(session) != GS_SUCCESS) {
                return GS_ERROR;
            }

            if (buf_read_page(session, curr_pagid, LATCH_MODE_S, ENTER_PAGE_NORMAL | ENTER_PAGE_SEQUENTIAL) !=
                GS_SUCCESS) {
                errno_t err_code = cm_get_error_code();
                if (err_code == ERR_PAGE_CORRUPTED) {
                    db_save_corrupt_info(session, curr_pagid, info);
                }
                return GS_ERROR;
            }
            page = BTREE_CURR_PAGE;

            if (extent_size == 0) {
                extent_size = spc_get_page_ext_size(space, page->head.ext_size);
                next_ext = (extent_count == extents) ? INVALID_PAGID : AS_PAGID(page->head.next_ext);
                extents++;
            }

            extent_size--;

            buf_leave_page(session, GS_FALSE);

            if (extent_size == 0) {
                curr_pagid = next_ext;
            } else {
                curr_pagid.page++;
            }
        }
    }

    return GS_SUCCESS;
}

static status_t knl_index_verify(knl_session_t *session, knl_dictionary_t *dc, index_t *index, knl_corrupt_info_t *info)
{
    table_t *table = &DC_ENTITY(dc)->table;
    part_table_t *part_table = table->part_table;
    btree_t *btree = NULL;

    if (!IS_PART_INDEX(index)) {
        btree = &index->btree;
        return knl_btree_corruption_scan(session, btree, info);
    }

    for (uint32 i = 0; i < part_table->desc.partcnt; i++) {
        part_index_t *part_index = index->part_index;
        index_part_t *index_part = PART_GET_ENTITY(part_index, i);
        table_part_t *table_part = PART_GET_ENTITY(part_table, i);
        if (!IS_READY_PART(table_part) || index_part == NULL) {
            continue;
        }
        btree = &index_part->btree;
        if ((btree->segment == NULL) && !IS_INVALID_PAGID(btree->entry)) {
            if (dc_load_index_part_segment(session, dc->handle, index_part) != GS_SUCCESS) {
                return GS_ERROR;
            }
        }

        if (knl_btree_corruption_scan(session, btree, info) != GS_SUCCESS) {
            return GS_ERROR;
        }
    }

    return GS_SUCCESS;
}

status_t knl_verify_index_by_name(knl_handle_t session, knl_dictionary_t *dc, text_t *index_name,
    knl_corrupt_info_t *info)
{
    knl_session_t *se = (knl_session_t *)session;
    index_t *index = NULL;
    bool32 lock_inuse = GS_FALSE;

    if (!lock_table_without_xact(se, dc->handle, &lock_inuse)) {
        return GS_ERROR;
    }

    if (DC_ENTITY(dc)->corrupted) {
        unlock_table_without_xact(se, dc->handle, lock_inuse);
        GS_THROW_ERROR(ERR_DC_CORRUPTED);
        return GS_ERROR;
    }

    index = dc_find_index_by_name(DC_ENTITY(dc), index_name);

    if (index == NULL) {
        unlock_table_without_xact(se, dc->handle, lock_inuse);
        GS_THROW_ERROR(ERR_INDEX_NOT_EXIST, DC_ENTITY(dc)->entry->user_name, T2S_EX(index_name));
        return GS_ERROR;
    }

    if (knl_index_verify(se, dc, index, info) != GS_SUCCESS) {
        unlock_table_without_xact(se, dc->handle, lock_inuse);
        return GS_ERROR;
    }

    unlock_table_without_xact(se, dc->handle, lock_inuse);

    return GS_SUCCESS;
}

status_t knl_verify_table(knl_handle_t session, knl_dictionary_t *dc, knl_corrupt_info_t *corrupt_info)
{
    bool32 lock_inuse = GS_FALSE;
    dc_entity_t *entity = DC_ENTITY(dc);
    table_t *table = (table_t *)&entity->table;
    if (!lock_table_without_xact(session, entity, &lock_inuse)) {
        return GS_ERROR;
    }

    if (entity->corrupted) {
        unlock_table_without_xact(session, dc->handle, lock_inuse);
        GS_THROW_ERROR(ERR_DC_CORRUPTED);
        return GS_ERROR;
    }

    if (IS_PART_TABLE(table)) {
        if (part_table_corruption_verify((knl_session_t *)session, dc, corrupt_info)) {
            unlock_table_without_xact(session, dc->handle, lock_inuse);
            return GS_ERROR;
        }
    } else {
        if (heap_table_corruption_verify((knl_session_t *)session, dc, corrupt_info)) {
            unlock_table_without_xact(session, dc->handle, lock_inuse);
            return GS_ERROR;
        }
    }
    unlock_table_without_xact(session, dc->handle, lock_inuse);
    return GS_SUCCESS;
}

void knl_dc_recycle_all(knl_handle_t session)
{
    knl_session_t *sess = (knl_session_t *)session;
    dc_context_t *ctx = &sess->kernel->dc_ctx;
    dc_lru_queue_t *queue = ctx->lru_queue;
    dc_entity_t *curr = NULL;
    dc_entity_t *head = NULL;
    dc_entity_t *prev = NULL;

    if (queue->count == 0) {
        return;
    }
    queue = ctx->lru_queue;
    cm_spin_lock(&queue->lock, NULL);

    if (queue->count == 0) {
        cm_spin_unlock(&queue->lock);
        return;
    }

    head = queue->head;
    curr = queue->tail;

    while (curr != NULL && curr != head) {
        prev = curr->lru_prev;
        if (!dc_try_recycle(ctx, queue, curr)) {
            dc_lru_shift(queue, curr);
        }
        curr = prev;
    }
    cm_spin_unlock(&queue->lock);
    return;
}

status_t knl_repair_catalog(knl_handle_t session)
{
    knl_session_t *se = (knl_session_t *)session;

    if (!DB_IS_MAINTENANCE(se)) {
        GS_THROW_ERROR(ERR_OPERATIONS_NOT_ALLOW, "repairing catalog with non-restrict or non-upgrade mode");
        return GS_ERROR;
    }

    if (knl_internal_repair_catalog(se) != GS_SUCCESS) {
        return GS_ERROR;
    }

    return GS_SUCCESS;
}

status_t knl_database_has_nolog_object(knl_handle_t se, bool32 *has_nolog)
{
    *has_nolog = GS_FALSE;
    knl_session_t *session = (knl_session_t *)se;
    CM_SAVE_STACK(session->stack);
    knl_cursor_t *cursor = knl_push_cursor(session);
    knl_open_sys_cursor(session, cursor, CURSOR_ACTION_SELECT, SYS_INSTANCE_INFO_ID, IX_SYS_INSTANCE_INFO_001_ID);
    knl_init_index_scan(cursor, GS_TRUE);
    char name[] = "NOLOGOBJECT_CNT";
    knl_set_scan_key(INDEX_DESC(cursor->index), &cursor->scan_range.l_key, GS_TYPE_STRING, name, (uint16)strlen(name),
        IX_COL_SYS_INSTANCE_INFO_001_NAME);

    if (knl_fetch(session, cursor) != GS_SUCCESS) {
        CM_RESTORE_STACK(session->stack);
        return GS_ERROR;
    }

    if (!cursor->eof) {
        uint64 nolog_cnt = *(uint64 *)CURSOR_COLUMN_DATA(cursor, SYS_INSTANCE_INFO_COL_VALUE);
        if (nolog_cnt > 0) {
            *has_nolog = GS_TRUE;
        }
    }

    CM_RESTORE_STACK(session->stack);
    return GS_SUCCESS;
}

// 将text_t结构的默认值转换为variant_t结构的默认值类型
status_t knl_parse_default_from_text(knl_handle_t handle, knl_handle_t entity, knl_handle_t column,
    memory_context_t *mem, void **expr_tree, void **expr_update_tree, text_t parse_text)
{
    status_t ret = GS_SUCCESS;
    knl_session_t *session = (knl_session_t *)handle;
    if (dc_alloc_mem(&session->kernel->dc_ctx, mem, sizeof(variant_t), (void **)expr_tree) != GS_SUCCESS) {
        GS_THROW_ERROR(ERR_DC_BUFFER_FULL);
        return GS_ERROR;
    }
    ret = memset_sp(*expr_tree, sizeof(variant_t), 0, sizeof(variant_t));
    knl_securec_check(ret);

    knl_column_t *col = (knl_column_t *)column;
    variant_t *value = (variant_t *)*expr_tree;
    if (parse_text.len == 0 || parse_text.str == NULL) {
        value->type = col->datatype;
        value->is_null = 1;
        *expr_update_tree = *expr_tree;
        return GS_SUCCESS;
    }
    
    internal_default_value_t* default_value = (internal_default_value_t*)parse_text.str;

    if( default_value->type == DEFAULT_VALUE_TYPE_LITERAL) {
        ret =  var_gen_variant(default_value->data, default_value->size, col->datatype, value);
        if( ret != GS_SUCCESS) {
            GS_THROW_ERROR(ERR_EXPR_DATA_TYPE_NOT_MATCH);
            return GS_ERROR;
        }
    } else if( default_value->type == DEFAULT_VALUE_TYPE_FUNC) {
        value->type = col->datatype;
        value->is_null = 1;
        *expr_update_tree = *expr_tree;
        return GS_SUCCESS;
    } else {
        GS_THROW_ERROR(ERR_EXPR_DATA_TYPE_NOT_MATCH);
        return GS_ERROR;
    }

    *expr_update_tree = *expr_tree;
    return GS_SUCCESS;
}

status_t knl_exec_default(void *stmt, void *expr_node, variant_t *value)
{
    CS_CHECK_NULL_PTR(expr_node);
    CS_CHECK_NULL_PTR(value);

    variant_t *expr_val = (variant_t *)expr_node;
    *value = *expr_val;
    return GS_SUCCESS;
}

#endif
#ifdef __cplusplus
}
#endif
