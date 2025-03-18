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
 * repl_log_replay.c
 *    implement of log replay function
 *
 * IDENTIFICATION
 *    src/storage/gstor/zekernel/kernel/replication/repl_log_replay.c
 *
 * -------------------------------------------------------------------------
 */
#include "repl_log_replay.h"
#include "cm_log.h"
#include "cm_file.h"
#include "knl_context.h"

#define REPLAY_FAIL_THRESHOLD  100

/*
 * redo apply from file routine, including from archive files and log files
 * @param kernel session, log point, data size, need more log flag
 */
status_t lrpl_replay(knl_session_t *session, log_point_t *point, uint32 data_size, 
    log_batch_t *batch, uint32 block_size)
{
    bool32 need_more = GS_FALSE;
    bool32 replay_fail = GS_FALSE;
    lrpl_context_t *ctx = &session->kernel->lrpl_ctx;

    if (rcy_replay(session, point, data_size, batch, block_size, &need_more, &replay_fail, GS_FALSE) != GS_SUCCESS) {
        GS_LOG_RUN_INF("[Log Replayer] failed to replay log at point [%u-%u/%u/%llu]",
                       point->rst_id, point->asn, point->block_id, (uint64)point->lfn);
        return GS_ERROR;
    }

    if (replay_fail) {
        if (ctx->replay_fail_cnt < REPLAY_FAIL_THRESHOLD) {
            ctx->replay_fail_cnt++;
        }
    } else {
        ctx->replay_fail_cnt = 0;
    }

    if (!need_more) {
        GS_LOG_RUN_INF("[Log Replayer] failed to replay log at point [%u-%u/%u/%llu], no more log needed",
                       point->rst_id, point->asn, point->block_id, (uint64)point->lfn);
        return GS_ERROR;
    }

    return GS_SUCCESS;
}

static bool32 lrpl_check_gap_exist(knl_session_t *session, log_point_t *point)
{
    uint32 file_id;

    log_lock_logfile(session);
    file_id = log_get_id_by_asn(session, (uint32)point->rst_id, point->asn, NULL);
    if (file_id == GS_INVALID_ID32) {
        log_unlock_logfile(session);
        return GS_TRUE;
    }

    log_unlatch_file(session, file_id);
    log_unlock_logfile(session);
    return GS_FALSE;
}

status_t lrpl_prepare_archfile(knl_session_t *session, log_point_t *point, bool32 *reset)
{
    lrpl_context_t *lrpl_ctx = &session->kernel->lrpl_ctx;
    gbp_aly_ctx_t *aly_ctx = &session->kernel->gbp_aly_ctx;
    thread_t *thread = SESSION_IS_LOG_ANALYZE(session) ? &aly_ctx->thread : &lrpl_ctx->thread;
    char arch_name[GS_FILE_NAME_BUFFER_SIZE] = {0};
    lftc_task_handle_t task_handle;
    bool32 fetch_done = GS_FALSE;

    *reset = GS_FALSE;

    bool32 file_exist = arch_get_archived_log_name(session, (uint32)point->rst_id, point->asn, ARCH_DEFAULT_DEST,
        arch_name, GS_FILE_NAME_BUFFER_SIZE);
    if (file_exist && cm_file_exist(arch_name)) {
        return GS_SUCCESS;
    }

    arch_set_archive_log_name(session, (uint32)point->rst_id, point->asn, ARCH_DEFAULT_DEST, arch_name,
        GS_FILE_NAME_BUFFER_SIZE);

    if (cm_file_exist(arch_name)) {
        return GS_SUCCESS;
    }

    GS_LOG_RUN_INF("[%s] Archive log %s not found, start to fetch it from primary.",
        LRPL_OR_GBPALY(session), arch_name);

    if (lftc_clt_create_task(session, (uint32)point->rst_id, point->asn, arch_name, &task_handle) != GS_SUCCESS) {
        return GS_ERROR;
    }

    for (;;) {
        if (thread->closed) {
            return GS_ERROR;
        }

        if (lftc_clt_task_running(session, &task_handle, &fetch_done)) {
            cm_sleep(100);
            continue;
        }

        if (fetch_done || session->kernel->lftc_client_ctx.arch_lost) {
            break;
        }

        // Check replay point is in online
        uint32 file_id = log_get_id_by_asn(session, (uint32)point->rst_id, point->asn, NULL);
        if (file_id != GS_INVALID_ID32) {
            log_unlatch_file(session, file_id);
            *reset = GS_TRUE;
            *point = session->kernel->redo_ctx.curr_point;
            return GS_SUCCESS;
        }

        // Check whether restlog has changed.
        if (point->rst_id < session->kernel->db.ctrl.core.resetlogs.rst_id &&
            point->asn > session->kernel->db.ctrl.core.resetlogs.last_asn) {
            GS_LOG_RUN_INF("[%s] point rstid/asn [%u/%u], resetlogs rstid/last_asn [%u/%u], "
                "curr_file rstid/asn [%u/%u], current point [%u-%u/%u/%llu]",
                LRPL_OR_GBPALY(session),
                point->rst_id, point->asn, session->kernel->db.ctrl.core.resetlogs.rst_id,
                session->kernel->db.ctrl.core.resetlogs.last_asn,
                session->kernel->redo_ctx.files[session->kernel->redo_ctx.curr_file].head.rst_id,
                session->kernel->redo_ctx.files[session->kernel->redo_ctx.curr_file].head.asn,
                session->kernel->redo_ctx.curr_point.rst_id, session->kernel->redo_ctx.curr_point.asn,
                session->kernel->redo_ctx.curr_point.block_id,
                (uint64)session->kernel->redo_ctx.curr_point.lfn);
            *reset = GS_TRUE;
            *point = session->kernel->redo_ctx.curr_point;
            return GS_SUCCESS;
        }

        if (knl_failover_triggered(session->kernel)) {
            lrpl_ctx->has_gap = lrpl_check_gap_exist(session, point);
            return GS_ERROR;
        }

        // Sleep 3 seconds and retry
        cm_sleep(3000);

        arch_set_archive_log_name(session, (uint32)point->rst_id, point->asn, ARCH_DEFAULT_DEST, arch_name,
            GS_FILE_NAME_BUFFER_SIZE);
        if (cm_file_exist(arch_name)) {
            GS_LOG_RUN_INF("[Log Replayer] Archive log %s already exists", arch_name);
            return GS_SUCCESS;
        }

        if (lftc_clt_create_task(session, (uint32)point->rst_id, point->asn,
            arch_name, &task_handle) != GS_SUCCESS) {
            return GS_ERROR;
        }
    }

    return GS_SUCCESS;
}

static inline void lrpl_switch_buf(lrpl_context_t *lrpl_ctx, rcy_context_t *rcy_ctx)
{
    if (rcy_ctx->paral_rcy) {
        if (rcy_ctx->swich_buf) {
            lrpl_ctx->read_buf = &rcy_ctx->read_buf2;
        } else {
            lrpl_ctx->read_buf = &rcy_ctx->read_buf;
        }
        rcy_ctx->swich_buf = !rcy_ctx->swich_buf;
    }
}

static bool32 lrpl_need_realloc_buf(log_batch_t *batch, lrpl_context_t *lrpl_ctx, rcy_context_t *rcy_ctx)
{
    if (rcy_ctx->paral_rcy) {
        if (rcy_ctx->swich_buf) {
            if (log_need_realloc_buf(batch, &rcy_ctx->read_buf, "rcy", GS_MAX_BATCH_SIZE)) {
                lrpl_ctx->read_buf = &rcy_ctx->read_buf;
                return GS_TRUE;
            }
        } else {
            if (log_need_realloc_buf(batch, &rcy_ctx->read_buf2, "rcy second buf", GS_MAX_BATCH_SIZE)) {
                lrpl_ctx->read_buf = &rcy_ctx->read_buf2;
                return GS_TRUE;
            }
        }
    } else {
        if (log_need_realloc_buf(batch, &rcy_ctx->read_buf, "rcy", GS_MAX_BATCH_SIZE)) {
            lrpl_ctx->read_buf = &rcy_ctx->read_buf;
            return GS_TRUE;
        }
    }
    return GS_FALSE;
}

status_t lrpl_load(knl_session_t *session, log_point_t *point, uint32 *data_size, uint32 *block_size)
{
    rcy_context_t *rcy = &session->kernel->rcy_ctx;
    lrpl_context_t *ctx = &session->kernel->lrpl_ctx;
    log_context_t *log = &session->kernel->redo_ctx;
    lrcv_context_t *lrcv = &session->kernel->lrcv_ctx;
    uint32 file_id = GS_INVALID_ID32;
    rcy->loading_curr_file = GS_FALSE;

    if (!lrcv->wait_info.waiting) {
        log_lock_logfile(session);
        file_id = log_get_id_by_asn(session, (uint32)point->rst_id, point->asn, &rcy->loading_curr_file);
        log_unlock_logfile(session);
    }

    if (file_id == GS_INVALID_ID32) {
        bool32 reset = GS_FALSE;
        if (lrpl_prepare_archfile(session, point, &reset) != GS_SUCCESS) {
            GS_LOG_RUN_INF("[Log Replayer] failed to prepare archive log at point [%u-%u/%u/%llu]",
                point->rst_id, point->asn, point->block_id, (uint64)point->lfn);
            return GS_ERROR;
        }

        if (reset) {
            return GS_SUCCESS;
        }

        if (rcy_load_from_arch(session, point, data_size, &rcy->arch_file, ctx->read_buf) != GS_SUCCESS) {
            GS_LOG_RUN_INF("[Log Replayer] failed to load archive log at point [%u-%u/%u/%llu]",
                point->rst_id, point->asn, point->block_id, (uint64)point->lfn);
            return GS_ERROR;
        }
        *block_size = (uint32)rcy->arch_file.head.block_size;
    } else {
        /*
         * Close archive logfile descriptor as soon as possible,
         * to free disk space after it has been cleaned automatically.
         */
        cm_close_file(rcy->arch_file.handle);
        rcy->arch_file.handle = GS_INVALID_HANDLE;

        /* rcy->read_buf.buf_size <= 64M, cannot overflow */
        if (rcy_load_from_online(session, file_id, point, data_size, ctx->log_handle + file_id,
                                 ctx->read_buf) != GS_SUCCESS) {
            GS_LOG_RUN_INF("[Log Replayer] failed to load online log[%u] at point [%u-%u/%u/%llu]",
                file_id, point->rst_id, point->asn, point->block_id, (uint64)point->lfn);
            return GS_ERROR;
        }
        *block_size = log->files[file_id].ctrl->block_size;
    }

    return GS_SUCCESS;
}

status_t lrpl_do_replay(knl_session_t *session, log_point_t *point, uint32 data_size,
    log_batch_t *batch, uint32 block_size)
{
    lrpl_context_t *ctx = &session->kernel->lrpl_ctx;
    rcy_context_t *rcy = &session->kernel->rcy_ctx;
    date_t start_time = g_timer()->now;
    status_t status;

    rcy->last_lrpl_time = start_time;

    if (rcy->paral_rcy) {
        rcy_wait_preload_complete(session);
        rcy->wait_stats_view[PRELOAD_WAIT_TIME] = (uint64)(g_timer()->now - start_time) / MILLISECS_PER_SECOND;

        rcy_wait_replay_complete(session);
        if (rcy->last_point.asn != GS_INVALID_ASN) {
            ckpt_set_trunc_point(session, &rcy->last_point);
            gbp_queue_set_trunc_point(session, &rcy->last_point);
        }
    }

    rcy->wait_stats_view[PARAL_PROC_WAIT_TIME] = (uint64)(g_timer()->now - start_time) / MILLISECS_PER_SECOND;
    start_time = g_timer()->now;
    rcy->add_page_time = 0;
    rcy->add_bucket_time = 0;

    rcy->curr_group = rcy->group_list;
    rcy->curr_group_id = 0;
    /*
     * data_size less than 0.9 * buf_size, means no more log to relpay. When switch log file, the log tail is also
     * less than buf_size, but mostly log tail > 0.9 * buf_size
     */
    rcy->replay_no_lag = (data_size < RCY_NO_LAG_TRESHOLD(ctx->read_buf->buf_size));
    status = lrpl_replay(session, point, data_size, batch, block_size);

    rcy->wait_stats_view[GROUP_ANALYZE_TIME] = (uint64)(g_timer()->now - start_time) / MILLISECS_PER_SECOND;
    rcy->wait_stats_view[ADD_PAGE_TIME] = (uint64)rcy->add_page_time / MILLISECS_PER_SECOND;
    rcy->wait_stats_view[ADD_BUCKET_TIME] = (uint64)rcy->add_bucket_time / MILLISECS_PER_SECOND;

    rcy->last_point = *point;

    return status;
}

status_t lrpl_perform(knl_session_t *session, log_point_t *point)
{
    uint32 data_size = 0;
    rcy_context_t *rcy = &session->kernel->rcy_ctx;
    lrpl_context_t *ctx = &session->kernel->lrpl_ctx;
    log_batch_t *batch = NULL;
    date_t start_time = g_timer()->now;
    date_t curr_time;
    uint32 block_size;

    lrpl_switch_buf(ctx, rcy);

    if (lrpl_load(session, point, &data_size, &block_size) != GS_SUCCESS) {
        return GS_ERROR;
    }

    batch = (log_batch_t *)ctx->read_buf->aligned_buf;
    if (lrpl_need_realloc_buf(batch, ctx, rcy)) {
        rcy_wait_replay_complete(session);
        return GS_SUCCESS;
    }

    curr_time = g_timer()->now;
    rcy->wait_stats_view[READ_LOG_TIME] = (uint64)(curr_time - start_time) / MILLISECS_PER_SECOND;
    rcy->wait_stats_view[READ_LOG_SIZE] = data_size / SIZE_M(1);
    rcy->wait_stats_view[REPALY_SPEED] = (uint64)data_size * MICROSECS_PER_SECOND / SIZE_M(1)
                                         / MAX(curr_time - rcy->last_lrpl_time, 1);
    
    if (lrpl_do_replay(session, point, data_size, batch, block_size) != GS_SUCCESS) {
        return GS_ERROR;
    }

    return GS_SUCCESS;
}

bool32 lrpl_need_replay(knl_session_t *session, log_point_t *point)
{
    log_context_t *redo_ctx = &session->kernel->redo_ctx;
    lrcv_context_t *lrcv = &session->kernel->lrcv_ctx;
    log_file_t *file = &redo_ctx->files[redo_ctx->curr_file];
    log_point_t *flush_point = &lrcv->flush_point;
    log_point_t *primary_point = &lrcv->primary_curr_point;
    log_point_t *wait_point = &lrcv->wait_info.wait_point;

    if (db_terminate_lfn_reached(session, point->lfn)) {
        return GS_FALSE;
    }

    // If standby is waiting for log switch, replay point should stop at wait point. 
    if (lrcv->wait_info.waiting && log_cmp_point(point, wait_point) == 0) {
        return GS_FALSE;
    }

    if (session->kernel->rcy_ctx.log_decrypt_failed) {
        return GS_FALSE;
    }

    // IF flush_point is valid, choose the latest log point to check whether LRPL has finished its work
    if (flush_point->asn != GS_INVALID_ASN && lrcv->status > LRCV_DISCONNECTED && lrcv->status != LRCV_NEED_REPAIR) {
        if (log_cmp_point(primary_point, flush_point) >= 0) {
            return (bool32)(log_cmp_point(point, primary_point) < 0 && !LOG_POINT_LFN_EQUAL(point, primary_point));
        } else {
            return (log_cmp_point(point, flush_point) < 0);
        }
    } else {  // Else need to use the latest log point on current log file to check
        // If status is LOG_FILE_INACTIVE, the asn is 0
        if (file->ctrl->status == LOG_FILE_CURRENT || file->ctrl->status == LOG_FILE_ACTIVE) {
            return !log_point_equal(point, redo_ctx);
        }

        return (log_cmp_point(point, flush_point) < 0);
    }
}

static inline uint16 lrpl_get_new_curr_fileid(knl_session_t *session, log_point_t *replay_point)
{
    log_context_t *redo_ctx = &session->kernel->redo_ctx;
    log_file_t *file = &redo_ctx->files[redo_ctx->curr_file];

    if (file->head.asn == GS_INVALID_ASN) {
        return redo_ctx->curr_file;
    }

    knl_panic(file->head.asn > replay_point->asn);
    return (redo_ctx->curr_file + redo_ctx->logfile_hwm - (file->head.asn - replay_point->asn) % redo_ctx->logfile_hwm)
           % redo_ctx->logfile_hwm;
}

static void lrpl_reset_single_loginfo(knl_session_t *session, uint32 file_id, uint32 replay_asn)
{
    log_context_t *redo_ctx = &session->kernel->redo_ctx;
    log_file_t *file = redo_ctx->files + file_id;
    uint64 start_pos;

    if (file->head.asn > replay_asn) {
        GS_LOG_RUN_INF("[Log Replayer] reset logfile [%d]-[%u/%u/%llu]",
            file_id, file->head.rst_id, file->head.asn, file->head.write_pos);

        start_pos = CM_CALC_ALIGN(sizeof(log_file_head_t), file->ctrl->block_size);
        file->head.asn = GS_INVALID_ASN;
        redo_ctx->free_size += (file_id == redo_ctx->curr_file) ? (file->head.write_pos - start_pos) : 0;
        file->head.write_pos = start_pos;
        file->ctrl->status = LOG_FILE_INACTIVE;
        file->ctrl->archived = GS_FALSE;
        log_flush_head(session, file);
        if (db_save_log_ctrl(session, file_id) != GS_SUCCESS) {
            CM_ABORT(0, "[Log Replayer] ABORT INFO: save control space file failed when reset log file");
        }
    }
}

static void lrpl_reset_loginfo(knl_session_t *session, uint32 replay_asn)
{
    log_context_t *redo_ctx = &session->kernel->redo_ctx;
    uint32 file_id = redo_ctx->active_file;

    log_lock_logfile(session);

    while (file_id != redo_ctx->curr_file) {
        lrpl_reset_single_loginfo(session, file_id, replay_asn);
        log_get_next_file(session, &file_id, GS_FALSE);
    }

    log_flush_head(session, redo_ctx->files + redo_ctx->curr_file);
    lrpl_reset_single_loginfo(session, redo_ctx->curr_file, replay_asn);

    log_unlock_logfile(session);
}

static void lrpl_process_for_failover(knl_session_t *session)
{
    log_context_t *redo_ctx = &session->kernel->redo_ctx;
    lrpl_context_t *lrpl_ctx = &session->kernel->lrpl_ctx;
    log_file_t *file = NULL;
    uint16 new_curr_file;

    if (!lrpl_ctx->has_gap) {
        return;
    }

    new_curr_file = lrpl_get_new_curr_fileid(session, &lrpl_ctx->curr_point);

    GS_LOG_RUN_INF("[Log Replayer] replay has gap, active file [%u]-[%u/%u/%llu], "
                   "current file [%u]-[%u/%u/%llu], new current file %u",
                   redo_ctx->active_file, redo_ctx->files[redo_ctx->active_file].head.rst_id,
                   redo_ctx->files[redo_ctx->active_file].head.asn,
                   redo_ctx->files[redo_ctx->active_file].head.write_pos,
                   redo_ctx->curr_file, redo_ctx->files[redo_ctx->curr_file].head.rst_id,
                   redo_ctx->files[redo_ctx->curr_file].head.asn,
                   redo_ctx->files[redo_ctx->curr_file].head.write_pos, new_curr_file);

    arch_reset_archfile(session, lrpl_ctx->curr_point.asn);
    lrpl_reset_loginfo(session, lrpl_ctx->curr_point.asn);
    ckpt_trigger(session, GS_TRUE, CKPT_TRIGGER_FULL);

    file = &redo_ctx->files[new_curr_file];
    redo_ctx->curr_file = new_curr_file;
    redo_ctx->active_file = new_curr_file;
    file->head.asn = lrpl_ctx->curr_point.asn;
    file->ctrl->status = LOG_FILE_CURRENT;
    file->ctrl->archived = GS_FALSE;
    log_flush_head(session, file);
    if (db_save_log_ctrl(session, new_curr_file) != GS_SUCCESS) {
        CM_ABORT(0, "[Log Replayer] ABORT INFO: save control space file failed when reset log file");
    }

    session->kernel->db.ctrl.core.log_first = redo_ctx->active_file;
    session->kernel->db.ctrl.core.log_last = redo_ctx->curr_file;
    if (db_save_core_ctrl(session) != GS_SUCCESS) {
        CM_ABORT(0, "[Log Replayer] ABORT INFO: save core control space file failed when reset log file");
    }
}

static void lrpl_try_repair_file_offset(knl_session_t *session)
{
    log_context_t *log_ctx = &session->kernel->redo_ctx;
    rcy_context_t *rcy = &session->kernel->rcy_ctx;
    lrpl_context_t *lrpl_ctx = &session->kernel->lrpl_ctx;
    switch_ctrl_t *ctrl = &session->kernel->switch_ctrl;
    log_file_t *file = &log_ctx->files[log_ctx->curr_file];
    int64 buf_size = rcy->read_buf.buf_size;
    uint64 lfn = 0;
    uint64 scn;

    if (session->kernel->lrcv_ctx.reconnected || DB_IS_RAFT_ENABLED(session->kernel)) {
        return;
    }

    GS_LOG_RUN_INF("[Log Replayer] start to repair file offset");
    if (log_get_file_offset(session, file->ctrl->name, &rcy->read_buf,
        (uint64 *)&file->head.write_pos, &lfn, &scn) != GS_SUCCESS) {
        GS_LOG_RUN_ERR("[Log Replayer] failed to get file offset for logfile[%u] %s, latest lfn %llu",
            log_ctx->curr_file, file->ctrl->name, lfn);
        if (buf_size != rcy->read_buf.buf_size) {
            lrpl_ctx->read_buf = &rcy->read_buf;
        }

        cm_spin_lock(&ctrl->lock, NULL);
        ctrl->handling = GS_FALSE;
        ctrl->request = SWITCH_REQ_NONE;
        cm_spin_unlock(&ctrl->lock);
    }
    GS_LOG_RUN_INF("[Log Replayer] end to repair file offset");

    /* Add current file's remain size into free size. */
    log_ctx->free_size += log_file_freesize(file);

    /*
     * Set reconnected to true, to prevent parsing current redo file
     * and increasing free size repeatably.
     */
    session->kernel->lrcv_ctx.reconnected = GS_TRUE;
}

static inline bool32 lrpl_can_failover(knl_session_t *session)
{
    lrpl_context_t *lrpl = &session->kernel->lrpl_ctx;
    lrcv_context_t *lrcv = &session->kernel->lrcv_ctx;
    
    return (bool32)(lrcv->status == LRCV_NEED_REPAIR || lrpl->has_gap || !lrpl_need_replay(session, &lrpl->curr_point));
}

/* Only happened when test GBP performance and set _MRP_RES_LOGSIZE > 0 */
static bool32 lrpl_need_pause_for_gbp(knl_session_t *session)
{
    knl_instance_t *kernel = session->kernel;
    log_context_t *redo_ctx = &kernel->redo_ctx;
    lrpl_context_t *lrpl = &kernel->lrpl_ctx;
    lrcv_context_t *lrcv = &kernel->lrcv_ctx;
    gbp_aly_ctx_t *aly_ctx = &kernel->gbp_aly_ctx;
    uint64 log_size = GS_INVALID_ID64;
    log_point_t curr_flushed_point;

    if (!DB_IS_OPEN(session) || kernel->gbp_attr.lrpl_res_logsize <= 0) {
        return GS_FALSE;
    }

    /* raft not inited */
    if (DB_IS_RAFT_ENABLED(kernel) && kernel->raft_ctx.status < RAFT_STATUS_INITED) {
        return GS_FALSE;
    }

    if (gbp_promote_triggered(kernel)) {
        return GS_FALSE;
    }

    if (lrcv->state == REP_STATE_DEMOTE_REQUEST || lrcv->state == REP_STATE_WAITING_DEMOTE) {
        return GS_FALSE; // standby switchover triggered
    }

    /* do not cross rst_id */
    if (aly_ctx->curr_point.rst_id == lrpl->curr_point.rst_id) {
        if ((gbp_aly_get_file_end_point(session, &curr_flushed_point, redo_ctx->curr_file) == GS_SUCCESS) &&
            (log_size_between_two_point(session, lrpl->curr_point, curr_flushed_point, &log_size) == GS_SUCCESS) &&
            (log_size < kernel->gbp_attr.lrpl_res_logsize)) {
            GS_LOG_DEBUG_INF("log replayer need keep log distance.log_size[%llu], lrpl_res_logsize[%llu]",
                             log_size, kernel->gbp_attr.lrpl_res_logsize);
            return GS_TRUE;
        }
    }

    return GS_FALSE;
}

static void lrpl_try_use_gbp(knl_session_t *session)
{
    knl_instance_t *kernel = session->kernel;
    log_context_t *redo = &kernel->redo_ctx;
    lrpl_context_t *lrpl = &kernel->lrpl_ctx;
    gbp_aly_ctx_t *aly = &kernel->gbp_aly_ctx;
    rcy_context_t *rcy = &kernel->rcy_ctx;
    uint64 lrpl_remain_size = GS_INVALID_ID64;
    uint64 gbp_remain_size = GS_INVALID_ID64;

    if (KNL_RECOVERY_WITH_GBP(kernel)) {
        return; // GBP turbo is running
    }

    if (!gbp_promote_triggered(kernel)) {
        return;
    }

    if (KNL_GBP_SAFE(kernel) && aly->is_done && // need wait log analysis finished
        gbp_replay_in_window(session, lrpl->curr_point)) { // current lrpl point is in GBP window
        if (rcy->paral_rcy) {
            rcy_wait_replay_complete(session);
        }

        if (!KNL_GBP_SAFE(kernel)) {
            return; // recheck again, because log replayer may set unsafe when rcy_wait_replay_complete
        }

        (void)log_size_between_two_point(session, lrpl->curr_point, aly->curr_point, &lrpl_remain_size);
        (void)log_size_between_two_point(session, redo->gbp_rcy_point, aly->curr_point, &gbp_remain_size);
        GS_LOG_RUN_INF("[GBP] failover points(asn-block-lfn): "
                       "curr_point[%u-%u-%llu], gbp_rcy_point[%u-%u-%llu], log_end_point[%u-%u-%llu]",
                       lrpl->curr_point.asn, lrpl->curr_point.block_id, (uint64)lrpl->curr_point.lfn,
                       redo->gbp_rcy_point.asn, redo->gbp_rcy_point.block_id, (uint64)redo->gbp_rcy_point.lfn,
                       aly->curr_point.asn, aly->curr_point.block_id, (uint64)aly->curr_point.lfn);

        GS_LOG_RUN_INF("[GBP] lrpl remain log size: before use gbp [%lluMB], after use gbp [%lluKB]",
                       lrpl_remain_size / SIZE_M(1), gbp_remain_size / SIZE_K(1));

        gbp_knl_begin_read(session, &lrpl->curr_point);
    }
}

static void lrpl_wait_replay_complete(knl_session_t *session)
{
    rcy_context_t *rcy = &session->kernel->rcy_ctx;

    if (rcy->paral_rcy && rcy->last_point.asn != GS_INVALID_ASN) {
        rcy_wait_replay_complete(session);
        ckpt_set_trunc_point(session, &rcy->last_point);
        gbp_queue_set_trunc_point(session, &rcy->last_point);
        rcy->last_point.asn = GS_INVALID_ASN;
    }
}

static void lrpl_proc_loop(thread_t *thread)
{
    knl_session_t *session = (knl_session_t *)thread->argument;
    lrpl_context_t *lrpl = &session->kernel->lrpl_ctx;
    bool32 sleep_needed = GS_FALSE;

    while (!thread->closed) {
        if (lrpl->is_closing) {
            break;
        }

        if (sleep_needed) {
            if (knl_failover_triggered(session->kernel)) {
                lrpl_try_repair_file_offset(session);
                if (lrpl_can_failover(session)) {
                    GS_LOG_RUN_INF("[Log Replayer] failover triggered");
                    break;
                }
            }

            cm_sleep(20);
        }

        if (!lrpl_need_replay(session, &lrpl->curr_point)) {
            lrpl_wait_replay_complete(session);
            sleep_needed = GS_TRUE;
            lrpl->replay_fail_cnt = 0;
            continue;
        }

        if (lrpl_need_pause_for_gbp(session)) {
            sleep_needed = GS_TRUE;
            continue;
        }

        /* try to use gbp */
        if (KNL_GBP_ENABLE(session->kernel)) {
            lrpl_try_use_gbp(session);
        }

        if (session->kernel->lftc_client_ctx.arch_lost) {
            sleep_needed = GS_TRUE;
            continue;
        }

        if (lrpl_perform(session, &lrpl->curr_point) != GS_SUCCESS) {
            sleep_needed = GS_TRUE;
            continue;
        }

        sleep_needed = GS_FALSE;
    }

    lrpl_wait_replay_complete(session);
}

static void lrpl_update_resetlog_scn(knl_session_t *session)
{
    log_context_t *redo_ctx = &session->kernel->redo_ctx;
    core_ctrl_t *core = &session->kernel->db.ctrl.core;
    switch_ctrl_t *ctrl = &session->kernel->switch_ctrl;

    if (ctrl->request != SWITCH_REQ_NONE || GS_INVALID_SCN(redo_ctx->curr_scn)) {
        return;
    }

    core->reset_log_scn = redo_ctx->curr_scn;
    if (db_save_core_ctrl(session) != GS_SUCCESS) {
        CM_ABORT(0, "[Log Replayer] ABORT INFO: failed to save core ctrlfile");
    }
}

/*
 * lrpl thread global apply routine, include real time apply and archive apply
 * @param thread
 */
static void lrpl_proc(thread_t *thread)
{
    knl_session_t *session = (knl_session_t *)thread->argument;
    lrpl_context_t *lrpl = &session->kernel->lrpl_ctx;
    rcy_context_t *rcy = &session->kernel->rcy_ctx;

    cm_set_thread_name("log_replayer");
    GS_LOG_RUN_INF("log replayer thread started");
    KNL_SESSION_SET_CURR_THREADID(session, cm_get_current_thread_id());

    lrpl->curr_point = session->kernel->redo_ctx.curr_point;
    lrpl->begin_point = lrpl->curr_point;
    rcy->last_lrpl_time = cm_now();

    lrpl_proc_loop(thread);

    GS_LOG_RUN_INF("[Log Replayer] recovery end with log point: rst_id %u asn %u lfn %llu offset %u",
        lrpl->curr_point.rst_id, lrpl->curr_point.asn, (uint64)lrpl->curr_point.lfn, lrpl->curr_point.block_id);

    lrpl_process_for_failover(session);

    thread->closed = GS_TRUE;
    lrpl->read_buf = NULL;

    /* rcy_ctx->file is opened in lrpl_perform=>rcy_load_from_arch */
    cm_close_file(session->kernel->rcy_ctx.arch_file.handle);
    session->kernel->rcy_ctx.arch_file.handle = INVALID_FILE_HANDLE;
    for (uint32 i = 0; i < GS_MAX_LOG_FILES; i++) {
        cm_close_file(lrpl->log_handle[i]);
        lrpl->log_handle[i] = INVALID_FILE_HANDLE;
    }
    lrpl->end_time = cm_now();

    lrpl_update_resetlog_scn(session);
    GS_LOG_RUN_INF("log replayer thread closed");
    KNL_SESSION_CLEAR_THREADID(session);
}

status_t lrpl_init(knl_session_t *session)
{
    knl_session_t *replay_session = session->kernel->sessions[SESSION_ID_REPLAY];
    lrpl_context_t *lrpl = &replay_session->kernel->lrpl_ctx;
    uint32 i;

    lrpl->has_gap = GS_FALSE;
    lrpl->replay_fail_cnt = 0;

    if (lrpl->read_buf == NULL) {
        lrpl->read_buf = &session->kernel->rcy_ctx.read_buf;
    }
    lrpl->is_closing = GS_FALSE;
    lrpl->is_done = GS_FALSE;
    lrpl->curr_point.asn = GS_INVALID_ASN;
    lrpl->begin_point.asn = GS_INVALID_ASN;
    lrpl->begin_time = cm_now();
    lrpl->end_time = 0;

    for (i = 0; i < GS_MAX_LOG_FILES; i++) {
        lrpl->log_handle[i] = -1;
    }

    if (GS_SUCCESS != cm_create_thread(lrpl_proc, 0, replay_session, &lrpl->thread)) {
        GS_LOG_RUN_INF("[Log Replayer] failed to start log replayer thread");
        return GS_ERROR;
    }

    return GS_SUCCESS;
}

void lrpl_close(knl_session_t *session)
{
    knl_instance_t *kernel = (knl_instance_t *)session->kernel;
    lrpl_context_t *lrpl = &kernel->lrpl_ctx;
    lrpl->is_closing = GS_TRUE;
    cm_close_thread(&lrpl->thread);
}

bool32 lrpl_replay_blocked(knl_session_t *session)
{
    knl_instance_t *kernel = (knl_instance_t *)session->kernel;
    lrpl_context_t *lrpl = &kernel->lrpl_ctx;
    lrcv_context_t *lrcv = &kernel->lrcv_ctx;

    if (DB_STATUS(session) != DB_STATUS_OPEN || lrcv->status < LRCV_READY) {
        return GS_FALSE;
    }

    if (lrpl_need_replay(session, &lrpl->curr_point) && lrpl->replay_fail_cnt >= REPLAY_FAIL_THRESHOLD) {
        return GS_TRUE;
    }

    return GS_FALSE;
}
