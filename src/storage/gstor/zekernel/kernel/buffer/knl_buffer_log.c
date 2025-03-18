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
 * knl_buffer_log.c
 *    kernel buffer redo routines
 *
 * IDENTIFICATION
 *    src/storage/gstor/zekernel/kernel/buffer/knl_buffer_log.c
 *
 * -------------------------------------------------------------------------
 */
#include "knl_buffer_log.h"
#include "knl_buflatch.h"
#include "knl_buffer_access.h"

void buf_enter_invalid_page(knl_session_t *session, latch_mode_t mode)
{
    session->curr_page = NULL;
    session->curr_page_ctrl = NULL;

    buf_push_page(session, NULL, mode);
}

/*
 * Repair a broken page using backup and redo log. Only replay one page and other pages should be skipped
 */
static void abr_enter_page(knl_session_t *session, page_id_t page_id)
{
    rcy_context_t *rcy = &session->kernel->rcy_ctx;
    bool32 is_skip = GS_TRUE;

    if (IS_SAME_PAGID(rcy->abr_ctrl->page_id, page_id)) {
        session->curr_page = (char *)rcy->abr_ctrl->page;
        session->curr_page_ctrl = rcy->abr_ctrl;
        buf_push_page(session, rcy->abr_ctrl, LATCH_MODE_X);
        is_skip = (session->curr_lsn <= ((page_head_t *)session->curr_page)->lsn);
    } else {
        buf_enter_invalid_page(session, LATCH_MODE_X);
    }

    session->page_stack.is_skip[session->page_stack.depth - 1] = is_skip;
}

void rd_enter_page(knl_session_t *session, log_entry_t *log)
{
    rd_enter_page_t *redo = (rd_enter_page_t *)log->data;
    datafile_t *df = DATAFILE_GET(redo->file);
    uint32 options = redo->options & RD_ENTER_PAGE_MASK;
    space_t *space = NULL;
    page_id_t page_id = MAKE_PAGID(redo->file, redo->page);

    if (session->kernel->db.status == DB_STATUS_OPEN) {
        /* NO_READ page does not need to be loaded from disk when standby lrpl perform */
        options = redo->options & (RD_ENTER_PAGE_MASK | ENTER_PAGE_NO_READ);
    }

    if (IS_BLOCK_RECOVER(session)) {
        abr_enter_page(session, page_id);
        return;
    }

    if (!DATAFILE_IS_ONLINE(df) || !df->ctrl->used || DF_FILENO_IS_INVAILD(df)) {
        buf_enter_invalid_page(session, LATCH_MODE_X);
        session->page_stack.is_skip[session->page_stack.depth - 1] = GS_TRUE;
        return;
    }
    space = SPACE_GET(df->space_id);
    if (!SPACE_IS_ONLINE(space) || !space->ctrl->used) {
        buf_enter_invalid_page(session, LATCH_MODE_X);
        session->page_stack.is_skip[session->page_stack.depth - 1] = GS_TRUE;
        return;
    }

    if (IS_FILE_RECOVER(session) && redo->file != session->kernel->rcy_ctx.repair_file_id) {
        buf_enter_invalid_page(session, LATCH_MODE_X);
        session->page_stack.is_skip[session->page_stack.depth - 1] = GS_TRUE;
        return;
    }

    buf_enter_page(session, page_id, LATCH_MODE_X, options);
    bool32 need_skip = (!(options & ENTER_PAGE_NO_READ) &&
        (session->curr_lsn <= ((page_head_t *)session->curr_page)->lsn));
    session->page_stack.is_skip[session->page_stack.depth - 1] = need_skip;
}

void print_enter_page(log_entry_t *log)
{
    rd_enter_page_t *redo = (rd_enter_page_t *)log->data;
    (void)printf("page %u-%u, pcn %u, options %u\n", (uint32)redo->file, (uint32)redo->page,
                 (uint32)redo->pcn, (uint32)redo->options);
}

void abr_leave_page(knl_session_t *session, bool32 changed, bool32 is_skip)
{
    buf_ctrl_t *ctrl = buf_curr_page(session);

    if (ctrl == NULL || is_skip) {
        buf_pop_page(session);
        return;
    }

    /* if page is allocated without initialized, then page->size_units=0 */
    if (DB_TO_RECOVERY(session) && ctrl->page->size_units != 0) {
        knl_panic_log(CHECK_PAGE_PCN(ctrl->page), "page pcn is abnormal, panic info: page %u-%u type %u",
                      ctrl->page_id.file, ctrl->page_id.page, ctrl->page->type);
    }

    if (changed) {
        knl_panic_log(PAGE_SIZE(*ctrl->page) != 0, "page size is abnormal, panic info: page %u-%u type %u",
                      ctrl->page_id.file, ctrl->page_id.page, ctrl->page->type);
        ctrl->page->pcn++;
        PAGE_TAIL(ctrl->page)->pcn++;

        if (!ctrl->is_readonly) {
            ctrl->is_readonly = 1;
            session->changed_pages[session->changed_count++] = ctrl;
            knl_panic_log(session->changed_count <= 1, "the changed page count of current session is abnormal, "
                          "panic info: page %u-%u type %u changed_count %u",
                          ctrl->page_id.file, ctrl->page_id.page, ctrl->page->type, session->changed_count);
        }
    }

    buf_pop_page(session);
}

void rd_leave_page(knl_session_t *session, log_entry_t *log)
{
    bool32 changed = *(bool32 *)log->data;
    bool32 is_skip = session->page_stack.is_skip[session->page_stack.depth - 1];

    if (IS_BLOCK_RECOVER(session)) {
        abr_leave_page(session, changed, is_skip);
        return;
    }

    if (!is_skip && changed) {
#ifdef _REPLICATION
        /* redo between redo->curr_point and gbp->rcy_point should be skipped */
        if (KNL_RECOVERY_WITH_GBP(session->kernel) && session->kernel->redo_ctx.gbp_rcy_lfn != 0 &&
            session->kernel->redo_ctx.lfn < session->kernel->redo_ctx.gbp_rcy_lfn) {
            knl_panic_log((session->page_stack.is_skip[session->page_stack.depth - 1] == GS_TRUE),
                          "[GBP] redo between redo->curr_point and gbp->rcy_point should be skipped");
        }

        /* should not replay log on hit page */
        if (DB_IS_PRIMARY(&session->kernel->db) && KNL_GBP_ENABLE(session->kernel) &&
            session->curr_page_ctrl->gbp_ctrl->page_status == GBP_PAGE_HIT) {
            knl_panic_log(0, "[GBP] should not replay log on hit page");
        }
#endif
        buf_leave_page(session, GS_TRUE);
    } else {
        buf_leave_page(session, GS_FALSE);
    }
}

void print_leave_page(log_entry_t *log)
{
    bool32 changed = *(bool32 *)log->data;
    (void)printf("changed %u\n", (uint32)changed);
}

/* gbp analyze proc entry for RD_ENTER_PAGE, when gbp analyze, replay txn page */
void gbp_aly_enter_page(knl_session_t *session, log_entry_t *log, uint64 lsn)
{
    log_context_t *ctx = &session->kernel->redo_ctx;
    rd_enter_page_t *redo = (rd_enter_page_t *)log->data;
    page_id_t page_id = MAKE_PAGID(redo->file, redo->page);

    ctx->replay_stat.analyze_pages++;
    if (redo->options & ENTER_PAGE_RESIDENT) {
        ctx->replay_stat.analyze_resident_pages++;
    }

    /* redo txn page when do log analysis */
    if (log->type == RD_ENTER_TXN_PAGE) {
        rd_enter_page(session, log);
    } else {
        buf_enter_invalid_page(session, LATCH_MODE_X);

        /* must do log analysis for page's redo log, so can not skip */
        session->page_stack.is_skip[session->page_stack.depth - 1] = GS_FALSE;
    }

    knl_panic_log(session->page_stack.depth > 0, "page_stack's depth is abnormal, panic info: page %u-%u depth %u",
                  page_id.file, page_id.page, session->page_stack.depth);
    session->page_stack.gbp_aly_page_id[session->page_stack.depth - 1] = page_id;
}

static void gbp_aly_set_page_backend(knl_session_t *session, page_id_t page_id, uint64 lsn, uint64 lfn)
{
#ifdef _REPLICATION
    gbp_aly_ctx_t *aly = &session->kernel->gbp_aly_ctx;
    rcy_context_t *rcy = &session->kernel->rcy_ctx;
    gbp_page_bucket_t *bucket = &aly->page_bucket;
    uint32 next = (bucket->tail + 1) % bucket->count;
    gbp_aly_page_t item;

    item.page_id = page_id;
    item.lsn = lsn;
    item.lfn = lfn;
    for (;;) {
        if (SECUREC_UNLIKELY(next == bucket->head)) {
            cm_spin_sleep();
            rcy->wait_stats_view[ADD_BUCKET_TIME]++;
        } else {
            break;
        }
    }

    cm_spin_lock(&bucket->lock, NULL);
    bucket->first[bucket->tail] = item;
    bucket->tail = next;
    cm_spin_unlock(&bucket->lock);
#endif
}

/* gbp analyze proc entry for RD_LEAVE_PAGE, when gbp analyze, replay txn page, and set page's latest lsn */
void gbp_aly_leave_page(knl_session_t *session, log_entry_t *log, uint64 lsn)
{
#ifdef _REPLICATION
    log_context_t *ctx = &session->kernel->redo_ctx;
    gbp_aly_ctx_t *aly = &session->kernel->gbp_aly_ctx;
    bool32 changed = *((bool32 *)log->data);
    page_id_t page_id = session->page_stack.gbp_aly_page_id[session->page_stack.depth - 1];

    if (IS_INVALID_PAGID(page_id)) {
        knl_panic_log(0, "[GBP] analysis get invalid page id, page [%u:%u]", page_id.file, page_id.page);
    }

    if (log->type == RD_LEAVE_TXN_PAGE) {
        rd_leave_page(session, log);
    } else {
        buf_pop_page(session);
    }

    if (changed && !aly->is_closing) {
        if (SECUREC_LIKELY(SESSION_IS_LOG_ANALYZE(session))) {
            gbp_aly_set_page_backend(session, page_id, lsn, ctx->analysis_lfn);
        } else {

            gbp_aly_set_page_lsn(session, page_id, lsn, ctx->analysis_lfn);

        }
    }
#endif
}