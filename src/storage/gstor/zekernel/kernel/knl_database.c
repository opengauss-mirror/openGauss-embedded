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
 * knl_database.c
 * implement of database
 *
 * IDENTIFICATION
 * src/storage/gstor/zekernel/kernel/knl_database.c
 *
 * -------------------------------------------------------------------------
 */
#include "knl_database.h"
#include "cm_file.h"
#include "cm_gts_timestamp.h"
#include "knl_context.h"
#include "knl_db_create.h"
#include "index_common.h"
#include "knl_ctrl_restore.h"

#ifdef __cplusplus
extern "C" {
#endif

#define BACKGROUD_LOG_INTERVAL 300 /* 5min */

static status_t db_start_daemon(knl_session_t *session);
static char *db_get_role(database_t *db);

/*
 * Initialize member of database
 * @param    kernel handle of the open kernel
 * @return
 * - GS_SUCCESS
 * - GS_ERROR
 * @note must call after instance is startup
 * @author wangjincheng 343637
 * @since 2017/4/26
 */

#define PRINT_SIZEOF(_WHAT_) GS_LOG_DEBUG_INF("sizeof(" #_WHAT_ ") = %d ", sizeof(_WHAT_))

status_t db_init(knl_session_t *session)
{
    uint32 i;
    knl_instance_t *kernel = (knl_instance_t *)session->kernel;
    database_t *db = &kernel->db;
    uint32 size = CTRL_MAX_PAGE * GS_DFLT_CTRL_BLOCK_SIZE;
    uint32 offset = CTRL_LOG_SEGMENT;
    errno_t err;


    PRINT_SIZEOF(kernel->server);
    PRINT_SIZEOF(kernel->db);
    PRINT_SIZEOF(kernel->redo_ctx);
    PRINT_SIZEOF(kernel->dc_ctx);
    PRINT_SIZEOF(kernel->ckpt_ctx);
    PRINT_SIZEOF(kernel->undo_ctx);
    PRINT_SIZEOF(kernel->tran_ctx);
    PRINT_SIZEOF(kernel->lock_ctx);
    PRINT_SIZEOF(kernel->lob_ctx);
    PRINT_SIZEOF(kernel->smon_ctx);
    PRINT_SIZEOF(kernel->rmon_ctx);
    PRINT_SIZEOF(kernel->job_ctx);
    PRINT_SIZEOF(kernel->synctimer_ctx);
    PRINT_SIZEOF(kernel->arch_ctx);
    // PRINT_SIZEOF(kernel->backup_ctx);
    PRINT_SIZEOF(kernel->index_ctx);
    PRINT_SIZEOF(kernel->rcy_ctx);
    PRINT_SIZEOF(kernel->buf_ctx);
    PRINT_SIZEOF(kernel->pcrp_ctx);
    PRINT_SIZEOF(kernel->encrypt_ctx);
    PRINT_SIZEOF(kernel->ashrink_ctx);
    PRINT_SIZEOF(kernel->stat);
    PRINT_SIZEOF(kernel->total_io_read);
    PRINT_SIZEOF(kernel->temp_pool);
    PRINT_SIZEOF(kernel->switch_ctrl);
    PRINT_SIZEOF(kernel->record_backup_trigger);
    PRINT_SIZEOF(kernel->compress_buf_ctx);
    PRINT_SIZEOF(kernel->buf_aio_ctx);
    PRINT_SIZEOF(kernel->alck_ctx);

    GS_LOG_RUN_INF("[DB INIT] db init start.");
    if (cm_aligned_malloc((int64)size, "ctrl", &db->ctrl.buf) != GS_SUCCESS) {
        return GS_ERROR;
    }
    db->ctrl.pages = (ctrl_page_t *)db->ctrl.buf.aligned_buf;
    err = memset_sp(db->ctrl.pages, size, 0, size);
    knl_securec_check(err);

    for (i = 0; i < GS_MAX_CTRL_FILES; i++) {
        db->ctrlfiles.items[i].handle = GS_INVALID_HANDLE;
    }

    db->ctrl.log_segment = offset;
    db_init_logfile_ctrl(session, &offset);
    db->ctrl.space_segment = offset;
    db_init_space_ctrl(session, &offset);
    db->ctrl.datafile_segment = offset;
    db_init_datafile_ctrl(session, &offset);
    db->ctrl.arch_segment = offset;

    err = memset_sp(&db->ctrl.core, sizeof(core_ctrl_t), 0, sizeof(core_ctrl_t));
    knl_securec_check(err);

    if (buf_init(session) != GS_SUCCESS) {
        return GS_ERROR;
    }
    pcrp_init(session);
    GS_LOG_RUN_INF("[DB INIT] init buf & pcrp finish.");

    if (pcb_init_ctx(session) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (db_start_daemon(session) != GS_SUCCESS) {
        return GS_ERROR;
    }
    GS_LOG_RUN_INF("[DB INIT] start daemon finish.");

    if (dc_preload(session, DB_STATUS_NOMOUNT) != GS_SUCCESS) {
        return GS_ERROR;
    }
#ifdef _REPLICATION
    lsnd_eventfd_init(&kernel->lsnd_ctx);
#endif
    GS_LOG_RUN_INF("[DB INIT] db init finish.");
    return GS_SUCCESS;
}

knl_scn_t db_inc_scn(knl_session_t *session)
{
    tx_area_t *area;
    timeval_t now;
    time_t init_time;
    knl_scn_t scn;
    knl_scn_t gts_scn;
    status_t status;
    uint64 seq = 1;

    area = &session->kernel->tran_ctx;
    init_time = DB_INIT_TIME(session);

    if (TX_XA_CONSISTENCY(session)) {
        status = gts_get_lcl_timestamp(&gts_scn);
        KNL_SCN_TO_TIMESEQ(gts_scn, &now, seq, CM_GTS_BASETIME);
        seq++;
        knl_panic(status == GS_SUCCESS);
        CM_ABORT(status == GS_SUCCESS, "[DB] ABORT INFO: increase scn failed");
    } else {
        (void)cm_gettimeofday(&now);
    }

    cm_spin_lock(&area->scn_lock, &session->stat_inc_scn);
    scn = knl_inc_scn(init_time, &now, seq, &session->kernel->scn, session->kernel->attr.systime_inc_threshold);
    knl_panic(DB_IS_PRIMARY(&session->kernel->db));
    cm_spin_unlock(&area->scn_lock);

    return scn;
}

knl_scn_t db_next_scn(knl_session_t *session)
{
    return db_inc_scn(session);
    /* atomic_t curr_scn;
    timeval_t now;
    time_t init_time;
    status_t status;
    uint64 seq = 1;
    knl_scn_t gts_scn;

    init_time = DB_INIT_TIME(session);
    curr_scn = (int64)DB_CURR_SCN(session);

    if (TX_XA_CONSISTENCY(session)) {
        status = gts_get_lcl_timestamp(&gts_scn);
        KNL_SCN_TO_TIMESEQ(gts_scn, &now, seq, CM_GTS_BASETIME);
        seq++;
        knl_panic(status == GS_SUCCESS);
        CM_ABORT(status == GS_SUCCESS, "[DB] ABORT INFO: get next scn failed");
    } else {
        (void)cm_gettimeofday(&now);
    }
    return knl_inc_scn(init_time, &now, seq, &curr_scn, session->kernel->attr.systime_inc_threshold); */
}

status_t file_name_preproc(knl_session_t *session, char *suffix, char *name)
{
    char tmp_name[GS_FILE_NAME_BUFFER_SIZE] = {0};
    PRTS_RETURN_IFERR(snprintf_s(tmp_name, GS_FILE_NAME_BUFFER_SIZE, GS_FILE_NAME_BUFFER_SIZE - 1,
                "%s/data/%s", session->kernel->home, suffix));
    if (strncmp(name, tmp_name, GS_FILE_NAME_BUFFER_SIZE) != 0){
        MEMS_RETURN_IFERR(memcpy_sp(name, GS_FILE_NAME_BUFFER_SIZE, tmp_name, GS_FILE_NAME_BUFFER_SIZE));
    }
    return GS_SUCCESS;
}

status_t db_load_logfiles(knl_session_t *session)
{
    uint32 i;
    log_file_t *logfile = NULL;
    database_t *db = &session->kernel->db;

    // mount redo files
    db->logfiles.hwm = db->ctrl.core.log_hwm;
    for (i = 0; i < db->ctrl.core.log_hwm; i++) {
        logfile = &db->logfiles.items[i];
        if (LOG_IS_DROPPED(logfile->ctrl->flg)) {
            continue;
        }

        char* suffix_name = strrchr(logfile->ctrl->name, '/');
        if (file_name_preproc(session, suffix_name + 1, logfile->ctrl->name) != GS_SUCCESS){
            GS_LOG_RUN_ERR("[DB] file %s mismatch with current path!", logfile->ctrl->name);
            return GS_ERROR;
        }
        /* logfile can be opened for a long time, closed in db_close_log_files */
        if (cm_open_device(logfile->ctrl->name, logfile->ctrl->type, knl_redo_io_flag(session), &logfile->handle) !=
            GS_SUCCESS) {
            GS_LOG_RUN_ERR("[DB] failed to open %s ", logfile->ctrl->name);
            return GS_ERROR;
        }
    }
    GS_LOG_RUN_INF("[DB] load logfiles finish");
    return GS_SUCCESS;
}

static void db_upgrade_original_space(core_ctrl_t *core_ctrl, space_t *space, bool32 *is_upgrade)
{
    switch (space->ctrl->id) {
        case SYS_SPACE_ID:
            if (core_ctrl->system_space == 0) {
                space->ctrl->type = SPACE_TYPE_SYSTEM | SPACE_TYPE_DEFAULT;
                core_ctrl->system_space = space->ctrl->id;
            }
            return;
        case FIXED_TEMP_SPACE_ID:
            if (core_ctrl->swap_space == 0) {
                space->ctrl->type = SPACE_TYPE_TEMP | SPACE_TYPE_SWAP | SPACE_TYPE_DEFAULT;
                core_ctrl->swap_space = space->ctrl->id;
            }
            return;
        case FIXED_UNDO_SPACE_ID:
            if (core_ctrl->undo_space == 0) {
                space->ctrl->type = SPACE_TYPE_UNDO | SPACE_TYPE_DEFAULT;
                core_ctrl->undo_space = space->ctrl->id;
            }
            return;
        case FIXED_USER_SPACE_ID:
            if (core_ctrl->user_space == 0) {
                space->ctrl->type = SPACE_TYPE_USERS | SPACE_TYPE_DEFAULT;
                core_ctrl->user_space = space->ctrl->id;
            }
            return;
        default:
            if (!SPACE_IS_DEFAULT(space)) {
                // 'typed' version will not hit this
                if (SPACE_IS_USER_NOLOGGING(space)) {
                    space->ctrl->type = SPACE_TYPE_TEMP;
                }
                space->ctrl->type |= SPACE_TYPE_USERS;
                return;
            }
    }

    *is_upgrade = GS_FALSE;
}

/*
 * for old version compatibility issue
 */
static void db_try_upgrade_space(knl_session_t *session, space_t *space, bool32 *is_upgrade)
{
    core_ctrl_t *core_ctrl = DB_CORE_CTRL(session);

    *is_upgrade = GS_TRUE;

    // if it is default space, need verify ****_space in core ctrl
    if (space->ctrl->type != SPACE_TYPE_UNDEFINED && !SPACE_IS_DEFAULT(space)) {
        *is_upgrade = GS_FALSE;
        return;
    }

    /* old version default space cannot be off-lined */
    if (!SPACE_IS_ONLINE(space) && SPACE_TYPE_IS_UNDEFINED(space)) {
        space->ctrl->type = SPACE_TYPE_USERS;
        return;
    }

    if (cm_text_str_equal(&g_temp2, space->ctrl->name) && (core_ctrl->temp_space == 0)) {
        space->ctrl->type = SPACE_TYPE_TEMP | SPACE_TYPE_USERS | SPACE_TYPE_DEFAULT;
        core_ctrl->temp_space = space->ctrl->id;
        return;
    }

    if (cm_text_str_equal(&g_temp2_undo, space->ctrl->name) && (core_ctrl->temp_undo_space == 0)) {
        space->ctrl->type = SPACE_TYPE_UNDO | SPACE_TYPE_TEMP | SPACE_TYPE_DEFAULT;
        core_ctrl->temp_undo_space = space->ctrl->id;
        return;
    }

    // for spc200 version compatibility
    if (cm_text_str_equal(&g_sysaux, space->ctrl->name) && (core_ctrl->sysaux_space == 0)) {
        space->ctrl->type = SPACE_TYPE_SYSAUX | SPACE_TYPE_DEFAULT;
        core_ctrl->sysaux_space = space->ctrl->id;
        return;
    }

    db_upgrade_original_space(core_ctrl, space, is_upgrade);
}

static status_t db_save_tablespace_ctrl(knl_session_t *session, space_t *space, bool32 is_upgrade)
{
    if (is_upgrade) {
        if (SPACE_IS_DEFAULT(space)) {
            if (db_save_core_ctrl(session) != GS_SUCCESS) {
                CM_ABORT(0, "[DB] ABORT INFO: failed to save core ctrl file when load tablespace");
                return GS_ERROR;
            }
        }

        if (db_save_space_ctrl(session, space->ctrl->id) != GS_SUCCESS) {
            CM_ABORT(0, "[DB] ABORT INFO: failed to save space ctrl file when load tablespace %s", space->ctrl->name);
            return GS_ERROR;
        }
    } else {
        if (!SPACE_IS_ONLINE(space)) {
            if (db_save_space_ctrl(session, space->ctrl->id) != GS_SUCCESS) {
                CM_ABORT(0, "[DB] ABORT INFO: failed to save space ctrl file when load tablespace %s",
                    space->ctrl->name);
                return GS_ERROR;
            }
        }
    }

    return GS_SUCCESS;
}

static status_t db_load_tablespaces(knl_session_t *session, bool32 *has_offline)
{
    space_t *space = NULL;
    bool32 is_upgrade = GS_FALSE;
    bool32 swap_encrypt = GS_FALSE;
    uint32 i;

    *has_offline = GS_FALSE;

    // mount spaces
    for (i = 0; i < GS_MAX_SPACES; i++) {
        space = SPACE_GET(i);
        if (space->ctrl->file_hwm == 0) {
            continue;
        }

        /* skip mount off-lined space */
        if (!SPACE_IS_ONLINE(space)) {
            continue;
        }

        if (spc_mount_space(session, space, GS_TRUE) != GS_SUCCESS) {
            return GS_ERROR;
        }

        if (SPACE_IS_ENCRYPT(space)) {
            swap_encrypt = GS_TRUE;
        }

        db_try_upgrade_space(session, space, &is_upgrade);
        if (db_save_tablespace_ctrl(session, space, is_upgrade) != GS_SUCCESS) {
            return GS_ERROR;
        }

        if (!SPACE_IS_ONLINE(space)) {
            *has_offline = GS_TRUE;
        }
    }

    if (swap_encrypt) {
        if (spc_active_swap_encrypt(session) != GS_SUCCESS) {
            return GS_ERROR;
        }
    }
    return GS_SUCCESS;
}

static void db_close_ctrl_files(knl_session_t *session)
{
    ctrlfile_t *ctrlfile = NULL;
    knl_instance_t *kernel = (knl_instance_t *)session->kernel;
    database_t *db = &kernel->db;
    uint32 i;

    for (i = 0; i < db->ctrlfiles.count; i++) {
        ctrlfile = &db->ctrlfiles.items[i];
        cm_close_device(ctrlfile->type, &ctrlfile->handle);
    }
}

void db_close_log_files(knl_session_t *session)
{
    log_file_t *file = NULL;
    database_t *db = &session->kernel->db;

    for (uint32 i = 0; i < db->logfiles.hwm; i++) {
        file = &db->logfiles.items[i];

        if (LOG_IS_DROPPED(file->ctrl->flg)) {
            continue;
        }

        cm_close_device(file->ctrl->type, &file->handle);
    }
}

void db_close(knl_session_t *session, bool32 need_ckpt)
{
    log_point_t *rcy_point = &session->kernel->db.ctrl.core.rcy_point;
    log_point_t *lrp_point = &session->kernel->db.ctrl.core.lrp_point;

    if (!DB_IS_PRIMARY(&session->kernel->db)) {
#ifdef _REPLICATION
        gbp_aly_close(session);
        lrpl_close(session);
        lftc_clt_close(session);
#endif
    }

    rcy_close(session);
#ifdef _REPLICATION
    gbp_agent_close(session);
#endif
#ifdef _BACKUP
    bak_close(session);
#endif
    undo_close(session);
    arch_close(session);
    tx_rollback_close(session);
    smon_close(session);
    rmon_close(session);
    ashrink_close(session);
#ifdef _STATISTICS
    stats_close(session);
#endif
    job_close(session);
    idx_recycle_close(session);
    synctimer_close(session);

#ifdef _STATISTICS
    if (need_ckpt) {
        GS_LOG_RUN_INF("begin to save table monitor stats");
        (void)knl_flush_table_monitor(session->kernel->sessions[SESSION_ID_STATS]);
        GS_LOG_RUN_INF("finished to save table monitor stats");
    }
#endif
#ifdef _REPLICATION
    if (DB_IS_RAFT_ENABLED(session->kernel)) {
        if (log_flush(session, NULL, NULL) != GS_SUCCESS) {
            CM_ABORT(0, "[LOG] ABORT INFO: redo log task flush redo file failed.");
        }
        if (session->kernel->raft_ctx.status == RAFT_STATUS_INITED) {
            session->kernel->raft_ctx.status = RAFT_STATUS_CLOSING;
        }
        GS_LOG_RUN_INF("commit_lfn : %llu, flushed_lfn : %llu", session->kernel->raft_ctx.commit_lfn,
            session->kernel->redo_ctx.flushed_lfn);
        raft_wait_for_log_flush(session, session->kernel->redo_ctx.flushed_lfn);
        cm_close_thread(&session->kernel->redo_ctx.async_thread);
        GS_LOG_RUN_INF("commit_lfn : %llu, flushed_lfn : %llu", session->kernel->raft_ctx.commit_lfn,
            session->kernel->redo_ctx.flushed_lfn);
    }
#endif
    if (DB_TO_RECOVERY(session) && need_ckpt) {
        GS_LOG_RUN_INF("begin full checkpoint");
        ckpt_trigger(session, GS_TRUE, CKPT_TRIGGER_FULL);
        GS_LOG_RUN_INF("full checkpoint completed: file:%u,point:%u,lfn:%llu", rcy_point->asn, rcy_point->block_id,
            (uint64)rcy_point->lfn);
        GS_LOG_RUN_INF("full checkpoint completed: file:%u,point:%u,lfn:%llu", lrp_point->asn, lrp_point->block_id,
            (uint64)lrp_point->lfn);
        knl_panic(log_cmp_point(rcy_point, lrp_point) == 0 && LOG_POINT_LFN_EQUAL(rcy_point, lrp_point));
        session->kernel->db.ctrl.core.shutdown_consistency = GS_TRUE;
        if (db_save_core_ctrl(session) != GS_SUCCESS) {
            CM_ABORT(0, "[DB] ABORT INFO: save core control file failed when consistent shutdown");
        }
    }
    ckpt_close(session);
    log_close(session);
#ifdef _REPLICATION
    lsnd_close_all_thread(session);

    if (DB_IS_RAFT_ENABLED((knl_instance_t *)session->kernel) &&
        ((knl_instance_t *)session->kernel)->raft_ctx.status == RAFT_STATUS_CLOSING) {
        raft_stop_consistency(session);
    }
#endif
    undo_context_t *ctx = &session->kernel->undo_ctx;
    if (ctx->extend_cnt != 0) {
        tx_extend_deinit(session);
    }

    if (DB_TO_RECOVERY(session)) {
        db_close_ctrl_files(session);
        db_close_log_files(session);
    }

    cm_aligned_free(&session->kernel->db.ctrl.buf);

    if (session->kernel->attr.enable_asynch) {
        cm_close_thread(&session->kernel->buf_aio_ctx.thread);
    }

    // status must set to close otherwise audit_log will core when shutdown.
    session->kernel->db.status = DB_STATUS_CLOSED;
}

static void db_load_systable(knl_session_t *session, uint32 table_id, page_id_t entry)
{
    table_t *table;

    table = db_sys_table(table_id);
    dc_set_table_accessor(table);

    table->desc.entry = entry;
    table->heap.entry = table->desc.entry;

    buf_enter_page(session, table->heap.entry, LATCH_MODE_S, ENTER_PAGE_RESIDENT);
    table->heap.segment = HEAP_SEG_HEAD;
    buf_leave_page(session, GS_FALSE);

    table->desc.seg_scn = table->heap.segment->seg_scn;
}

static void db_load_systables(knl_session_t *session)
{
    database_t *db = &session->kernel->db;
    index_t *index = NULL;

    db_load_systable(session, SYS_TABLE_ID, db->ctrl.core.sys_table_entry);
    db_load_systable(session, SYS_COLUMN_ID, db->ctrl.core.sys_column_entry);
    db_load_systable(session, SYS_INDEX_ID, db->ctrl.core.sys_index_entry);
    db_load_systable(session, SYS_USER_ID, db->ctrl.core.sys_user_entry);

    index = db_sys_index(IX_SYS_TABLE1_ID);
    index->desc.entry = db->ctrl.core.ix_sys_table1_entry;

    index = db_sys_index(IX_SYS_TABLE2_ID);
    index->desc.entry = db->ctrl.core.ix_sys_table2_entry;

    index = db_sys_index(IX_SYS_COLUMN_ID);
    index->desc.entry = db->ctrl.core.ix_sys_column_entry;

    index = db_sys_index(IX_SYS_INDEX1_ID);
    index->desc.entry = db->ctrl.core.ix_sys_index1_entry;

    index = db_sys_index(IX_SYS_INDEX2_ID);
    index->desc.entry = db->ctrl.core.ix_sys_index2_entry;

    index = db_sys_index(IX_SYS_USER1_ID);
    index->desc.entry = db->ctrl.core.ix_sys_user1_entry;

    index = db_sys_index(IX_SYS_USER2_ID);
    index->desc.entry = db->ctrl.core.ix_sys_user2_entry;
}

static status_t db_verify_systime(knl_session_t *session, bool32 ignore_systime)
{
    time_t init_time = DB_INIT_TIME(session);
    knl_scn_t curr_scn = (uint64)session->kernel->scn;
    int64 threshold = session->kernel->attr.systime_inc_threshold;
    timeval_t sys_time, db_time;

    (void)cm_gettimeofday(&sys_time);
    KNL_SCN_TO_TIME(curr_scn, &db_time, init_time);

    if (threshold == 0 || (int64)(sys_time.tv_sec - db_time.tv_sec) <= threshold) {
        return GS_SUCCESS;
    }

    if (!ignore_systime) {
        GS_THROW_ERROR(ERR_SYSTEM_TIME, db_time.tv_sec, sys_time.tv_sec);
        return GS_ERROR;
    }

    curr_scn = KNL_TIMESEQ_TO_SCN(&sys_time, init_time, 0);
    KNL_SET_SCN(&session->kernel->scn, curr_scn);

    return GS_SUCCESS;
}

status_t db_mount_ctrl(knl_session_t *session)
{
    text_t ctrlfiles;
    bool32 is_found = GS_FALSE;

    if (db_check(session, &ctrlfiles, &is_found) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (!is_found) {
        GS_THROW_ERROR(ERR_LOAD_CONTROL_FILE, "ctrl file does not exist!");
        return GS_ERROR;
    }

    if (db_load_ctrlspace(session, &ctrlfiles) != GS_SUCCESS) {
        return GS_ERROR;
    }
    GS_LOG_RUN_INF("mount ctrl finish");

    return GS_SUCCESS;
}

status_t db_mount(knl_session_t *session)
{
    knl_instance_t *kernel = (knl_instance_t *)session->kernel;
    database_t *db = &kernel->db;

    GS_LOG_RUN_INF("start to alter database MOUNT");

    if (!cm_spin_try_lock(&kernel->lock)) {
        GS_THROW_ERROR(ERR_DB_START_IN_PROGRESS);
        return GS_ERROR;
    }

    knl_panic(db->status == DB_STATUS_NOMOUNT || db->status == DB_STATUS_CREATING);
    kernel->undo_segid = 0;

    if (db_mount_ctrl(session) != GS_SUCCESS) {
        cm_spin_unlock(&kernel->lock);
        return GS_ERROR;
    }

    if (ckpt_recover_partial_write(session) != GS_SUCCESS) {
        cm_spin_unlock(&kernel->lock);
        return GS_ERROR;
    }

    if (arch_init(session) != GS_SUCCESS) {
        cm_spin_unlock(&kernel->lock);
        return GS_ERROR;
    }

    if (db_load_logfiles(session) != GS_SUCCESS) {
        cm_spin_unlock(&kernel->lock);
        return GS_ERROR;
    }

    if (dc_preload(session, DB_STATUS_MOUNT) != GS_SUCCESS) {
        cm_spin_unlock(&kernel->lock);
        return GS_ERROR;
    }
#ifdef _REPLICATION
    if (KNL_GBP_ENABLE(session->kernel)) {
        if (gbp_agent_start(session) != GS_SUCCESS) {
            cm_spin_unlock(&kernel->lock);
            return GS_ERROR;
        }
    }
#endif
    if (rcy_init(session) != GS_SUCCESS) {
        cm_spin_unlock(&kernel->lock);
        return GS_ERROR;
    }

    rmon_load(session);

    db->status = DB_STATUS_MOUNT;

    cm_spin_unlock(&kernel->lock);
    GS_LOG_RUN_INF("sucessfully alter database MOUNT");
    return GS_SUCCESS;
}

static status_t db_start_writer(knl_instance_t *kernel, ckpt_context_t *ckpt)
{
    // start log writer thread
    if (cm_create_thread(log_proc, 0, kernel->sessions[SESSION_ID_LOGWR], &kernel->redo_ctx.thread) != GS_SUCCESS) {
        return GS_ERROR;
    }
#ifdef _REPLICATION
    if (DB_IS_RAFT_ENABLED(kernel)) {
        if (cm_create_thread(log_async_proc, 0, kernel->sessions[SESSION_ID_LOGWR_ASYNC],
            &kernel->redo_ctx.async_thread) != GS_SUCCESS) {
            return GS_ERROR;
        }
    }
#endif
    // start db writer threads
    for (uint32 i = 0; i < ckpt->dbwr_count; i++) {
        if (cm_create_thread(dbwr_proc, 0, &ckpt->dbwr[i], &ckpt->dbwr[i].thread) != GS_SUCCESS) {
            return GS_ERROR;
        }
    }

    // start checkpoint thread
    if (cm_create_thread(ckpt_proc, 0, kernel->sessions[SESSION_ID_DBWR], &kernel->ckpt_ctx.thread) != GS_SUCCESS) {
        return GS_ERROR;
    }

    return GS_SUCCESS;
}

static status_t db_start_daemon(knl_session_t *session)
{
    knl_instance_t *kernel = session->kernel;
    ckpt_context_t *ckpt = &kernel->ckpt_ctx;

    if (log_init(session) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (ckpt_init(session) != GS_SUCCESS) {
        return GS_ERROR;
    }

#ifdef _BACKUP
    bak_init(session);
#endif

    GS_LOG_RUN_INF("int log & ckpt & bak finish.");
    if (db_start_writer(kernel, ckpt) != GS_SUCCESS) {
        return GS_ERROR;
    }

    // start smon thread
    if (cm_create_thread(smon_proc, 0, kernel->sessions[SESSION_ID_SMON], &kernel->smon_ctx.thread) != GS_SUCCESS) {
        return GS_ERROR;
    }

#ifdef _STATISTICS
    // start stats thread
    if (cm_create_thread(stats_proc, 0, kernel->sessions[SESSION_ID_STATS], &kernel->stats_ctx.thread) != GS_SUCCESS) {
        return GS_ERROR;
    }
#endif

    if (KNL_IDX_RECYCLE_ENABLED(kernel)) {
        if (cm_create_thread(idx_recycle_proc, 0, kernel->sessions[SESSION_ID_IDX_RECYCLE],
            &kernel->index_ctx.recycle_ctx.thread) != GS_SUCCESS) {
            return GS_ERROR;
        }
    }
#ifdef _REPLICATION
    if (DB_IS_PRIMARY(&session->kernel->db) && !DB_IS_RAFT_ENABLED(kernel)) {
#else
    if (DB_IS_PRIMARY(&session->kernel->db)) {
#endif
        if (tx_rollback_start(session) != GS_SUCCESS) {
            return GS_ERROR;
        }
    }

    if (kernel->attr.enable_asynch) {
        if (cm_create_thread(buf_aio_proc, 0, kernel, &kernel->buf_aio_ctx.thread) != GS_SUCCESS) {
            return GS_ERROR;
        }
    }

    if (cm_create_thread(rmon_proc, 0, kernel->sessions[SESSION_ID_RMON], &kernel->rmon_ctx.thread) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (ashrink_init(session) != GS_SUCCESS) {
        return GS_ERROR;
    }

    // knl_session_t *ashrink_se = kernel->sessions[SESSION_ID_ASHRINK];
    // if (cm_create_thread(ashrink_proc, 0, ashrink_se, &kernel->ashrink_ctx.thread) != GS_SUCCESS) {
    //     return GS_ERROR;
    // }

    return GS_SUCCESS;
}
#ifdef _REPLICATION
static status_t db_switchover_proc_init(knl_session_t *session)
{
    knl_instance_t *kernel = (knl_instance_t *)session->kernel;
    database_t *db = &kernel->db;

    if (!DB_IS_PRIMARY(db) || DB_IS_RAFT_ENABLED(kernel)) {
        if (KNL_GBP_ENABLE(kernel)) {
            if (gbp_aly_init(session) != GS_SUCCESS) {
                return GS_ERROR;
            }
        }

        rcy_init_proc(session);
        if (lrpl_init(session) != GS_SUCCESS) {
            return GS_ERROR;
        }
    }

    if (DB_IS_PRIMARY(db) || DB_IS_PHYSICAL_STANDBY(db)) {
        if (lsnd_init(session) != GS_SUCCESS) {
            return GS_ERROR;
        }
    }
    GS_LOG_RUN_INF("[DB]: db switchover proc init finish.");
    return GS_SUCCESS;
}
#endif
/**
 * When redo log is corrupted and database cannot recover, we can use recover database until cancel
 * Then need open database with resetlogs or force ignore logs
 * If force ignore logs and current point less than consistent point, database can also open
 * modified log:
 * date      author      desc
 * 20241025  吴锦锋      原函数为static,无法对外部文件提供服务，因忽略损坏日志修复需要在knl_recovery.c中使用，故对其开放
 */
status_t db_reset_ignore_log(knl_session_t *session, bool32 resetlogs, bool32 force_ignore_logs)
{
    database_t *db = &session->kernel->db;
    rcy_context_t *rcy = &session->kernel->rcy_ctx;
    log_point_t lrp_point = db->ctrl.core.lrp_point;
    uint64 consistent_lfn = db->ctrl.core.consistent_lfn;

    if (!resetlogs && !force_ignore_logs && RCY_IGNORE_CORRUPTED_LOG(rcy)) {
        GS_THROW_ERROR(ERR_CANNOT_OPEN_DATABASE, "after recover until cancel",
            "open resetlog or open force ignore log");
        return GS_ERROR;
    }

    if (consistent_lfn == 0) { // for database version downward compatibility
        consistent_lfn = lrp_point.lfn;
    }

    GS_LOG_RUN_INF("[DB] db is recover until cancel, rcy point lfn %llu, consistent point %llu",
        (uint64)db->ctrl.core.rcy_point.lfn, (uint64)consistent_lfn);

    if (db->ctrl.core.rcy_point.lfn < consistent_lfn) {
        if (force_ignore_logs) {
            session->kernel->db.ctrl.core.open_inconsistency = GS_TRUE;
            GS_LOG_RUN_WAR("[DB] force ignore redo log, database may not be in consistency");
        } else {
            GS_THROW_ERROR(ERR_OPEN_RESETLOGS, db->ctrl.core.rcy_point.lfn, consistent_lfn);
            return GS_ERROR;
        }
    }

    db_reset_log(session, GS_INVALID_ASN, GS_TRUE, GS_TRUE);

    if (db_save_core_ctrl(session) != GS_SUCCESS) {
        CM_ABORT(0, "[DB] ABORT INFO: save core control file failed when reset log.");
    }

    return GS_SUCCESS;
}

static status_t db_mode_set(knl_session_t *session, db_open_opt_t *options)
{
    knl_instance_t *kernel = (knl_instance_t *)session->kernel;
    rcy_context_t *rcy = &session->kernel->rcy_ctx;
    database_t *db = &kernel->db;

    if (options->open_status >= DB_OPEN_STATUS_RESTRICT) {
        if (session->uid != DB_SYS_USER_ID) {
            GS_THROW_ERROR(ERR_OPERATIONS_NOT_SUPPORT, "of opening for restrict or upgrade mode", "non-system user");
            return GS_ERROR;
        }
        db->open_status = options->open_status;
    }
    db->terminate_lfn = options->lfn;
    db->has_load_role = GS_FALSE;

    if (options->readonly || !DB_IS_PRIMARY(db)) {
        db->is_readonly = GS_TRUE;
        db->readonly_reason = DB_IS_PRIMARY(db) ? MANUALLY_SET : PHYSICAL_STANDBY_SET;
        tx_rollback_close(session);
    }

    if (options->resetlogs && !RCY_IGNORE_CORRUPTED_LOG(rcy)) {
        if (!LOG_POINT_LFN_EQUAL(&db->ctrl.core.rcy_point, &db->ctrl.core.lrp_point)) {
            GS_THROW_ERROR(ERR_OPEN_RESETLOGS, db->ctrl.core.rcy_point.lfn, db->ctrl.core.lrp_point.lfn);
            return GS_ERROR;
        }

        db_reset_log(session, GS_INVALID_ASN, GS_TRUE, GS_TRUE);

        if (db_save_core_ctrl(session) != GS_SUCCESS) {
            CM_ABORT(0, "[DB] ABORT INFO: save core control file failed when reset log.");
        }
    }

    if (RCY_IGNORE_CORRUPTED_LOG(rcy)) {
        if (db_reset_ignore_log(session, options->resetlogs, options->ignore_logs) != GS_SUCCESS) {
            return GS_ERROR;
        }
    }

    if(session->kernel->attr.enforced_ignore_all_redo_logs){
        rcy->action = RECOVER_UNTIL_CANCEL;
        if (db_reset_ignore_log(session, GS_TRUE, GS_TRUE) != GS_SUCCESS) {
            return GS_ERROR;
        }
    }
    rcy->action = RECOVER_NORMAL;
    return GS_SUCCESS;
}

status_t db_callback_function(knl_session_t *session)
{
    if (g_knl_callback.pl_init(session) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (g_knl_callback.init_shard_resource(session) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (g_knl_callback.init_sql_maps(session) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (g_knl_callback.init_resmgr(session) != GS_SUCCESS) {
        return GS_ERROR;
    }

    return GS_SUCCESS;
}

static status_t db_mount_to_recovery(knl_session_t *session, db_open_opt_t *options, bool32 *has_offline)
{
    core_ctrl_t *core_ctrl = DB_CORE_CTRL(session);
    bool32 is_upgrade = (options->open_status >= DB_OPEN_STATUS_UPGRADE) ? GS_TRUE : GS_FALSE;

    if (log_load(session) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (log_check_asn(session, options->ignore_logs) != GS_SUCCESS) {
        GS_THROW_ERROR(ERR_INVALID_OPERATION, ", check log asn failed when open database");
        return GS_ERROR;
    }

    if (!db_sysdata_version_is_equal(session, is_upgrade)) {
        return GS_ERROR;
    }

    if (db_mode_set(session, options) != GS_SUCCESS) {
        return GS_ERROR;
    }
#ifdef _REPLICATION
    if (raft_load(session) != GS_SUCCESS) {
        return GS_ERROR;
    }
#endif
    if (db_load_tablespaces(session, has_offline) != GS_SUCCESS) {
        return GS_ERROR;
    }

    db_load_systables(session);

    undo_init(session, 0, core_ctrl->undo_segments);

    if (lock_area_init(session) != GS_SUCCESS) {
        return GS_ERROR;
    }

    btree_area_init(session);

    lob_area_init(session);

    if (tx_area_init(session, 0, core_ctrl->undo_segments) != GS_SUCCESS) {
        return GS_ERROR;
    }

    ckpt_load(session);

    if (arch_start(session) != GS_SUCCESS) {
        return GS_ERROR;
    }

    return GS_SUCCESS;
}

static status_t db_check_terminate_lfn(knl_session_t *session, uint64 lfn)
{
    database_t *db = &session->kernel->db;

    if (lfn == GS_INVALID_LFN) {
        return GS_SUCCESS;
    }

    if (DB_IS_PRIMARY(db)) {
        GS_LOG_RUN_ERR("[UPGRADE] The operation entering terminated lfn for primary role was not allowed");
        GS_THROW_ERROR(ERR_OPERATIONS_NOT_ALLOW, "entering terminated lfn for primary role");
        return GS_ERROR;
    }

    if (lfn < (uint64)db->ctrl.core.lrp_point.lfn) {
        GS_LOG_RUN_ERR("[UPGRADE] terminated lfn [%llu] is less than lrp point's lfn [%llu]", lfn,
            (uint64)db->ctrl.core.lrp_point.lfn);
        GS_THROW_ERROR(ERR_OPERATIONS_NOT_ALLOW, "entering terminated lfn less than lrp point");
        return GS_ERROR;
    }

    return GS_SUCCESS;
}

static status_t db_recovery_to_initphase2(knl_session_t *session, bool32 has_offline)
{
    knl_instance_t *kernel = (knl_instance_t *)session->kernel;
    database_t *db = &kernel->db;

    if (db->ctrl.core.shutdown_consistency) {
        GS_LOG_RUN_INF("The last shutdown is consistent");
#ifdef _REPLICATION
        if (!DB_IS_RAFT_ENABLED(kernel)) {
            knl_panic(log_cmp_point(&db->ctrl.core.rcy_point, &db->ctrl.core.lrp_point) == 0 &&
                LOG_POINT_LFN_EQUAL(&db->ctrl.core.rcy_point, &db->ctrl.core.lrp_point));
        }
#else
        knl_panic(log_cmp_point(&db->ctrl.core.rcy_point, &db->ctrl.core.lrp_point) == 0 &&
            LOG_POINT_LFN_EQUAL(&db->ctrl.core.rcy_point, &db->ctrl.core.lrp_point));
#endif
    } else {
        GS_LOG_RUN_INF("The last shutdown is inconsistent");
    }

    if (rcy_recover(session) != GS_SUCCESS) {
        session->kernel->rcy_ctx.is_working = GS_FALSE;
        return GS_ERROR;
    }
#ifdef _REPLICATION
    if (kernel->lrcv_ctx.is_building) {
#ifdef _BACKUP
        if (rst_truncate_datafile(session) != GS_SUCCESS) {
            return GS_ERROR;
        }
#endif
    }
#endif
    ckpt_trigger(session, has_offline, CKPT_TRIGGER_FULL);
    db->ctrl.core.shutdown_consistency = GS_FALSE;
    if (db_save_core_ctrl(session) != GS_SUCCESS) {
        CM_ABORT(0, "[DB] ABORT INFO: save core control file failed after ckpt is completed");
    }
    tx_area_release(session);

    if (DB_IS_PRIMARY(db)) {
        if (spc_clean_garbage_space(session) != GS_SUCCESS) {
            GS_LOG_RUN_WAR("[SPACE] failed to clean garbage tablespace");
        }
    }

    return GS_SUCCESS;
}

static status_t db_initphase2_to_open(knl_session_t *session)
{
    repl_role_t old_role = REPL_ROLE_PRIMARY;
    knl_instance_t *kernel = (knl_instance_t *)session->kernel;
    database_t *db = &kernel->db;

    if (!DB_IS_MAINTENANCE(session) && !DB_IS_READONLY(session)) {
        undo_shrink_inactive_segments(session);
    }

    db_garbage_segment_init(session);
    if (dc_init(session) != GS_SUCCESS) {
        return GS_ERROR;
    }
    GS_LOG_RUN_INF("[DB]: dc init finish.");

    if (!DB_IS_UPGRADE(session)) {
        if (db_callback_function(session) != GS_SUCCESS) {
            return GS_ERROR;
        }
    }

    db->ctrl.core.open_count++;
    if (db_save_core_ctrl(session) != GS_SUCCESS) {
        CM_ABORT(0, "[DB] ABORT INFO: failed to save core control file when open database");
    }

#ifdef _REPLICATION
    if (db_switchover_proc_init(session) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (raft_db_start_follower(session, old_role) != GS_SUCCESS) {
        GS_LOG_RUN_ERR("RAFT: db start follower failed.");
        return GS_ERROR;
    }
#endif
    return GS_SUCCESS;
}

status_t db_open_precheck(knl_session_t *session, db_open_opt_t *options)
{
    knl_instance_t *kernel = (knl_instance_t *)session->kernel;
    database_t *db = &kernel->db;

    if (!options->is_creating && !db->ctrl.core.build_completed) {
        GS_THROW_ERROR(ERR_DATABASE_NOT_COMPLETED);
        return GS_ERROR;
    }

    if (db_check_terminate_lfn(session, options->lfn) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (db_verify_systime(session, options->ignore_systime) != GS_SUCCESS) {
        return GS_ERROR;
    }
    return GS_SUCCESS;
}

static status_t db_open_check_nolog(knl_session_t *session, db_open_opt_t *options)
{
    bool32 has_nolog = GS_FALSE;
    knl_instance_t *kernel = (knl_instance_t *)session->kernel;

    if (!DB_IS_PRIMARY(&kernel->db)) {
        return GS_SUCCESS;
    }

    if (options->open_status >= DB_OPEN_STATUS_RESTRICT || options->is_creating) {
        return GS_SUCCESS;
    }

    if (knl_database_has_nolog_object(session, &has_nolog) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (!has_nolog) {
        return GS_SUCCESS;
    }
#ifdef _REPLICATION
    if (!DB_IS_SINGLE(session)) {
        GS_LOG_RUN_ERR("[DB] can not open database in HA mode with nolog object exists.");
        return GS_ERROR;
    }
#endif
    if (kernel->db.ctrl.core.lrep_mode == LOG_REPLICATION_ON) {
        GS_LOG_RUN_ERR("[DB] can not open database with replication on while nolog object exists.");
        return GS_ERROR;
    }

    if (kernel->attr.rcy_check_pcn) {
        GS_LOG_RUN_ERR("[DB] can not open database with nolog object while rcy_check_pcn is true");
        return GS_ERROR;
    }

    return GS_SUCCESS;
}

status_t db_open(knl_session_t *session, db_open_opt_t *options)
{
    knl_instance_t *kernel = (knl_instance_t *)session->kernel;
    database_t *db = &kernel->db;
    bool32 has_offline = GS_FALSE;

    GS_LOG_RUN_INF("[DB OPEN] start to alter database OPEN");
    if (!cm_spin_try_lock(&kernel->lock)) {
        GS_THROW_ERROR(ERR_DB_START_IN_PROGRESS);
        return GS_ERROR;
    }

    if (db_open_precheck(session, options) != GS_SUCCESS) {
        cm_spin_unlock(&kernel->lock);
        return GS_ERROR;
    }

    if (db_mount_to_recovery(session, options, &has_offline) != GS_SUCCESS) {
        cm_spin_unlock(&kernel->lock);
        return GS_ERROR;
    }

    db->status = DB_STATUS_RECOVERY;
    GS_LOG_RUN_INF("[DB OPEN] db status is RECOVRY.");
    if (db_recovery_to_initphase2(session, has_offline) != GS_SUCCESS) {
        cm_spin_unlock(&kernel->lock);
        return GS_ERROR;
    }

    // temp space may extra replay create datafile after rebuild
    spc_init_swap_space(session, SPACE_GET(db->ctrl.core.swap_space));

    if (ctrl_backup_ctrl_info(session) != GS_SUCCESS) {
        cm_spin_unlock(&kernel->lock);
        return GS_ERROR;
    }
    db->status = DB_STATUS_INIT_PHASE2;
    GS_LOG_RUN_INF("[DB OPEN] db status is INIT PHASE2.");

    if (db_initphase2_to_open(session) != GS_SUCCESS) {
        cm_spin_unlock(&kernel->lock);
        return GS_ERROR;
    }

    if (db_open_check_nolog(session, options) != GS_SUCCESS) {
        CM_ABORT(0, "[DB] ABORT INFO: The database cannot be opened because of the nolog object.");
    }

    cm_spin_unlock(&kernel->lock);
    db->status = DB_STATUS_OPEN;

    if (DB_IS_PRIMARY(db) && db->ctrl.core.is_restored) {
        db_set_ctrl_restored(session, GS_FALSE);
    }
    GS_LOG_RUN_INF("[DB OPEN] db status is OPEN.");

    GS_LOG_RUN_INF("[DB OPEN] sse42 available %d", cm_crc32c_sse42_available());
    GS_LOG_RUN_INF("[DB OPEN] successfully alter database OPEN, running as %s role", db_get_role(db));
    return GS_SUCCESS;
}

status_t db_recover(knl_session_t *session, knl_scn_t max_recover_scn)
{
    knl_instance_t *kernel = (knl_instance_t *)session->kernel;
    database_t *db = &kernel->db;
    bool32 has_offline = GS_FALSE;

    if (!cm_spin_try_lock(&kernel->lock)) {
        GS_THROW_ERROR(ERR_DB_START_IN_PROGRESS);
        return GS_ERROR;
    }

    if (db_load_tablespaces(session, &has_offline) != GS_SUCCESS) {
        cm_spin_unlock(&kernel->lock);
        return GS_ERROR;
    }

    if (log_load(session) != GS_SUCCESS) {
        cm_spin_unlock(&kernel->lock);
        return GS_ERROR;
    }

    ckpt_load(session);

    db->status = DB_STATUS_RECOVERY;
    session->kernel->rcy_ctx.max_scn = max_recover_scn;
    if (rcy_recover(session) != GS_SUCCESS) {
        session->kernel->rcy_ctx.is_working = GS_FALSE;
        cm_spin_unlock(&kernel->lock);
        return GS_ERROR;
    }

    session->kernel->rcy_ctx.max_scn = GS_INVALID_ID64;
    ckpt_trigger(session, GS_TRUE, CKPT_TRIGGER_FULL);
    db->ctrl.core.build_completed = GS_TRUE;

    if (db_save_core_ctrl(session) != GS_SUCCESS) {
        cm_spin_unlock(&kernel->lock);
        return GS_ERROR;
    }
    db->status = DB_STATUS_MOUNT;
    cm_spin_unlock(&kernel->lock);
    return GS_SUCCESS;
}

static bool32 db_build_need_retry(knl_session_t *session, status_t status)
{
#ifdef BACKUP_
    bak_t *bak = &session->kernel->backup_ctx.bak;
    int32 err_code;
    const char *error_msg = NULL;

    if (status == GS_SUCCESS) {
        return GS_FALSE;
    }

    bak_get_error(session, &err_code, &error_msg);
    GS_LOG_DEBUG_ERR("build failed, error code:%d, msg:%s", err_code, error_msg);
    if (err_code == ERR_BUILD_CANCELLED) {
        return GS_FALSE;
    }
    if (err_code == ERR_BACKUP_IN_PROGRESS) {
        bak->build_retry_time = 0;
        return GS_TRUE;
    }
    if (bak->need_retry) {
        return GS_TRUE;
    }
#endif
    return GS_FALSE;
}

static void db_build_set_role(database_t *db, build_type_t build_type)
{
    if (build_type == BUILD_AUTO) {
        if (!DB_IS_CASCADED_PHYSICAL_STANDBY(db)) {
            db->ctrl.core.db_role = REPL_ROLE_PHYSICAL_STANDBY;
        }
    } else if (build_type == BUILD_STANDBY) {
        db->ctrl.core.db_role = REPL_ROLE_PHYSICAL_STANDBY;
    } else {
        db->ctrl.core.db_role = REPL_ROLE_CASCADED_PHYSICAL_STANDBY;
    }
}
#ifdef _REPLICATION
static bool32 db_build_check_peer_role(knl_session_t *session, build_type_t build_type)
{
    lrcv_context_t *lrcv = &session->kernel->lrcv_ctx;
    uint32 retry_count = GS_BACKUP_RETRY_COUNT;

    while (lrcv->peer_role == PEER_UNKNOWN) {
        if (retry_count == 0) {
            GS_THROW_ERROR(ERR_PRI_NOT_CONNECT, "peer role");
            return GS_FALSE;
        }

        cm_sleep(5);

        if (retry_count > 0) {
            retry_count--;
        }
    }

    if (lrcv->peer_role == PEER_STANDBY && build_type == BUILD_STANDBY) {
        GS_THROW_ERROR(ERR_INVALID_OPERATION, ",build standby not connect to primary database");
        return GS_FALSE;
    }

    return GS_TRUE;
}
#endif
static status_t db_build_restore_precheck(knl_session_t *session, knl_build_def_t *def)
{
    knl_instance_t *kernel = (knl_instance_t *)session->kernel;
    database_t *db = &kernel->db;
#ifdef _REPLICATION
    if (DB_IS_RAFT_ENABLED(session->kernel)) {
        GS_THROW_ERROR(ERR_INVALID_OPERATION,
            ",RAFT: build not supported when raft is enabled, please use builddb.sh instead.");
        return GS_ERROR;
    }
#endif
    if (def->param_ctrl.is_increment || def->param_ctrl.is_repair) {
        if (db->status != DB_STATUS_MOUNT) {
            GS_THROW_ERROR(ERR_INVALID_OPERATION, ",increment or repair build can only be executed on MOUNT status");
            return GS_ERROR;
        }

        if (!session->kernel->db.ctrl.core.build_completed) {
            GS_THROW_ERROR(ERR_DATABASE_NOT_COMPLETED);
            return GS_ERROR;
        }
    } else {
        if (db->status != DB_STATUS_NOMOUNT) {
            GS_THROW_ERROR(ERR_INVALID_OPERATION, ",operation can only be executed on NOMOUNT status");
            return GS_ERROR;
        }
    }

    if (def->param_ctrl.is_repair) {
        if (DB_IS_PRIMARY(db)) {
            GS_THROW_ERROR(ERR_INVALID_OPERATION, ",repair build can not be executed on primary");
            return GS_ERROR;
        }

#ifdef _BACKUP
        if (!knl_brain_repair_check(session)) {
            return GS_ERROR;
        }
#endif
    }
#ifdef _REPLICATION
    if (!db_build_check_peer_role(session, def->build_type)) {
        return GS_ERROR;
    }
#endif
    return GS_SUCCESS;
}
#ifdef _REPLICATION
static status_t db_build_restore(knl_session_t *session, knl_build_def_t *def)
{
    knl_instance_t *kernel = (knl_instance_t *)session->kernel;
    database_t *db = &kernel->db;
    status_t status = GS_ERROR;
    bak_t *bak = &kernel->backup_ctx.bak;

    if (db_build_restore_precheck(session, def) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (def->build_type != BUILD_AUTO) {
        session->kernel->lrcv_ctx.role_spec_building = GS_TRUE;
    }

    kernel->lrcv_ctx.is_building = GS_TRUE;
    db->status = def->param_ctrl.is_increment || def->param_ctrl.is_repair ? db->status : DB_STATUS_CREATING;
    db->ctrl.core.db_role = REPL_ROLE_PHYSICAL_STANDBY;
    bak->build_retry_time = BAK_BUILD_INIT_RETRY_TIME;
    def->param_ctrl.base_lsn = GS_INVALID_LSN;

#ifdef _BACKUP
    do {
        cm_reset_error();
        status = bak_build_restore(session, &def->param_ctrl);
        cm_sleep(1000);
    } while (db_build_need_retry(session, status));
#endif

    if (status != GS_SUCCESS) {
        db->status = DB_STATUS_NOMOUNT;
        kernel->lrcv_ctx.is_building = GS_FALSE;
        GS_LOG_RUN_ERR("[DB] failed to backup database");
        return GS_ERROR;
    }

    return GS_SUCCESS;
}

status_t db_build_baseline(knl_session_t *session, knl_build_def_t *def)
{
    knl_instance_t *kernel = (knl_instance_t *)session->kernel;
    database_t *db = &kernel->db;
    bool32 no_wait = GS_FALSE;
    errno_t err;

    if (db_build_restore(session, def) != GS_SUCCESS) {
        return GS_ERROR;
    }

    GS_LOG_RUN_INF("[DB] database backup successfully");

    db->ctrl.core.build_completed = GS_TRUE;
    err = memset_sp(db->ctrl.core.archived_log, sizeof(arch_log_id_t) * GS_MAX_ARCH_DEST, 0,
        sizeof(arch_log_id_t) * GS_MAX_ARCH_DEST);
    knl_securec_check(err);

    db_build_set_role(db, def->build_type);

    if (db_save_core_ctrl(session) != GS_SUCCESS) {
        CM_ABORT(0, "[DB] ABORT INFO: failed to save core control file when build baseline");
    }

    if (!def->param_ctrl.is_increment && !def->param_ctrl.is_repair) {
        if (db_mount(session) != GS_SUCCESS) {
            kernel->lrcv_ctx.is_building = GS_FALSE;
            return GS_ERROR;
        }
    }

    kernel->lrcv_ctx.reconnected = GS_FALSE;

    /*
     * If one node connects to primary and wants to build a cascaded standby, the primary will disconnect
     * with it after build successfully. So in this scenario, there is no need to wait log receiver to become prepared.
     */
    no_wait = (bool32)(DB_IS_CASCADED_PHYSICAL_STANDBY(db) && kernel->lrcv_ctx.role_spec_building &&
        kernel->lrcv_ctx.peer_role == PEER_PRIMARY);

    db_open_opt_t open_options = { GS_FALSE,      GS_FALSE, GS_FALSE, GS_FALSE, GS_TRUE, DB_OPEN_STATUS_NORMAL,
                                   GS_INVALID_LFN };
    if (db_open(session, &open_options) != GS_SUCCESS) {
        kernel->lrcv_ctx.is_building = GS_FALSE;
        return GS_ERROR;
    }
#ifdef _REPLICATION
    if (!no_wait) {
        lrcv_wait_status_prepared(session);
    }
#endif
    kernel->lrcv_ctx.is_building = GS_FALSE;
    GS_LOG_RUN_INF("database build successfully");
    return GS_SUCCESS;
}
#endif
static char *db_get_role(database_t *db)
{
    if (DB_IS_PRIMARY(db)) {
        return "primary";
    } else if (DB_IS_PHYSICAL_STANDBY(db)) {
        return "physical standby";
    } else if (DB_IS_CASCADED_PHYSICAL_STANDBY(db)) {
        return "cascaded physical standby";
    } else {
        return "unknown";
    }
}

char *db_get_switchover_status(knl_session_t *session)
{
    knl_instance_t *kernel = (knl_instance_t *)session->kernel;

    if (DB_IS_PHYSICAL_STANDBY(&kernel->db)) {
        return "TO PRIMARY";
    } else {
        return "NOT ALLOWED";
    }
}

char *db_get_failover_status(knl_session_t *session)
{
    knl_instance_t *kernel = (knl_instance_t *)session->kernel;

    if (!DB_IS_PRIMARY(&kernel->db)) {
        return "TO PRIMARY";
    } else {
        return "NOT ALLOWED";
    }
}
#ifdef _REPLICATION
char *db_get_condition(knl_session_t *session)
{
    knl_instance_t *kernel = (knl_instance_t *)session->kernel;
    switch_ctrl_t *ctrl = &kernel->switch_ctrl;
    lrcv_context_t *lrcv = &kernel->lrcv_ctx;
    lftc_clt_ctx_t *lftc_clt = &kernel->lftc_client_ctx;

    switch (ctrl->request) {
        case SWITCH_REQ_DEMOTE:
            return "DEMOTING";
        case SWITCH_REQ_PROMOTE:
            return "PROMOTING";
        case SWITCH_REQ_FAILOVER_PROMOTE:
        case SWITCH_REQ_FORCE_FAILOVER_PROMOTE:
            return "FAILOVER PROMOTING";
        case SWITCH_REQ_RAFT_PROMOTE:
            return "RAFT FAILOVER";
        default:
            break;
    }
#ifdef _REPLICATION
    // for other scenarios, just return normal in primary role
    if (DB_IS_PRIMARY(&kernel->db)) {
        return "NORMAL";
    }
#endif
    // for other scenarios, check log receiver in standby role
#ifdef _REPLICATION
    // if (lrcv->status == LRCV_NEED_REPAIR || lftc_clt->arch_lost
    //     || kernel->rcy_ctx.log_decrypt_failed || lrpl_replay_blocked(session)) {
#endif
    if (lrcv->status == LRCV_NEED_REPAIR || lftc_clt->arch_lost || kernel->rcy_ctx.log_decrypt_failed) {
        return "NEED REPAIR";
    } else if (lrcv->state == REP_STATE_WAITING_DEMOTE) {
        return "WAIT TO PROMOTE";
    } else if (lrcv->status < LRCV_PREPARE) {
        return "DISCONNECTED";
    } else {
        return "NORMAL";
    }
}
#endif
char *db_get_needrepair_reason(knl_session_t *session)
{
    knl_instance_t *kernel = (knl_instance_t *)session->kernel;
#ifdef _REPLICATION
    lrcv_context_t *lrcv = &kernel->lrcv_ctx;
    lftc_clt_ctx_t *lftc_clt = &kernel->lftc_client_ctx;
    if (DB_IS_PRIMARY(&kernel->db)) {
        return "NONE";
    }

    if (lrpl_replay_blocked(session)) {
        return "REPLAY BLOCKED";
    }

    if (lftc_clt->arch_lost) {
        return "ARCHIVE LOG LOST";
    }
#endif
    if (kernel->rcy_ctx.log_decrypt_failed) {
        return "LOG DECRYPT FAILED";
    }
#ifdef _REPLICATION
    if (lrcv->status == LRCV_NEED_REPAIR) {
        return "OTHER REASONS";
    }
#endif
    return "NONE";
}

char *db_get_readonly_reason(knl_session_t *session)
{
    database_t *db = &session->kernel->db;

    switch (db->readonly_reason) {
        case PRIMARY_SET:
            return "NONE";
        case PHYSICAL_STANDBY_SET:
            return "PHYSICAL STANDBY";
        case MANUALLY_SET:
            return "MANUALLY SET";
        case RMON_SET:
            return "RMON SET";
        default:
            return "OTHER REASONS";
    }
}

char *db_get_status(knl_session_t *session)
{
    knl_instance_t *kernel = (knl_instance_t *)session->kernel;
    database_t *db = &kernel->db;

    switch (db->status) {
        case DB_STATUS_CLOSED:
            return "CLOSED";
        case DB_STATUS_NOMOUNT:
            return "NOMOUNT";
        case DB_STATUS_CREATING:
            return "CREATING";
        case DB_STATUS_MOUNT:
            return "MOUNT";
        case DB_STATUS_RECOVERY:
            return "RECOVERY";
        case DB_STATUS_INIT_PHASE2:
            return "INIT PHASE2";
        case DB_STATUS_WAIT_CLEAN:
            return "WAIT CLEAN";
        case DB_STATUS_OPEN:
            return "OPEN";
        default:
            GS_LOG_RUN_ERR("[DB] unexpected database status %d", db->status);
            return "INVALID STATUS";
    }
}

uint64 knl_current_scn(knl_handle_t session)
{
    return (uint64)DB_CURR_SCN((knl_session_t *)session);
}

uint64 knl_next_scn(knl_handle_t session)
{
    return (uint64)db_next_scn((knl_session_t *)session);
}

time_t knl_init_time(knl_handle_t session)
{
    return DB_INIT_TIME((knl_session_t *)session);
}

/*
 * reset logfile to a new time line, we set update rst_id of the
 * current logfile, skip recovery if requested.
 * @param kernel session, reset recover, reset archive
 */
void db_reset_log(knl_session_t *session, uint32 switch_asn, bool32 reset_recover, bool32 reset_archive)
{
    database_t *db = &session->kernel->db;
    core_ctrl_t *core = &db->ctrl.core;
    reset_log_t *reset_log = &core->resetlogs;
    log_context_t *log = &session->kernel->redo_ctx;
    log_file_t *logfile = &log->files[log->curr_file];
    uint32 last_asn;
    errno_t err;

    last_asn = (switch_asn != GS_INVALID_ASN) ? (switch_asn - 1) : (logfile->head.asn - 1);
    GS_LOG_RUN_INF("reset log from rst_id %u asn %u lfn %llu, reset_log_scn %llu, curr_scn %llu",
        log->curr_point.rst_id, last_asn, (uint64)log->curr_point.lfn, core->reset_log_scn, log->curr_scn);

    reset_log->last_asn = last_asn;
    reset_log->last_lfn = log->curr_point.lfn;
    reset_log->rst_id = (uint32)log->curr_point.rst_id + 1;
    if (!GS_INVALID_SCN(log->curr_scn)) {
        core->reset_log_scn = log->curr_scn;
    }

    logfile->head.rst_id = reset_log->rst_id;
    log_flush_head(session, logfile);

    if (reset_recover) {
        log_point_t point;

        point.asn = logfile->head.asn;
        /*
         * write_pos is calcaulated by CM_CALC_ALIGN use sizeof(log_file_head_t) and block_size,
         * block_size is 512 or 4096, so value is smaller than uint32.
         */
        point.block_id = (uint32)(logfile->head.write_pos / (uint32)logfile->head.block_size);
        point.rst_id = logfile->head.rst_id;
        point.lfn = core->rcy_point.lfn;

        ckpt_reset_point(session, &point);
    }

    if (reset_archive) {
        err = memset_sp(core->archived_log, sizeof(arch_log_id_t) * GS_MAX_ARCH_DEST, 0,
            sizeof(arch_log_id_t) * GS_MAX_ARCH_DEST);
        knl_securec_check(err);
    }

    if (ctrl_backup_reset_logs(session) != GS_SUCCESS) {
        CM_ABORT(0, "[DB] ABORT INFO: Failed to backup reset logs when reset logs");
    }
}

status_t db_change_storage_path(file_convert_t *convert, char *name, uint32 name_size)
{
    file_name_convert_t *list = NULL;
    text_t left;
    text_t right;
    text_t text;
    char right_str[GS_FILE_NAME_BUFFER_SIZE];
    uint32 i;
    errno_t err;

    if (!convert->is_convert) {
        return GS_SUCCESS;
    }

    cm_str2text(name, &text);
    (void)cm_split_rtext(&text, SLASH, '\0', &left, &right);
    cm_delete_text_end_slash(&left);
    (void)cm_text2str(&right, right_str, GS_FILE_NAME_BUFFER_SIZE);

    for (i = 0; i < convert->count; i++) {
        list = &convert->convert_list[i];
        if (cm_check_exist_special_char(list->primry_path, (uint32)strlen(list->primry_path))) {
            GS_THROW_ERROR(ERR_INVALID_DIR, list->primry_path);
            return GS_ERROR;
        }
        if (cm_check_exist_special_char(list->standby_path, (uint32)strlen(list->standby_path))) {
            GS_THROW_ERROR(ERR_INVALID_DIR, list->standby_path);
            return GS_ERROR;
        }
    }

    for (i = 0; i < convert->count; i++) {
        list = &convert->convert_list[i];
        cm_try_delete_end_slash(list->primry_path);
        if (cm_text_str_equal_ins(&left, list->primry_path)) {
            cm_try_delete_end_slash(list->standby_path);
            err = snprintf_s(name, name_size, name_size - 1, "%s/%s", list->standby_path, right_str);
            knl_securec_check_ss(err);

            return GS_SUCCESS;
        }
    }

    return GS_SUCCESS;
}

uint64 db_get_datafiles_used_size(knl_session_t *session)
{
    space_t *space = NULL;
    uint64 total_pages = 0;
    uint32 i;

    for (i = 0; i < GS_MAX_SPACES; i++) {
        space = SPACE_GET(i);
        if (!SPACE_IS_ONLINE(space) || !space->ctrl->used) {
            continue;
        }

        if (IS_SWAP_SPACE(space)) {
            total_pages++;
        } else {
            total_pages += spc_count_backup_pages(session, space);
        }
    }

    return total_pages * DEFAULT_PAGE_SIZE;
}

uint64 db_get_datafiles_size(knl_session_t *session)
{
    datafile_ctrl_t *ctrl = NULL;
    uint64 total_size = 0;
    uint32 i;

    for (i = 0; i < GS_MAX_DATA_FILES; i++) {
        ctrl = session->kernel->db.datafiles[i].ctrl;
        if (!ctrl->used) {
            continue;
        }

        total_size += (uint64)ctrl->size;
    }

    return total_size;
}

uint64 db_get_logfiles_size(knl_session_t *session)
{
    log_file_ctrl_t *ctrl = NULL;
    uint64 total_size = 0;
    uint32 i;

    for (i = 0; i < session->kernel->db.ctrl.core.log_hwm; i++) {
        ctrl = session->kernel->db.logfiles.items[i].ctrl;
        if (LOG_IS_DROPPED(ctrl->flg)) {
            continue;
        }

        total_size += (uint64)ctrl->size;
    }

    return total_size;
}

/*
 * clean nologging data, called when:
 * 1. db restart, no matter restart as primary or standby;
 * 2. db failover/switchover;
 */
static void db_clean_nologging_data(knl_session_t *session)
{
#ifdef _STATISTICS
    stats_clean_nologging_stats(session);
#endif
    GS_LOG_RUN_INF("[DB] Clean nologging data start.");

    /* 1. make in-memory nologging dc as empty */
    dc_invalidate_nologging(session);

    /* 2. reinit undo->temp_free_page_list before reset tablepsace */
    temp2_undo_init(session);

    /* 3. make on-disk nologging space as empty */
    spc_clean_nologging_data(session);
    GS_LOG_RUN_INF("[DB] Clean nologging data end.");
}

/*
 * drop nologging tables if needed, call when:
 * 1. db restart to primary;
 * 2. db failover/switchover;
 */
static status_t db_drop_nologging_table(knl_session_t *session)
{
    status_t status = GS_SUCCESS;

    if (session->kernel->attr.drop_nologging) {
        status = spc_drop_nologging_table(session);
        GS_LOG_RUN_INF("[DB] Drop nologging table status:%d", status);
    }

    return status;
}

status_t db_clean_nologging_all(knl_session_t *session)
{
    if (DB_IS_MAINTENANCE(session)) {
        return GS_SUCCESS;
    }

    GS_LOG_RUN_INF("[DB] Clean nologging tables start.");
    /* 1. clean nologging table data */
    db_clean_nologging_data(session);

    /* 2. drop nologging table if needed */
    if (db_drop_nologging_table(session) != GS_SUCCESS) {
        return GS_ERROR;
    }

    /* 3. make on-disk nologging space as empty again */
    spc_clean_nologging_data(session);
    GS_LOG_RUN_INF("[DB] Clean nologging tables end.");

    return GS_SUCCESS;
}

void db_convert_temp_path(knl_session_t *session, const char *path)
{
    knl_instance_t *kernel = session->kernel;
    datafile_ctrl_t *datafile = NULL;
    log_file_ctrl_t *logfile = NULL;
    errno_t err;
    uint32 i;

    for (i = 0; i < GS_MAX_DATA_FILES; i++) {
        datafile = kernel->db.datafiles[i].ctrl;
        err = snprintf_s(datafile->name, GS_FILE_NAME_BUFFER_SIZE, GS_FILE_NAME_BUFFER_SIZE - 1, "%s/%s_%d", path,
            "data", i);
        knl_securec_check_ss(err);
    }

    for (i = 0; i < kernel->db.ctrl.core.log_hwm; i++) {
        logfile = kernel->db.logfiles.items[i].ctrl;
        err = snprintf_s(logfile->name, GS_FILE_NAME_BUFFER_SIZE, GS_FILE_NAME_BUFFER_SIZE - 1, "%s/%s_%d", path, "log",
            i);
        knl_securec_check_ss(err);
    }
}

void db_set_with_switchctrl_lock(switch_ctrl_t *ctrl, volatile bool32 *working)
{
    if (!cm_spin_try_lock(&ctrl->lock)) {
        return;
    }

    if (ctrl->request != SWITCH_REQ_NONE) {
        cm_spin_unlock(&ctrl->lock);
        return;
    }

    *working = GS_TRUE;
    cm_spin_unlock(&ctrl->lock);
}

static void db_update_lobs_seg_scn(knl_session_t *session, knl_dictionary_t *dc)
{
    lob_t *lob = NULL;
    knl_column_t *column = NULL;
    lob_part_t *lob_part = NULL;
    lob_part_t *lob_subpart = NULL;
    table_part_t *table_part = NULL;
    dc_entity_t *entity = DC_ENTITY(dc);
    table_t *table = DC_TABLE(dc);

    for (uint32 i = 0; i < entity->column_count; i++) {
        column = dc_get_column(entity, i);
        if (!COLUMN_IS_LOB(column)) {
            continue;
        }

        lob = (lob_t *)column->lob;
        if (!IS_PART_TABLE(table)) {
            if (lob->lob_entity.segment != NULL) {
                lob->desc.seg_scn = LOB_SEGMENT(lob->lob_entity.entry, lob->lob_entity.segment)->seg_scn;
            }

            continue;
        }

        for (uint32 j = 0; j < table->part_table->desc.partcnt; j++) {
            table_part = TABLE_GET_PART(table, j);
            lob_part = LOB_GET_PART(lob, j);
            if (!IS_READY_PART(table_part) || lob_part == NULL) {
                continue;
            }

            if (!IS_PARENT_LOBPART(&lob_part->desc)) {
                if (lob_part->lob_entity.segment != NULL) {
                    lob_part->desc.seg_scn =
                        LOB_SEGMENT(lob_part->lob_entity.entry, lob_part->lob_entity.segment)->seg_scn;
                }

                continue;
            }

            for (uint32 k = 0; k < lob_part->desc.subpart_cnt; k++) {
                lob_subpart = PART_GET_SUBENTITY(lob->part_lob, lob_part->subparts[k]);
                if (lob_subpart == NULL) {
                    continue;
                }

                if (lob_subpart->lob_entity.segment != NULL) {
                    lob_subpart->desc.seg_scn =
                        LOB_SEGMENT(lob_subpart->lob_entity.entry, lob_subpart->lob_entity.segment)->seg_scn;
                }
            }
        }
    }
}

static void db_update_index_seg_scn(knl_session_t *session, table_t *table, index_t *index)
{
    index_part_t *index_part = NULL;
    table_part_t *table_part = NULL;
    index_part_t *index_subpart = NULL;

    if (!IS_PART_INDEX(index)) {
        if (index->btree.segment != NULL) {
            index->desc.seg_scn = BTREE_SEGMENT(index->btree.entry, index->btree.segment)->seg_scn;
        }

        return;
    }

    for (uint32 i = 0; i < index->part_index->desc.partcnt; i++) {
        table_part = TABLE_GET_PART(table, i);
        index_part = INDEX_GET_PART(index, i);
        if (!IS_READY_PART(table_part) || index_part == NULL) {
            continue;
        }

        if (!IS_PARENT_IDXPART(&index_part->desc)) {
            if (index_part->btree.segment != NULL) {
                index_part->desc.seg_scn = BTREE_SEGMENT(index_part->btree.entry, index_part->btree.segment)->seg_scn;
            }

            continue;
        }

        for (uint32 j = 0; j < index_part->desc.subpart_cnt; j++) {
            index_subpart = PART_GET_SUBENTITY(index->part_index, index_part->subparts[j]);
            if (index_subpart == NULL) {
                continue;
            }

            if (index_subpart->btree.segment != NULL) {
                index_subpart->desc.seg_scn =
                    BTREE_SEGMENT(index_subpart->btree.entry, index_subpart->btree.segment)->seg_scn;
            }
        }
    }
}

void db_update_seg_scn(knl_session_t *session, knl_dictionary_t *dc)
{
    index_t *index = NULL;
    table_part_t *table_part = NULL;
    table_part_t *table_subpart = NULL;
    dc_entity_t *entity = DC_ENTITY(dc);
    table_t *table = DC_TABLE(dc);

    if (entity->contain_lob) {
        db_update_lobs_seg_scn(session, dc);
    }

    if (table->desc.type == TABLE_TYPE_TRANS_TEMP || table->desc.type == TABLE_TYPE_SESSION_TEMP) {
        return;
    }

    for (uint32 i = 0; i < table->index_set.total_count; i++) {
        index = table->index_set.items[i];
        db_update_index_seg_scn(session, table, index);
    }

    if (!IS_PART_TABLE(table)) {
        table->desc.seg_scn = HEAP_SEGMENT(table->heap.entry, table->heap.segment)->seg_scn;
        return;
    }

    for (uint32 i = 0; i < table->part_table->desc.partcnt; i++) {
        table_part = TABLE_GET_PART(table, i);
        if (!IS_READY_PART(table_part)) {
            continue;
        }

        if (!IS_PARENT_TABPART(&table_part->desc)) {
            if (table_part->heap.segment != NULL) {
                table_part->desc.seg_scn = HEAP_SEGMENT(table_part->heap.entry, table_part->heap.segment)->seg_scn;
            }

            continue;
        }

        for (uint32 j = 0; j < table_part->desc.subpart_cnt; j++) {
            table_subpart = PART_GET_SUBENTITY(table->part_table, table_part->subparts[j]);
            if (table_subpart == NULL) {
                continue;
            }

            if (table_subpart->heap.segment != NULL) {
                table_subpart->desc.seg_scn =
                    HEAP_SEGMENT(table_subpart->heap.entry, table_subpart->heap.segment)->seg_scn;
            }
        }
    }
}

void db_segments_stats_record(knl_session_t *session, seg_stat_t temp_stat, seg_stat_t *seg_stat)
{
    seg_stat_t stat;

    stat.logic_reads = session->stat.buffer_gets - temp_stat.logic_reads;
    stat.physical_reads = session->stat.disk_reads - temp_stat.physical_reads;
    stat.physical_writes = session->stat.disk_writes - temp_stat.physical_writes;
    stat.buf_busy_waits = session->stat_page.misses - temp_stat.buf_busy_waits;

    if (stat.logic_reads == 0 && stat.physical_reads == 0 && stat.physical_writes == 0 && stat.buf_busy_waits == 0) {
        return;
    }

    seg_stat->logic_reads += stat.logic_reads;
    seg_stat->physical_reads += stat.physical_reads;
    seg_stat->physical_writes += stat.physical_writes;
    seg_stat->buf_busy_waits += stat.buf_busy_waits;
}

void db_segment_stats_init(knl_session_t *session, seg_stat_t *temp_stat)
{
    temp_stat->physical_reads = session->stat.disk_reads;
    temp_stat->physical_writes = session->stat.disk_writes;
    temp_stat->logic_reads = session->stat.buffer_gets;
    temp_stat->buf_busy_waits = session->stat_page.misses;
}

static void db_record_runing_job(knl_session_t *session, bool32 demote, bool32 sync)
{
    knl_instance_t *kernel = session->kernel;

#ifdef _STATISTICS
    if (kernel->stats_ctx.stats_gathering) {
        GS_LOG_RUN_INF("[DB] [%s] Stats gather is working", demote ? "SWITCHOVER" : "RAEDONLY");
    }
#endif

    if (kernel->smon_ctx.undo_shrinking) {
        GS_LOG_RUN_INF("[DB] [%s] Smon undo shrink is working", demote ? "SWITCHOVER" : "RAEDONLY");
    }

    if (kernel->index_ctx.recycle_ctx.is_working) {
        GS_LOG_RUN_INF("[DB] [%s] Index recycle is working", demote ? "SWITCHOVER" : "RAEDONLY");
    }

    if (sync) {
        GS_LOG_RUN_INF("[DB] [%s] Time sync is working", demote ? "SWITCHOVER" : "RAEDONLY");
    }

    if (kernel->rmon_ctx.working) {
        GS_LOG_RUN_INF("[DB] [%s] Rmon thread is working", demote ? "SWITCHOVER" : "RAEDONLY");
    }

    if (kernel->ashrink_ctx.working) {
        GS_LOG_RUN_INF("[DB] [%s] Ashrink thread is working", demote ? "SWITCHOVER" : "RAEDONLY");
    }
}

bool32 db_check_backgroud_blocked(knl_session_t *session, bool32 demote, bool32 sync)
{
    knl_instance_t *kernel = session->kernel;
    switch_ctrl_t *ctrl = &kernel->switch_ctrl;

#ifdef _STATISTICS
    if (!kernel->stats_ctx.stats_gathering && !kernel->smon_ctx.undo_shrinking &&
        !kernel->index_ctx.recycle_ctx.is_working && !sync && !kernel->rmon_ctx.working &&
        !kernel->ashrink_ctx.working) {
        ctrl->has_logged = GS_FALSE;
        ctrl->last_log_time = 0;
        return GS_FALSE;
    }
#endif

    if (ctrl->has_logged && (g_timer()->now - ctrl->last_log_time) / MICROSECS_PER_SECOND < BACKGROUD_LOG_INTERVAL) {
        return GS_TRUE;
    }

    db_record_runing_job(session, demote, sync);

    ctrl->has_logged = GS_TRUE;
    ctrl->last_log_time = g_timer()->now;

    return GS_TRUE;
}

static status_t dump_ctrl_space_item(cm_dump_t *dump, space_ctrl_t *space, uint32 space_no)
{
    cm_dump(dump, "\t#%-2u", space_no);
    cm_dump(dump, "\t%u", space->id);
    cm_dump(dump, "\t%u", (uint32)space->used);
    cm_dump(dump, "\t%-*s", (int)strlen(space->name), NULL_2_STR(space->name));
    cm_dump(dump, "\t%u", (uint32)space->flag);
    cm_dump(dump, "\t%u", (uint32)space->block_size);
    cm_dump(dump, "\t%u", space->extent_size);
    cm_dump(dump, "\t%u", space->file_hwm);
    cm_dump(dump, "\t%llu", space->org_scn);
    cm_dump(dump, "\t%u", (uint32)space->encrypt_version);
    cm_dump(dump, "\t%u", (uint32)space->cipher_reserve_size);
    cm_dump(dump, "\t%u", space->files[0]);
    for (uint32 j = 1; j < space->file_hwm; j++) {
        cm_dump(dump, ", %u", space->files[j]);
        CM_DUMP_WRITE_FILE(dump);
    }
    cm_dump(dump, "\n");
    CM_DUMP_WRITE_FILE(dump);

    return GS_SUCCESS;
}

static status_t dump_ctrl_datafile_item(cm_dump_t *dump, datafile_ctrl_t *datafile, uint32 datfile_no)
{
    cm_dump(dump, "\t#%-2u", datfile_no);
    cm_dump(dump, "\t%u", datafile->id);
    cm_dump(dump, "\t%u", (uint32)datafile->used);
    cm_dump(dump, "\t%-*s", (int)strlen(datafile->name), NULL_2_STR(datafile->name));
    cm_dump(dump, "\t%lld", datafile->size);
    cm_dump(dump, "\t%u", (uint32)datafile->block_size);
    cm_dump(dump, "\t%u", (uint32)datafile->flag);
    cm_dump(dump, "\t%u", (uint32)datafile->type);
    cm_dump(dump, "\t%lld", datafile->auto_extend_size);
    cm_dump(dump, "\t%lld\n", datafile->auto_extend_maxsize);
    CM_DUMP_WRITE_FILE(dump);

    return GS_SUCCESS;
}

static status_t dump_ctrl_arch_item(cm_dump_t *dump, arch_ctrl_t *arch_ctrl, uint32 arch_no)
{
    cm_dump(dump, "\t#%-2u", arch_no);
    cm_dump(dump, "\t%u", arch_ctrl->recid);
    cm_dump(dump, "\t%u", arch_ctrl->dest_id);
    cm_dump(dump, "\t%u", arch_ctrl->rst_id);
    cm_dump(dump, "\t%u", arch_ctrl->asn);
    cm_dump(dump, "\t%llu", arch_ctrl->stamp);
    cm_dump(dump, "\t%u", arch_ctrl->blocks);
    cm_dump(dump, "\t%u", arch_ctrl->block_size);
    cm_dump(dump, "\t%llu", arch_ctrl->first);
    cm_dump(dump, "\t%llu", arch_ctrl->last);
    cm_dump(dump, "\t%-*s\n", (int)strlen(arch_ctrl->name), NULL_2_STR(arch_ctrl->name));
    CM_DUMP_WRITE_FILE(dump);

    return GS_SUCCESS;
}

status_t dump_ctrl_internal_page(database_ctrl_t *page, cm_dump_t *dump)
{
    space_ctrl_t *space = NULL;
    datafile_ctrl_t *datafile = NULL;
    arch_ctrl_t *arch_ctrl = NULL;
    database_ctrl_t *ctrl = page;

    cm_dump(dump, "\tspaces information:\n");
    cm_dump(dump, "\tid\tspaceid\tused\tname\tflg\tblock_size\textent_size\tfile_hwm\torg_scn\tfiles\n");
    CM_DUMP_WRITE_FILE(dump);
    for (uint32 i = 0; i < GS_MAX_SPACES; i++) {
        space = (space_ctrl_t *)db_get_ctrl_item(ctrl->pages, i, sizeof(space_ctrl_t), ctrl->space_segment);
        if (space->name != NULL && (*(char *)(space->name)) != '\0') {
            if (dump_ctrl_space_item(dump, space, i) != GS_SUCCESS) {
                return GS_ERROR;
            }
        }
    }
    cm_dump(dump, "\tdatafiles information:\n");
    cm_dump(dump, "\tid\tdfileid\tused\tname\tsize\tblock_size\tflg\ttype\tauto_extend_size\tauto_extend_maxsize\n");
    CM_DUMP_WRITE_FILE(dump);
    for (uint32 i = 0; i < GS_MAX_DATA_FILES; i++) {
        datafile = (datafile_ctrl_t *)db_get_ctrl_item(ctrl->pages, i, sizeof(datafile_ctrl_t), ctrl->datafile_segment);
        if (datafile->name != NULL && (*(char *)(datafile->name)) != '\0') {
            if (dump_ctrl_datafile_item(dump, datafile, i) != GS_SUCCESS) {
                return GS_ERROR;
            }
        }
    }
    cm_dump(dump, "\tarchive log information:\n");
    cm_dump(dump, "\tid\trecid\tdest_id\trst_id\tasn\tstamp\tblocks\tblock_size\tfirst\tlast\tname\n");
    CM_DUMP_WRITE_FILE(dump);
    for (uint32 i = 0; i < GS_MAX_ARCH_NUM; i++) {
        arch_ctrl = (arch_ctrl_t *)db_get_ctrl_item(ctrl->pages, i, sizeof(arch_ctrl_t), ctrl->arch_segment);
        if (arch_ctrl->name != NULL && (*(char *)(arch_ctrl->name)) != '\0') {
            if (dump_ctrl_arch_item(dump, arch_ctrl, i) != GS_SUCCESS) {
                return GS_ERROR;
            }
        }
    }

    return GS_SUCCESS;
}

static status_t ctrl_time2str(time_t time, char *str, uint16 size)
{
    text_t fmt_text, time_text;
    if (strlen("YYYY-MM-DD HH24:MI:SS") >= size) {
        GS_THROW_ERROR(ERR_BUFFER_UNDERFLOW, size, strlen("YYYY-MM-DD HH24:MI:SS"));
        return GS_ERROR;
    }

    cm_str2text("YYYY-MM-DD HH24:MI:SS", &fmt_text);
    time_text.str = str;
    time_text.len = 0;

    return cm_time2text(time, &fmt_text, &time_text, size);
}

static status_t dump_db_core(database_ctrl_t *ctrl, cm_dump_t *dump, char *str, uint32 strlen)
{
    cm_dump(dump, "core information:\n");
    cm_dump(dump, "\tversion:                %u-%u-%u-%u\n", ctrl->core.version.main, ctrl->core.version.major,
        ctrl->core.version.revision, ctrl->core.version.inner);
    cm_dump(dump, "\tstartup times:        %u\n", ctrl->core.open_count);
    cm_dump(dump, "\tdbid times:           %u\n", ctrl->core.dbid);
    cm_dump(dump, "\tdatabase name:        %s\n", NULL_2_STR(ctrl->core.name));
    if (ctrl_time2str(ctrl->core.init_time, str, strlen) != GS_SUCCESS) {
        cm_dump(dump, "\tinit time:            %s\n", "null");
    } else {
        cm_dump(dump, "\tinit time:            %s\n", NULL_2_STR(str));
    }
    CM_DUMP_WRITE_FILE(dump);
    cm_dump(dump, "\tscn:                  %llu\n", ctrl->core.scn);
    return GS_SUCCESS;
}

static void dump_systable_core(database_ctrl_t *ctrl, cm_dump_t *dump)
{
    cm_dump(dump, "\ttable$ entry:         %llu\n", *(uint64 *)&ctrl->core.sys_table_entry);
    cm_dump(dump, "\tix_table$1 entry:     %llu\n", *(uint64 *)&ctrl->core.ix_sys_table1_entry);
    cm_dump(dump, "\tix_table$2 entry:     %llu\n", *(uint64 *)&ctrl->core.ix_sys_table2_entry);
    cm_dump(dump, "\tcolumn$ entry:        %llu\n", *(uint64 *)&ctrl->core.sys_column_entry);
    cm_dump(dump, "\tix_column$ entry:     %llu\n", *(uint64 *)&ctrl->core.ix_sys_column_entry);
    cm_dump(dump, "\tindex$ entry:         %llu\n", *(uint64 *)&ctrl->core.sys_index_entry);
    cm_dump(dump, "\tix_index$1 entry:     %llu\n", *(uint64 *)&ctrl->core.ix_sys_index1_entry);
    cm_dump(dump, "\tix_index$2 entry:     %llu\n", *(uint64 *)&ctrl->core.ix_sys_index2_entry);
    cm_dump(dump, "\tuser$_entry:          %llu\n", *(uint64 *)&ctrl->core.sys_user_entry);
    cm_dump(dump, "\tix_user$1 entry:      %llu\n", *(uint64 *)&ctrl->core.ix_sys_user1_entry);
    cm_dump(dump, "\tix_user$2 entry:      %llu\n", *(uint64 *)&ctrl->core.ix_sys_user2_entry);
}

static status_t dump_log_core(database_ctrl_t *ctrl, cm_dump_t *dump)
{
    cm_dump(dump, "\trcy point:            %u-%u-%u-%llu\n", (uint32)ctrl->core.rcy_point.rst_id,
        ctrl->core.rcy_point.asn, ctrl->core.rcy_point.block_id, (uint64)ctrl->core.rcy_point.lfn);
    cm_dump(dump, "\tlrp point:            %u-%u-%u-%llu\n", (uint32)ctrl->core.lrp_point.rst_id,
        ctrl->core.lrp_point.asn, ctrl->core.lrp_point.block_id, (uint64)ctrl->core.lrp_point.lfn);
    cm_dump(dump, "\traft flush point:     scn(%llu)-lfn(%llu)-raft_index(%llu)\n", ctrl->core.raft_flush_point.scn,
        ctrl->core.raft_flush_point.lfn, ctrl->core.raft_flush_point.raft_index);
    cm_dump(dump, "\tckpt_id:              %llu\n", ctrl->core.ckpt_id);
    cm_dump(dump, "\tdw_start:             %u\n", ctrl->core.dw_start);
    cm_dump(dump, "\tdw_end:               %u\n", ctrl->core.dw_end);
    cm_dump(dump, "\tlsn:                  %llu\n", ctrl->core.lsn);
    cm_dump(dump, "\tlfn:                  %llu\n", ctrl->core.lfn);
    cm_dump(dump, "\tbuild completed:      %u\n", (uint32)ctrl->core.build_completed);
    cm_dump(dump, "\tlog count:            %u\n", ctrl->core.log_count);
    cm_dump(dump, "\tlog hwm:              %u\n", ctrl->core.log_hwm);
    cm_dump(dump, "\tlog first:            %u\n", ctrl->core.log_first);
    cm_dump(dump, "\tlog last:             %u\n", ctrl->core.log_last);
    CM_DUMP_WRITE_FILE(dump);
    cm_dump(dump, "\tarchive mode:         %u\n", (uint32)ctrl->core.log_mode);
    cm_dump(dump, "\tarchive logs:         %llu", ctrl->core.archived_log[0].arch_log);
    CM_DUMP_WRITE_FILE(dump);

    for (uint32 i = 1; i < GS_MAX_ARCH_DEST; i++) {
        cm_dump(dump, "-%llu", ctrl->core.archived_log[i].arch_log);
        CM_DUMP_WRITE_FILE(dump);
    }

    return GS_SUCCESS;
}

static void dump_logfile_item(cm_dump_t *dump, log_file_ctrl_t *logfile, uint32 logfile_no)
{
    cm_dump(dump, "\t#%-2u", logfile_no);
    cm_dump(dump, "\t%-*s", (int)strlen(logfile->name), NULL_2_STR(logfile->name));
    cm_dump(dump, "\t%lld", logfile->size);
    cm_dump(dump, "\t%lld", logfile->hwm);
    cm_dump(dump, "\t%u", logfile->seq);
    cm_dump(dump, "\t%u", logfile->block_size);
    cm_dump(dump, "\t%u", (uint32)logfile->flg);
    cm_dump(dump, "\t%u", (uint32)logfile->type);
    cm_dump(dump, "\t%u", (uint32)logfile->status);
    cm_dump(dump, "\t%u", (uint32)logfile->forward);
    cm_dump(dump, "\t%u\n", (uint32)logfile->backward);
}

static status_t dump_rebuild_ctrl_datafile_list(database_ctrl_t *ctrl, cm_dump_t *dump)
{
    datafile_ctrl_t *datafile = NULL;

    for (uint32 i = 0; i < ctrl->core.device_count; i++) {
        datafile = (datafile_ctrl_t *)db_get_ctrl_item(ctrl->pages, i, sizeof(datafile_ctrl_t), ctrl->datafile_segment);
        if (datafile->used && (datafile->flag & DATAFILE_FLAG_ONLINE) && !(datafile->flag & DATAFILE_FLAG_ALARMED)) {
            if (i == ctrl->core.device_count - 1) {
                cm_dump(dump, "'%s'\n", datafile->name);
            } else {
                cm_dump(dump, "'%s',\n", datafile->name);
            }
            CM_DUMP_WRITE_FILE(dump);
        }
    }

    return GS_SUCCESS;
}

static status_t dump_rebuild_ctrl_logfile_list(database_ctrl_t *ctrl, cm_dump_t *dump)
{
    log_file_ctrl_t *logfile = NULL;

    for (uint32 i = 0; i < ctrl->core.log_count; i++) {
        logfile = (log_file_ctrl_t *)db_get_ctrl_item(ctrl->pages, i, sizeof(log_file_ctrl_t), ctrl->log_segment);
        if (!LOG_IS_DROPPED(logfile->flg) && !LOG_IS_ALARMED(logfile->flg)) {
            if (i == ctrl->core.log_count - 1) {
                cm_dump(dump, "'%s'\n", logfile->name);
            } else {
                cm_dump(dump, "'%s',\n", logfile->name);
            }
            CM_DUMP_WRITE_FILE(dump);
        }
    }

    return GS_SUCCESS;
}


status_t dump_rebuild_ctrl_statement(database_ctrl_t *ctrl, cm_dump_t *dump)
{
    cm_dump(dump, "\n");
    cm_dump(dump, "create ctrlfile datafile(\n");
    CM_DUMP_WRITE_FILE(dump);
    (void)dump_rebuild_ctrl_datafile_list(ctrl, dump);
    cm_dump(dump, ") logfile(\n");
    (void)dump_rebuild_ctrl_logfile_list(ctrl, dump);
    cm_dump(dump, ") charset set ");
    if (ctrl->core.charset_id == CHARSET_GBK) {
        cm_dump(dump, "GBK ");
    } else {
        cm_dump(dump, "UTF8 ");
    }

    if (ctrl->core.log_mode == ARCHIVE_LOG_ON) {
        cm_dump(dump, "archivelog;\n");
    } else {
        cm_dump(dump, "noarchivelog;\n");
    }

    cm_dump(dump, "\n");
    cm_dump(dump,
        "Tips: You can change the character set and log mode in the SQL statement above based on your needs.");
    cm_dump(dump, "\n");

    CM_DUMP_WRITE_FILE(dump);
    return GS_SUCCESS;
}

status_t dump_ctrl_page(database_ctrl_t *page, cm_dump_t *dump)
{
    log_file_ctrl_t *logfile = NULL;
    char *str = NULL;
    uint32 i;
    database_ctrl_t *ctrl = page;

    str = (char *)malloc(GS_MAX_TIME_STRLEN);
    if (str == NULL) {
        GS_THROW_ERROR(ERR_ALLOC_MEMORY, (uint64)GS_MAX_TIME_STRLEN, "core init time");
        return GS_ERROR;
    }
    str[0] = '\0';

    if (dump_db_core(ctrl, dump, str, GS_MAX_TIME_STRLEN) != GS_SUCCESS) {
        free(str);
        return GS_ERROR;
    }

    free(str);
    dump_systable_core(ctrl, dump);
    if (dump_log_core(ctrl, dump) != GS_SUCCESS) {
        return GS_ERROR;
    }

    cm_dump(dump, "\n");
    cm_dump(dump, "\tdb_role:              %u\n", (uint32)ctrl->core.db_role);
    cm_dump(dump, "\tprotect mode:         %u\n", (uint32)ctrl->core.protect_mode);
    cm_dump(dump, "\tspace count:          %u\n", ctrl->core.space_count);
    cm_dump(dump, "\tdevice count:         %u\n", ctrl->core.device_count);
    cm_dump(dump, "\tpage size:            %u\n", ctrl->core.page_size);
    cm_dump(dump, "\tundo segments:        %u\n", ctrl->core.undo_segments);
    cm_dump(dump, "\tundo segments extend: %u\n", ctrl->core.undo_segments_extended);
    cm_dump(dump, "\treset logs:           %u-%u-%llu\n", ctrl->core.resetlogs.rst_id, ctrl->core.resetlogs.last_asn,
        ctrl->core.resetlogs.last_lfn);
    cm_dump(dump, "\tarchived_start:          %u\n", (uint32)ctrl->core.archived_start);
    cm_dump(dump, "\tarchived_end:            %u\n", (uint32)ctrl->core.archived_end);
    cm_dump(dump, "\tlogic replication mode:  %u\n", (uint32)ctrl->core.lrep_mode);
    cm_dump(dump, "\tshutdown consistency:    %u\n", ctrl->core.shutdown_consistency);
    cm_dump(dump, "\topen inconsistency:      %u\n", ctrl->core.open_inconsistency);
    cm_dump(dump, "\tconsistent lfn:          %llu\n", (uint64)ctrl->core.consistent_lfn);
    cm_dump(dump, "storage information:\n");
    cm_dump(dump, "\tlogfiles information:\n");
    cm_dump(dump, "\tid\tname\tsize\thwm\tseq\tblock_size\tflg\ttype\tstatus\tforward\tbackward\n");
    CM_DUMP_WRITE_FILE(dump);
    for (i = 0; i < GS_MAX_LOG_FILES; i++) {
        logfile = (log_file_ctrl_t *)db_get_ctrl_item(ctrl->pages, i, sizeof(log_file_ctrl_t), ctrl->log_segment);
        if (logfile->name != NULL && (*(char *)(logfile->name)) != '\0') {
            dump_logfile_item(dump, logfile, i);
            CM_DUMP_WRITE_FILE(dump);
        }
    }

    return dump_ctrl_internal_page(page, dump);
}

status_t knl_create_database(knl_handle_t session, knl_database_def_t *def)
{
    return dbc_create_database(session, def);
}

void db_save_corrupt_info(knl_session_t *session, page_id_t page_id, knl_corrupt_info_t *info)
{
    errno_t ret;

    info->page_id = page_id;
    datafile_t *df = DATAFILE_GET(page_id.file);
    ret = strncpy_s(info->datafile_name, GS_FILE_NAME_BUFFER_SIZE, df->ctrl->name, GS_FILE_NAME_BUFFER_SIZE - 1);
    knl_securec_check(ret);
    space_t *space = SPACE_GET(df->space_id);
    ret = strncpy_s(info->space_name, GS_NAME_BUFFER_SIZE, space->ctrl->name, GS_NAME_BUFFER_SIZE - 1);
    knl_securec_check(ret);
}

void db_set_ctrl_restored(knl_session_t *session, bool32 is_restored)
{
    session->kernel->db.ctrl.core.is_restored = is_restored;
    if (db_save_core_ctrl(session) != GS_SUCCESS) {
        CM_ABORT(0, "ABORT INFO: failed to save core ctrlfile");
    }
}

#ifdef __cplusplus
}
#endif
