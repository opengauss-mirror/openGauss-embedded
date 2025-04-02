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
 * knl_buffer_access.c
 * kernel buffer manager interface routines
 *
 * IDENTIFICATION
 * src/storage/gstor/zekernel/kernel/buffer/knl_buffer_access.c
 *
 * -------------------------------------------------------------------------
 */
#include "knl_buffer_access.h"
#include "knl_buflatch.h"

static inline void buf_free_iocb(knl_aio_iocbs_t *iocbs, buf_iocb_t *iocb);

/*
 * initialize async io when kernel starting up
 */
status_t buf_aio_init(knl_session_t *session)
{
    knl_instance_t *kernel = session->kernel;
    buf_aio_ctx_t *buf_aio_ctx = &kernel->buf_aio_ctx;
    knl_aio_iocbs_t *knl_iocbs = &kernel->buf_aio_ctx.buf_aio_iocbs;
    buf_iocb_t *buf_iocb = NULL;
    errno_t ret;
    uint32 i;

    /* setup aio context */
    ret = memset_sp(buf_aio_ctx, sizeof(buf_aio_ctx_t), 0, sizeof(buf_aio_ctx_t));
    knl_securec_check(ret);
#ifdef _LIBAIO
    if (cm_aio_setup(&kernel->aio_lib, BUF_IOCBS_MAX_NUM, &buf_aio_ctx->io_ctx) != GS_SUCCESS) {
        GS_LOG_RUN_WAR("[BUFFER]: setup asynchronous I/O context failed, errno %d", errno);
        return GS_ERROR;
    }
#endif
    /* allocate and initialize kernel iocbs */
    knl_iocbs->iocbs = (buf_iocb_t *)kernel->attr.buf_iocbs;
    ret =
        memset_sp(knl_iocbs->iocbs, sizeof(buf_iocb_t) * BUF_IOCBS_MAX_NUM, 0, sizeof(buf_iocb_t) * BUF_IOCBS_MAX_NUM);
    knl_securec_check(ret);

    cm_spin_lock(&knl_iocbs->lock, NULL);
    for (i = 0; i < BUF_IOCBS_MAX_NUM - 1; i++) {
        buf_iocb = &knl_iocbs->iocbs[i];
        buf_iocb->next = &knl_iocbs->iocbs[i + 1];
    }
    knl_iocbs->last = &knl_iocbs->iocbs[BUF_IOCBS_MAX_NUM - 1];
    knl_iocbs->first = knl_iocbs->iocbs;
    knl_iocbs->count = BUF_IOCBS_MAX_NUM;
    cm_spin_unlock(&knl_iocbs->lock);

    return GS_SUCCESS;
}


/*
 * async io thread processing, waiting async io event completely and handle it
 */
void buf_aio_proc(thread_t *thread)
{
#ifdef _LIBAIO
    knl_instance_t *kernel = (knl_instance_t *)thread->argument;
    buf_aio_ctx_t *buf_aio_ctx = &kernel->buf_aio_ctx;
    knl_aio_iocbs_t *buf_iocbs = &buf_aio_ctx->buf_aio_iocbs;
    cm_io_event_t *events = NULL;
    buf_iocb_t *buf_iocb = NULL;
    int32 aio_ret, i;
    uint32 size;

    cm_set_thread_name("buf async prefetch");
    GS_LOG_RUN_INF("buffer async io thread started");

    size = sizeof(cm_io_event_t) * BUF_IOCBS_MAX_NUM;
    events = (cm_io_event_t *)malloc(size);
    if (events == NULL) {
        GS_LOG_RUN_WAR("[BUFFER]failed to allocate memory for aio events");
        return;
    }

    while (!thread->closed) {
        if (cm_aio_getevents(&kernel->aio_lib, buf_aio_ctx->io_ctx, 1, BUF_IOCBS_MAX_NUM, events, &aio_ret) !=
            GS_SUCCESS) {
            continue;
        }

        for (i = 0; i < aio_ret; i++) {
            /* read of a iocb completely, handle event */
            buf_iocb = (buf_iocb_t *)events[i].obj;
            ((cm_io_callback_t)(events[i].data))(buf_aio_ctx->io_ctx, events[i].obj, events[i].res, events[i].res2);

            /* release buffer iocb and large pool page */
            if (buf_iocb->large_pool_id != GS_INVALID_ID32) {
                mpool_free_page(kernel->attr.large_pool, buf_iocb->large_pool_id);
            }
            buf_free_iocb(buf_iocbs, buf_iocb);
        }
    }
    free(events);
    cm_aio_destroy(&kernel->aio_lib, buf_aio_ctx->io_ctx);
    GS_LOG_RUN_INF("buffer async io thread closed");
#endif
}

static inline bool32 buf_changed_verifiable(knl_session_t *session, buf_ctrl_t *ctrl, latch_mode_t mode, uint8 options)
{
    return (bool32)((mode == LATCH_MODE_X) && !ctrl->is_readonly && !(options & ENTER_PAGE_NO_READ));
}

static bool32 buf_verify_checksum(knl_session_t *session, page_head_t *page, page_id_t page_id)
{
    datafile_t *df = NULL;
    space_t *space = NULL;

    /* curr page may be all zero page,can't use PAGE_TAIL or PAGE_SIZE */
    if (PAGE_CHECKSUM(page, DEFAULT_PAGE_SIZE) == GS_INVALID_CHECKSUM) {
        return GS_TRUE;
    }

    /*
     * nologging table has no redo, so its page maybe partial when db restart and crc maybe not match,
     * but it does not matter, because we will discard its data, so skip crc check.
     */
    if (!SPC_IS_LOGGING_BY_PAGEID(page_id)) {
        return GS_TRUE;
    }

    if (!page_verify_checksum(page, DEFAULT_PAGE_SIZE)) {
        df = DATAFILE_GET(page_id.file);
        space = SPACE_GET(df->space_id);
        GS_LOG_RUN_ERR("[BUFFER] page %u-%u corrupted: "
            "checksum level %s, checksum %u, page size %u, "
            "page type %s, space name %s, datafile name %s",
            page_id.file, page_id.page, knl_checksum_level(g_cks_level), PAGE_CHECKSUM(page, DEFAULT_PAGE_SIZE),
            PAGE_SIZE(*page), page_type(page->type), space->ctrl->name, df->ctrl->name);
        return GS_FALSE;
    }

    return GS_TRUE;
}

static bool32 buf_verify_compress_checksum(knl_session_t *session, page_head_t *page, page_id_t page_id)
{
    datafile_t *df = NULL;
    space_t *space = NULL;

    /* curr page may be all zero page,can't use checksum */
    if (COMPRESS_PAGE_HEAD(page)->checksum == GS_INVALID_CHECKSUM) {
        return GS_TRUE;
    }

    if (!page_compress_verify_checksum(page, DEFAULT_PAGE_SIZE)) {
        df = DATAFILE_GET(page_id.file);
        space = SPACE_GET(df->space_id);
        GS_LOG_RUN_ERR("[BUFFER] page %u-%u corrupted: "
            "checksum level %s, checksum %u, page size %u, "
            "page type %s, space name %s, datafile name %s",
            page_id.file, page_id.page, knl_checksum_level(g_cks_level), COMPRESS_PAGE_HEAD(page)->checksum,
            PAGE_SIZE(*page), page_type(page->type), space->ctrl->name, df->ctrl->name);
        return GS_FALSE;
    }

    return GS_TRUE;
}


bool32 buf_check_load_page(knl_session_t *session, page_head_t *page, page_id_t page_id, bool32 is_backup_process)
{
    if (!DB_IS_CHECKSUM_OFF(session) && !buf_verify_checksum(session, page, page_id)) {
        GS_LOG_RUN_ERR("[BUFFER] page checksum failed when load page");
        return GS_FALSE;
    }

    /* nothing to do for zero page */
    if (PAGE_SIZE(*page) == 0 && page->lsn == 0) {
        return GS_TRUE;
    }

    if (page->pcn != PAGE_TAIL(page)->pcn) {
        GS_LOG_RUN_ERR("[BUFFER] page_head pcn %u doesn't match with page_tail pcn %u", (uint32)page->pcn,
            (uint32)PAGE_TAIL(page)->pcn);
        return GS_FALSE;
    }

    if (!IS_SAME_PAGID(AS_PAGID(page->id), page_id)) {
        GS_LOG_RUN_ERR("[BUFFER] read page_id %u-%u doesn't match with expected page_id %u-%u",
            (uint32)AS_PAGID(page->id).file, (uint32)AS_PAGID(page->id).page, (uint32)page_id.file,
            (uint32)page_id.page);
        return GS_FALSE;
    }

    /* must after checksum verify */
    if (!is_backup_process && page->encrypted) {
        if (page_decrypt(session, page) != GS_SUCCESS) {
            return GS_FALSE;
        }
    }
    return GS_TRUE;
}

status_t buf_load_page_from_disk(knl_session_t *session, buf_ctrl_t *ctrl, page_id_t page_id)
{
    datafile_t *df = DATAFILE_GET(page_id.file);
    int32 *handle = DATAFILE_FD(page_id.file);
    space_t *space = SPACE_GET(df->space_id);
    int64 offset;

    if (!DATAFILE_IS_ONLINE(df) || df->space_id >= GS_MAX_SPACES || DF_FILENO_IS_INVAILD(df) || space->is_empty) {
        tx_record_sql(session);
        GS_LOG_RUN_ERR("[BUFFER] offlined tablespace %u or datafile of page_id %u-%u", df->space_id,
            (uint32)page_id.file, (uint32)page_id.page);
        char *space_name = df->space_id >= GS_MAX_SPACES ? "invalid space" : space->ctrl->name;
        GS_THROW_ERROR(ERR_SPACE_OFFLINE, space_name, "buf load page failed");
        return GS_ERROR;
    }

    offset = (int64)page_id.page * DEFAULT_PAGE_SIZE;
    knl_begin_session_wait(session, DB_FILE_SEQUENTIAL_READ, GS_TRUE);

    if (spc_read_datafile(session, df, handle, offset, ctrl->page, DEFAULT_PAGE_SIZE) != GS_SUCCESS) {
        GS_LOG_RUN_ERR("[BUFFER] failed to read datafile %s, offset %lld, size %u, error code %d", df->ctrl->name,
            offset, DEFAULT_PAGE_SIZE, errno);
        spc_close_datafile(df, handle);
        knl_end_session_wait(session);
        return GS_ERROR;
    }

    knl_end_session_wait(session);

    /* generally, one session can not wait for more than 0xffffffffffffffff us */
    session->stat.disk_read_time += session->wait.usecs;
    session->stat.disk_reads++;
    cm_atomic_inc(&session->kernel->total_io_read);
    g_knl_callback.accumate_io(session, IO_TYPE_READ);

    if (!buf_check_load_page(session, ctrl->page, page_id, GS_FALSE)) {
        if (abr_repair_page_from_standy(session, ctrl)) {
            return GS_SUCCESS;
        }

        /* record alarm log if repair failed */
        GS_LOG_ALARM(WARN_PAGECORRUPTED, "{'page-type':'%s','space-name':'%s','file-name':'%s'}",
            page_type(ctrl->page->type), space->ctrl->name, df->ctrl->name);

        GS_THROW_ERROR(ERR_PAGE_CORRUPTED, page_id.file, page_id.page);
        return GS_ERROR;
    }

    return GS_SUCCESS;
}

bool32 buf_check_load_compress_page(knl_session_t *session, page_head_t *page, page_id_t page_id)
{
    if (!DB_IS_CHECKSUM_OFF(session) && !buf_verify_compress_checksum(session, page, page_id)) {
        GS_LOG_RUN_ERR("[BUFFER] page checksum failed when load compress page");
        return GS_FALSE;
    }

    /* nothing to do for zero page */
    if (PAGE_SIZE(*page) == 0 && page->lsn == 0) {
        return GS_TRUE;
    }

    if (!IS_SAME_PAGID(AS_PAGID(page->id), page_id)) {
        GS_LOG_RUN_ERR("[BUFFER] read page_id %u-%u doesn't match with expected page_id %u-%u",
            (uint32)AS_PAGID(page->id).file, (uint32)AS_PAGID(page->id).page, (uint32)page_id.file,
            (uint32)page_id.page);
        return GS_FALSE;
    }

    return GS_TRUE;
}

status_t buf_decompress_group(knl_session_t *session, char *dst, const char *src, uint32 *size)
{
    size_t actual_size;
    uint32 remaining_size, zsize;
    uint32 dst_offset, src_offset;
    compress_page_head_t group_head = { 0 };
    errno_t ret;
    pcb_assist_t pcb_assist;

    if (pcb_get_buf(session, &pcb_assist) != GS_SUCCESS) {
        return GS_ERROR;
    }
    group_head.compressed_size = COMPRESS_PAGE_HEAD(src)->compressed_size;
    group_head.compress_algo = COMPRESS_PAGE_HEAD(src)->compress_algo;
    group_head.group_cnt = COMPRESS_PAGE_HEAD(src)->group_cnt;
    remaining_size = group_head.compressed_size;
    zsize = COMPRESS_PAGE_VALID_SIZE;
    dst_offset = 0;
    src_offset = DEFAULT_PAGE_SIZE - zsize;
    *size = 0;
    do {
        if (remaining_size > zsize) {
            actual_size = zsize;
        } else {
            actual_size = remaining_size;
        }

        ret = memcpy_sp((char *)pcb_assist.aligned_buf + dst_offset, DEFAULT_PAGE_SIZE * PAGE_GROUP_COUNT,
            (char *)src + src_offset, actual_size);
        knl_securec_check(ret);
        remaining_size -= actual_size;
        dst_offset += actual_size;
        src_offset += actual_size + DEFAULT_PAGE_SIZE - zsize;
    } while (remaining_size != 0);

    if (group_head.compress_algo == COMPRESS_ZSTD) {
#ifdef _ZSTD
        actual_size = ZSTD_decompress(dst, DEFAULT_PAGE_SIZE * PAGE_GROUP_COUNT, pcb_assist.aligned_buf,
            group_head.compressed_size);
        if (ZSTD_isError(actual_size)) {
            GS_LOG_RUN_ERR("[BUFFER] failed to decompress(zstd) group, first page %u-%u, error code: %lu, reason: %s",
                AS_PAGID_PTR(((page_head_t *)src)->id)->file, AS_PAGID_PTR(((page_head_t *)src)->id)->page, actual_size,
                ZSTD_getErrorName(actual_size));
            GS_THROW_ERROR(ERR_DECOMPRESS_ERROR, "zstd", actual_size, ZSTD_getErrorName(actual_size));
            return GS_ERROR;
        }
#endif
    } else {
        knl_panic_log(GS_FALSE, "compress algorithm %d not supported", group_head.compress_algo);
    }

    pcb_release_buf(session, &pcb_assist);
    *size = (uint32)actual_size; // decompress size always equals 64K，convert is safe
    return GS_SUCCESS;
}

static status_t buf_construct_group_members(knl_session_t *session, buf_ctrl_t *head_ctrl, const char *src)
{
    datafile_t *df = DATAFILE_GET(head_ctrl->page_id.file);
    space_t *space = SPACE_GET(df->space_id);
    status_t status = GS_SUCCESS;
    buf_ctrl_t *ctrl = NULL;

    for (int32 i = (PAGE_GROUP_COUNT - 1); i >= 0; i--) {
        ctrl = head_ctrl->compress_group[i];

        BUF_UNPROTECT_PAGE(ctrl->page);
        errno_t ret = memcpy_sp(ctrl->page, DEFAULT_PAGE_SIZE, src + i * DEFAULT_PAGE_SIZE, DEFAULT_PAGE_SIZE);
        knl_securec_check(ret);
        ctrl->page->compressed = 0;

        if (!buf_check_load_page(session, ctrl->page, ctrl->page_id, GS_FALSE)) {
            if (!abr_repair_page_from_standy(session, ctrl)) {
                /* record alarm log if repair failed */
                GS_LOG_ALARM(WARN_PAGECORRUPTED, "{'page-type':'%s','space-name':'%s','file-name':'%s'}",
                    page_type(ctrl->page->type), space->ctrl->name, df->ctrl->name);
                GS_THROW_ERROR(ERR_PAGE_CORRUPTED, ctrl->page_id.file, ctrl->page_id.page);
                status = GS_ERROR;
                continue; // continue to look other pages' result
            }
        }

#if defined(__arm__) || defined(__aarch64__)
        CM_MFENCE;
#endif

        BUF_PROTECT_PAGE(ctrl->page);
    }

    return status;
}

static status_t buf_construct_group(knl_session_t *session, buf_ctrl_t *head_ctrl, char *read_buf)
{
    pcb_assist_t unzip_pcb_assist;
    bool32 really_compressed;
    char *src = NULL;
    uint32 size;

    /* we need to rely on the actual compression properties of the page
     * to determine whether it needs to be decompressed */
    really_compressed = ((page_head_t *)read_buf)->compressed;
    if (pcb_get_buf(session, &unzip_pcb_assist) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (SECUREC_UNLIKELY(!really_compressed)) {
        /* Pages are plainly written without compression for some reason */
        src = read_buf;
    } else {
        if (buf_decompress_group(session, unzip_pcb_assist.aligned_buf, read_buf, &size) != GS_SUCCESS) {
            pcb_release_buf(session, &unzip_pcb_assist);
            return GS_ERROR;
        }
        knl_panic_log(size == DEFAULT_PAGE_SIZE * PAGE_GROUP_COUNT, "decompress size %u incorrect", size);
        src = unzip_pcb_assist.aligned_buf;
    }

    if (buf_construct_group_members(session, head_ctrl, src) != GS_SUCCESS) {
        GS_LOG_RUN_ERR("[BUFFER] construct compress group failed");
        pcb_release_buf(session, &unzip_pcb_assist);
        return GS_ERROR;
    }

    pcb_release_buf(session, &unzip_pcb_assist);
    return GS_SUCCESS;
}

status_t buf_check_load_compress_group(knl_session_t *session, page_id_t head_page_id, const char *read_buf)
{
    datafile_t *df = DATAFILE_GET(head_page_id.file);
    space_t *space = SPACE_GET(df->space_id);
    page_head_t *compress_page = NULL;
    page_id_t page_id = head_page_id;
    status_t status = GS_SUCCESS;

    /* we only verify compress page here */
    if (!((page_head_t *)read_buf)->compressed) {
        return status;
    }

    for (uint16 i = 0; i < PAGE_GROUP_COUNT; i++) {
        compress_page = (page_head_t *)((char *)read_buf + i * DEFAULT_PAGE_SIZE);
        if (!buf_check_load_compress_page(session, compress_page, page_id)) {
            GS_THROW_ERROR(ERR_PAGE_CORRUPTED, page_id.file, page_id.page);
            GS_LOG_ALARM(WARN_PAGECORRUPTED, "{'page-type':'%s','space-name':'%s','file-name':'%s'}",
                page_type(compress_page->type), space->ctrl->name, df->ctrl->name);
            status = GS_ERROR;
        }
        page_id.page++;
    }

    return status;
}

static status_t buf_read_and_construct(knl_session_t *session, buf_ctrl_t *head_ctrl)
{
    pcb_assist_t pcb_assist;
    page_id_t head_page = head_ctrl->page_id;
    datafile_t *df = DATAFILE_GET(head_page.file);
    int32 *handle = DATAFILE_FD(head_page.file);
    int64 offset = (int64)head_page.page * DEFAULT_PAGE_SIZE;

    space_t *space = SPACE_GET(df->space_id);
    if (!SPACE_IS_ONLINE(space) || !DATAFILE_IS_ONLINE(df)) {
        GS_LOG_RUN_ERR("[BUFFER] offlined tablespace or datafile of page_id %u-%u", (uint32)head_page.file,
            (uint32)head_page.page);
        GS_THROW_ERROR(ERR_SPACE_OFFLINE, space->ctrl->name, "buf load page failed");
        return GS_ERROR;
    }

    if (pcb_get_buf(session, &pcb_assist) != GS_SUCCESS) {
        return GS_ERROR;
    }

    knl_begin_session_wait(session, DB_FILE_SCATTERED_READ, GS_TRUE);
    if (spc_read_datafile(session, df, handle, offset, pcb_assist.aligned_buf, DEFAULT_PAGE_SIZE * PAGE_GROUP_COUNT) !=
        GS_SUCCESS) {
        GS_LOG_RUN_ERR("[BUFFER] failed to read datafile %s, offset %lld, size %u, error code %d", df->ctrl->name,
            offset, DEFAULT_PAGE_SIZE * PAGE_GROUP_COUNT, errno);
        spc_close_datafile(df, handle);
        knl_end_session_wait(session);
        pcb_release_buf(session, &pcb_assist);
        return GS_ERROR;
    }
    knl_end_session_wait(session);

    session->stat.disk_read_time += session->wait.usecs;
    session->stat.disk_reads++;
    cm_atomic_inc(&session->kernel->total_io_read);
    g_knl_callback.accumate_io(session, IO_TYPE_READ);

    if (buf_check_load_compress_group(session, head_ctrl->page_id, pcb_assist.aligned_buf) != GS_SUCCESS) {
        pcb_release_buf(session, &pcb_assist);
        return GS_ERROR;
    }

    if (buf_construct_group(session, head_ctrl, pcb_assist.aligned_buf) != GS_SUCCESS) {
        pcb_release_buf(session, &pcb_assist);
        return GS_ERROR;
    }

    pcb_release_buf(session, &pcb_assist);
    return GS_SUCCESS;
}

static inline void buf_load_update_status(knl_session_t *session, buf_ctrl_t *head_ctrl, status_t read_status)
{
    if (SECUREC_UNLIKELY(read_status != GS_SUCCESS)) {
        for (int32 i = PAGE_GROUP_COUNT - 1; i >= 0; i--) {
            head_ctrl->compress_group[i]->load_status = BUF_LOAD_FAILED;
        }
    } else {
        for (int32 i = PAGE_GROUP_COUNT - 1; i >= 0; i--) {
            head_ctrl->compress_group[i]->load_status = BUF_IS_LOADED;
        }
    }
}
#ifdef _MSC_VER
status_t buf_load_group(knl_session_t *session, buf_ctrl_t *ctrl)
#else
inline status_t buf_load_group(knl_session_t *session, buf_ctrl_t *ctrl)
#endif
{
    status_t status;
    buf_ctrl_t *head_ctrl = ctrl->compress_group[0];

    status = buf_read_and_construct(session, head_ctrl);
    buf_load_update_status(session, head_ctrl, status);
    return status;
}

void buf_aio_prefetch_compress(knl_session_t *session, char *read_buf, buf_ctrl_t *head_ctrl, uint32 large_pool_id)
{
    status_t status;

    if (large_pool_id != GS_INVALID_ID32) {
        knl_panic_log(PAGE_IS_COMPRESS_HEAD(head_ctrl->page_id),
            "invalid next extent page id in bitmap file, file:%d, page:%d.", head_ctrl->page_id.file,
            head_ctrl->page_id.page);
        if (buf_check_load_compress_group(session, head_ctrl->page_id, read_buf) != GS_SUCCESS) {
            status = GS_ERROR;
        } else {
            status = buf_construct_group(session, head_ctrl, read_buf);
        }
        buf_load_update_status(session, head_ctrl, status);
    }

    BUF_PROTECT_PAGE(head_ctrl->page);
    buf_unlatch(session, head_ctrl, GS_TRUE);
}

void buf_aio_prefetch_normal(knl_session_t *session, const char *read_buf, buf_ctrl_t *ctrl, uint32 large_pool_id)
{
    errno_t ret;
    if (large_pool_id != GS_INVALID_ID32) {
        BUF_UNPROTECT_PAGE(ctrl->page);
        ret = memcpy_sp(ctrl->page, DEFAULT_PAGE_SIZE, read_buf, DEFAULT_PAGE_SIZE);
        knl_securec_check(ret);
    }

    if (!buf_check_load_page(session, ctrl->page, ctrl->page_id, GS_FALSE)) {
        if (!abr_repair_page_from_standy(session, ctrl)) {
            ctrl->load_status = (uint8)BUF_LOAD_FAILED;
            buf_unlatch(session, ctrl, GS_TRUE);
            return;
        }
    }

    BUF_PROTECT_PAGE(ctrl->page);
    ctrl->load_status = (uint8)BUF_IS_LOADED;
    buf_unlatch(session, ctrl, GS_TRUE);
}
#ifdef _LIBAIO
/*
 * async prefetch callback function
 * if read completely, copy page from large pool to data buffer
 */
void buf_aio_prefetch_ext_callback(cm_io_context_t ctx, cm_iocb_t *iocb, long res, long res2)
{
    buf_iocb_t *buf_iocb = (buf_iocb_t *)iocb;
    knl_session_t *session = buf_iocb->session;
    char *read_buf = buf_iocb->large_buf;
    buf_ctrl_t **ctrls = buf_iocb->ctrls;
    uint32 i, skip;

    for (i = 0; i < buf_iocb->page_cnt; i += skip) {
        skip = 1;
        if (ctrls[i] == NULL) {
            continue;
        }

        if (page_compress(session, ctrls[i]->page_id)) {
            buf_aio_prefetch_compress(session, read_buf + i * DEFAULT_PAGE_SIZE, ctrls[i], buf_iocb->large_pool_id);
            skip = PAGE_GROUP_COUNT; // skip all the group
        } else {
            buf_aio_prefetch_normal(session, read_buf + i * DEFAULT_PAGE_SIZE, ctrls[i], buf_iocb->large_pool_id);
        }
    }
}
#endif
#define BUF_AIO_TRY_TIMES 1000
static buf_iocb_t *buf_alloc_iocb(knl_aio_iocbs_t *iocbs)
{
    buf_iocb_t *iocb = NULL;
    uint32 count = 0;

    for (;;) {
        cm_spin_lock(&iocbs->lock, NULL);
        if (iocbs->count <= 1) {
            cm_spin_unlock(&iocbs->lock);

            if (SECUREC_UNLIKELY(count > BUF_AIO_TRY_TIMES)) {
                break;
            }
            cm_spin_sleep();
            count++;
            continue;
        }
        iocb = iocbs->first;
        iocb->used = 1;
        iocbs->first = iocbs->first->next;
        iocbs->count--;
        iocb->next = NULL;
        cm_spin_unlock(&iocbs->lock);
        break;
    }
    return iocb;
}
#ifdef _LIBAIO
static void buf_aio_prefetch_ext_prepare(knl_session_t *session, buf_iocb_t *buf_iocb, page_id_t curr_page,
    uint32 count, char *read_buf)
{
    cm_aio_prep_read(&buf_iocb->iocb, *DATAFILE_FD(curr_page.file), read_buf, count * DEFAULT_PAGE_SIZE,
        (uint64)(curr_page.page) * DEFAULT_PAGE_SIZE);
    cm_aio_set_callback(&buf_iocb->iocb, buf_aio_prefetch_ext_callback);

    buf_iocb->large_buf = read_buf;
    buf_iocb->page_id.file = curr_page.file;
    buf_iocb->page_id.page = curr_page.page;
    buf_iocb->page_cnt = count;
    buf_iocb->session = session->kernel->sessions[SESSION_ID_AIO];
}
#endif
static inline void buf_free_iocb(knl_aio_iocbs_t *buf_iocbs, buf_iocb_t *buf_iocb)
{
    cm_spin_lock(&buf_iocbs->lock, NULL);
    buf_iocb->used = 0;
    buf_iocbs->last->next = buf_iocb;
    buf_iocbs->last = buf_iocb;
    buf_iocbs->count++;
    cm_spin_unlock(&buf_iocbs->lock);
}

static void buf_aio_prefetch_clean_status(knl_session_t *session, buf_ctrl_t *ctrl, uint32 *skip)
{
    if (ctrl->load_status != (uint8)BUF_IS_LOADED) {
        if (page_compress(session, ctrl->page_id)) {
            *skip = PAGE_GROUP_COUNT;
            buf_load_update_status(session, ctrl, GS_ERROR);
        } else {
            ctrl->load_status = (uint8)BUF_LOAD_FAILED;
        }
    }
    buf_unlatch(session, ctrl, GS_TRUE);
}

#ifdef _LIBAIO
/*
 * if prefetch failed, we need to release large pages and buffer ctrls that have been allocated
 */
static void buf_aio_prefetch_clean(knl_session_t *session, uint32 *mpool_pages, cm_iocb_t **iocbs, uint32 read_times,
    uint32 page_cnt_per_time, buf_ctrl_t *first_ctrl)
{
    buf_iocb_t *iocb = NULL;
    buf_ctrl_t *ctrl = NULL;
    uint32 i, j, skip;

    for (i = 0; i < read_times; i++) {
        if (mpool_pages != NULL) {
            mpool_free_page(session->kernel->attr.large_pool, mpool_pages[i]);
        }

        iocb = (buf_iocb_t *)iocbs[i];
        for (j = 0; j < page_cnt_per_time; j += skip) {
            skip = 1;
            ctrl = iocb->ctrls[j];
            /* skip the first page in the extent */
            if (ctrl == NULL || ctrl->page == first_ctrl->page) {
                continue;
            }

            buf_aio_prefetch_clean_status(session, ctrl, &skip);
        }

        buf_free_iocb(&session->kernel->buf_aio_ctx.buf_aio_iocbs, iocb);
    }
}

static status_t buf_aio_submit(knl_session_t *session, datafile_t *df, uint32 read_times, cm_iocb_t **iocbs)
{
    space_t *space = SPACE_GET(df->space_id);
    if (!SPACE_IS_ONLINE(space) || !DATAFILE_IS_ONLINE(df)) {
        GS_LOG_RUN_WAR("[BUFFER] tablespace has been dropped");
        return GS_ERROR;
    }

    /* submit prefetch request */
    if (cm_aio_submit(&session->kernel->aio_lib, session->kernel->buf_aio_ctx.io_ctx, (int32)read_times, iocbs) !=
        GS_SUCCESS) {
        GS_LOG_RUN_WAR("[BUFFER] failed to submit aio, error code: %d", errno);
        return GS_ERROR;
    }
    return GS_SUCCESS;
}
#endif
static void buf_aio_alloc_ctrls(knl_session_t *session, buf_ctrl_t *ctrl, page_id_t curr_page, uint32 page_cnt_per_time,
    buf_iocb_t *buf_iocb)
{
    page_id_t page_id = curr_page;

    if (page_compress(session, ctrl->page_id)) {
        knl_panic_log(PAGE_IS_COMPRESS_HEAD(ctrl->page_id),
            "Invalid next extent page id in bitmap file, file:%u, page:%u.", ctrl->page_id.file, ctrl->page_id.page);
        knl_panic_log(page_cnt_per_time % PAGE_GROUP_COUNT == 0, "Invalid bitmap file extent size %u.",
            page_cnt_per_time);
        for (uint32 j = 0; j < page_cnt_per_time; j++) {
            page_id.page = curr_page.page + j;
            if (page_id.page == ctrl->page_id.page) {
                buf_iocb->ctrls[j] = ctrl;
            } else if (PAGE_IS_COMPRESS_HEAD(page_id)) {
                buf_iocb->ctrls[j] =
                    buf_try_alloc_compress(session, page_id, LATCH_MODE_S, ENTER_PAGE_NORMAL, BUF_ADD_OLD);
            } else {
                buf_iocb->ctrls[j] = NULL; // we only store the ctrl of group-head-page
            }
        }
    } else {
        for (uint32 j = 0; j < page_cnt_per_time; j++) {
            page_id.page = curr_page.page + j;
            if (page_id.page == ctrl->page_id.page) {
                buf_iocb->ctrls[j] = ctrl;
            } else {
                buf_iocb->ctrls[j] = buf_try_alloc_ctrl(session, page_id, LATCH_MODE_S, ENTER_PAGE_NORMAL, BUF_ADD_OLD);
            }
        }
    }
}
#ifdef _LIBAIO
/*
 * prefetch extent in background with async io, including 3 steps:
 * 1.partition extent to several prefetch uints, each of which is manged by a buffer iocb.
 * 2.for each buffer iocb, allocate large pool page and buffer ctrls, setup prefetch info and callback function.
 * 3.submit all async read at the same time.
 */
static status_t buf_aio_prefetch_ext(knl_session_t *session, buf_ctrl_t *ctrl, uint32 extent_size)
{
    datafile_t *df = DATAFILE_GET(ctrl->page_id.file);
    buf_iocb_t *buf_iocb = NULL;
    uint32 max_cnt = GS_LARGE_PAGE_SIZE / DEFAULT_PAGE_SIZE;
    uint32 page_cnt_per_time = extent_size < max_cnt ? extent_size : max_cnt;
    uint32 read_times = extent_size / page_cnt_per_time;

    if (spc_open_datafile(session, df, DATAFILE_FD(ctrl->page_id.file)) != GS_SUCCESS) {
        GS_LOG_RUN_ERR("[SPACE] failed to open datafile %s", df->ctrl->name);
        return GS_ERROR;
    }

    CM_SAVE_STACK(session->stack);
    uint32 *mpool_pages = (uint32 *)cm_push(session->stack, sizeof(uint32) * read_times);
    char **read_buf = (char **)cm_push(session->stack, sizeof(char *) * read_times);
    cm_iocb_t **iocbs = (cm_iocb_t **)cm_push(session->stack, sizeof(struct iocb *) * read_times);

    page_id_t curr_page = ctrl->page_id;
    for (uint32 i = 0; i < read_times; i++) {
        /* alloc read buffer from large pool */
        mpool_pages[i] = GS_INVALID_ID32;
        if (!mpool_try_alloc_page(session->kernel->attr.large_pool, &mpool_pages[i])) {
            GS_LOG_DEBUG_WAR("[BUFFER] no large pool page available");
            buf_aio_prefetch_clean(session, mpool_pages, iocbs, i, page_cnt_per_time, ctrl);
            CM_RESTORE_STACK(session->stack);
            return GS_ERROR;
        }
        read_buf[i] = mpool_page_addr(session->kernel->attr.large_pool, mpool_pages[i]);

        buf_iocb = buf_alloc_iocb(&session->kernel->buf_aio_ctx.buf_aio_iocbs);
        if (buf_iocb == NULL) {
            GS_LOG_DEBUG_WAR("[BUFFER] no aio resource available");
            buf_aio_prefetch_clean(session, mpool_pages, iocbs, i, page_cnt_per_time, ctrl);
            CM_RESTORE_STACK(session->stack);
            return GS_ERROR;
        }
        buf_iocb->large_pool_id = mpool_pages[i];
        iocbs[i] = &buf_iocb->iocb;
        buf_aio_prefetch_ext_prepare(session, buf_iocb, curr_page, page_cnt_per_time, read_buf[i]);
        session->stat.aio_reads++;

        /* allocate buffer ctrl for prefetch page */
        buf_aio_alloc_ctrls(session, ctrl, curr_page, page_cnt_per_time, buf_iocb);

        curr_page.page += page_cnt_per_time;
    }

    /* submit prefetch request */
    if (buf_aio_submit(session, df, read_times, iocbs) != GS_SUCCESS) {
        buf_aio_prefetch_clean(session, mpool_pages, iocbs, read_times, page_cnt_per_time, ctrl);
        CM_RESTORE_STACK(session->stack);
        return GS_ERROR;
    }

    CM_RESTORE_STACK(session->stack);
    return GS_SUCCESS;
}

static status_t buf_aio_prefetch_page(knl_session_t *session, buf_ctrl_t *ctrl)
{
    knl_instance_t *kernel = session->kernel;
    cm_iocb_t **iocbs = (cm_iocb_t **)cm_push(session->stack, sizeof(struct iocb *));

    buf_iocb_t *buf_iocb = buf_alloc_iocb(&kernel->buf_aio_ctx.buf_aio_iocbs);
    if (buf_iocb == NULL) {
        cm_pop(session->stack);
        return GS_ERROR;
    }

    buf_iocb->large_pool_id = GS_INVALID_ID32;
    iocbs[0] = &buf_iocb->iocb;
    buf_iocb->ctrls[0] = ctrl;

    BUF_UNPROTECT_PAGE(ctrl->page);
    buf_aio_prefetch_ext_prepare(session, buf_iocb, ctrl->page_id, 1, (char *)ctrl->page);
    session->stat.aio_reads++;

    datafile_t *df = DATAFILE_GET(ctrl->page_id.file);
    space_t *space = SPACE_GET(df->space_id);
    if (!SPACE_IS_ONLINE(space) || !DATAFILE_IS_ONLINE(df)) {
        buf_aio_prefetch_clean(session, NULL, iocbs, 1, 1, ctrl);
        cm_pop(session->stack);
        return GS_ERROR;
    }

    /* submit prefetch request */
    if (cm_aio_submit(&kernel->aio_lib, kernel->buf_aio_ctx.io_ctx, 1, iocbs) != GS_SUCCESS) {
        buf_aio_prefetch_clean(session, NULL, iocbs, 1, 1, ctrl);
        cm_pop(session->stack);
        return GS_ERROR;
    }

    cm_pop(session->stack);
    return GS_SUCCESS;
}
#endif
status_t buf_read_page_asynch(knl_session_t *session, page_id_t page_id)
{
    buf_ctrl_t *ctrl = NULL;
    page_id_t head;
    bool32 is_compress = page_compress(session, page_id);

    if (!session->kernel->attr.enable_asynch) {
        return GS_SUCCESS;
    }

    if (is_compress) {
        head = page_first_group_id(session, page_id);
        ctrl = buf_try_alloc_compress(session, head, LATCH_MODE_S, ENTER_PAGE_NORMAL, BUF_ADD_COLD);
    } else {
        ctrl = buf_try_alloc_ctrl(session, page_id, LATCH_MODE_S, ENTER_PAGE_NORMAL, BUF_ADD_COLD);
    }
    if (ctrl == NULL) {
        return GS_SUCCESS;
    }

    if (ctrl->load_status == (uint8)BUF_IS_LOADED) {
        buf_unlatch(session, ctrl, GS_TRUE);
        return GS_SUCCESS;
    }
#ifdef _LIBAIO
    if (is_compress) {
        if (buf_aio_prefetch_ext(session, ctrl, PAGE_GROUP_COUNT) != GS_SUCCESS) {
            if (ctrl->load_status != (uint8)BUF_IS_LOADED) {
                buf_load_update_status(session, ctrl, GS_ERROR);
            }
            buf_unlatch(session, ctrl, GS_TRUE);
            return GS_ERROR;
        }
    } else {
        if (buf_aio_prefetch_page(session, ctrl) != GS_SUCCESS) {
            if (ctrl->load_status != (uint8)BUF_IS_LOADED) {
                ctrl->load_status = (uint8)BUF_LOAD_FAILED;
            }

            buf_unlatch(session, ctrl, GS_TRUE);
            return GS_ERROR;
        }
    }
#endif
    return GS_SUCCESS;
}
/*
 * skip prefetch if next extent is invalid or extent has already loaded
 */
static status_t buf_try_prefetch_next_ext(knl_session_t *session, buf_ctrl_t *ctrl)
{
    datafile_t *df = DATAFILE_GET(ctrl->page_id.file);
    space_t *space = SPACE_GET(df->space_id);
    page_id_t next_ext = AS_PAGID(ctrl->page->next_ext);
    buf_ctrl_t *next_ctrl = NULL;
    uint32 extent_size;

    if (SPACE_IS_BITMAPMANAGED(space)) {
        if (IS_INVALID_PAGID(next_ext)) {
            return GS_SUCCESS;
        }
    } else {
        if (!spc_is_extent_first(session, space, ctrl->page_id) || IS_INVALID_PAGID(next_ext)) {
            return GS_SUCCESS;
        }
    }

    if (page_compress(session, next_ext)) {
        next_ctrl = buf_try_alloc_compress(session, next_ext, LATCH_MODE_S, ENTER_PAGE_SEQUENTIAL, BUF_ADD_OLD);
    } else {
        next_ctrl = buf_try_alloc_ctrl(session, next_ext, LATCH_MODE_S, ENTER_PAGE_SEQUENTIAL, BUF_ADD_OLD);
    }
    if (next_ctrl == NULL) {
        return GS_SUCCESS;
    }

    if (next_ctrl->load_status == (uint8)BUF_IS_LOADED) {
        buf_unlatch(session, next_ctrl, GS_TRUE);
        return GS_SUCCESS;
    }

    if (SPACE_IS_BITMAPMANAGED(space)) {
        extent_size = spc_ext_size_by_id((uint8)ctrl->page->ext_size);
    } else {
        extent_size = space->ctrl->extent_size;
    }
#ifdef _LIBAIO
    if (buf_aio_prefetch_ext(session, next_ctrl, extent_size) != GS_SUCCESS) {
        if (next_ctrl->load_status != (uint8)BUF_IS_LOADED) {
            if (page_compress(session, next_ctrl->page_id)) {
                buf_load_update_status(session, next_ctrl, GS_ERROR);
            } else {
                next_ctrl->load_status = (uint8)BUF_LOAD_FAILED;
            }
        }
        buf_unlatch(session, next_ctrl, GS_TRUE);
        return GS_ERROR;
    }
#endif
    return GS_SUCCESS;
}

/*
 * When DB recover with gbp, buf_load_page will try load page from GBP at first.
 * If page cannot be loaded from GBP, it will be loaded from disk
 */
static status_t buf_load_page_from_GBP(knl_session_t *session, buf_ctrl_t *ctrl, page_id_t page_id)
{
#ifdef _REPLICATION
    gbp_context_t *gbp_ctx = &session->kernel->gbp_context;

    gbp_page_status_e status;

    uint32 lock_id = page_id.page % GS_GBP_RD_LOCK_COUNT;

    cm_spin_lock(&gbp_ctx->buf_read_lock[lock_id], NULL);
    if (!KNL_RECOVERY_WITH_GBP(session->kernel)) {
        cm_spin_unlock(&gbp_ctx->buf_read_lock[lock_id]);
        return GS_ERROR; /* recheck rcy_with_gbp flag, if GS_FALSE, all GBP pages are pulled to local buffer */
    }

    knl_begin_session_wait(session, DB_FILE_GBP_READ, GS_TRUE);
#ifdef _REPLICATION
    status = knl_read_page_from_gbp(session, ctrl);
#endif
    ctrl->gbp_ctrl->page_status = status;
    knl_end_session_wait(session);
    cm_spin_unlock(&gbp_ctx->buf_read_lock[lock_id]);

    if (status == GBP_PAGE_MISS || status == GBP_PAGE_OLD || status == GBP_PAGE_AHEAD) {
        /* page not exists on gbp */
        return GS_ERROR;
    } else {
        knl_panic_log(CHECK_PAGE_PCN(ctrl->page), "page pcn is abnormal, panic info: ctrl_page %u-%u type %u",
            ctrl->page_id.file, ctrl->page_id.page, ctrl->page->type);
        return GS_SUCCESS;
    }
#endif
    return GS_SUCCESS;
}

/*
 * When DB recover with gbp, current session whether need read page from GBP
 * log analysis session always read page from local disk
 * gbp backround always read page from disk because of it has already pulled page from gbp
 */
static inline bool32 session_need_read_gbp(knl_session_t *session)
{
    if (SESSION_IS_LOG_ANALYZE(session) || SESSION_IS_GBP_BG(session)) {
        return GS_FALSE;
    } else {
        return GS_TRUE;
    }
}

/* If failover with GBP, try load page from GBP. If page is not exists on GBP, load page from disk */
status_t buf_load_page(knl_session_t *session, buf_ctrl_t *ctrl, page_id_t page_id)
{
    status_t status = GS_ERROR;
#ifdef _REPLICATION
    if (SECUREC_UNLIKELY(KNL_RECOVERY_WITH_GBP(session->kernel)) && session_need_read_gbp(session)) {
        ctrl->page->lsn = GS_INVALID_LSN; // reset page lsn to 0 here, because it is not loaded from disk
        status = buf_load_page_from_GBP(session, ctrl, page_id);
    }
#endif
    if (status != GS_SUCCESS) {
        status = buf_load_page_from_disk(session, ctrl, page_id);
    }

    if (status != GS_SUCCESS) {
        ctrl->load_status = (uint8)BUF_LOAD_FAILED;
    } else {
        ctrl->load_status = (uint8)BUF_IS_LOADED;
    }

    return status;
}

static status_t buf_batch_load_pages(knl_session_t *session, char *read_buf, buf_ctrl_t *ctrl, page_id_t begin,
    uint32 count)
{
    datafile_t *df = DATAFILE_GET(begin.file);
    int32 *handle = DATAFILE_FD(begin.file);
    space_t *space = SPACE_GET(df->space_id);
    page_id_t page_id = begin;
    buf_ctrl_t **ctrl_array = NULL;
    status_t status = GS_SUCCESS;
    int64 offset;
    uint32 i;
    errno_t ret;

    ctrl_array = (buf_ctrl_t **)cm_push(session->stack, sizeof(buf_ctrl_t *) * count);

    for (i = 0; i < count; i++) {
        page_id.page = begin.page + i;
        if (page_id.page == ctrl->page_id.page) {
            ctrl_array[i] = ctrl;
            continue;
        }

        ctrl_array[i] = buf_try_alloc_ctrl(session, page_id, LATCH_MODE_S, ENTER_PAGE_SEQUENTIAL, BUF_ADD_OLD);
        if (ctrl_array[i] != NULL) {
            knl_panic_log(IS_SAME_PAGID(page_id, ctrl_array[i]->page_id),
                "the page_id and current ctrl page are not "
                "same, panic info: ctrl_page %u-%u type %u, page %u-%u",
                ctrl_array[i]->page_id.file, ctrl_array[i]->page_id.page, ctrl_array[i]->page->type, page_id.file,
                page_id.page);
        }
    }

    do {
        if (!SPACE_IS_ONLINE(space) || !DATAFILE_IS_ONLINE(df)) {
            GS_LOG_RUN_ERR("[BUFFER] offlined tablespace or datafile of page_id %u-%u", (uint32)begin.file,
                (uint32)begin.page);
            GS_THROW_ERROR(ERR_SPACE_OFFLINE, space->ctrl->name, "buf load page failed");
            status = GS_ERROR;
            break;
        }

        offset = (int64)begin.page * DEFAULT_PAGE_SIZE;
        knl_begin_session_wait(session, DB_FILE_SCATTERED_READ, GS_TRUE);

        if (spc_read_datafile(session, df, handle, offset, read_buf, DEFAULT_PAGE_SIZE * count) != GS_SUCCESS) {
            GS_LOG_RUN_ERR("[BUFFER] failed to read datafile %s, offset %lld, size %u, error code %d", df->ctrl->name,
                offset, DEFAULT_PAGE_SIZE * count, errno);
            spc_close_datafile(df, handle);
            knl_end_session_wait(session);
            status = GS_ERROR;
            break;
        }

        knl_end_session_wait(session);

        /* generally, one session can not wait for more than 0xffffffffffffffff us */
        session->stat.disk_read_time += session->wait.usecs;
        session->stat.disk_reads++;
        cm_atomic_inc(&session->kernel->total_io_read);
        g_knl_callback.accumate_io(session, IO_TYPE_READ);

        for (i = 0; i < count; i++) {
            if (ctrl_array[i] == NULL) {
                continue;
            }
            BUF_UNPROTECT_PAGE(ctrl_array[i]->page);
            ret =
                memcpy_sp(ctrl_array[i]->page, DEFAULT_PAGE_SIZE, read_buf + i * DEFAULT_PAGE_SIZE, DEFAULT_PAGE_SIZE);
            knl_securec_check(ret);

            if (!buf_check_load_page(session, ctrl_array[i]->page, ctrl_array[i]->page_id, GS_FALSE)) {
                if (!abr_repair_page_from_standy(session, ctrl_array[i])) {
                    if (ctrl_array[i]->page_id.page == ctrl->page_id.page) {
                        /* record alarm log if repair failed */
                        GS_LOG_ALARM(WARN_PAGECORRUPTED, "{'page-type':'%s','space-name':'%s','file-name':'%s'}",
                            page_type(ctrl->page->type), space->ctrl->name, df->ctrl->name);
                        GS_THROW_ERROR(ERR_PAGE_CORRUPTED, ctrl->page_id.file, ctrl->page_id.page);
                        status = GS_ERROR;
                    }
                    continue;
                }
            }

#if defined(__arm__) || defined(__aarch64__)
            CM_MFENCE;
#endif

            BUF_PROTECT_PAGE(ctrl_array[i]->page);
            ctrl_array[i]->load_status = (uint8)BUF_IS_LOADED;
        }
    } while (0);

    /* set load status and unlatch batch load buffer */
    for (i = 0; i < count; i++) {
        if (ctrl_array[i] == NULL) {
            continue;
        }

        if (ctrl_array[i]->load_status != (uint8)BUF_IS_LOADED) {
            ctrl_array[i]->load_status = (uint8)BUF_LOAD_FAILED;
        }

        /* For the incoming ctrl we do not un_latch it, since the caller wil handl the latch */
        if (ctrl_array[i]->page_id.page != ctrl->page_id.page) {
            buf_unlatch(session, ctrl_array[i], GS_TRUE);
        }
    }

    cm_pop(session->stack);
    return status;
}

/*
 * load pages in batch
 * @param kernel session, current page ctrl, page_id(start load), load count
 * @note only if fail to load current page, can we throw error to caller
 */
static status_t buf_load_pages(knl_session_t *session, buf_ctrl_t *ctrl, page_id_t begin, uint32 total_count)
{
    page_id_t page_id = begin;
    uint32 mpool_page_id;
    uint32 count;

    /* read single page when failed to alloc large page */
    if (total_count == 1 || !mpool_try_alloc_page(session->kernel->attr.large_pool, &mpool_page_id)) {
        return buf_load_page(session, ctrl, ctrl->page_id);
    }

    char *read_buf = mpool_page_addr(session->kernel->attr.large_pool, mpool_page_id);
    uint32 max_count = GS_LARGE_PAGE_SIZE / DEFAULT_PAGE_SIZE;

    while (total_count > 0) {
        count = total_count < max_count ? total_count : max_count;

        if (buf_batch_load_pages(session, read_buf, ctrl, page_id, count) != GS_SUCCESS) {
            mpool_free_page(session->kernel->attr.large_pool, mpool_page_id);
            return GS_ERROR;
        }

        total_count -= count;
        page_id.page += count;
    }

    mpool_free_page(session->kernel->attr.large_pool, mpool_page_id);
    return GS_SUCCESS;
}

static inline uint32 buf_log_entry_length(knl_session_t *session)
{
    uint32 size = session->page_stack.log_begin[session->page_stack.depth - 1];
    size += sizeof(rd_enter_page_t);
    size += LOG_ENTRY_SIZE;
    return size;
}

void buf_clean_log(knl_session_t *session)
{
    uint32 enter_page_size = LOG_ENTRY_SIZE + CM_ALIGN4(sizeof(rd_enter_page_t));
    log_group_t *group = (log_group_t *)(session->log_buf);
    log_entry_t *entry = (log_entry_t *)(session->log_buf + group->size - enter_page_size);

    knl_panic(entry->size == enter_page_size && RD_TYPE_IS_ENTER_PAGE(entry->type));
    group->size -= enter_page_size;
}

#ifdef LOG_DIAG
static void buf_validate_page(knl_session_t *session, buf_ctrl_t *ctrl, bool32 changed)
{
    page_head_t *page = (page_head_t *)session->curr_page;
    char *copy_page = NULL;
    log_group_t *group = NULL;
    uint32 depth;
    errno_t ret;

    switch (page->type) {
        case PAGE_TYPE_HEAP_MAP:
            heap_validate_map(session, page);
            break;

        case PAGE_TYPE_HEAP_DATA:
            heap_validate_page(session, page);
            break;

        case PAGE_TYPE_PCRH_DATA:
            pcrh_validate_page(session, page);
            break;

        case PAGE_TYPE_BTREE_NODE:
            btree_validate_page(session, page);
            break;

        case PAGE_TYPE_PCRB_NODE:
            pcrb_validate_page(session, page);
            return;

        case PAGE_TYPE_LOB_DATA:
            lob_validate_page(session, page);
            break;

        default:
            break; // missed validate function
    }

    if (DB_NOT_READY(session) || DB_IS_READONLY(session)) {
        return;
    }

#ifdef _REPLICATION
    if (KNL_RECOVERY_WITH_GBP(session->kernel) && SESSION_IS_GBP_BG(session)) {
        return;
    }

    /* page first load from gbp */
    if (KNL_RECOVERY_WITH_GBP(session->kernel) && ctrl->gbp_ctrl->is_from_gbp && ctrl->is_dirty &&
        ctrl->gbp_ctrl->gbp_read_version == KNL_GBP_READ_VER(session->kernel) && !changed) {
        return;
    }
#endif
    depth = session->page_stack.depth - 1;

    if (!changed) {
        if (memcmp(session->log_diag_page[depth] + sizeof(page_head_t), session->curr_page + sizeof(page_head_t),
            PAGE_VALID_SIZE) != 0) {
            GS_LOG_DEBUG_WAR("WARNING: leave page with no change, but changed [file: %d, page: %d, type: %d].\n",
                (uint32)AS_PAGID_PTR(page->id)->file, (uint32)AS_PAGID_PTR(page->id)->page, (uint32)page->type);
            knl_panic(0);
        }
        return;
    }

    /* nologging table maybe changed but has no redo, so skip it */
    if (!SPC_IS_LOGGING_BY_PAGEID(ctrl->page_id) || !session->rm->logging) {
        return;
    }

    group = (log_group_t *)(session->log_buf);
    if (buf_log_entry_length(session) >= group->size) {
        GS_LOG_DEBUG_WAR("WARNING: leave page with change, but no log [file: %d, page: %d, type: %d].\n",
            (uint32)AS_PAGID_PTR(page->id)->file, (uint32)AS_PAGID_PTR(page->id)->page, (uint32)page->type);
        knl_panic(0);
    }

    // Check redo log.
    copy_page = (char *)cm_push(session->stack, DEFAULT_PAGE_SIZE);
    ret = memcpy_sp(copy_page, DEFAULT_PAGE_SIZE, session->log_diag_page[depth], DEFAULT_PAGE_SIZE);
    knl_securec_check(ret);

    log_diag_page(session);

    if (memcmp(session->log_diag_page[depth], session->curr_page, DEFAULT_PAGE_SIZE) != 0) {
        if (memcmp(session->log_diag_page[depth], copy_page, DEFAULT_PAGE_SIZE) == 0) {
            GS_LOG_DEBUG_WAR("WARNING: loss log for page [file: %d, page: %d, type: %d].\n",
                (uint32)AS_PAGID_PTR(page->id)->file, (uint32)AS_PAGID_PTR(page->id)->page, (uint32)page->type);
            knl_panic(0);
        } else {
            GS_LOG_DEBUG_WAR("WARNING: diagnose log failed for page [file: %d, page: %d, type: %d].\n",
                (uint32)AS_PAGID_PTR(page->id)->file, (uint32)AS_PAGID_PTR(page->id)->page, (uint32)page->type);
            knl_panic(0);
        }
    }
    cm_pop(session->stack);
}
#endif

static void buf_log_enter_page(knl_session_t *session, buf_ctrl_t *ctrl, latch_mode_t mode, uint8 options)
{
    if (DB_NOT_READY(session)) {
        return;
    }
#ifdef _REPLICATION
    if (SECUREC_UNLIKELY(KNL_RECOVERY_WITH_GBP(session->kernel)) && SESSION_IS_GBP_BG(session)) {
        return;
    }
#endif
    session->page_stack.log_begin[session->page_stack.depth - 1] = ((log_group_t *)session->log_buf)->size;
#ifdef LOG_DIAG
    errno_t ret;
    ret = memcpy_sp(session->log_diag_page[session->page_stack.depth - 1], DEFAULT_PAGE_SIZE, ctrl->page,
        DEFAULT_PAGE_SIZE);
    knl_securec_check(ret);
#endif

    if (DB_IS_READONLY(session)) {
        return;
    }

    if (mode == LATCH_MODE_X) {
        rd_enter_page_t redo;

        redo.page = ctrl->page_id.page;
        redo.file = ctrl->page_id.file;
        redo.pcn = ctrl->page->pcn;
        redo.options = options;

        /* because we replay txn page when do log analysis on standby(gbp enabled), we should identify it */
        if (ctrl->is_resident && ctrl->page->type == PAGE_TYPE_TXN) {
            log_put(session, RD_ENTER_TXN_PAGE, &redo, sizeof(rd_enter_page_t), LOG_ENTRY_FLAG_NONE);
        } else {
            log_put(session, RD_ENTER_PAGE, &redo, sizeof(rd_enter_page_t), LOG_ENTRY_FLAG_NONE);
        }
    }
}

static void buf_log_leave_page(knl_session_t *session, buf_ctrl_t *ctrl, bool32 changed)
{
    log_group_t *group = NULL;

    if (SECUREC_UNLIKELY(DB_NOT_READY(session) || DB_IS_READONLY(session))) {
        return;
    }
#ifdef _REPLICATION
    if (SECUREC_UNLIKELY(KNL_RECOVERY_WITH_GBP(session->kernel)) && SESSION_IS_GBP_BG(session)) {
        return;
    }
#endif
    if (session->page_stack.latch_modes[session->page_stack.depth - 1] == LATCH_MODE_X) {
        group = (log_group_t *)(session->log_buf);
        if (SECUREC_LIKELY(buf_log_entry_length(session) != group->size)) {
#ifdef LOG_DIAG
            /* skip space entry page, because we always record log for entry page even it's nologging */
            if (ctrl->page_id.page > SPACE_ENTRY_PAGE) {
                knl_panic_log(SPC_IS_LOGGING_BY_PAGEID(ctrl->page_id),
                    "the space is not logging table space, panic info: page %u-%u type %u", ctrl->page_id.file,
                    ctrl->page_id.page, ctrl->page->type);
            }
#endif
            /* there is some other log entry behind RD_ENTER_PAGE, it means the page changed */
            if (ctrl->is_resident && ctrl->page->type == PAGE_TYPE_TXN) {
                /* because we replay txn page when do log analysis on standby(gbp enabled), we should identify it */
                log_put(session, RD_LEAVE_TXN_PAGE, &changed, sizeof(bool32), LOG_ENTRY_FLAG_NONE);
            } else {
                log_put(session, RD_LEAVE_PAGE, &changed, sizeof(bool32), LOG_ENTRY_FLAG_NONE);
            }
        } else {
            /* nologging table maybe changed but has no redo, so skip it */
            if (SPC_IS_LOGGING_BY_PAGEID(ctrl->page_id) && session->rm->logging) {
                knl_panic_log(!changed, "the page is changed, panic info: page %u-%u type %u", ctrl->page_id.file,
                    ctrl->page_id.page, ctrl->page->type);
            }
            /* there is only RD_ENTER_PAGE in group, it means the page not changed, we can clean it */
            buf_clean_log(session);
        }
    } else {
        knl_panic_log(!changed, "the page is changed, panic info: page %u-%u type %u", ctrl->page_id.file,
            ctrl->page_id.page, ctrl->page->type);
    }
}

/**
 * This fucnction is used before repair page using backup
 * If page id is not supported or page is not corrupted on disk, we do not repair
 */
status_t buf_validate_corrupted_page(knl_session_t *session, knl_validate_t *param)
{
    page_id_t page_id = param->page_id;

    if (!abr_verify_pageid(session, page_id)) {
        return GS_ERROR;
    }

    if (session->kernel->db.status == DB_STATUS_OPEN) {
        /* If read page successfully, page is not corrupted */
        if (buf_read_page(session, page_id, LATCH_MODE_S, ENTER_PAGE_NORMAL) == GS_SUCCESS) {
            buf_leave_page(session, GS_FALSE);
            return GS_SUCCESS;
        }
    }

    /* *
     * When database status is open, we need check disk page again.
     * Because CRC mode if is FULL, disk page may not be corrupted. We can not repair it
     */
    if (!abr_precheck_corrupted_page(session, page_id)) {
        GS_THROW_ERROR(ERR_PAGE_CORRUPTED, page_id.file, page_id.page);
        return GS_ERROR;
    }

    return GS_SUCCESS;
}

static inline bool32 buf_check_loaded_page_checksum(knl_session_t *session, buf_ctrl_t *ctrl, latch_mode_t mode,
    uint8 options)
{
    if (g_cks_level != CKS_FULL) {
        return GS_TRUE;
    }

    if (!buf_changed_verifiable(session, ctrl, mode, options)) {
        return GS_TRUE;
    }

    return buf_verify_checksum(session, ctrl->page, ctrl->page_id);
}

static inline void buf_try_read_gbp_page(knl_session_t *session, buf_ctrl_t *ctrl)
{
#ifdef _REPLICATION
    if (ctrl->gbp_ctrl->page_status == GBP_PAGE_NOREAD) {
        ctrl->page->lsn = GS_INVALID_LSN; // page is not loaded, set lsn to 0
        ctrl->gbp_ctrl->page_status = GBP_PAGE_NONE;
    }

    if (session_need_read_gbp(session)) {
        buf_check_page_version(session, ctrl); // if local page is old, read page from gbp
    }
#endif
}

static inline void buf_read_compress_update_no_read(knl_session_t *session, buf_ctrl_t *head_ctrl)
{
    for (int32 i = PAGE_GROUP_COUNT - 1; i >= 0; i--) {
        head_ctrl->compress_group[i]->page->type = PAGE_TYPE_FREE_PAGE; // to avoid delayed read to member page
        // that does't hold x lock, but is set to loaded and not formated.
        CM_MFENCE;
        head_ctrl->compress_group[i]->load_status = BUF_IS_LOADED;
#ifdef _REPLICATION
        if (SECUREC_UNLIKELY(KNL_GBP_ENABLE(session->kernel))) {
            head_ctrl->compress_group[i]->gbp_ctrl->page_status = GBP_PAGE_NOREAD;
        }
#endif
    }
}

static status_t buf_read_compress(knl_session_t *session, buf_ctrl_t *ctrl, page_id_t page_id, latch_mode_t mode,
    uint8 options)
{
    if (ctrl->load_status == (uint8)BUF_NEED_LOAD) {
        if (options & ENTER_PAGE_NO_READ) {
            knl_panic_log(PAGE_IS_COMPRESS_HEAD(ctrl->page_id), "buf_read_compress er: non head");
            buf_read_compress_update_no_read(session, ctrl);
            return GS_SUCCESS;
        }
#ifdef _ZSTD
        if (buf_load_group(session, ctrl) != GS_SUCCESS) {
            return GS_ERROR;
        }
#endif
    } else {
        if (!buf_check_loaded_page_checksum(session, ctrl, mode, options)) {
            GS_THROW_ERROR(ERR_PAGE_CORRUPTED, ctrl->page_id.file, ctrl->page_id.page);
            return GS_ERROR;
        }
    }

    return GS_SUCCESS;
}

static status_t buf_read_normal(knl_session_t *session, buf_ctrl_t *ctrl, page_id_t page_id, latch_mode_t mode,
    uint8 options)
{
    if (ctrl->load_status == (uint8)BUF_NEED_LOAD) {
        if (options & ENTER_PAGE_NO_READ) {
            ctrl->load_status = (uint8)BUF_IS_LOADED;
#ifdef _REPLICATION
            if (SECUREC_UNLIKELY(KNL_GBP_ENABLE(session->kernel))) {
                ctrl->gbp_ctrl->page_status = GBP_PAGE_NOREAD;
            }
#endif
            return GS_SUCCESS;
        }

        if (buf_load_page(session, ctrl, page_id) != GS_SUCCESS) {
            return GS_ERROR;
        }
    } else {
        if (!buf_check_loaded_page_checksum(session, ctrl, mode, options)) {
            GS_THROW_ERROR(ERR_PAGE_CORRUPTED, ctrl->page_id.file, ctrl->page_id.page);
            return GS_ERROR;
        }
    }

    return GS_SUCCESS;
}

status_t buf_read_page(knl_session_t *session, page_id_t page_id, latch_mode_t mode, uint8 options)
{
    buf_ctrl_t *ctrl = NULL;
    knl_buf_wait_t temp_stat;
    status_t status;

    if (SECUREC_UNLIKELY(IS_INVALID_PAGID(page_id))) {
        GS_LOG_RUN_ERR("[BUFFER] invalid page_id %u-%u", (uint32)page_id.file, (uint32)page_id.page);
        GS_THROW_ERROR(ERR_INVALID_PAGE_ID, "");
        return GS_ERROR;
    }
#ifdef _STATISTICS
    stats_buf_init(session, &temp_stat);
#endif

    ctrl = page_compress(session, page_id) ? buf_alloc_compress(session, page_id, mode, options) :
                                             buf_alloc_ctrl(session, page_id, mode, options);
    if (SECUREC_UNLIKELY(ctrl == NULL)) {
        knl_panic_log(options & ENTER_PAGE_TRY, "options is invalid, panic info: page %u-%u type %u",
            ctrl->page_id.file, ctrl->page_id.page, ctrl->page->type);
        session->curr_page = NULL;
        session->curr_page_ctrl = NULL;
        return GS_SUCCESS;
    }

    BUF_UNPROTECT_PAGE(ctrl->page);
    status = page_compress(session, page_id) ? buf_read_compress(session, ctrl, page_id, mode, options) :
                                               buf_read_normal(session, ctrl, page_id, mode, options);
    if (status != GS_SUCCESS) {
        buf_unlatch(session, ctrl, GS_TRUE);
        return GS_ERROR;
    }

    knl_panic_log(IS_SAME_PAGID(page_id, ctrl->page_id),
        "page_id and ctrl's page_id are not same, panic info: page %u-%u ctrl page %u-%u type %u", page_id.file,
        page_id.page, ctrl->page_id.file, ctrl->page_id.page, ctrl->page->type);
#ifdef _REPLICATION
    if (SECUREC_UNLIKELY(KNL_GBP_ENABLE(session->kernel))) {
        buf_try_read_gbp_page(session, ctrl);
    }
#endif
    session->curr_page = (char *)ctrl->page;
    session->curr_page_ctrl = ctrl;
    session->stat.buffer_gets++;

#ifdef __PROTECT_BUF__
    if (mode != LATCH_MODE_X && !ctrl->is_readonly) {
        BUF_PROTECT_PAGE(ctrl->page);
    }
#endif

#ifdef _STATISTICS
    stats_buf_record(session, &temp_stat, ctrl);
#endif

    buf_push_page(session, ctrl, mode);
    buf_log_enter_page(session, ctrl, mode, options);

    return GS_SUCCESS;
}

static status_t buf_read_prefetch_compress(knl_session_t *session, buf_ctrl_t *ctrl, page_id_t page_id,
    latch_mode_t mode, uint8 options)
{
    return buf_read_compress(session, ctrl, page_id, mode, options);
}

static status_t buf_read_prefetch_normal(knl_session_t *session, buf_ctrl_t *ctrl, page_id_t page_id, latch_mode_t mode,
    uint8 options)
{
    datafile_t *df = DATAFILE_GET(page_id.file);
    space_t *space = SPACE_GET(df->space_id);
    uint32 start_id = spc_first_extent_id(session, space, page_id);
    page_id_t first = page_id;
    uint32 load_count;

    if (page_id.page >= start_id) {
        first.page = page_id.page - ((page_id.page - start_id) % BUF_PREFETCH_UNIT);
        first.aligned = 0;
        load_count = BUF_PREFETCH_UNIT;
    } else {
        load_count = 1;
    }

    if (ctrl->load_status == (uint8)BUF_NEED_LOAD) {
        if (buf_load_pages(session, ctrl, first, load_count) != GS_SUCCESS) {
            return GS_ERROR;
        }
    } else {
        if (!buf_check_loaded_page_checksum(session, ctrl, mode, options)) {
            GS_THROW_ERROR(ERR_PAGE_CORRUPTED, ctrl->page_id.file, ctrl->page_id.page);
            return GS_ERROR;
        }
    }

    return GS_SUCCESS;
}
status_t buf_read_prefetch_page(knl_session_t *session, page_id_t page_id, latch_mode_t mode, uint8 options)
{
    knl_panic_log(!(options & ENTER_PAGE_NO_READ), "buf_read_prefetch_page er:not no read");
    buf_ctrl_t *ctrl = NULL;
    knl_buf_wait_t temp_stat;
    status_t status;

    if (SECUREC_UNLIKELY(IS_INVALID_PAGID(page_id))) {
        GS_LOG_RUN_ERR("[BUFFER] invalid page_id %u-%u", (uint32)page_id.file, (uint32)page_id.page);
        GS_THROW_ERROR(ERR_INVALID_PAGE_ID, "");
        return GS_ERROR;
    }
#ifdef _REPLICATION
    /* RTO = 0, disable prefetch when db is recovery from GBP */
    if (KNL_RECOVERY_WITH_GBP(session->kernel)) {
        return buf_read_page(session, page_id, mode, options);
    }
#endif
#ifdef _STATISTICS
    stats_buf_init(session, &temp_stat);
#endif

    if (page_compress(session, page_id)) {
        ctrl = buf_alloc_compress(session, page_id, mode, options);
        knl_panic_log(ctrl != NULL, "ctrl alloc failed, panic info: page %u-%u", page_id.file, page_id.page);
        status = buf_read_prefetch_compress(session, ctrl, page_id, mode, options);
    } else {
        ctrl = buf_alloc_ctrl(session, page_id, mode, options);
        knl_panic_log(ctrl != NULL, "ctrl alloc failed, panic info: page %u-%u", page_id.file, page_id.page);
        status = buf_read_prefetch_normal(session, ctrl, page_id, mode, options);
    }

    if (status != GS_SUCCESS) {
        buf_unlatch(session, ctrl, GS_TRUE);
        return GS_ERROR;
    }

    knl_panic_log(IS_SAME_PAGID(page_id, ctrl->page_id),
        "page_id and ctrl's page_id are not same, panic info: page %u-%u ctrl page %u-%u type %u", page_id.file,
        page_id.page, ctrl->page_id.file, ctrl->page_id.page, ctrl->page->type);

    session->curr_page = (char *)ctrl->page;
    session->curr_page_ctrl = ctrl;
    session->stat.buffer_gets++;

#ifdef _STATISTICS
    stats_buf_record(session, &temp_stat, ctrl);
#endif
    buf_push_page(session, ctrl, mode);
    buf_log_enter_page(session, ctrl, mode, options);

    if (session->kernel->attr.enable_asynch) {
        if (buf_try_prefetch_next_ext(session, ctrl) != GS_SUCCESS) {
            GS_LOG_DEBUG_WAR("[BUFFER] failed to prefetch next extent file : %u , page: %llu",
                (uint32)ctrl->page_id.file, (uint64)ctrl->page_id.page);
        }
    }

    return GS_SUCCESS;
}

status_t buf_read_prefetch_page_num(knl_session_t *session, page_id_t page_id, uint32 prefetch_num, latch_mode_t mode,
    uint8 options)
{
    buf_ctrl_t *ctrl = NULL;
    space_t *space = NULL;
    datafile_t *df = NULL;
    uint32 hwm;
    knl_buf_wait_t temp_stat;

    if (SECUREC_UNLIKELY(IS_INVALID_PAGID(page_id))) {
        GS_LOG_RUN_ERR("[BUFFER] invalid page_id %u-%u", (uint32)page_id.file, (uint32)page_id.page);
        GS_THROW_ERROR(ERR_INVALID_PAGE_ID, "");
        return GS_ERROR;
    }
#ifdef _REPLICATION
    /* RTO = 0, disable prefetch when db is recovery from GBP */
    if (KNL_RECOVERY_WITH_GBP(session->kernel)) {
        return buf_read_page(session, page_id, mode, options);
    }
#endif
#ifdef _STATISTICS
    stats_buf_init(session, &temp_stat);
#endif
    ctrl = buf_alloc_ctrl(session, page_id, mode, options);
    knl_panic_log(ctrl != NULL, "ctrl alloc failed, panic info: page %u-%u", page_id.file, page_id.page);

    if (ctrl->load_status == (uint8)BUF_NEED_LOAD) {
        df = DATAFILE_GET(page_id.file);
        space = SPACE_GET(df->space_id);
        hwm = space->head->hwms[df->file_no]; // do not need SPACE_HEAD_RESIDENT

        if (prefetch_num > BUF_MAX_PREFETCH_NUM) {
            prefetch_num = BUF_MAX_PREFETCH_NUM;
        }

        /* the page no is less than hwm forever */
        if (page_id.page + prefetch_num > hwm) {
            prefetch_num = hwm - page_id.page;
        }

        if (buf_load_pages(session, ctrl, page_id, prefetch_num) != GS_SUCCESS) {
            buf_unlatch(session, ctrl, GS_TRUE);
            return GS_ERROR;
        }
    } else {
        if (!buf_check_loaded_page_checksum(session, ctrl, mode, options)) {
            GS_THROW_ERROR(ERR_PAGE_CORRUPTED, ctrl->page_id.file, ctrl->page_id.page);
            buf_unlatch(session, ctrl, GS_TRUE);
            return GS_ERROR;
        }
    }

    knl_panic_log(IS_SAME_PAGID(page_id, ctrl->page_id),
        "page_id and ctrl's page_id are not same, panic info: page %u-%u ctrl_page %u-%u type %u", page_id.file,
        page_id.page, ctrl->page_id.file, ctrl->page_id.page, ctrl->page->type);

    session->curr_page = (char *)ctrl->page;
    session->curr_page_ctrl = ctrl;
    session->stat.buffer_gets++;

#ifdef _STATISTICS
    stats_buf_record(session, &temp_stat, ctrl);
#endif
    buf_push_page(session, ctrl, mode);
    buf_log_enter_page(session, ctrl, mode, options);

    return GS_SUCCESS;
}

/*
 * log_set_page_lsn is the last time modify for page when db is open,
 * but it's not the list time modify when db rcy,need checksum again when redo change page
 */
static inline void buf_calc_checksum(knl_session_t *session, buf_ctrl_t *ctrl)
{
    // checksum is invalid if page has changed
    knl_panic_log(PAGE_SIZE(*ctrl->page) != 0, "the page size is abnormal, panic info: page %u-%u type %u size %u",
        ctrl->page_id.file, ctrl->page_id.page, ctrl->page->type, PAGE_SIZE(*ctrl->page));
    PAGE_CHECKSUM(ctrl->page, PAGE_SIZE(*ctrl->page)) = GS_INVALID_CHECKSUM;

    if (!DB_IS_CHECKSUM_FULL(session)) {
        return;
    }

    page_calc_checksum(ctrl->page, PAGE_SIZE(*ctrl->page));
}

void buf_leave_page(knl_session_t *session, bool32 changed)
{
    buf_ctrl_t *ctrl = buf_curr_page(session);

    if (SECUREC_UNLIKELY(ctrl == NULL)) {
        buf_pop_page(session);
        return;
    }

    /* if page is allocated without initialized, then page->size_units=0 */
    if (DB_TO_RECOVERY(session) && ctrl->page->size_units != 0) {
        knl_panic_log(CHECK_PAGE_PCN(ctrl->page), "page pcn is abnormal, panic info: page %u-%u type %u",
            ctrl->page_id.file, ctrl->page_id.page, ctrl->page->type);
    }

#ifdef LOG_DIAG
    buf_validate_page(session, ctrl, changed);
#endif

    if (changed) {
        knl_panic_log(PAGE_SIZE(*ctrl->page) != 0, "the page size is abnormal, panic info: page %u-%u type %u size %u",
            ctrl->page_id.file, ctrl->page_id.page, ctrl->page->type, PAGE_SIZE(*ctrl->page));
        ctrl->page->pcn++;
        PAGE_TAIL(ctrl->page)->pcn++;

        if (!ctrl->is_dirty) {
            ctrl->is_dirty = 1;
            if (session->dirty_count > 0) {
                session->dirty_pages[session->dirty_count - 1]->ckpt_next = ctrl;
                ctrl->ckpt_prev = session->dirty_pages[session->dirty_count - 1];
            }
            session->dirty_pages[session->dirty_count++] = ctrl;
        }

        if (!ctrl->is_readonly) {
            ctrl->is_readonly = 1;
            session->changed_pages[session->changed_count++] = ctrl;
            knl_panic_log(session->changed_count <= KNL_MAX_ATOMIC_PAGES,
                "the changed page count of current session "
                "is abnormal, panic info: page %u-%u type %u changed_count %u",
                ctrl->page_id.file, ctrl->page_id.page, ctrl->page->type, session->changed_count);
        }
#ifdef _REPLICATION
        if (SECUREC_UNLIKELY(KNL_GBP_ENABLE(session->kernel))) {
            ctrl->gbp_ctrl->page_status = GBP_PAGE_NONE;
            if (!ctrl->gbp_ctrl->is_gbpdirty && DB_IS_PRIMARY(&session->kernel->db)) {
                ctrl->gbp_ctrl->is_gbpdirty = GS_TRUE;
                session->gbp_dirty_pages[session->gbp_dirty_count++] = ctrl;
                knl_panic_log(session->gbp_dirty_count <= KNL_MAX_ATOMIC_PAGES,
                    "gbp_dirty_count is abnormal, panic info: page %u-%u type %u gbp_dirty_count %u",
                    ctrl->page_id.file, ctrl->page_id.page, ctrl->page->type, session->gbp_dirty_count);
            }
        }
#endif
        if (SECUREC_UNLIKELY(DB_NOT_READY(session))) {
            buf_calc_checksum(session, ctrl);
        }

        session->stat.db_block_changes++;
    }

    buf_log_leave_page(session, ctrl, changed);
    buf_unlatch(session, ctrl, GS_TRUE);
    buf_pop_page(session);
}

void buf_unreside(knl_session_t *session, buf_ctrl_t *ctrl)
{
    buf_set_t *set = &session->kernel->buf_ctx.buf_set[ctrl->buf_pool_id];
    buf_bucket_t *bucket = BUF_GET_BUCKET(set, ctrl->bucket_id);

    cm_spin_lock(&bucket->lock, &session->stat_bucket);
    if (ctrl->is_resident) {
        ctrl->is_resident = 0;
    }
    cm_spin_unlock(&bucket->lock);
}

void buf_unreside_page(knl_session_t *session, page_id_t page_id)
{
    buf_ctrl_t *ctrl = NULL;

    if (IS_INVALID_PAGID(page_id)) {
        return;
    }

    if (buf_read_page(session, page_id, LATCH_MODE_S, ENTER_PAGE_RESIDENT) != GS_SUCCESS) {
        return;
    }
    ctrl = session->curr_page_ctrl;
    buf_leave_page(session, GS_FALSE);
    buf_unreside(session, ctrl);
}

/*
 * for temp page in buf_enter_temp_page
 * 1) we don't record redo log
 * 2) we don't read page
 */
void buf_enter_temp_page(knl_session_t *session, page_id_t page_id, latch_mode_t mode, uint8 options)
{
    buf_ctrl_t *ctrl = NULL;

    if (SECUREC_UNLIKELY(page_id.page == GS_INVALID_ID32)) {
        knl_panic_log(0, "page number is invalid, panic info: page %u-%u", page_id.file, page_id.page);
    }

    ctrl = buf_alloc_ctrl(session, page_id, mode, options);
    knl_panic_log(ctrl != NULL, "ctrl alloc failed, panic info: page %u-%u", page_id.file, page_id.page);

    if (ctrl->load_status != (uint8)BUF_IS_LOADED) {
        ctrl->load_status = (uint8)BUF_IS_LOADED;
    }

    session->curr_page = (char *)ctrl->page;
    session->curr_page_ctrl = ctrl;

    buf_push_page(session, ctrl, mode);

#ifdef __PROTECT_BUF__
    if (mode == LATCH_MODE_X) {
        BUF_UNPROTECT_PAGE(ctrl->page);
    }
#endif
}

/*
 * for temp page in buf_leave_temp_page
 * 1) we don't record redo log
 */
void buf_leave_temp_page(knl_session_t *session)
{
    buf_ctrl_t *ctrl = buf_curr_page(session);
    if (ctrl == NULL) {
        buf_pop_page(session);
        return;
    }

    BUF_PROTECT_PAGE(ctrl->page);

    buf_unlatch(session, ctrl, GS_TRUE);
    buf_pop_page(session);
}
