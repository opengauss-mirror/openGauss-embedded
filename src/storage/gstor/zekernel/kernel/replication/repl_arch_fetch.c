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
 * repl_arch_fetch.c
 *    implement of fetch archive logfile thread
 *
 * IDENTIFICATION
 *    src/storage/gstor/zekernel/kernel/replication/repl_arch_fetch.c
 *
 * -------------------------------------------------------------------------
 */
#include "repl_arch_fetch.h"
#include "cm_file.h"
#include "knl_context.h"

#define LFTC_SUPPORT_COMPRESS      1

status_t lftc_write_stream(cs_pipe_t *pipe, rep_msg_type_t type, uint32 size, const char *data, int32 max_pkg_size)
{
    rep_msg_header_t msg;

    msg.size = size;
    msg.type = type;
    if (cs_write_stream(pipe, (char *)&msg, sizeof(rep_msg_header_t), max_pkg_size) != GS_SUCCESS) {
        GS_LOG_RUN_ERR("[Log Fetcher] failed to send message to standby");
        return GS_ERROR;
    }

    if (cs_write_stream(pipe, data, size, max_pkg_size) != GS_SUCCESS) {
        GS_LOG_RUN_ERR("[Log Fetcher] failed to send message to standby");
        return GS_ERROR;
    }

    return GS_SUCCESS;
}

status_t lftc_send_req(cs_pipe_t *pipe, uint32 size, const char *data, int32 max_pkg_size)
{
    rep_msg_header_t msg;

    msg.size = sizeof(rep_msg_header_t) + LFTC_SUPPORT_COMPRESS;
    msg.type = REP_ARCH_REQ;
    if (cs_write_stream(pipe, (char *)&msg, sizeof(rep_msg_header_t), max_pkg_size) != GS_SUCCESS) {
        GS_LOG_RUN_ERR("[Log Fetcher] failed to send message to primary");
        return GS_ERROR;
    }

    if (cs_write_stream(pipe, data, size, max_pkg_size) != GS_SUCCESS) {
        GS_LOG_RUN_ERR("[Log Fetcher] failed to send message to primary");
        return GS_ERROR;
    }

    return GS_SUCCESS;
}

static inline status_t lftc_zstd_compress(lftc_srv_ctx_t *ctx, const char *buf, uint32 data_size)
{
    ctx->cmp_ctx.data_size = (uint32)ZSTD_compress(ctx->cmp_ctx.compress_buf.aligned_buf, 
        ctx->cmp_ctx.buf_size, buf, data_size, 1);
    if (ZSTD_isError(ctx->cmp_ctx.data_size)) {
        GS_LOG_RUN_ERR("[Log Fetcher] failed to compress(zstd) archive log message");
        return GS_ERROR;
    }
    return GS_SUCCESS;
}

static inline status_t lftc_zstd_decompress(lftc_clt_task_t *task, const char *buf, uint32 size, uint32 *data_size)
{
    *data_size = (uint32)ZSTD_decompress(task->msg_buf.aligned_buf, task->msg_buf_size, buf, size);
    if (ZSTD_isError(*data_size)) {
        GS_LOG_RUN_ERR("[Log Fetcher] failed to decompress(zstd) archive log message");
        return GS_ERROR;
    }

    return GS_SUCCESS;
}

static inline status_t lftc_lz4_compress(lftc_srv_ctx_t *ctx, const char *buf, uint32 data_size)
{
    ctx->cmp_ctx.data_size = (uint32)LZ4_compress_default(buf, ctx->cmp_ctx.compress_buf.aligned_buf,
        (int32)data_size, (int32)ctx->cmp_ctx.buf_size);
    if (ctx->cmp_ctx.data_size == 0) {
        GS_LOG_RUN_ERR("[Log Fetcher] failed to compress(lz4) archive log message");
        return GS_ERROR;
    }
    return GS_SUCCESS;
}

static inline status_t lftc_lz4_decompress(lftc_clt_task_t *task, const char *buf, uint32 size, uint32 *data_size)
{
    int result = LZ4_decompress_safe(buf, task->msg_buf.aligned_buf, (int32)size, (int32)task->msg_buf_size);
    if (result <= 0) {
        GS_LOG_RUN_ERR("[Log Fetcher] failed to decompress(lz4) archive log message");
        return GS_ERROR;
    }
    *data_size = (uint32)result;
    return GS_SUCCESS;
}

static status_t lftc_compress_send_data(lftc_srv_ctx_t *ctx, uint32 data_size, int32 max_pkg_size)
{
    lftc_file_ctx_t *file_ctx = &ctx->file_ctx;
    lftc_cmp_ctx_t *cmp_ctx = &ctx->cmp_ctx;
    rep_msg_type_t data_type;

    switch (ctx->compress_alg) {
        case COMPRESS_ZSTD:
            if (lftc_zstd_compress(ctx, file_ctx->msg_buf.aligned_buf, data_size) != GS_SUCCESS) {
                return GS_ERROR;
            }
            data_type = REP_LFTC_ZSTD_DATA;
            break;
        case COMPRESS_LZ4:
            if (lftc_lz4_compress(ctx, file_ctx->msg_buf.aligned_buf, data_size) != GS_SUCCESS) {
                return GS_ERROR;
            }
            data_type = REP_LFTC_LZ4_DATA;
            break;
        default:
            GS_LOG_RUN_ERR("[Log Fetcher] unknown compress algorithm.");
            return GS_ERROR;
    }

    if (lftc_write_stream(file_ctx->pipe, data_type, cmp_ctx->data_size,
        cmp_ctx->compress_buf.aligned_buf, max_pkg_size) != GS_SUCCESS) {
        return GS_ERROR;
    }

    return GS_SUCCESS;
}

static status_t lftc_send_data(lftc_srv_ctx_t *ctx, uint32 data_size, int32 max_pkg_size)
{
    lftc_file_ctx_t *file_ctx = &ctx->file_ctx;

    if (ctx->compress_alg == COMPRESS_NONE) {
        if (lftc_write_stream(file_ctx->pipe, REP_ARCH_DATA, data_size,
            file_ctx->msg_buf.aligned_buf, max_pkg_size) != GS_SUCCESS) {
            return GS_ERROR;
        }
    } else {
        if (lftc_compress_send_data(ctx, data_size, max_pkg_size) != GS_SUCCESS) {
            return GS_ERROR;
        }
    }

    return GS_SUCCESS;
}

static status_t lftc_decompress_receive_data(lftc_clt_task_t *task, rep_msg_header_t *msg, uint32 *data_size)
{
    int32 comp_size;

    if (cs_read_stream(&task->pipe, task->cmp_ctx.compress_buf.aligned_buf, GS_INVALID_ID32,
        msg->size, &comp_size) != GS_SUCCESS) {
        GS_LOG_RUN_ERR("[Log Fetcher] failed to receive message from primary");
        return GS_ERROR;
    }

    if (comp_size != msg->size) {
        GS_LOG_RUN_ERR("[Log Fetcher] invalid comp_size %u received, expected size is %u",
                       (uint32)comp_size, (uint32)msg->size);
        return GS_ERROR;
    }

    switch (msg->type) {
        case REP_LFTC_LZ4_DATA:
            return lftc_lz4_decompress(task, task->cmp_ctx.compress_buf.aligned_buf, (uint32)comp_size, data_size);
        case REP_LFTC_ZSTD_DATA:
            return lftc_zstd_decompress(task, task->cmp_ctx.compress_buf.aligned_buf, (uint32)comp_size, data_size);
        default:
            GS_LOG_RUN_ERR("[Log Fetcher] unknown decompress type.");
            return GS_ERROR;
    }
}

status_t lftc_clt_send_request(lftc_clt_task_t *task)
{
    cs_pipe_t *pipe = &task->pipe;
    lftc_clt_req_t request;

    request.asn = task->asn;
    request.rst_id = task->rst_id;
    request.offset = task->offset;

    GS_LOG_RUN_INF("[Log Fetcher] cli thread send archived info [%u/%u/%llu] ",
        request.rst_id, request.asn, request.offset);

    return lftc_send_req(pipe, sizeof(lftc_clt_req_t), (char *)&request,
        (int32)cm_atomic_get(&task->session->kernel->attr.repl_pkg_size));
}

status_t lftc_clt_connect_server(lftc_clt_task_t *task)
{
    char url[GS_HOST_NAME_BUFFER_SIZE + GS_TCP_PORT_MAX_LENGTH + 1] = {0};     
    char server_host[GS_HOST_NAME_BUFFER_SIZE];
    char bind_host[GS_HOST_NAME_BUFFER_SIZE];
    uint16 port;
    cs_pipe_t *pipe = &task->pipe;
    errno_t err;

    if (lrcv_get_primary_server(task->session, (int32)GS_INVALID_ID32, server_host, GS_HOST_NAME_BUFFER_SIZE,
        &port) != GS_SUCCESS) { 
        return GS_ERROR;
    }

    err = snprintf_s(url, sizeof(url), sizeof(url) - 1, "%s:%u", server_host, (uint32)port);
    knl_securec_check_ss(err);

    pipe->connect_timeout = REPL_CONNECT_TIMEOUT;
    pipe->socket_timeout = REPL_SOCKET_TIMEOUT;
    arch_get_bind_host(task->session, server_host, bind_host, GS_HOST_NAME_BUFFER_SIZE);
    if (cs_connect(url, pipe, bind_host, NULL, NULL) != GS_SUCCESS) {
        cs_disconnect(pipe);
        GS_LOG_RUN_ERR("[Log Fetcher] failed to connect %s", url);
        return GS_ERROR;
    }

    if (knl_login(task->session, pipe, REP_LOGIN_LFTC, (const char *)bind_host, NULL) != GS_SUCCESS) {
        GS_LOG_RUN_ERR("[Log Fetcher] failed to login primary lftc server thread");
        return GS_ERROR;
    }

    return GS_SUCCESS;
}

status_t lftc_check_connect(lftc_clt_task_t *task)
{
    return lftc_clt_connect_server(task);
}

static void lftc_clt_clean_task(lftc_clt_task_t *task)
{
    cm_release_thread(&task->thread);
    cm_close_file(task->handle);
    task->handle = INVALID_FILE_HANDLE;
    knl_disconnect(&task->pipe);
    cm_spin_lock(&task->lock, NULL);
    task->is_running = GS_FALSE;
    task->canceled = GS_FALSE;
    cm_spin_unlock(&task->lock);
}

static status_t lftc_clt_open_tmp(lftc_clt_task_t *task, const char *tmp_file)
{
    log_file_head_t log_head;
    int32 read_size;
    errno_t err;

    if (!cm_file_exist(tmp_file)) {
        if (cm_create_file(tmp_file, O_BINARY | O_SYNC | O_RDWR | O_EXCL, &task->handle) != GS_SUCCESS) {
            GS_LOG_RUN_ERR("[Log Fetcher] failed to create temp archived log file %s", tmp_file);
            return GS_ERROR;
        }
        task->offset = 0;
        return GS_SUCCESS;
    }

    err = strcpy_sp(task->tmp_file_name, GS_FILE_NAME_BUFFER_SIZE + 4, tmp_file); /* 4 bytes for ".tmp" */
    knl_securec_check(err);

    if (cm_open_file(tmp_file, O_BINARY | O_SYNC | O_RDWR, &task->handle) != GS_SUCCESS) {
        GS_LOG_RUN_ERR("[Log Fetcher] failed to open temp archived log file %s", tmp_file);
        return GS_ERROR;
    }

    if (cm_read_file(task->handle, (void *)&log_head, sizeof(log_file_head_t), &read_size) != GS_SUCCESS) {
        GS_LOG_RUN_ERR("[Log Fetcher] failed to read temp archived log file %s", tmp_file);
        return GS_ERROR;
    }

    if ((uint32)read_size < sizeof(log_file_head_t)) {
        task->offset = 0;
        return GS_SUCCESS;
    }

    if (log_head.rst_id != task->rst_id || log_head.asn != task->asn) {
        GS_LOG_RUN_ERR("[Log Fetcher] wrong tempory archive file %s exists.", tmp_file);
        return GS_ERROR;
    }

    task->offset = log_head.write_pos;

    return GS_SUCCESS;
}

static void lftc_wait_tasks_finished(lftc_clt_ctx_t *ctx)
{
    uint32 task_cnt;

    for (;;) {
        task_cnt = 0;
        for (uint32 i = 0; i < ctx->hwm; i++) {
            if (!ctx->tasks[i].is_running) {
                task_cnt++;
            }
        }

        if (task_cnt == ctx->hwm) {
            return;
        }

        cm_sleep(10);
    }
}

void lftc_clt_close(knl_session_t *session)
{
    knl_instance_t *kernel = session->kernel;
    lftc_clt_ctx_t *ctx = &kernel->lftc_client_ctx;

    for (uint32 i = 0; i < ctx->hwm; i++) {
        ctx->tasks[i].canceled = GS_TRUE;
    }

    lftc_wait_tasks_finished(ctx);

    for (uint32 i = 0; i < ctx->hwm; i++) {
        /*
         * Log fetcher thread has detached itself before thread exit,
         * so the stack resources it owns will be freed automatically.
         */
        ctx->tasks[i].canceled = GS_FALSE;
        cm_aligned_free(&ctx->tasks[i].msg_buf);
        cm_aligned_free(&ctx->tasks[i].cmp_ctx.compress_buf);
    }
    ctx->hwm = 0;
    GS_LOG_RUN_ERR("[Log Fetcher] log fetcher context close finish.");
}

status_t lftc_srv_send_head(lftc_srv_ctx_t *lftc_server_ctx, const char *arch_file_name)
{
    cs_pipe_t *pipe = lftc_server_ctx->pipe;
    lftc_file_ctx_t *file_ctx = &lftc_server_ctx->file_ctx;
    int32 read_size;

    if (cm_seek_file(file_ctx->handle, 0, SEEK_SET) != 0) {
        GS_LOG_RUN_ERR("[Log Fetcher] failed to seek log file %s", arch_file_name);
        return GS_ERROR;
    }

    if (cm_read_file(file_ctx->handle, (void *)&file_ctx->log_head, sizeof(log_file_head_t),
                     &read_size) != GS_SUCCESS) {
        GS_LOG_RUN_ERR("[Log Fetcher] failed to read log file %s", arch_file_name);
        return GS_ERROR;
    }

    if ((uint32)read_size < sizeof(log_file_head_t)) {
        GS_LOG_RUN_ERR("[Log Fetcher] failed to read archive log head %s", arch_file_name);
        return GS_ERROR;
    }

    return lftc_write_stream(pipe, REP_ARCH_HEAD, sizeof(log_file_head_t), (char *)&file_ctx->log_head,
        (int32)cm_atomic_get(&lftc_server_ctx->session->kernel->attr.repl_pkg_size));
}

/*
 * lftc server receive  lftc_client request and send receive
 * @param[in] lftc_server_ctx - lftc_server_context_t
 * return
 * - GS_SUCCESS
 * - GS_ERROR
 */
status_t lftc_srv_proc_request(knl_session_t *session, lftc_srv_ctx_t *lftc_ctx)
{
    lftc_file_ctx_t *file_ctx = &lftc_ctx->file_ctx;
    lftc_clt_req_t request;
    char arch_file_name[GS_FILE_NAME_BUFFER_SIZE];
    int32 recv_size;

    if (cs_read_stream(lftc_ctx->pipe, (char *)&request, GS_INVALID_ID32,
        sizeof(lftc_clt_req_t), &recv_size) != GS_SUCCESS) {
        GS_LOG_RUN_ERR("[Log Fetcher] failed to receive message from standby");
        return GS_ERROR;
    }
    knl_panic(sizeof(lftc_clt_req_t) == recv_size);

    GS_LOG_RUN_INF("[Log Fetcher] server thread get archived info [%u/%u/%llu] ", request.rst_id, request.asn,
                   request.offset);

    if (!arch_get_archived_log_name(lftc_ctx->session, request.rst_id, request.asn, ARCH_DEFAULT_DEST,
        arch_file_name, GS_FILE_NAME_BUFFER_SIZE)) {
        arch_set_archive_log_name(session, request.rst_id, request.asn, ARCH_DEFAULT_DEST,
            arch_file_name, GS_FILE_NAME_BUFFER_SIZE);

        if (!arch_log_not_archived(session, request.rst_id, request.asn) && !cm_file_exist(arch_file_name)) {
            rep_msg_header_t msg;
            msg.size = sizeof(rep_msg_header_t);
            msg.type = REP_ARCH_LOST;

            GS_LOG_RUN_INF("[Log Fetcher] archive log [%u-%u] lost on primary, send REP_ARCH_LOST message to standby",
                request.rst_id, request.asn);
            if (cs_write_stream(lftc_ctx->pipe, (char *)&msg, sizeof(rep_msg_header_t),
                (int32)cm_atomic_get(&session->kernel->attr.repl_pkg_size)) != GS_SUCCESS) {
                GS_LOG_RUN_ERR("[Log Fetcher] failed to send REP_ARCH_LOST message to standby");
            }

            return GS_ERROR;
        }

        GS_LOG_RUN_ERR("[Log Fetcher] failed to get file name for archived log file[%u-%u] temporarily",
            request.rst_id, request.asn);
        return GS_ERROR;
    }

    file_ctx->offset = MAX(request.offset, sizeof(log_file_head_t));
    file_ctx->pipe = lftc_ctx->pipe;

    if (cm_open_file(arch_file_name, O_BINARY | O_SYNC | O_RDWR, &file_ctx->handle) != GS_SUCCESS) {
        GS_LOG_RUN_ERR("[Log Fetcher] failed to open arch file %s on primary.", arch_file_name);
        return GS_ERROR;
    }

    if (lftc_srv_send_head(lftc_ctx, arch_file_name) != GS_SUCCESS) {
        GS_LOG_RUN_ERR("[Log Fetcher] failed to send log file head to standby");
        return GS_ERROR;
    }

    return GS_SUCCESS;
}

/*
 * lftc_server send logfile to lftc_client
 * @param[in] lftc_server_ctx - lftc_server_context_t
 * @result
 * - GS_SUCCESS
 * - GS_ERROR
 */
status_t lftc_srv_send_file(lftc_srv_ctx_t *lftc_ctx)
{
    lftc_file_ctx_t *file_ctx = &lftc_ctx->file_ctx;
    int32 data_size;

    if (cm_seek_file(file_ctx->handle, file_ctx->offset, SEEK_SET) != (int64)file_ctx->offset) {
        GS_LOG_RUN_ERR("[Log Fetcher] failed to seek file, offset:%llu, origin:%d", file_ctx->offset, SEEK_SET);
        return GS_ERROR;
    }

    while (!lftc_ctx->thread.closed) {
        if (cm_read_file(file_ctx->handle, file_ctx->msg_buf.aligned_buf, file_ctx->msg_buf_size,
                         &data_size) != GS_SUCCESS) {
            GS_LOG_RUN_ERR("[Log Fetcher] failed to read log file with actual read size %u", data_size);
            return GS_ERROR;
        }

        if (data_size > 0) {
            if (lftc_send_data(lftc_ctx, (uint32)data_size,
                (int32)cm_atomic_get(&lftc_ctx->session->kernel->attr.repl_pkg_size)) != GS_SUCCESS) {
                return GS_ERROR;
            }
        }

        file_ctx->offset += (uint64)data_size;
        if ((uint32)data_size < file_ctx->msg_buf_size) {
            GS_LOG_RUN_INF("[Log Fetcher] send archive file finished, compress_alg[%u], "
                "offset[%llu] rst_id [%u] asn [%u].",
                lftc_ctx->compress_alg, file_ctx->offset, file_ctx->log_head.rst_id, file_ctx->log_head.asn);
            break;
        }
    }

    return lftc_write_stream(file_ctx->pipe, REP_ARCH_TAIL, sizeof(uint64), (char *)&file_ctx->offset,
        (int32)cm_atomic_get(&lftc_ctx->session->kernel->attr.repl_pkg_size));
}

status_t lftc_srv_alloc_buf(lftc_srv_ctx_t *lftc_srv_ctx)
{
    int64 buf_size = LOG_LGWR_BUF_SIZE(lftc_srv_ctx->session);
    uint32 zstd_buf_size = (uint32)ZSTD_compressBound((size_t)buf_size);
    uint32 lz4_buf_size = (uint32)LZ4_compressBound((int32)buf_size);

    if (cm_aligned_malloc(buf_size, "lftc_server", &lftc_srv_ctx->file_ctx.msg_buf) != GS_SUCCESS) {
        GS_LOG_RUN_ERR("[Log Fetcher] failed to alloc buffer with size %lld", buf_size);
        return GS_ERROR;
    }

    lftc_srv_ctx->file_ctx.msg_buf_size = (uint32)buf_size;  /* buf_size <= 64M, cannot overflow */

    lftc_srv_ctx->cmp_ctx.buf_size = zstd_buf_size > lz4_buf_size ? zstd_buf_size : lz4_buf_size;
    lftc_srv_ctx->cmp_ctx.data_size = 0;
    if (cm_aligned_malloc((int64)lftc_srv_ctx->cmp_ctx.buf_size, "lftc_server compress buffer",
        &lftc_srv_ctx->cmp_ctx.compress_buf) != GS_SUCCESS) {
        GS_LOG_RUN_ERR("[Log Fetcher] failed to alloc compress buffer with size %u", lftc_srv_ctx->cmp_ctx.buf_size);
        return GS_ERROR;
    }

    return GS_SUCCESS;
}

static status_t lftc_proc_compress(lftc_srv_ctx_t *lftc_srv_ctx, rep_msg_header_t *msg)
{
    char ipstr[CM_MAX_IP_LEN];
    char host[GS_HOST_NAME_BUFFER_SIZE] = { 0 };
    errno_t errcode;
    lsnd_context_t *lsnd_ctx = &lftc_srv_ctx->session->kernel->lsnd_ctx;
    lsnd_t *lsnd = NULL;
    uint32 type = msg->size - sizeof(rep_msg_header_t);

    /*
     * if an old client send req to a new server, msg.size is sizeof(lftc_clt_req_t), so we return immediately here.
     * if a new client send req to a new server, msg.size is sizeof(rep_msg_header_t) + LFTC_SUPPORT_COMPRESS, so the
     * server can judge that the client can support compression, and then find the specific compress algorithm
     */
    lftc_srv_ctx->compress_alg = COMPRESS_NONE;
    if (type != LFTC_SUPPORT_COMPRESS) {
        return GS_SUCCESS;
    }

    /*
     * we find the compress algorithm from lsnd:
     * 1. get the ip of standby
     * 2. find standby's lsnd according to ip
     * 3. get the compress algorithm of the lsnd
     * 4. if more then one standbys are configured with one ip and the algorithms of them are different, our strategy
     *    is zstd > lz4 > none
     */
    (void)cm_inet_ntop((struct sockaddr *)&lftc_srv_ctx->pipe->link.tcp.remote.addr, ipstr, CM_MAX_IP_LEN);
    errcode = strncpy_s(host, GS_HOST_NAME_BUFFER_SIZE, ipstr, GS_HOST_NAME_BUFFER_SIZE - 1);
    knl_securec_check(errcode);

    cm_latch_s(&lsnd_ctx->latch, lftc_srv_ctx->session->id, GS_FALSE, NULL);
    for (uint16 i = 0; i < lsnd_ctx->standby_num; i++) {
        lsnd = lsnd_ctx->lsnd[i];
        if (lsnd == NULL || lsnd->is_disable) {
            continue;
        }
        if (cm_strcmpi(host, lsnd->dest_info.peer_host) == 0) {
            if (lsnd->dest_info.compress_alg == COMPRESS_ZSTD) {
                lftc_srv_ctx->compress_alg = COMPRESS_ZSTD;
            } else if (lsnd->dest_info.compress_alg == COMPRESS_LZ4 && lftc_srv_ctx->compress_alg == COMPRESS_NONE) {
                lftc_srv_ctx->compress_alg = COMPRESS_LZ4;
            }
        }
    }
    cm_unlatch(&lsnd_ctx->latch, NULL);

    return GS_SUCCESS;
}

// there is no need to close pipe in this function, pipe will be closed when session is released
status_t lftc_srv_proc(knl_session_t *session, lftc_srv_ctx_t *lftc_srv_ctx)
{
    rep_msg_header_t msg;
    cs_pipe_t *pipe = lftc_srv_ctx->pipe;
    int32 recv_size;

    if (lftc_srv_alloc_buf(lftc_srv_ctx) != GS_SUCCESS) {
        GS_LOG_RUN_ERR("[Log Fetcher] Failed to alloc buffer");
        return GS_ERROR;
    }

    cm_set_thread_name("log_fetcher"); 
    GS_LOG_RUN_INF("log fetcher thread started");

    while (!lftc_srv_ctx->thread.closed) {
        if (lftc_srv_ctx->session->killed) {
            GS_LOG_RUN_ERR("[Log Fetcher] session killed");
            return GS_ERROR;
        }

        if (cs_read_stream(pipe, (char *)&msg, GS_INVALID_ID32, sizeof(rep_msg_header_t), &recv_size) != GS_SUCCESS) {
            GS_LOG_RUN_ERR("[Log Fetcher] failed to receive message from standby");
            return GS_ERROR;
        }

        if (recv_size != sizeof(rep_msg_header_t)) {
            GS_LOG_RUN_ERR("[Log Fetcher] invalid recv_size %u received, expected size is %u",
                           (uint32)recv_size, (uint32)sizeof(rep_msg_header_t));
            return GS_ERROR;
        }

        if (msg.type == REP_ARCH_REQ) {
            if (lftc_proc_compress(lftc_srv_ctx, &msg) != GS_SUCCESS) {
                GS_LOG_RUN_ERR("[Log Fetcher] Failed to process LFTC compress");
                return GS_ERROR;
            }

            if (lftc_srv_proc_request(session, lftc_srv_ctx) != GS_SUCCESS) {
                GS_LOG_RUN_ERR("[Log Fetcher] Failed to process LFTC request");
                return GS_ERROR;
            }

            if (lftc_srv_send_file(lftc_srv_ctx) != GS_SUCCESS) {
                GS_LOG_RUN_ERR("[Log Fetcher] Failed to send file data");
                return GS_ERROR;
            }
        }
    }

    return GS_SUCCESS;
}

static status_t lftc_flush_file_writepos(lftc_clt_task_t *task)
{
    if (cm_seek_file(task->handle, (int64)OFFSET_OF(log_file_head_t, write_pos), SEEK_SET) !=
        OFFSET_OF(log_file_head_t, write_pos)) {
        GS_LOG_RUN_ERR("[Log Fetcher] failed to seek file, offset:%lu, origin:%d",
            OFFSET_OF(log_file_head_t, write_pos), SEEK_SET);
        return GS_ERROR;
    }

    if (cm_write_file(task->handle, (void *)&task->offset, sizeof(uint64)) != GS_SUCCESS) {
        GS_LOG_RUN_ERR("[Log Fetcher] failed to flush file write pos");
        return GS_ERROR;
    }
    task->log_head.write_pos = task->offset;

    return GS_SUCCESS;
}

status_t lftc_adjust_buf_size(lftc_clt_task_t *task, rep_msg_header_t *msg)
{
    if (msg->type == REP_ARCH_DATA) {
        if (msg->size <= task->msg_buf_size) {
            return GS_SUCCESS;
        }

        int64 new_buf_size = GS_MAX_BATCH_SIZE + SIZE_K(4);
        if (cm_aligned_realloc(new_buf_size, "lftc client buffer", &task->msg_buf) != GS_SUCCESS) {
            GS_LOG_RUN_ERR("[Log Fetcher] failed to alloc lftc client buffer with size %lld", new_buf_size);
            return GS_ERROR;
        }
        task->msg_buf_size = (uint32)new_buf_size;
    } else {
        if (msg->size <= task->cmp_ctx.buf_size) {
            return GS_SUCCESS;
        }

        uint32 zstd_new_buf_size = (uint32)ZSTD_compressBound((size_t)GS_MAX_BATCH_SIZE);
        uint32 lz4_new_buf_size = (uint32)LZ4_compressBound((int32)GS_MAX_BATCH_SIZE);
        uint32 new_compress_buf_size = zstd_new_buf_size > lz4_new_buf_size ? zstd_new_buf_size : lz4_new_buf_size;
        if (cm_aligned_realloc((int64)new_compress_buf_size, "lftc client compress buffer",
            &task->cmp_ctx.compress_buf) != GS_SUCCESS) {
            GS_LOG_RUN_ERR("[Log Fetcher] failed to alloc lftc client compress buffer with size %u",
                new_compress_buf_size);
            return GS_ERROR;
        }
        task->cmp_ctx.buf_size = new_compress_buf_size;
    }
    return GS_SUCCESS;
}

static status_t lftc_flush_arch_data(lftc_clt_task_t *task, rep_msg_header_t *msg)
{
    int32 data_size;

    if (lftc_adjust_buf_size(task, msg) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (msg->type == REP_ARCH_DATA) {
        if (cs_read_stream(&task->pipe, task->msg_buf.aligned_buf, GS_INVALID_ID32,
            msg->size, &data_size) != GS_SUCCESS) {
            GS_LOG_RUN_ERR("[Log Fetcher] failed to receive message from primary");
            return GS_ERROR;
        }
        if (data_size != msg->size) {
            GS_LOG_RUN_ERR("[Log Fetcher] invalid data_size %u received, expected size is %u",
                           (uint32)data_size, msg->size);
            return GS_ERROR;
        }
    } else {
        if (lftc_decompress_receive_data(task, msg, (uint32 *)&data_size) != GS_SUCCESS) {
            return GS_ERROR;
        }
    }

    if (cm_seek_file(task->handle, task->offset, SEEK_SET) != (int64)task->offset) {
        GS_LOG_RUN_ERR("[Log Fetcher] failed to seek file, offset:%llu, origin:%d", task->offset, SEEK_SET);
        return GS_ERROR;
    }

    if (cm_write_file(task->handle, task->msg_buf.aligned_buf, data_size) != GS_SUCCESS) {
        GS_LOG_RUN_ERR("[Log Fetcher] failed to write file");
        return GS_ERROR;
    }

    task->offset += data_size;
    if (lftc_flush_file_writepos(task) != GS_SUCCESS) {
        return GS_ERROR;
    }

    return GS_SUCCESS;
}

status_t lftc_recv_file_head(lftc_clt_task_t *task)
{
    cs_pipe_t *pipe = &task->pipe;
    rep_msg_header_t msg;
    int32 recv_size;

    if (cs_read_stream(pipe, (char *)&msg, GS_INVALID_ID32, sizeof(rep_msg_header_t), &recv_size) != GS_SUCCESS) {
        GS_LOG_RUN_ERR("[Log Fetcher] failed to receive message from primary");
        return GS_ERROR;
    }

    if (recv_size != sizeof(rep_msg_header_t)) {
        GS_LOG_RUN_ERR("[Log Fetcher] invalid recv_size %u received, expected size is %u",
                       (uint32)recv_size, (uint32)sizeof(rep_msg_header_t));
        return GS_ERROR;
    }

    if (msg.type == REP_ARCH_LOST) {
        task->session->kernel->lftc_client_ctx.arch_lost = GS_TRUE;
        GS_LOG_RUN_ERR("[Log Fetcher] archive log [%u-%u] does not exist on primary, need repair",
            task->rst_id, task->asn);
        return GS_ERROR;
    }

    if (cs_read_stream(pipe, (char *)&task->log_head, GS_INVALID_ID32,
                       sizeof(log_file_head_t), &recv_size) != GS_SUCCESS) {
        GS_LOG_RUN_ERR("[Log Fetcher] failed to receive message from primary");
        return GS_ERROR;
    }

    if (recv_size != sizeof(log_file_head_t)) {
        GS_LOG_RUN_ERR("[Log Fetcher] invalid recv_size %u received, expected size is %u",
                       (uint32)recv_size, (uint32)sizeof(log_file_head_t));
        return GS_ERROR;
    }

    if (task->offset > sizeof(log_file_head_t)) {
        return GS_SUCCESS;
    }

    task->log_head.write_pos = sizeof(log_file_head_t);

    if (cm_seek_file(task->handle, 0, SEEK_SET) != 0) {
        GS_LOG_RUN_ERR("[Log Fetcher] failed to seek file, offset:%u, origin:%d", 0, SEEK_SET);
        return GS_ERROR;
    }

    if (cm_write_file(task->handle, (void *)&task->log_head, sizeof(log_file_head_t)) != GS_SUCCESS) {
        GS_LOG_RUN_ERR("[Log Fetcher] failed to write file");
        return GS_ERROR;
    }

    if (task->offset < sizeof(log_file_head_t)) {
        task->offset = sizeof(log_file_head_t);
    }

    return GS_SUCCESS;
}

status_t lftc_recv_file_data(lftc_clt_task_t *task)
{
    cs_pipe_t *pipe = &task->pipe;
    rep_msg_header_t msg;
    int32 recv_size;
    uint64 offset = 0;

    while (!task->canceled) {
        if (cs_read_stream(pipe, (char *)&msg, GS_INVALID_ID32, sizeof(rep_msg_header_t), &recv_size) != GS_SUCCESS) {
            GS_LOG_RUN_ERR("[Log Fetcher] failed to receive message from primary");
            return GS_ERROR;
        }

        if (recv_size != sizeof(rep_msg_header_t)) {
            GS_LOG_RUN_ERR("[Log Fetcher] invalid recv_size %u received, expected size is %u",
                           (uint32)recv_size, (uint32)sizeof(rep_msg_header_t));
            return GS_ERROR;
        }

        if (msg.type != REP_ARCH_DATA && msg.type != REP_ARCH_TAIL && msg.type != REP_LFTC_ZSTD_DATA &&
            msg.type != REP_LFTC_LZ4_DATA) {
            GS_LOG_RUN_ERR("[Log Fetcher] received unexpected type message from primary");
            return GS_ERROR;
        }

        if (msg.type == REP_ARCH_TAIL) {
            GS_LOG_RUN_INF("[Log Fetcher] Tail received");
            break;
        }

        if (lftc_flush_arch_data(task, &msg) != GS_SUCCESS) {
            GS_LOG_RUN_ERR("[Log Fetcher] Failed to flush arch data");
            return GS_ERROR;
        }
    }

    if (task->canceled) {
        GS_LOG_RUN_ERR("[Log Fetcher] fetch task has been canceled");
        return GS_ERROR;
    }

    if (cs_read_stream(pipe, (char *)&offset, GS_INVALID_ID32, sizeof(uint64), &recv_size) != GS_SUCCESS) {
        GS_LOG_RUN_ERR("[Log Fetcher] failed to receive message from primary");
        return GS_ERROR;
    }

    if (recv_size != sizeof(uint64) || offset != task->offset) {
        GS_LOG_RUN_ERR("[Log Fetcher] invalid recv_size %u received, expected size is %u, "
                       "write position %llu does not match bytes recieved %llu.",
                       (uint32)recv_size, (uint32)sizeof(uint64), task->offset, offset);
        return GS_ERROR;
    }

    if (lftc_flush_file_writepos(task) != GS_SUCCESS) {
        GS_LOG_RUN_ERR("[Log Fetcher] Failed to flush write pos");
        return GS_ERROR;
    }

    return GS_SUCCESS;
}

status_t lftc_recv_file(lftc_clt_task_t *task)
{
    if (lftc_recv_file_head(task) != GS_SUCCESS) {
        return GS_ERROR;
    }

    GS_LOG_RUN_INF("[Log Fetcher] Head received");

    if (lftc_recv_file_data(task) != GS_SUCCESS) {
        return GS_ERROR;
    }
    GS_LOG_RUN_INF("[Log Fetcher] Data received");

    return GS_SUCCESS;
}

status_t lftc_srv_ctx_alloc(lftc_srv_ctx_t **lftc_srv_ctx)
{
    lftc_srv_ctx_t *ctx = NULL;

    ctx = (lftc_srv_ctx_t *)malloc(sizeof(lftc_srv_ctx_t));
    if (ctx == NULL) {
        GS_LOG_RUN_ERR("[Log Fetcher] failed to alloc server context");
        return GS_ERROR;
    }

    *lftc_srv_ctx = ctx;
    return GS_SUCCESS;
}

static status_t lftc_fetch_archive_logfile(lftc_clt_task_t *task, const char *tmp_file)
{
    if (lftc_clt_open_tmp(task, tmp_file) != GS_SUCCESS) {
        GS_LOG_RUN_ERR("[Log Fetcher] failed to open temp log file");
        return GS_ERROR;
    }

    if (lftc_check_connect(task) != GS_SUCCESS) {
        GS_LOG_RUN_ERR("[Log Fetcher] failed to check connect");
        return GS_ERROR;
    }

    if (lftc_clt_send_request(task) != GS_SUCCESS) {
        GS_LOG_RUN_ERR("[Log Fetcher] failed to send requset");
        return GS_ERROR;
    }

    if (lftc_recv_file(task) != GS_SUCCESS) {
        GS_LOG_RUN_ERR("[Log Fetcher] failed to receive log file");
        return GS_ERROR;
    }

    if (cm_rename_file_durably(tmp_file, task->file_name) != GS_SUCCESS) {
        GS_LOG_RUN_ERR("[Log Fetcher] failed to rename temp log file to %s", task->file_name);
        return GS_ERROR;
    }

    if (!arch_archive_log_recorded(task->session, task->log_head.rst_id, task->log_head.asn, ARCH_DEFAULT_DEST)) {
        if (arch_record_archinfo(task->session, ARCH_DEFAULT_DEST, task->file_name, &task->log_head) != GS_SUCCESS) {
            GS_LOG_RUN_ERR("[Log Fetcher] failed to record archive log file %s with [%u-%u]",
                task->file_name, task->log_head.rst_id, task->log_head.asn);
            return GS_ERROR;
        }
    }

    return GS_SUCCESS;
}

void lftc_clt_proc(thread_t *thread)
{
    lftc_clt_task_t *task = (lftc_clt_task_t *)thread->argument; 
    char tmp_file[GS_FILE_NAME_BUFFER_SIZE + 4]; // 4 bytes for ".tmp"
    errno_t err;

    err = memset_sp(tmp_file, GS_FILE_NAME_BUFFER_SIZE + 4, 0, GS_FILE_NAME_BUFFER_SIZE + 4);
    knl_securec_check(err);
    err = snprintf_s(tmp_file, sizeof(tmp_file), sizeof(tmp_file) - 1, "%s.tmp", task->file_name); 
    knl_securec_check_ss(err);

    if (lftc_fetch_archive_logfile(task, tmp_file) == GS_SUCCESS) {
        GS_LOG_RUN_INF("[Log Fetcher] recvfile process fetched archive log file_name/offset [%s/%llu]",
            task->file_name, task->offset);
    }

    lftc_clt_clean_task(task);
}

static status_t lftc_clt_task_start(knl_session_t *session, lftc_clt_ctx_t *ctx, uint32 task_id)
{
    lftc_clt_task_t *task = ctx->tasks + (task_id % LFTC_MAX_TASK);

    task->handle = INVALID_FILE_HANDLE;
    if (cm_create_thread(lftc_clt_proc, 0, task, &task->thread) != GS_SUCCESS) {
        GS_LOG_RUN_ERR("[Log Fetcher] failed to create log fetcher client thread");
        return GS_ERROR;
    }
    GS_LOG_RUN_INF("[Log Fetcher] Start task for asn %u.", task->asn);
    return GS_SUCCESS;
}

static inline void lftc_clt_task_reset(knl_session_t *session, lftc_clt_task_t *task, uint32 rst_id,
    uint32 asn, const char *arch_name)
{
    errno_t err;

    task->id += LFTC_MAX_TASK;
    task->is_running = GS_TRUE;
    task->canceled = GS_FALSE;
    task->rst_id = rst_id;
    task->asn = asn;
    task->session = session;
    err = strcpy_sp(task->file_name, GS_FILE_NAME_BUFFER_SIZE, arch_name);
    knl_securec_check(err);
    task->tmp_file_name[0] = 0;
}

status_t lftc_clt_task_init(knl_session_t *session, lftc_clt_ctx_t *ctx, uint32 task_id)
{
    lftc_clt_task_t *task = ctx->tasks + (task_id % LFTC_MAX_TASK);
    /* 16 bytes for "LFTC CLIENT %u" , task_id(%u) <= 4 */
    char task_name[16];               
    errno_t ret;
    int64 buf_size = LOG_LGWR_BUF_SIZE(session);
    uint32 zstd_buf_size = (uint32)ZSTD_compressBound((size_t)buf_size);
    uint32 lz4_buf_size = (uint32)LZ4_compressBound((int32)buf_size);

    ret = snprintf_s(task_name, sizeof(task_name), sizeof(task_name) - 1, "LFTC CLIENT %u", task_id);
    knl_securec_check_ss(ret);
    if (cm_aligned_malloc(buf_size, task_name, &task->msg_buf) != GS_SUCCESS) {
        GS_LOG_RUN_ERR("[Log Fetcher] failed to alloc buffer with size %lld", buf_size);
        return GS_ERROR;
    }

    task->msg_buf_size = (uint32)buf_size;

    task->cmp_ctx.buf_size = zstd_buf_size > lz4_buf_size ? zstd_buf_size : lz4_buf_size;
    task->cmp_ctx.data_size = 0;
    if (cm_aligned_malloc((int64)task->cmp_ctx.buf_size, task_name, &task->cmp_ctx.compress_buf) != GS_SUCCESS) {
        GS_LOG_RUN_ERR("[Log Fetcher] failed to alloc compress buffer with size %u", task->cmp_ctx.buf_size);
        return GS_ERROR;
    }

    return GS_SUCCESS;
}

static void lftc_clt_reuse_task(knl_session_t *session, uint32 rst_id, uint32 asn, const char *arch_name,
    uint32 *task_id, bool32 *task_exist)
{
    lftc_clt_ctx_t *ctx = &session->kernel->lftc_client_ctx;
    lftc_clt_task_t *task = NULL;

    for (uint32 i = 0; i < ctx->hwm; i++) {
        task = ctx->tasks + i;
        cm_spin_lock(&task->lock, NULL);
        if (!task->is_running) {
            lftc_clt_task_reset(session, task, rst_id, asn, arch_name);
            *task_id = task->id;
            cm_spin_unlock(&task->lock);
            return;
        }

        if (task->rst_id == rst_id && task->asn == asn) {
            *task_id = task->id;
            cm_spin_unlock(&task->lock);
            *task_exist = GS_TRUE;
            return;
        }
        cm_spin_unlock(&task->lock);
    }
}

status_t lftc_clt_create_task(knl_session_t *session, uint32 rst_id, uint32 asn,
    const char *arch_name, lftc_task_handle_t *handle)
{
    lftc_clt_ctx_t *ctx = &session->kernel->lftc_client_ctx;
    lftc_clt_task_t *task = NULL;
    uint32 task_id = GS_INVALID_ID32;
    bool32 task_exist = GS_FALSE;
    errno_t err;
    knl_session_t *lftc_session = session->kernel->sessions[SESSION_ID_LFTC_CLIENT];

    cm_spin_lock(&ctx->lock, NULL);
    lftc_clt_reuse_task(lftc_session, rst_id, asn, arch_name, &task_id, &task_exist);

    if (task_id == GS_INVALID_ID32) {
        if (ctx->hwm == LFTC_MAX_TASK) {
            GS_LOG_RUN_WAR("[Log Fetcher] failed to create task, %u tasks are running.", LFTC_MAX_TASK);
            cm_spin_unlock(&ctx->lock);
            return GS_SUCCESS;
        }

        task = ctx->tasks + ctx->hwm;
        err = memset_sp((void *)task, sizeof(lftc_clt_task_t), 0, sizeof(lftc_clt_task_t));
        knl_securec_check(err);
        task->is_running = GS_TRUE;
        task->canceled = GS_FALSE;
        task->rst_id = rst_id;
        task->asn = asn;
        task->id = ctx->hwm++;
        task->session = lftc_session;
        err = strcpy_sp(task->file_name, GS_FILE_NAME_BUFFER_SIZE, arch_name);
        knl_securec_check(err);
        task->tmp_file_name[0] = 0;

        task_id = task->id;
        if (lftc_clt_task_init(lftc_session, ctx, task_id) != GS_SUCCESS) {
            GS_LOG_RUN_ERR("[Log Fetcher] failed to init task");
            lftc_clt_clean_task(task);
            cm_spin_unlock(&ctx->lock);
            return GS_ERROR;
        }
    }

    handle->task_id = task_id;
    handle->rst_id = rst_id;
    handle->asn = asn;
    task = ctx->tasks + (task_id % LFTC_MAX_TASK);
    if (!task_exist) {
        if (lftc_clt_task_start(session, ctx, task_id) != GS_SUCCESS) {
            GS_LOG_RUN_ERR("[Log Fetcher] failed to start task");
            lftc_clt_clean_task(task);
            cm_spin_unlock(&ctx->lock);
            return GS_ERROR;
        }
    }

    cm_spin_unlock(&ctx->lock);
    return GS_SUCCESS;
}

bool32 lftc_clt_task_running(knl_session_t *session, lftc_task_handle_t *handle, bool32 *is_done)
{
    lftc_clt_ctx_t *ctx = &session->kernel->lftc_client_ctx;
    lftc_clt_task_t *task;
    char *arch_name = NULL;
    bool32 is_running = GS_FALSE;

    task = &ctx->tasks[handle->task_id % LFTC_MAX_TASK];
    cm_spin_lock(&task->lock, NULL);
    if (task->id != handle->task_id) {
        is_running = GS_FALSE;
    } else {
        is_running = task->is_running;
    }
    cm_spin_unlock(&task->lock);

    if (!is_running) {
        arch_name = (char *)cm_push(session->stack, GS_FILE_NAME_BUFFER_SIZE);
        arch_set_archive_log_name(session, handle->rst_id, handle->asn, ARCH_DEFAULT_DEST,
            arch_name, GS_FILE_NAME_BUFFER_SIZE);
        *is_done = cm_file_exist((const char *)arch_name);
        cm_pop(session->stack);
    }

    return is_running;
}
