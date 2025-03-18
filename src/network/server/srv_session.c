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
 * srv_session.c
 *
 * IDENTIFICATION
 *    src/server/srv_session.c
 *
 * -------------------------------------------------------------------------
 */
#include "cm_memory.h"
#include "srv_instance.h"
#include "srv_session.h"
#include "srv_agent.h"
#include "srv_handle.h"
#include "util_defs.h"
#include "srv_param.h"
#include "cm_timer.h"
#ifndef _WIN32
#include <sys/resource.h>
#include <dirent.h>
#endif


typedef struct st_srv_exc_get_ack {
    bool32 eof;
    uint32 fetch_nums;
} srv_exc_get_ack_t;

static inline status_t srv_send_rsp(session_t *session, int32 result)
{
    cs_packet_t *pack = NULL;
    pack = &session->agent->send_pack;
    pack->head->cmd = session->agent->recv_pack.head->cmd;
    pack->head->serial_number = session->agent->recv_pack.head->serial_number;
    pack->head->result = (uint8)result;
    return cs_write(session->pipe, pack);
}

static inline uint32 kv_actual_key_size(const text_t *key)
{
    return CM_ALIGN4(key->len) + sizeof(uint32);
}

static inline uint32 kv_actual_size(const text_t *key, const text_t *val)
{
    return CM_ALIGN4(key->len) + CM_ALIGN4(val->len) + 2 * sizeof(uint32);
}

status_t srv_kill_sess(session_t *session)
{
    //srv_unwatch_by_sess_id(session);
    reactor_unregister_session(session);
    srv_process_free_session(session);
    reactor_pool_t *pool = &g_srv_inst->reactor_pool;
    GS_LOG_RUN("[SessionCount] Number:%u", --pool->all_session_count);
    return GS_SUCCESS;
}

static inline void srv_process_init_session(session_t *session)
{
    cs_init_get(session->recv_pack);
    cs_init_set(session->send_pack, CS_LOCAL_VERSION);

#if defined(_DEBUG) || defined(DEBUG) || defined(DB_DEBUG_VERSION)
    /* reset packet memory to find pointer from context to packet memory */
    (void)memset_s(session->agent->recv_pack.buf, session->agent->recv_pack.buf_size,
        'Z', session->agent->recv_pack.buf_size);
#endif
}

static inline status_t srv_read_packet(session_t *session)
{
    if (cs_read(session->pipe, &session->agent->recv_pack, GS_FALSE) != GS_SUCCESS) {
        GS_LOG_DEBUG_ERR("srv read packet fail, sessId=%u", session->id);
        return GS_ERROR;
    }
    return GS_SUCCESS;
}

uint32 char2uint(const unsigned char * head)
{
    uint32 size = 0;
    for(int i = 0; i < HEAD_SIZE; i++) {
        size = size | (head[i] << ((HEAD_SIZE - i - 1) * 8));
    }
    return size;
}

static status_t srv_attach_reactor(session_t *session)
{
    CS_CHECK_NULL_PTR(session);
    return reactor_register_session(session);
}

static inline void srv_set_session_pipe(session_t *session, const cs_pipe_t *pipe)
{
    if (pipe != NULL) {
        session->pipe_entity = *pipe;
        session->pipe = &session->pipe_entity;
    } else {
        session->pipe = NULL;
    }
}

static void srv_reset_session(session_t *session, const cs_pipe_t *pipe)
{
    srv_set_session_pipe(session, pipe);
    //todo reset handle

    GS_LOG_DEBUG_INF("[SESS] reset session %u", session->id);
}

static void srv_try_reuse_session(session_t **session, const cs_pipe_t *pipe, bool32 *reused)
{
    session_pool_t *pool = &g_srv_inst->session_pool;
    biqueue_node_t *node = NULL;

    *session = NULL;
    *reused = GS_FALSE;

    if (biqueue_empty(&pool->idle_sessions)) {
        return;
    }

    cm_spin_lock(&pool->lock, NULL);
    node = biqueue_del_head(&pool->idle_sessions);
    if (node == NULL) {
        cm_spin_unlock(&pool->lock);
        return;
    }
    *session = OBJECT_OF(session_t, node);
    cm_spin_unlock(&pool->lock);

    srv_reset_session(*session, pipe);
    *reused = GS_TRUE;

    GS_LOG_DEBUG_INF("[SESS] srv try reuse session succeed, sessid:%u", (*session)->id);
    return;
}

static bool8 is_srv_session_over_max_limit(const session_pool_t *pool)
{
    return (pool->hwm >= pool->max_sessions);
}

static status_t srv_new_session(const cs_pipe_t *pipe, session_t **session)
{
    session_pool_t *pool = &g_srv_inst->session_pool;

    if (is_srv_session_over_max_limit(pool)) {
        return GS_ERROR;
    }

    session_t *sess = (session_t *)malloc(sizeof(session_t));
    if (sess == NULL) {
        return GS_ERROR;
    }
    if (memset_s(sess, sizeof(session_t), 0, sizeof(session_t)) != EOK) {
        CM_FREE_PTR(sess);
        return GS_ERROR;
    }

    srv_set_session_pipe(sess, pipe);

    cm_spin_lock(&pool->lock, NULL);
    sess->id = pool->hwm;
    pool->sessions[sess->id] = sess;
    pool->hwm++;
    cm_spin_unlock(&pool->lock);

    GS_LOG_DEBUG_INF("[SESS] srv alloc new session succeed, sessid:%u", sess->id);
    *session = sess;
    return GS_SUCCESS;
}

status_t srv_alloc_session(session_t **session, const cs_pipe_t *pipe, session_type_e type)
{
    bool32 reused = GS_FALSE;
    status_t ret;
    session_t *sess = NULL;

    srv_try_reuse_session(&sess, pipe, &reused);
    if (!reused) {
        ret = srv_new_session(pipe, &sess);
        if (ret != GS_SUCCESS) {
            return ret;
        }
    }

    sess->ses_type = type;
    sess->index = 0;
    sess->qry_eof = GS_TRUE;
    // update sess write_key
    (sess->serial_id)++;
    uint32 node_id = g_srv_inst->sess_apply_mgr.node_id;
    uint64 write_key = node_id;
    write_key = (write_key << EXC_BIT_MOVE_TWO_BYTES) | sess->id;
    write_key = (write_key << EXC_BIT_MOVE_FOUR_BYTES) | sess->serial_id;
    sess->write_key = write_key;

    set_prepare_stmt(sess);
    sess->auto_commit = true;
    sess->is_logged = false;
    sess->is_used = false;

    GS_LOG_DEBUG_INF("[SESS] update sess write_key:%llx, nodeid:%u sessid:%u serial_id:%u",
        sess->write_key, node_id, sess->id, sess->serial_id);

    if (type == SESSION_TYPE_API && sess->req_buf == NULL) {
        sess->req_buf = (char *)malloc(SRV_SESS_API_REQ_BUFF_LEN);
        if (sess->req_buf == NULL) {
            GS_LOG_RUN_ERR("[SESS] srv_alloc_session malloc req_buf failed");
            srv_return_session(sess);
            return GS_ERROR;
        }
    }

    *session = sess;

    GS_LOG_DEBUG_INF("[SESS] srv alloc session succeed, sessid:%u", sess->id);

    return GS_SUCCESS;
}

static void srv_save_remote_host(const cs_pipe_t *pipe, session_t *session)
{
    if (pipe->type == CS_TYPE_TCP) {
        (void)cm_inet_ntop((struct sockaddr *)&pipe->link.tcp.remote.addr,
                           session->os_host, (int)GS_HOST_NAME_BUFFER_SIZE);
    }
    return;
}

status_t srv_create_session(const cs_pipe_t *pipe)
{
    session_t *session = NULL;

    CS_CHECK_NULL_PTR(pipe);

    // try to reuse free session, if failed, create a new one
    if (srv_alloc_session(&session, pipe, SESSION_TYPE_CS) != GS_SUCCESS) {
        return GS_ERROR;
    }

    srv_save_remote_host(pipe, session);

    if (srv_attach_reactor(session) != GS_SUCCESS) {
        GS_LOG_RUN_WAR("[SESS] session(%u) attach reactor failed", session->id);
        srv_release_session(session);
        return GS_ERROR;
    }

    return GS_SUCCESS;
}

#if defined(__GLIBC__)
#include <malloc.h>
void malloc_trim_release() {
    malloc_trim(0);
}
#else
void malloc_trim_release() {
    // 其他平台的内存释放逻辑或空实现
}
#endif

void srv_deinit_session(session_t *session)
{
    if (session->pipe != NULL) {
        cs_disconnect(session->pipe);
    }
    GS_LOG_DEBUG_INF("[SESS] disconnect db_conn id:%u", session->id);
    delete_prepare_stmt(session);
    intarkdb_disconnect(&session->db_conn);
    malloc_trim_release(); // force release memory to system
    session->is_reg = GS_FALSE;
    session->proto_type = PROTO_TYPE_UNKNOWN;
    session->os_host[0] = '\0';
    return;
}

void srv_return_session(session_t *session)
{
    session_pool_t *sess_pool = &g_srv_inst->session_pool;
    session->reactor = NULL;
    session->is_free = GS_TRUE;
    cm_spin_lock(&sess_pool->lock, NULL);
    biqueue_add_tail(&sess_pool->idle_sessions, QUEUE_NODE_OF(session));
    cm_spin_unlock(&sess_pool->lock);

    GS_LOG_DEBUG_INF("[SESS] try return session %u", session->id);
}

void srv_release_session(session_t *session)
{
    srv_deinit_session(session);
    CM_MFENCE;
    srv_return_session(session);
}

static status_t srv_load_params(void)
{
    param_value_t param_value;
    GS_RETURN_IFERR(srv_get_param(DCC_PARAM_LSNR_ADDR, &param_value));
    PRTS_RETURN_IFERR(snprintf_s(g_srv_inst->lsnr.tcp_service.host[0], CM_MAX_IP_LEN, CM_MAX_IP_LEN - 1,
        "%s", param_value.str_val));

    GS_RETURN_IFERR(srv_get_param(DCC_PARAM_LSNR_PORT, &param_value));
    g_srv_inst->lsnr.tcp_service.port = (uint16)param_value.uint32_val;

    GS_RETURN_IFERR(srv_get_param(DCC_PARAM_REACTOR_THREADS, &param_value));
    reactor_pool_t *reactor_pool = &g_srv_inst->reactor_pool;
    reactor_pool->reactor_count = param_value.uint32_val;

    GS_RETURN_IFERR(srv_get_param(DCC_PARAM_OPTIMIZED_WORKER_THREADS, &param_value));
    g_srv_inst->attr.optimized_worker_count = param_value.uint32_val;

    GS_RETURN_IFERR(srv_get_param(DCC_PARAM_MAX_WORKER_THREADS, &param_value));
    g_srv_inst->attr.max_worker_count = param_value.uint32_val;

    GS_RETURN_IFERR(srv_get_param(DCC_PARAM_MAX_ALLOWED_PACKET, &param_value));
    g_srv_inst->attr.max_allowed_packet = param_value.uint32_val;

    GS_RETURN_IFERR(srv_get_param(DCC_PARAM_SRV_AGENT_SHRINK_THRESHOLD, &param_value));
    g_srv_inst->reactor_pool.agents_shrink_threshold = param_value.uint32_val;

    GS_LOG_RUN_INF("[INST] server load params successfully, lsnr host:%s port:%u reactor_cnt:%u optimized_worker_count:%u "
        "max_worker_count:%u max_allowed_packet:%u agent_shrink_threshold:%u",
        g_srv_inst->lsnr.tcp_service.host[0], g_srv_inst->lsnr.tcp_service.port, reactor_pool->reactor_count,
        g_srv_inst->attr.optimized_worker_count, g_srv_inst->attr.max_worker_count,
        g_srv_inst->attr.max_allowed_packet, g_srv_inst->reactor_pool.agents_shrink_threshold);

    return GS_SUCCESS;
}

status_t srv_init_session_pool(void)
{
    srv_inst_t *instance = srv_get_instance();
    CM_ASSERT(instance != NULL);

    session_pool_t *session_pool = &instance->session_pool;
    MEMS_RETURN_IFERR(memset_s(session_pool, sizeof(session_pool_t), 0, sizeof(session_pool_t)));

    session_pool->lock = 0;
    biqueue_init(&instance->session_pool.idle_sessions);
    session_pool->hwm = 0;
    param_value_t param_value;
    GS_RETURN_IFERR(srv_get_param(DCC_PARAM_MAX_SESSIONS, &param_value));
    session_pool->max_sessions = param_value.uint32_val;

    return GS_SUCCESS;
}

void srv_kill_all_session(void)
{
    uint32 i;
    srv_inst_t *instance = srv_get_instance();
    CM_ASSERT(instance != NULL);
    GS_LOG_RUN_INF("[SESS] kill all session start");

    session_pool_t *session_pool = &instance->session_pool;
    GS_LOG_RUN_INF("[SESS] session_pool hwm:%u", session_pool->hwm);
    /* kill all user session */
    for (i = 0; i < session_pool->hwm; i++) {
        session_t *sess = session_pool->sessions[i];
        if (sess == NULL) {
            continue;
        }
        if (sess->ses_type == SESSION_TYPE_API) {
            CM_FREE_PTR(sess->req_buf);
        }
        if (sess->is_reg && !(sess->is_free)) {
            GS_LOG_RUN_ERR("[SESS] srv kill i:%d", i);
            if (srv_kill_sess(sess) != GS_SUCCESS) {
                GS_LOG_RUN_ERR("[SESS] srv kill sess error");
            }
        }
        CM_FREE_PTR(session_pool->sessions[i]);
    }
    GS_LOG_RUN_INF("[SESS] kill all session end");
    return;
}

void srv_wait_all_session_free(void)
{
    uint32 i;
    session_pool_t *pool = &g_srv_inst->session_pool;
    for (;;) {
        for (i = 0; i < pool->hwm; i++) {
            if (!pool->sessions[i]->is_free) {
                break;
            }
        }
        if (i >= pool->hwm) {
            break;
        }
        cm_sleep(CM_SLEEP_50_FIXED);
    }
}

status_t srv_get_sess_by_id(uint32 sessid, session_t **session)
{
    if (sessid >= GS_MAX_SESSIONS) {
        GS_LOG_DEBUG_ERR("[SESS] invalid session id:%u", sessid);
        return GS_ERROR;
    }

    srv_inst_t *instance = srv_get_instance();
    CM_ASSERT(instance != NULL);
    session_pool_t *session_pool = &instance->session_pool;
    session_t *sess = session_pool->sessions[sessid];
    if (sess == NULL) {
        GS_LOG_DEBUG_ERR("[SESS] get null sess by id:%u", sessid);
        return GS_ERROR;
    }
    if (sess->id != sessid || (sess->pipe != NULL && sess->is_reg == GS_FALSE)) {
        GS_LOG_DEBUG_ERR("[SESS] failed to get sess by id:%u, sess info:is_reg:%u sessid:%u",
            sessid, sess->is_reg, sess->id);
        return GS_ERROR;
    }

    *session = sess;
    return GS_SUCCESS;
}
