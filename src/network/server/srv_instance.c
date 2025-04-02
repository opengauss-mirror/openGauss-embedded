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
 * srv_instance.c
 *    instance interface
 *
 * IDENTIFICATION
 *    src/server/srv_instance.c
 *
 * -------------------------------------------------------------------------
 */
#include "stdio.h"
#include "srv_reactor.h"
#include "srv_session.h"
#include "srv_param.h"
//#include "srv_watch.h"
//#include "cm_cipher.h"
#include "cm_file.h"
#include "cm_utils.h"
#include "srv_instance.h"
#include "login/login.h"

srv_inst_t *g_srv_inst = NULL;

srv_inst_t* srv_get_instance(void)
{
    return g_srv_inst;
}

static status_t instance_load_params(const server_config* cfg)
{
    memcpy(g_srv_inst->lsnr.tcp_service.host, cfg->host, sizeof(cfg->host));
    g_srv_inst->lsnr.tcp_service.port = cfg->port;

    param_value_t param_value;

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

static status_t srv_instance_create(const server_config* cfg)
{
    if (g_srv_inst != NULL) {
        GS_THROW_ERROR(ERR_SERVER_STARTED);
        return GS_ERROR;
    }

    g_srv_inst = (srv_inst_t *)malloc(sizeof(srv_inst_t));
    if (g_srv_inst == NULL) {
        return GS_ERROR;
    }
    if (memset_s(g_srv_inst, sizeof(srv_inst_t), 0, sizeof(srv_inst_t)) != EOK) {
        CM_FREE_PTR(g_srv_inst);
        return GS_ERROR;
    }

    g_srv_inst->attr.inst_type = INST_TYPE_CS;

    memcpy(g_srv_inst->dbs, cfg->db_items, sizeof(cfg->db_items));
    cm_spin_lock(&g_srv_inst->db_list_lock, NULL);
    for (int i = 0; i < MAX_DB_NUM; i++) {
        if (g_srv_inst->dbs[i].isUsed) {
            if (intarkdb_open(g_srv_inst->dbs[i].location, &g_srv_inst->dbs[i].db) != SQL_SUCCESS) {
                GS_LOG_RUN_INF("intarkdb_open %s failed\n", g_srv_inst->dbs[i].name);
                return GS_ERROR;
            }
            GS_LOG_RUN_INF("intarkdb_open %s success\n", g_srv_inst->dbs[i].name);  // todo del
        }
    }
    cm_spin_unlock(&g_srv_inst->db_list_lock);

    return GS_SUCCESS;
}

status_t srv_instance_add_database(const char* dbname, const char* location)
{
    if (g_srv_inst == NULL) {
        GS_THROW_ERROR(ERR_SERVER_NOT_STARTED);
        return GS_ERROR;
    }
    int loc = -1;
    cm_spin_lock(&g_srv_inst->db_list_lock, NULL);
    for (int i = 0; i < MAX_DB_NUM; i++) {

        if (g_srv_inst->dbs[i].isUsed) {
            if (strcmp(dbname, g_srv_inst->dbs[i].db) == 0)  {
                if (strcmp(location, g_srv_inst->dbs[i].location) == 0) {
                    GS_LOG_RUN_INF("server add db:%s is exist\n", g_srv_inst->dbs[i].name);
                    cm_spin_unlock(&g_srv_inst->db_list_lock);
                    return GS_SUCCESS;
                } else {
                    GS_LOG_RUN_INF("server add db:%s is conflicting\n", g_srv_inst->dbs[i].name);
                    cm_spin_unlock(&g_srv_inst->db_list_lock);
                    return GS_ERROR;
                }
            } 
        } else if (loc == -1)
            loc = i;
    }

    if (intarkdb_open(location, &g_srv_inst->dbs[loc].db) != SQL_SUCCESS) {
        GS_LOG_RUN_INF("intarkdb_open %s failed\n", dbname);
        cm_spin_unlock(&g_srv_inst->db_list_lock);
        return GS_ERROR;
    }
    strcpy(g_srv_inst->dbs[loc].name, dbname);
    strcpy(g_srv_inst->dbs[loc].location, location);
    g_srv_inst->dbs[loc].isUsed = true;
    cm_spin_unlock(&g_srv_inst->db_list_lock); 
    return GS_SUCCESS;
}

void srv_instance_delete_database(const char* dbname)
{
    cm_spin_lock(&g_srv_inst->db_list_lock, NULL);
    for (int i = 0; i < MAX_DB_NUM; i++) {
        if (g_srv_inst->dbs[i].isUsed && strcmp(dbname, g_srv_inst->dbs[i].name) == 0) {
            g_srv_inst->dbs[i].isUsed = false;
            strcpy(g_srv_inst->dbs[i].name, "");
            strcpy(g_srv_inst->dbs[i].location, "");
            intarkdb_close(&g_srv_inst->dbs[i].db);
        }
    }
    cm_spin_unlock(&g_srv_inst->db_list_lock); 
    return;
}

const database_item* srv_instance_list_database()
{
    return g_srv_inst->dbs;
}

status_t srv_instance_startup(const server_config* cfg)
{
    if (srv_instance_create(cfg) != GS_SUCCESS) {
        GS_LOG_RUN_ERR("[INST] failed to create server instance");
        return GS_ERROR;
    }

    if (instance_load_params(cfg) != GS_SUCCESS) {
        CM_FREE_PTR(g_srv_inst);
        GS_LOG_RUN_ERR("[INST] failed to load server params");
        return GS_ERROR;
    }


    if (srv_init_session_pool() != GS_SUCCESS) {
        CM_FREE_PTR(g_srv_inst);
        GS_LOG_RUN_ERR("[INST] failed to init session pool");
        return GS_ERROR;
    }

    if (reactor_create_pool() != GS_SUCCESS) {
        CM_FREE_PTR(g_srv_inst);
        GS_LOG_RUN_ERR("[INST] failed to create reactor pool");
        return GS_ERROR;
    }

    if (srv_start_lsnr() != GS_SUCCESS) {
        reactor_destroy_pool();
        CM_FREE_PTR(g_srv_inst);
        GS_LOG_RUN_ERR("[INST] failed to start lsnr");
        return GS_ERROR;
    }
    /* todo: SSL
    if (srv_init_ssl() != GS_SUCCESS) {
        srv_stop_lsnr(LSNR_TYPE_ALL);
        reactor_destroy_pool();
        CM_FREE_PTR(g_srv_inst);
        GS_LOG_RUN_ERR("[INST] failed to init ssl");
        return GS_ERROR;
    }
    

    if (srv_init_watch_mgr() != GS_SUCCESS) {
        srv_deinit_ssl();
        srv_stop_lsnr(LSNR_TYPE_ALL);
        reactor_destroy_pool();
        srv_uninit_watch_mgr();
        CM_FREE_PTR(g_srv_inst);
        GS_LOG_RUN_ERR("[INST] failed to init watch mgr");
        return GS_ERROR;
    }
    */ 
    GS_LOG_RUN_INF("[INST] srv instance started.");
    return GS_SUCCESS;
}

static void srv_add_all_sess_kill(void)
{
    uint32 i;
    session_pool_t *pool = &g_srv_inst->session_pool;

    /* add all user session to be killed */
    for (i = 0; i < pool->hwm; i++) {
        session_t *session = pool->sessions[i];
        if (session == NULL || session->is_free) {
            continue;
        }
        // wait until session registered
        while (!session->is_reg) {
            cm_sleep(CM_SLEEP_5_FIXED);
        }
        reactor_add_kill_event(session);
    }
    GS_LOG_RUN_INF("srv add all session kill end");
}

static void srv_wait_agents_done(void)
{
    GS_LOG_RUN_INF("begin to wait agents done, pause all listener and reactor pool");
    srv_pause_lsnr(LSNR_TYPE_ALL);
    reactor_pause_pool();

    // wait agents done and all session free
    srv_add_all_sess_kill();
    srv_wait_all_session_free();

    GS_LOG_RUN_INF("end to wait all agents done");
    return;
}

void srv_instance_destroy(void)
{
    if (g_srv_inst == NULL) {
        return;
    }

    srv_wait_agents_done();
    srv_kill_all_session();
    //srv_deinit_ssl();

    // uninit watch mgr
    //GS_LOG_RUN_INF("[INST] begin to uninit watch mgr");
    //srv_uninit_watch_mgr();

    // stop listener
    GS_LOG_RUN_INF("[INST] begin to stop all listener");
    srv_stop_lsnr(LSNR_TYPE_ALL);

    // stop reactor pool
    GS_LOG_RUN_INF("[INST] begin to stop reactor");
    reactor_destroy_pool();

    for (int i = 0; i < MAX_DB_NUM; i++) {
        if (g_srv_inst->dbs[i].isUsed) {
            intarkdb_close(&g_srv_inst->dbs[i].db);
            GS_LOG_RUN_INF("close db %s", g_srv_inst->dbs[i].name);
        }
    }

    CM_FREE_PTR(g_srv_inst);
}

