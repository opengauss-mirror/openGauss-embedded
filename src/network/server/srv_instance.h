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
 * srv_instance.h
 *    instance interface
 *
 * IDENTIFICATION
 *    src/server/srv_instance.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef __SRV_INSTANCE_H__
#define __SRV_INSTANCE_H__

#include "cm_defs.h"
#include "srv_lsnr.h"
#include "srv_agent.h"
#include "srv_reactor.h"
#include "intarkdb_sql.h"
#include "srv_def.h"
#include "cm_base.h"
#include "srv_interface.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum en_inst_type {
    INST_TYPE_API,
    INST_TYPE_CS,
    INST_TYPE_MIXED,
} inst_type_e;

typedef struct st_instance_attr {
    uint32 optimized_worker_count;
    uint32 max_worker_count;
    uint32 max_allowed_packet;
    uint32 stack_size;
    inst_type_e inst_type;
} instance_attr_t;

typedef struct st_srv_instance {
    lsnr_t lsnr;
    session_pool_t session_pool;
    reactor_pool_t reactor_pool;
    instance_attr_t attr;
    ssl_ctx_t *ssl_acceptor_fd;
    sess_apply_mgr_t sess_apply_mgr;
    database_item dbs[MAX_DB_NUM];
    spinlock_t db_list_lock;
} srv_inst_t;

extern srv_inst_t *g_srv_inst;

#define KEY_RAND_FILE       "server.key.rand"
#define KEY_SALT_FILE       "server.key.salt"
#define KEY_IV_FILE         "server.key.iv"
#define KEY_CIPHER_FILE     "server.key.cipher"

srv_inst_t* srv_get_instance(void);
status_t srv_instance_startup(const server_config* cfg);
status_t srv_instance_add_database(const char* dbname, const char* location);
void srv_instance_delete_database(const char* dbname);
const database_item* srv_instance_list_database();
void srv_instance_destroy(void);
status_t srv_chk_ssl_cert_expire(void);
void srv_deinit_ssl(void);


#ifdef __cplusplus
}
#endif

#endif
