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
 * srv_session.h
 *    header file for session-related functions
 *
 * IDENTIFICATION
 *    src/server/srv_session.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef __SRV_SESSION_H__
#define __SRV_SESSION_H__

#include "cm_defs.h"
#include "cs_pipe.h"
#include "cm_spinlock.h"
#include "cm_queue.h"
#include "cm_sync.h"
#include "cm_thread.h"
#include "srv_session_base.h"
//#include "executor.h"
//#include "dcc_interface.h"
//#include "cm_profile_stat.h"



#ifdef __cplusplus
extern "C" {
#endif

#define SRV_SESS_API_REQ_BUFF_LEN (MAX_KV_KEY_LEN + MAX_KV_VALUE_LEN * 2 + 2 * sizeof(uint32) + 64)
#define DCC_MAX_SESS_APPLY_INST_NUM 40



typedef struct st_session_pool {
    spinlock_t lock;
    biqueue_t idle_sessions;
    uint32 hwm; /* high water mark */
    uint32 max_sessions;
    session_t *sessions[GS_MAX_SESSIONS];
} session_pool_t;

typedef struct st_sess_apply_inst {
    uint32 id;
    spinlock_t lock;
    biqueue_t apply_que;
    thread_t thread;
    cs_packet_t pack;
    cm_event_t event;
} sess_apply_inst_t;

typedef struct st_sess_apply_mgr {
    uint32 apply_inst_num;
    bool32 is_ready;
    sess_apply_inst_t *appy_inst[DCC_MAX_SESS_APPLY_INST_NUM];
    uint32 node_id;
} sess_apply_mgr_t;

typedef struct st_sess_consense_obj {
    uint32 nodeid;
    uint32 sid;
    uint32 serial_id;
    uint32 cmd;
    bool32 cmd_result;
    uint64 index;
    int32 sequence;
    struct st_sess_consense_obj *prev;
    struct st_sess_consense_obj *next;
} sess_consense_obj_t;

status_t srv_alloc_session(session_t **session, const cs_pipe_t *pipe, session_type_e type);
status_t srv_create_session(const cs_pipe_t *pipe);
status_t srv_kill_sess(session_t *session);
void srv_release_session(session_t *session);
void srv_deinit_session(session_t *session);
void srv_return_session(session_t *session);
status_t srv_init_session_pool(void);
void srv_kill_all_session(void);
void srv_wait_all_session_free(void);
//status_t srv_process_command(session_t *session);  srv_handle c++实现

status_t srv_get_sess_by_id(uint32 sessid, session_t **session);
//status_t srv_sess_consensus_proc(const exc_consense_obj_t* obj);
status_t srv_init_sess_apply_mgr(void);
void srv_uninit_sess_apply_mgr(void);

#ifdef __cplusplus
}
#endif
#endif /* __SRV_SESSION_H__ */
