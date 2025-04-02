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
 * srv_reactor.h
 *    reactor realization
 *
 * IDENTIFICATION
 *    src/server/srv_reactor.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef __SRV_REACTOR_H__
#define __SRV_REACTOR_H__

#include "cm_defs.h"
#include "cm_thread.h"
#include "srv_agent.h"

#ifdef __cplusplus
extern "C" {
#endif

#define REACTOR_STATUS_INVALID_FOR_RETURN(reactor)                                     \
    {                                                                                  \
        if ((reactor)->status != REACTOR_STATUS_RUNNING || (reactor)->thread.closed) { \
            return GS_SUCCESS;                                                         \
        }                                                                              \
    }

typedef enum en_reactor_status {
    REACTOR_STATUS_RUNNING,
    REACTOR_STATUS_PAUSING,
    REACTOR_STATUS_PAUSED,
    REACTOR_STATUS_STOPPED,
} reactor_status_t;

typedef struct st_kill_event_queue {
    uint32 r_pos;
    uint32 w_pos;
    spinlock_t w_lock;
    uint32     count;
    session_t **sesses;
} kill_event_queue_t;

typedef struct st_reactor {
    uint32 id;
    thread_t thread;
    int epollfd;
    atomic32_t session_count;
    agent_pool_t agent_pool;
    reactor_status_t status;
    kill_event_queue_t kill_events;
} reactor_t;

typedef struct st_reactor_pool {
    uint32 reactor_count;
    uint32 roudroubin;
    uint32 roudroubin2;
    uint32 agents_shrink_threshold;
    reactor_t *reactors;
    char *sess_buf;
    uint32 all_session_count;
} reactor_pool_t;

struct st_session;

status_t reactor_set_oneshot(session_t *session);
status_t reactor_register_session(session_t *session);
void reactor_unregister_session(session_t *session);
status_t reactor_create_pool(void);
void reactor_destroy_pool(void);
void reactor_pause_pool(void);
static inline bool32 reactor_in_dedicated_mode(const reactor_t *reactor)
{
    return reactor->agent_pool.curr_count >= (uint32)reactor->session_count;
}
void reactor_add_kill_event(session_t *sess);

#ifdef __cplusplus
}
#endif

#endif
