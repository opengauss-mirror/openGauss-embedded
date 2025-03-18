/*
* Copyright (c) 2022 Huawei Technologies Co.,Ltd.
* Copyright (c) GBA-NCTI-ISDC. 2022-2024.
*
* openGauss embedded is licensed under Mulan PSL v2.
* You can use this software according to the terms and conditions of the Mulan PSL v2.
* You may obtain a copy of Mulan PSL v2 at:
*
* http://license.coscl.org.cn/MulanPSL2
*
* THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
* EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
* MERCHANTABILITY OR FITFOR A PARTICULAR PURPOSE.
* See the Mulan PSL v2 for more details.
* -------------------------------------------------------------------------
*
* srv_session_base.h
*
* IDENTIFICATION
* openGauss-embedded/src/network/server/srv_session_base.h
*
* -------------------------------------------------------------------------
*/

#include "cm_sync.h"
#include "cs_pipe.h"
#include "compute/kv/intarkdb_kv.h"
#include "interface/c/intarkdb_sql.h"
#include "cm_base.h"

#ifndef __SRV_SESSION_BASE_H__
#define __SRV_SESSION_BASE_H__

#ifdef __cplusplus
extern "C" {
#endif

typedef enum en_session_type {
    SESSION_TYPE_CS = 1,
    SESSION_TYPE_API,
} session_type_e;

typedef struct st_sess_watch_record {
    text_t key;
    bool32 is_prefix;
    uint32 session_id;
    struct st_sess_watch_record *prev;
    struct st_sess_watch_record *next;
} sess_watch_record_t;

typedef struct st_session {
    uint32 id;
    uint32 serial_number; // cs msg SN
    void *stg_handle;  // handle of storage todo delete
    session_type_e ses_type;
    uint32 proto_type;
    intarkdb_connection db_conn; //sql
    intarkdb_prepared_statement stmt; //sql prepare
    void * kv_handle;       // kv
    bool32 auto_commit;
    bool32 is_logged;
    bool32 is_used;
    

    cs_pipe_t pipe_entity;  // if pipe info specified when create a session, pointer pipe will point to
    // if pipe info specified when create a session, it will point to pipe_entity, otherwise null assigned
    cs_pipe_t *pipe;
    char os_host[GS_HOST_NAME_BUFFER_SIZE];
    union {
        cs_packet_t *recv_pack;
        char *req_buf; // for api req buf
    };
    cs_packet_t *send_pack;
    struct st_agent *agent;
    struct st_reactor *reactor;
    struct st_session *prev;
    struct st_session *next;
    spinlock_t detaching_lock;
    struct {
        bool32 is_reg : 1;
        bool32 is_free : 1;
        bool32 unused : 30;
    };
    uint64 index;
    cm_event_t event;
    int64 start_proc_time;
    uint64 write_key;
    uint32 serial_id; // attach session cnt
    uint32 qry_eof;
    sess_watch_record_t *watch_head;
} session_t;


status_t kill_sess(session_t *session);

#ifdef __cplusplus
}
#endif

#endif /* __SRV_SESSION_BASE_H__ */