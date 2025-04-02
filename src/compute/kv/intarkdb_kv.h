/*
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
* intarkdb_kv.h
*
* IDENTIFICATION
* openGauss-embedded/src/compute/kv/intarkdb_kv.h
*
* -------------------------------------------------------------------------
*/

#pragma once

#ifdef _WIN32
#define EXP_KV_API __declspec(dllexport)
#else
#define EXP_KV_API __attribute__((visibility("default")))
#endif

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif

/*--------------------------------------------------------------------*/
// Type define
/*--------------------------------------------------------------------*/
typedef struct st_intarkdb_database_kv {
    void *db;
} *intarkdb_database_kv;

typedef struct st_intarkdb_connection_kv {
    void *conn;
} *intarkdb_connection_kv;

/* This is the reply object */
typedef struct KvReply_t {
    int type;   /* return type */
    size_t len; /* Length of string */
    char *str;  /* err or value*/
} KvReply;

typedef enum en_status_kv {
    KV_ERROR = -1,
    KV_SUCCESS = 0,
} intarkdb_state_kv;

// --------------------------------------------------------------------------------------------------
EXP_KV_API int intarkdb_open_kv(const char *path, intarkdb_database_kv *db);

EXP_KV_API void intarkdb_close_kv(intarkdb_database_kv *db);

EXP_KV_API int intarkdb_connect_kv(intarkdb_database_kv database, intarkdb_connection_kv *kvconn);

EXP_KV_API void intarkdb_disconnect_kv(intarkdb_connection_kv *kvconn);

EXP_KV_API int intarkdb_open_table_kv(intarkdb_connection_kv kvconn, const char *table_name);

EXP_KV_API int intarkdb_open_memtable_kv(intarkdb_connection_kv kvconn, const char *table_name);

EXP_KV_API void * intarkdb_set(intarkdb_connection_kv kvconn, const char *key, const char *val);

EXP_KV_API void * intarkdb_get(intarkdb_connection_kv kvconn, const char *key);

EXP_KV_API void * intarkdb_del(intarkdb_connection_kv kvconn, const char *key);

EXP_KV_API intarkdb_state_kv intarkdb_begin(intarkdb_connection_kv kvconn);
EXP_KV_API intarkdb_state_kv intarkdb_commit(intarkdb_connection_kv kvconn);
EXP_KV_API intarkdb_state_kv intarkdb_rollback(intarkdb_connection_kv kvconn);
EXP_KV_API intarkdb_state_kv intarkdb_multi(intarkdb_connection_kv kvconn);
EXP_KV_API intarkdb_state_kv intarkdb_exec(intarkdb_connection_kv kvconn);
EXP_KV_API intarkdb_state_kv intarkdb_discard(intarkdb_connection_kv kvconn);

#ifdef __cplusplus
}
#endif
