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
 * intarkdb.h
 *
 * IDENTIFICATION
 * openGauss-embedded/interface/go/feisi/intarkdb_interface/include/intarkdb.h
 *
 * -------------------------------------------------------------------------
 */

#pragma once
#include <bits/types.h>

#ifdef __cplusplus
extern "C" {
#endif

#ifdef WIN32
#define EXP_SQL_API __declspec(dllexport)
#define EXPORT_API __declspec(dllexport)
#else
#define EXP_SQL_API __attribute__((visibility("default")))
#define EXPORT_API __attribute__((visibility("default")))
#endif

// sql
typedef __int64_t int64_t;

typedef struct st_api_text {
    char *str;
    int64_t len;
    int64_t data_type;
} api_text_t;

typedef struct st_result_column {
    char *data;
    int64_t data_len;
} result_column;

typedef struct st_result_row {
    int64_t column_count;            // 列数
    result_column *row_column_list;  // 行包含的列列表
    struct st_result_row *next;
} result_row;

typedef struct st_intarkdb_res_def {
    int64_t row_count;  // 行数
    bool is_select;
    void *res_row;  // 行结果集    //这里实际是 RecordBatch*

    int64_t column_count;      // 列数
    api_text_t *column_names;  // 列名
    char *msg;                 // 执行结果信息

    char *value_ptr;  // for free column value
    int64_t row_idx;  // for next
} intarkdb_res_def;

typedef struct st_intarkdb_database {
    void *db;
} *intarkdb_database;

typedef struct st_intarkdb_connection {
    void *conn;
} *intarkdb_connection;

typedef enum en_status_def {
    SQL_ERROR = -1,
    SQL_SUCCESS = 0,
    SQL_TIMEDOUT = 1,
} intarkdb_state_t;

typedef struct st_intarkdb_res_def *intarkdb_result;

EXP_SQL_API intarkdb_state_t intarkdb_open(const char *path, intarkdb_database *db);

EXP_SQL_API void intarkdb_close(intarkdb_database *db);

EXP_SQL_API intarkdb_state_t intarkdb_connect(intarkdb_database database, intarkdb_connection *conn);

EXP_SQL_API void intarkdb_disconnect(intarkdb_connection *conn);

EXP_SQL_API intarkdb_state_t intarkdb_query(intarkdb_connection connection, const char *query, intarkdb_result result);

EXP_SQL_API intarkdb_result intarkdb_init_result();

EXP_SQL_API int64_t intarkdb_row_count(intarkdb_result result);

EXP_SQL_API int64_t intarkdb_column_count(intarkdb_result result);

EXP_SQL_API const char *intarkdb_column_name(intarkdb_result result, int64_t col);

EXP_SQL_API char *intarkdb_value_varchar(intarkdb_result result, int64_t row, int64_t col);

EXP_SQL_API void intarkdb_free_row(intarkdb_result result);

EXP_SQL_API void intarkdb_destroy_result(intarkdb_result result);

EXP_SQL_API const char * intarkdb_result_msg(intarkdb_result result);

// kv
/* This is the reply object returned by redisCommand() */
typedef __SIZE_TYPE__ size_t;

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

EXP_SQL_API int intarkdb_connect_kv(intarkdb_database database, intarkdb_connection_kv *kvconn);

EXP_SQL_API void intarkdb_disconnect_kv(intarkdb_connection_kv *kvconn);

EXP_SQL_API int intarkdb_open_table_kv(intarkdb_connection_kv kvconn, const char *table_name);

EXP_SQL_API void *intarkdb_set(intarkdb_connection_kv kvconn, const char *key, const char *val);

EXP_SQL_API void *intarkdb_get(intarkdb_connection_kv kvconn, const char *key);

EXP_SQL_API void *intarkdb_del(intarkdb_connection_kv kvconn, const char *key);

EXP_SQL_API intarkdb_state_kv intarkdb_begin(intarkdb_connection_kv kvconn);

EXP_SQL_API intarkdb_state_kv intarkdb_commit(intarkdb_connection_kv kvconn);

EXP_SQL_API intarkdb_state_kv intarkdb_rollback(intarkdb_connection_kv kvconn);

#ifdef __cplusplus
}
#endif
