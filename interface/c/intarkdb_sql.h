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
* intarkdb_sql.h
*
* IDENTIFICATION
* openGauss-embedded/src/interface/c/intarkdb_sql.h
*
* -------------------------------------------------------------------------
*/

#pragma once

#ifdef _WIN32
#define EXP_SQL_API __declspec(dllexport)
#else
#define EXP_SQL_API __attribute__((visibility("default")))
#endif

#include <stdarg.h>
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
    int64_t row_count;  // 结果集行数
    int64_t effect_row; // 影响行数
    bool is_select;
    void *res_row;  // 行结果集    //这里实际是 RecordBatch*

    int64_t column_count;      // 列数
    api_text_t *column_names;  // 列名
    char *msg;                 // 执行结果信息

    char *value_ptr;  // for free column value
    int64_t row_idx;  // for next
    bool need_result_ex;        // insert 返回结果集
    int64_t limit_rows_ex;      // 结果集最大行数
    uint32_t res_type;
    bool has_row_record;
    void *row_record;   // intarkdb_query_iterator模式下next row的内容

    int64_t last_insert_rowid;  // last insert rowid
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

typedef enum en_result_type_def {
    RESULT_BATCH = 0,
    RESULT_ITERATOR = 1,
} result_type_t;

typedef struct st_intarkdb_res_def *intarkdb_result;

typedef struct st_intarkdb_prepared_statement {
    void *prep_stmt;
} *intarkdb_prepared_statement;


// --------------------------------------------------------------------------------------------------

EXP_SQL_API intarkdb_state_t intarkdb_open(const char *path, intarkdb_database *db);

EXP_SQL_API void intarkdb_close(intarkdb_database *db);

EXP_SQL_API const char *intarkdb_db_path(intarkdb_database database);

EXP_SQL_API intarkdb_state_t intarkdb_connect(intarkdb_database database, intarkdb_connection *conn);

EXP_SQL_API intarkdb_state_t intarkdb_connect_with_user(intarkdb_database database, intarkdb_connection *conn,
                                                        const char *user_name, const char *user_password,
                                                        const char *user_ip, uint32_t sid,
                                                        uint32_t *rescode, char *err_msg);

EXP_SQL_API void intarkdb_disconnect(intarkdb_connection *conn);

EXP_SQL_API bool intarkdb_is_autocommit(intarkdb_connection *conn);

EXP_SQL_API intarkdb_state_t intarkdb_query(intarkdb_connection connection, const char *query, intarkdb_result result);

EXP_SQL_API intarkdb_state_t intarkdb_query_format(intarkdb_connection connection, intarkdb_result result, const char *format, ...);

EXP_SQL_API intarkdb_state_t intarkdb_query_iterator(intarkdb_connection connection, const char *query, intarkdb_result result);

EXP_SQL_API intarkdb_result intarkdb_init_result();

EXP_SQL_API void intarkdb_need_result_ex(intarkdb_result result, bool need);

EXP_SQL_API void intarkdb_limit_rows_ex(intarkdb_result result, uint64_t limit);

EXP_SQL_API int32_t intarkdb_result_effect_row(intarkdb_result result);

EXP_SQL_API int64_t intarkdb_row_count(intarkdb_result result);

EXP_SQL_API int64_t intarkdb_column_count(intarkdb_result result);

EXP_SQL_API const char *intarkdb_column_name(intarkdb_result result, int64_t col);

EXP_SQL_API int32_t intarkdb_column_type(intarkdb_result result, int64_t col);

EXP_SQL_API void intarkdb_column_typename(intarkdb_result result, int64_t col, char *type_name, size_t max_len);

EXP_SQL_API char *intarkdb_value_varchar(intarkdb_result result, int64_t row, int64_t col);

EXP_SQL_API void intarkdb_free_row(intarkdb_result result);

EXP_SQL_API void intarkdb_destroy_result(intarkdb_result result);

EXP_SQL_API const char *intarkdb_result_msg(intarkdb_result result);

EXP_SQL_API int64_t intarkdb_last_insert_rowid(intarkdb_database db);
// ----------------------------------------result value-----------------------------------------------
EXP_SQL_API bool intarkdb_next_row(intarkdb_result result);

EXP_SQL_API char *intarkdb_column_value(intarkdb_result result, int64_t col);

EXP_SQL_API bool intarkdb_value_boolean(intarkdb_result result, int64_t row, int64_t col);

EXP_SQL_API int32_t intarkdb_value_int32(intarkdb_result result, int64_t row, int64_t col);

EXP_SQL_API int64_t intarkdb_value_int64(intarkdb_result result, int64_t row, int64_t col);

EXP_SQL_API uint32_t intarkdb_value_uint32(intarkdb_result result, int64_t row, int64_t col);

EXP_SQL_API uint64_t intarkdb_value_uint64(intarkdb_result result, int64_t row, int64_t col);

EXP_SQL_API double intarkdb_value_double(intarkdb_result result, int64_t row, int64_t col);

EXP_SQL_API char *intarkdb_value_date(intarkdb_result result, int64_t row, int64_t col);

EXP_SQL_API char *intarkdb_value_timestamp(intarkdb_result result, int64_t row, int64_t col);

EXP_SQL_API int64_t intarkdb_value_timestamp_ms(intarkdb_result result, int64_t row, int64_t col);

EXP_SQL_API int64_t intarkdb_value_timestamp_us(intarkdb_result result, int64_t row, int64_t col);

EXP_SQL_API void *intarkdb_value_blob(intarkdb_result result, int64_t row, int64_t col, int32_t *val_len);

EXP_SQL_API void intarkdb_blob_len(intarkdb_result result, int64_t row, int64_t col, int32_t *val_len);

// ----------------------------------------prepare----------------------------------------------------
EXP_SQL_API intarkdb_prepared_statement intarkdb_init_prepare_stmt(void);

EXP_SQL_API intarkdb_state_t intarkdb_prepare(intarkdb_connection conn, const char *query, intarkdb_prepared_statement *out);

EXP_SQL_API int64_t intarkdb_prepare_nparam(intarkdb_prepared_statement prepared_statement);

EXP_SQL_API bool intarkdb_prepare_is_select(intarkdb_prepared_statement prepared_statement);

EXP_SQL_API char *intarkdb_prepare_errmsg(intarkdb_prepared_statement prepared_statement);

EXP_SQL_API intarkdb_state_t intarkdb_execute_prepared(intarkdb_prepared_statement prepared_statement, intarkdb_result result);

EXP_SQL_API void intarkdb_destroy_prepare(intarkdb_prepared_statement *prepared_statement);

EXP_SQL_API intarkdb_state_t intarkdb_clear_bindings(intarkdb_prepared_statement prepared_statement);

EXP_SQL_API intarkdb_state_t intarkdb_bind_boolean(intarkdb_prepared_statement prepared_statement, uint32_t idx, bool val);

EXP_SQL_API intarkdb_state_t intarkdb_bind_int8(intarkdb_prepared_statement prepared_statement, uint32_t idx, int8_t val);

EXP_SQL_API intarkdb_state_t intarkdb_bind_int16(intarkdb_prepared_statement prepared_statement, uint32_t idx, int16_t val);

EXP_SQL_API intarkdb_state_t intarkdb_bind_int32(intarkdb_prepared_statement prepared_statement, uint32_t idx, int32_t val);

EXP_SQL_API intarkdb_state_t intarkdb_bind_int64(intarkdb_prepared_statement prepared_statement, uint32_t idx, int64_t val);

EXP_SQL_API intarkdb_state_t intarkdb_bind_uint8(intarkdb_prepared_statement prepared_statement, uint32_t idx, uint8_t val);

EXP_SQL_API intarkdb_state_t intarkdb_bind_uint16(intarkdb_prepared_statement prepared_statement, uint32_t idx, uint16_t val);

EXP_SQL_API intarkdb_state_t intarkdb_bind_uint32(intarkdb_prepared_statement prepared_statement, uint32_t idx, uint32_t val);

EXP_SQL_API intarkdb_state_t intarkdb_bind_uint64(intarkdb_prepared_statement prepared_statement, uint32_t idx, uint64_t val);

EXP_SQL_API intarkdb_state_t intarkdb_bind_float(intarkdb_prepared_statement prepared_statement, uint32_t idx, float val);

EXP_SQL_API intarkdb_state_t intarkdb_bind_double(intarkdb_prepared_statement prepared_statement, uint32_t idx, double val);

EXP_SQL_API intarkdb_state_t intarkdb_bind_date(intarkdb_prepared_statement prepared_statement, uint32_t idx, const char *val);

EXP_SQL_API intarkdb_state_t intarkdb_bind_timestamp_ms(intarkdb_prepared_statement prepared_statement, uint32_t idx, int64_t val);

EXP_SQL_API intarkdb_state_t intarkdb_bind_timestamp_us(intarkdb_prepared_statement prepared_statement, uint32_t idx, int64_t val);

EXP_SQL_API intarkdb_state_t intarkdb_bind_varchar(intarkdb_prepared_statement prepared_statement, uint32_t idx, const char *val);

EXP_SQL_API intarkdb_state_t intarkdb_bind_decimal(intarkdb_prepared_statement prepared_statement, uint32_t idx, const char *val);

EXP_SQL_API intarkdb_state_t intarkdb_bind_null(intarkdb_prepared_statement prepared_statement, uint32_t idx);

EXP_SQL_API intarkdb_state_t intarkdb_bind_blob(intarkdb_prepared_statement prepared_statement, uint32_t idx, const void *data, uint32_t len);

EXP_SQL_API char *intarkdb_expanded_sql(intarkdb_prepared_statement prepared_statement);

EXP_SQL_API const char *intarkdb_sql(intarkdb_prepared_statement prepared_statement);

#ifdef __cplusplus
}
#endif
