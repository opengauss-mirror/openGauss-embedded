/*
* Copyright (c) GBA-NCTI-ISDC. 2022-2024.
* Copyright (c) 2022 Huawei Technologies Co.,Ltd.
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
* openGauss-embedded/interface/go/gorm/sql-driver/intarkdb/include/intarkdb.h
*
* -------------------------------------------------------------------------
*/

#pragma once
#include <bits/types.h>

#ifdef __cplusplus
extern "C"
{
#endif

#ifdef WIN32
#define EXP_SQL_API __declspec(dllexport)
#else
#define EXP_SQL_API __attribute__((visibility("default")))
#endif

/*--------------------------------------------------------------------*/
// Type define
/*--------------------------------------------------------------------*/

typedef __int8_t int8_t;
typedef __int16_t int16_t;
typedef __int32_t int32_t;
typedef __int64_t int64_t;
typedef __uint8_t uint8_t;
typedef __uint16_t uint16_t;
typedef __uint32_t uint32_t;
typedef __uint64_t uint64_t;
typedef __SIZE_TYPE__ size_t;

typedef enum en_gs_type {
    GS_TYPE_UNKNOWN = -1,
    GS_TYPE_BASE = 20000,
    GS_TYPE_INTEGER = GS_TYPE_BASE + 1,    /* native 32 bits integer */
    GS_TYPE_BIGINT = GS_TYPE_BASE + 2,     /* native 64 bits integer */
    GS_TYPE_REAL = GS_TYPE_BASE + 3,       /* 8-byte native double */
    GS_TYPE_NUMBER = GS_TYPE_BASE + 4,     /* number */
    GS_TYPE_DECIMAL = GS_TYPE_BASE + 5,    /* decimal, internal used */
    GS_TYPE_DATE = GS_TYPE_BASE + 6,       /* datetime */
    GS_TYPE_TIMESTAMP = GS_TYPE_BASE + 7,  /* timestamp */
    GS_TYPE_CHAR = GS_TYPE_BASE + 8,       /* char(n) */
    GS_TYPE_VARCHAR = GS_TYPE_BASE + 9,    /* varchar, varchar2 */
    GS_TYPE_STRING = GS_TYPE_BASE + 10,    /* native char * */
    GS_TYPE_BINARY = GS_TYPE_BASE + 11,    /* binary */
    GS_TYPE_VARBINARY = GS_TYPE_BASE + 12, /* varbinary */
    GS_TYPE_CLOB = GS_TYPE_BASE + 13,      /* clob */
    GS_TYPE_BLOB = GS_TYPE_BASE + 14,      /* blob */
    GS_TYPE_CURSOR = GS_TYPE_BASE + 15,    /* resultset, for stored procedure */
    GS_TYPE_COLUMN = GS_TYPE_BASE + 16,    /* column type, internal used */
    GS_TYPE_BOOLEAN = GS_TYPE_BASE + 17,

    /* timestamp with time zone ,this type is fake, it is abandoned now,
     * you can treat it as GS_TYPE_TIMESTAMP just for compatibility */
    GS_TYPE_TIMESTAMP_TZ_FAKE = GS_TYPE_BASE + 18,
    GS_TYPE_TIMESTAMP_LTZ = GS_TYPE_BASE + 19, /* timestamp with local time zone */
    GS_TYPE_INTERVAL = GS_TYPE_BASE + 20,      /* interval of Postgre style, no use */
    GS_TYPE_INTERVAL_YM = GS_TYPE_BASE + 21,   /* interval YEAR TO MONTH */
    GS_TYPE_INTERVAL_DS = GS_TYPE_BASE + 22,   /* interval DAY TO SECOND */
    GS_TYPE_RAW = GS_TYPE_BASE + 23,           /* raw */
    GS_TYPE_IMAGE = GS_TYPE_BASE + 24,         /* image, equals to longblob */
    GS_TYPE_UINT32 = GS_TYPE_BASE + 25,        /* unsigned integer */
    GS_TYPE_UINT64 = GS_TYPE_BASE + 26,        /* unsigned bigint */
    GS_TYPE_SMALLINT = GS_TYPE_BASE + 27,      /* 16-bit integer */
    GS_TYPE_USMALLINT = GS_TYPE_BASE + 28,     /* unsigned 16-bit integer */
    GS_TYPE_TINYINT = GS_TYPE_BASE + 29,       /* 8-bit integer */
    GS_TYPE_UTINYINT = GS_TYPE_BASE + 30,      /* unsigned 8-bit integer */
    GS_TYPE_FLOAT = GS_TYPE_BASE + 31,         /* 4-byte float */
    // !!!add new member must ensure not exceed the limitation of g_type_maps in sql_oper_func.c
    /* the real tz type , GS_TYPE_TIMESTAMP_TZ_FAKE will be not used , it will be the same as GS_TYPE_TIMESTAMP */
    GS_TYPE_TIMESTAMP_TZ = GS_TYPE_BASE + 32, /* timestamp with time zone */
    GS_TYPE_ARRAY = GS_TYPE_BASE + 33,        /* array */
    /* com */
    /* caution: SCALAR type must defined above */
    GS_TYPE_OPERAND_CEIL = GS_TYPE_BASE + 40, // ceil of operand type

    /* The datatype can't used in datatype caculation system. only used for
     * decl in/out param in pl/sql */
    GS_TYPE_RECORD = GS_TYPE_BASE + 41,
    GS_TYPE_COLLECTION = GS_TYPE_BASE + 42,
    GS_TYPE_OBJECT = GS_TYPE_BASE + 43,
    // new data type
    GS_TYPE_HUGEINT = GS_TYPE_BASE + 44,
    /* The datatype below the GS_TYPE__DO_NOT_USE can be used as database DATATYPE.
     * In some extend, GS_TYPE__DO_NOT_USE represents the maximal number
     * of DATATYPE that Zenith are supported. The newly adding datatype
     * must before GS_TYPE__DO_NOT_USE, and the type_id must be consecutive
     */
    GS_TYPE__DO_NOT_USE = GS_TYPE_BASE + 45,

    /* The following datatypes are functional datatypes, which can help
     * to implement some features when needed. Note that they can not be
     * used as database DATATYPE */
    /* to present a datatype node, for example cast(para1, typenode),
     * the second argument is an expr_node storing the information of
     * a datatype, such as length, precision, scale, etc.. */
    GS_TYPE_FUNC_BASE = GS_TYPE_BASE + 200,
    GS_TYPE_TYPMODE = GS_TYPE_FUNC_BASE + 1,

    /* This datatype only be used in winsort aggr */
    GS_TYPE_VM_ROWID = GS_TYPE_FUNC_BASE + 2,
    GS_TYPE_ITVL_UNIT = GS_TYPE_FUNC_BASE + 3,
    GS_TYPE_UNINITIALIZED = GS_TYPE_FUNC_BASE + 4,

    /* The following datatypes be used for native date or timestamp type value to bind */
    GS_TYPE_NATIVE_DATE = GS_TYPE_FUNC_BASE + 5,      // native datetime, internal used
    GS_TYPE_NATIVE_TIMESTAMP = GS_TYPE_FUNC_BASE + 6, // native timestamp, internal used
    GS_TYPE_LOGIC_TRUE = GS_TYPE_FUNC_BASE + 7,       // native true, internal used

} gs_type_t;

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
    int64_t column_count;             //列数
    result_column* row_column_list;  //行包含的列列表
	struct st_result_row* next;
} result_row;

typedef struct st_intarkdb_res_def {
    int64_t row_count;   //行数
    bool is_select;
    void* res_row;      //行结果集    //这里实际是 RecordBatch*

    int64_t column_count;     //列数
    api_text_t* column_names; //列名
    char* msg; //执行结果信息

    char* value_ptr;    // for free column value
    int64_t row_idx;    // for next
} intarkdb_res_def;

typedef struct st_intarkdb_database {
	void* db;
} *intarkdb_database;

typedef struct st_intarkdb_connection {
	void* conn;
} *intarkdb_connection;

typedef enum en_status_def {
    SQL_ERROR = -1,
    SQL_SUCCESS = 0,
    SQL_TIMEDOUT = 1,
} intarkdb_state_t;

typedef struct st_intarkdb_res_def* intarkdb_result;

typedef struct st_intarkdb_prepared_statement {
	void* prep_stmt;
} *intarkdb_prepared_statement;

EXP_SQL_API intarkdb_state_t intarkdb_open(const char *path, intarkdb_database *db);

EXP_SQL_API void intarkdb_close(intarkdb_database *db);

EXP_SQL_API intarkdb_state_t intarkdb_connect(intarkdb_database database, intarkdb_connection *conn);

EXP_SQL_API void intarkdb_disconnect(intarkdb_connection *conn);

EXP_SQL_API intarkdb_state_t intarkdb_query(intarkdb_connection connection, const char *query, intarkdb_result result);

EXP_SQL_API intarkdb_result intarkdb_init_result();

EXP_SQL_API int32_t intarkdb_result_effect_row(intarkdb_result result);

EXP_SQL_API int64_t intarkdb_row_count(intarkdb_result result);

EXP_SQL_API int64_t intarkdb_column_count(intarkdb_result result);

EXP_SQL_API const char * intarkdb_column_name(intarkdb_result result, int64_t col);

EXP_SQL_API int32_t intarkdb_column_type(intarkdb_result result, int64_t col);

EXP_SQL_API void intarkdb_column_typename(intarkdb_result result, int64_t col, char *type_name, size_t max_len);

EXP_SQL_API char * intarkdb_value_varchar(intarkdb_result result, int64_t row, int64_t col);

EXP_SQL_API void intarkdb_free_row(intarkdb_result result);

EXP_SQL_API void intarkdb_destroy_result(intarkdb_result result);

EXP_SQL_API const char * intarkdb_result_msg(intarkdb_result result);

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

// ----------------------------------------prepare----------------------------------------------------
EXP_SQL_API intarkdb_state_t intarkdb_prepare(intarkdb_connection conn, const char *query, intarkdb_prepared_statement *out);

EXP_SQL_API int64_t intarkdb_prepare_nparam(intarkdb_prepared_statement prepared_statement);

EXP_SQL_API char *intarkdb_prepare_errmsg(intarkdb_prepared_statement prepared_statement);

EXP_SQL_API intarkdb_state_t intarkdb_execute_prepared(intarkdb_prepared_statement prepared_statement, intarkdb_result result);

EXP_SQL_API void intarkdb_destroy_prepare(intarkdb_prepared_statement *prepared_statement);

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

#ifdef __cplusplus
}
#endif