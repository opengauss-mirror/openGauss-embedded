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
 * intarkdb_sql-c.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/main/capi/intarkdb_sql-c.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <vector>

#include "main/connection.h"
#include "type/type_str.h"
#include "common/exception.h"
#include "interface/c/intarkdb_sql.h"

const int32_t DATE_FMT_YYYYMMDDHHMISS_LEN = 19;

struct DatabaseWrapper {
    std::shared_ptr<IntarkDB> instance;
};

struct PreparedStatementWrapper {
    std::unique_ptr<PreparedStatement> statement;
    std::vector<Value> values;
    std::string expanded_sql; // cache the expanded sql result
};

intarkdb_state_t intarkdb_open(const char *path, intarkdb_database *db) {
    auto wrapper = new DatabaseWrapper();
    try {
        wrapper->instance = std::shared_ptr<IntarkDB>(IntarkDB::GetInstance(path));
        wrapper->instance->Init();
    } catch (...) {
        delete wrapper;
        wrapper = nullptr;
        return SQL_ERROR;
    }
    *db = (intarkdb_database)wrapper;
    return SQL_SUCCESS;
}

void intarkdb_close(intarkdb_database *db) {
    if (db && *db) {
        auto wrapper = (DatabaseWrapper *)*db;
        delete wrapper;
        *db = nullptr;
    }
}

EXP_SQL_API const char *intarkdb_db_path(intarkdb_database database) {
    if (!database) {
        return nullptr;
    }
    auto wrapper = (DatabaseWrapper *)database;
    return wrapper->instance->GetDbPath().c_str();
}

intarkdb_state_t intarkdb_connect(intarkdb_database database, intarkdb_connection *conn) {
    if (!database || !conn) {
        return SQL_ERROR;
    }
    auto wrapper = (DatabaseWrapper *)database;
    Connection *connection = nullptr;
    try {
        connection = new Connection(wrapper->instance);

        connection->Init();
    } catch (...) {
        if (connection) {
            delete connection;
            connection = nullptr;
        }
        return SQL_ERROR;
    }
    *conn = (intarkdb_connection)connection;
    return SQL_SUCCESS;
}

intarkdb_state_t intarkdb_connect_with_user(intarkdb_database database,
                                            intarkdb_connection *conn,
                                            const char* user_name,
                                            const char* user_password,
                                            const char* user_ip,
                                            uint32_t sid,
                                            uint32_t* rescode,
                                            char* err_msg) {
    if (!database || !conn) {
        return SQL_ERROR;
    }
    auto wrapper = (DatabaseWrapper *)database;
    Connection *connection = nullptr;
    try {
        UserInfo user(user_name, user_password, user_ip, sid);
        connection = new Connection(wrapper->instance, user);
        connection->Init();
    } catch (intarkdb::Exception& e) {
        if (connection) {
            delete connection;
            connection = nullptr;
        }
        if (ExceptionType::ACCOUNT_AUTH_FAILED == e.type) {
            *rescode = ERR_ACCOUNT_AUTH_FAILED;
            strcpy_s(err_msg, GS_BUFLEN_1K, e.what());
        }
        return SQL_ERROR;
    } catch (...) {
        if (connection) {
            delete connection;
            connection = nullptr;

        }
        return SQL_ERROR;
    }
    *conn = (intarkdb_connection)connection;
    return SQL_SUCCESS;
}

void intarkdb_disconnect(intarkdb_connection *conn) {
    if (conn && *conn) {
        Connection *connection = (Connection *)*conn;
        delete connection;
        *conn = nullptr;
    }
}

EXP_SQL_API bool intarkdb_is_autocommit(intarkdb_connection *conn) {
    if (conn && *conn) {
        Connection *connection = (Connection *)*conn;
        return connection->IsAutoCommit();
    }
    return false;
}

intarkdb_result intarkdb_init_result() {
    intarkdb_result result = (intarkdb_res_def *)malloc(sizeof(intarkdb_res_def));
    if (!result) {
        return nullptr;
    }
    result->row_count = 0;
    result->res_row = nullptr;
    result->is_select = false;
    result->column_count = 0;
    result->column_names = nullptr;
    result->msg = nullptr;
    result->value_ptr = nullptr;
    result->row_idx = -1;
    result->need_result_ex = false;
    result->limit_rows_ex = 0;
    result->res_type = result_type_t::RESULT_BATCH;
    result->has_row_record = false;
    result->row_record = nullptr;
    return result;
}

void intarkdb_need_result_ex(intarkdb_result result, bool need) {
    if (result) {
        result->need_result_ex = need;
    }
}

void intarkdb_limit_rows_ex(intarkdb_result result, uint64_t limit) {
    if (result) {
        result->limit_rows_ex = limit;
    }
}

int64_t intarkdb_row_count(intarkdb_result result) {
    if (result) {
        return result->row_count;
    }
    return 0;
}

int64_t intarkdb_column_count(intarkdb_result result) {
    if (result) {
        return result->column_count;
    }
    return 0;
}

static void intarkdb_free_value(intarkdb_result result) {
    if (result && result->value_ptr) {
        free(result->value_ptr);
        result->value_ptr = nullptr;
    }
}

void intarkdb_free_row(intarkdb_result result) {
    intarkdb_free_value(result);

    // free res_row
    if (result) {
        if (result->res_row) {
            if (result->res_type == result_type_t::RESULT_ITERATOR) {
                auto ri = (RecordIterator *)result->res_row;
                delete ri;
                ri = nullptr;
                result->res_row = nullptr;
            } else {
                auto rb = (RecordBatch *)result->res_row;
                delete rb;
                rb = nullptr;
                result->res_row = nullptr;
            }
        }
        result->row_count = 0;
        result->column_count = 0;
        result->is_select = false;
        result->row_idx = -1;
        result->res_type = result_type_t::RESULT_BATCH;
        if (result->has_row_record) {
            result->has_row_record = false;
            auto r = (Record *)result->row_record;
            delete r;
            r = nullptr;
            result->row_record = nullptr;
        }
    }
}

void intarkdb_destroy_result(intarkdb_result result) {
    intarkdb_free_row(result);

    if (result) {
        free(result);
        result = nullptr;
    }
}

// -------------------------------------------------------------------------------------
static intarkdb_state_t intarkdb_translate_result(std::weak_ptr<IntarkDB> instance,
                        RecordBatch *rb, intarkdb_result result_out);
static intarkdb_state_t intarkdb_translate_result_iterator(RecordIterator *ri, intarkdb_result result_out);

static void reset_need_result_ex(intarkdb_result result) {
    if (result) {
        result->need_result_ex = false;
    }
}

static void reset_limit_rows_ex(intarkdb_result result) {
    if (result) {
        result->limit_rows_ex = 0;
    }
}

intarkdb_state_t intarkdb_query(intarkdb_connection connection, const char *query, intarkdb_result result) {
    intarkdb_free_row(result);

    if (!connection || !query) {
        return SQL_ERROR;
    }
    Connection *conn = (Connection *)connection;

    try {
        if (result) {
            conn->SetNeedResultSetEx(result->need_result_ex);
            conn->SetLimitRowsEx(result->limit_rows_ex);
            reset_need_result_ex(result);
            reset_limit_rows_ex(result);
        } else {
            conn->SetNeedResultSetEx(false);
        }
        auto rb = conn->Query(query);
        auto instance = conn->GetStorageInstance();
        return intarkdb_translate_result(instance, rb.release(), result);
    } catch (const std::exception &ex) {
        GS_LOG_RUN_WAR(ex.what());
        return SQL_ERROR;
    } catch (...) {
        return SQL_ERROR;
    }
}

intarkdb_state_t intarkdb_query_format(intarkdb_connection connection, intarkdb_result result, const char *format, ...) {
    if (!connection || !format) {
        return SQL_ERROR;
    }

    char query[GS_LOG_LONGSQL_LENGTH_16K] = {0};
    va_list args;
    va_start(args, format);
    vsnprintf_s(query, GS_LOG_LONGSQL_LENGTH_16K, GS_LOG_LONGSQL_LENGTH_16K - 1, format, args);
    va_end(args);

    return intarkdb_query(connection, query, result);
}

intarkdb_state_t intarkdb_query_iterator(intarkdb_connection connection, const char *query, intarkdb_result result) {
    intarkdb_free_row(result);

    if (!connection || !query) {
        return SQL_ERROR;
    }
    Connection *conn = (Connection *)connection;

    try {
        if (result) {
            conn->SetNeedResultSetEx(result->need_result_ex);
            conn->SetLimitRowsEx(result->limit_rows_ex);
            reset_need_result_ex(result);
            reset_limit_rows_ex(result);
        } else {
            conn->SetNeedResultSetEx(false);
        }
        auto ri = conn->QueryIterator(query);
        return intarkdb_translate_result_iterator(ri.release(), result);
    } catch (const std::exception &ex) {
        GS_LOG_RUN_WAR(ex.what());
        return SQL_ERROR;
    }
}

int32_t intarkdb_result_effect_row(intarkdb_result result_out) { return result_out->effect_row; }

static intarkdb_state_t intarkdb_translate_result(std::weak_ptr<IntarkDB> instance,
                                                RecordBatch *rb, intarkdb_result result_out) {
    if (!rb) {
        return SQL_ERROR;
    }
    if (!result_out) {
        auto ret = rb->GetRetCode();
        delete rb;
        rb = nullptr;
        return (intarkdb_state_t)ret;
    }

    if (rb->GetRecordBatchType() != RecordBatchType::Select) {
        // need_result_ex
        result_out->res_type = result_type_t::RESULT_BATCH;
        result_out->row_count = rb->RowCount();
        result_out->effect_row = rb->GetEffectRow();
        result_out->is_select = false;
        result_out->res_row = rb;
        result_out->column_count = rb->GetSchema().GetColumnInfos().size();
        result_out->msg = (char *)rb->GetRetMsg().c_str();
        if (rb->GetRecordBatchType() == RecordBatchType::Insert) {
            auto instance_ptr = instance.lock();
            if (instance_ptr) {
                instance_ptr->SetLastInsertRowid(rb->LastInsertRowid());
            }
            result_out->last_insert_rowid = rb->LastInsertRowid();
        }
        return (intarkdb_state_t)rb->GetRetCode();
    }

    result_out->res_type = result_type_t::RESULT_BATCH;
    result_out->row_count = rb->RowCount();
    result_out->effect_row = result_out->row_count;
    result_out->is_select = true;
    result_out->res_row = rb;
    result_out->column_count = rb->GetSchema().GetColumnInfos().size();
    result_out->msg = (char *)rb->GetRetMsg().c_str();
    return (intarkdb_state_t)rb->GetRetCode();
}

static intarkdb_state_t intarkdb_translate_result_iterator(RecordIterator *ri, intarkdb_result result_out) {
    if (!ri) {
        return SQL_ERROR;
    }
    if (!result_out) {
        auto ret = ri->GetRetCode();
        return (intarkdb_state_t)ret;
    }

    result_out->res_type = result_type_t::RESULT_ITERATOR;
    result_out->effect_row = ri->GetEffectRow();
    if (ri->GetStmtType() == StatementType::SELECT_STATEMENT || ri->GetStmtType() == StatementType::SHOW_STATEMENT) {
        result_out->is_select = true;
    } else {
        result_out->is_select = false;
    }
    result_out->res_row = ri;
    result_out->column_count = ri->GetSchema().GetColumnInfos().size();
    result_out->msg = (char *)ri->GetRetMsg().c_str();
    return (intarkdb_state_t)ri->GetRetCode();
}

static bool intarkdb_value_internal(intarkdb_result result, int64_t row, int64_t col, Value &val) {
    if (row < 0 || col < 0) {
        return false;
    }
    if (result) {
        if (result->res_type == result_type_t::RESULT_ITERATOR) {
            // RecordIterator can't get value from here, go to intarkdb_next
            return false;
        }
        if (result->res_row && row < result->row_count && col < result->column_count) {
            auto rb = (RecordBatch *)result->res_row;
            val = rb->RowRef(row).Field(col);
            if (val.IsNull()) {
                // null return nullptr
                return false;
            }
            return true;
        }
    }
    return false;
}

const char *intarkdb_column_name(intarkdb_result result, int64_t col) {
    if (col < 0) {
        return nullptr;
    }
    if (result) {
        if (result->res_row) {
            if (result->res_type == result_type_t::RESULT_ITERATOR) {
                auto ri = (RecordIterator *)result->res_row;
                const auto &columns = ri->GetSchema().GetColumnInfos();
                if (col < (int64_t)columns.size()) {
                    return columns[col].GetColNameWithoutTableName().c_str();
                }
            } else {
                auto rb = (RecordBatch *)result->res_row;
                const auto &columns = rb->GetSchema().GetColumnInfos();
                if (col < (int64_t)columns.size()) {
                    return columns[col].GetColNameWithoutTableName().c_str();
                }
            }
        }
    }
    return nullptr;
}

int32_t intarkdb_column_type(intarkdb_result result, int64_t col) {
    if (col < 0) {
        return GS_TYPE_UNKNOWN;
    }
    if (result) {
        if (result->res_row) {
            if (result->res_type == result_type_t::RESULT_ITERATOR) {
                auto ri = (RecordIterator *)result->res_row;
                const auto &columns = ri->GetSchema().GetColumnInfos();
                if (col < (int64_t)columns.size()) {
                    return columns[col].col_type.TypeId();
                }
            } else {
                auto rb = (RecordBatch *)result->res_row;
                const auto &columns = rb->GetSchema().GetColumnInfos();
                if (col < (int64_t)columns.size()) {
                    return columns[col].col_type.TypeId();
                }
            }
        }
    }
    return GS_TYPE_UNKNOWN;
}

void intarkdb_column_typename(intarkdb_result result, int64_t col, char *type_name, size_t max_len) {
    int32_t col_type = intarkdb_column_type(result, col);
    std::string col_type_name = fmt::format("{}",static_cast<GStorDataType>(col_type));
    strcpy_s(type_name, max_len, col_type_name.data()); 
    return;
}

//-------------------------------------------------------------------------//
// Fetch Default Value
//-------------------------------------------------------------------------//
struct FetchDefaultValue {
	template <class T>
	static T fetch_default_value() {
		return 0;
	}
};

template <>
char *FetchDefaultValue::fetch_default_value() {
    return nullptr;
}
template <>
std::string FetchDefaultValue::fetch_default_value() {
    return "";
}

template <class RESULT_TYPE>
static RESULT_TYPE try_cast_internal(const Value &val) {
    RESULT_TYPE result_value;
    try {
        return val.GetCastAs<RESULT_TYPE>();
    } catch (...) {
		return FetchDefaultValue::fetch_default_value<RESULT_TYPE>();
	}
	return result_value;
}

static char *intarkdb_value_ptr(intarkdb_result result, const Value &val) {
    auto value_str = try_cast_internal<std::string>(val);
    auto len = value_str.length();
    auto value_ptr = std::make_unique<char[]>(len + 1);
    memcpy_s(value_ptr.get(), len, value_str.c_str(), len);
    value_ptr.get()[len] = '\0';
    result->value_ptr = (char *)value_ptr.release();
    return result->value_ptr;
}

char *intarkdb_value_varchar(intarkdb_result result, int64_t row, int64_t col) {
    intarkdb_free_value(result);

    Value val;
    if (intarkdb_value_internal(result, row, col, val)) {
        return intarkdb_value_ptr(result, val);
    }
    return nullptr;
}

const char *intarkdb_result_msg(intarkdb_result result) {
    if (result && result->msg) {
        return result->msg;
    }
    return nullptr;
}

int64_t intarkdb_last_insert_rowid(intarkdb_database db) {
    if (db) {
        auto wrapper = (DatabaseWrapper *)db;
        auto instance = wrapper->instance;
        if (instance) {
            return instance->LastInsertRowid();
        }
    }
    return 0;
}
// ----------------------------------------result value-----------------------------------------------
bool intarkdb_next_row(intarkdb_result result) {
    if (result) {
        if (result->res_type == result_type_t::RESULT_ITERATOR) {
            if (result->res_row) {
                auto ri = (RecordIterator *)result->res_row;
                auto [r, eof] = ri->Next();
                if (eof) {
                    // delete the last row first
                    if (result->row_record) {
                        auto rd = (Record *)result->row_record;
                        delete rd;
                        result->row_record = nullptr;
                    }
                    return false;
                } else {
                    // delete the last row first
                    if (result->row_record) {
                        auto rd = (Record *)result->row_record;
                        delete rd;
                        result->row_record = nullptr;
                    }
                    // assign the next row
                    std::unique_ptr<Record> next_record = std::make_unique<Record>(std::move(r));
                    result->row_record = next_record.release();
                    result->has_row_record = true;
                    return true;
                }
            }
        } else {
            if (result->row_idx >= result->row_count - 1) {
                return false;
            } else {
                result->row_idx++;
                return true;
            }
        }
    }
    return false;
}

char *intarkdb_column_value(intarkdb_result result, int64_t col) {
    intarkdb_free_value(result);

    if (result->res_type == result_type_t::RESULT_ITERATOR) {
        if (result->has_row_record && result->row_record) {
            Record *row_record = (Record *)result->row_record;
            const auto &val = row_record->Field(col);
            return intarkdb_value_ptr(result, val);
        }
        return nullptr;
    }

    int64_t row = result->row_idx;
    Value val;
    if (intarkdb_value_internal(result, row, col, val)) {
        return intarkdb_value_ptr(result, val);
    }
    return nullptr;
}

bool intarkdb_value_boolean(intarkdb_result result, int64_t row, int64_t col) {
    Value val;
    if (intarkdb_value_internal(result, row, col, val)) {
        if (val.GetType() == GS_TYPE_BOOLEAN || val.IsInteger()) {
            return try_cast_internal<int64_t>(val) != 0 ? true : false;
        }
    }
    return false;
}

int32_t intarkdb_value_int32(intarkdb_result result, int64_t row, int64_t col) {
    Value val;
    if (intarkdb_value_internal(result, row, col, val)) {
        if (val.GetType() == GS_TYPE_BOOLEAN || val.IsInteger()) {
            return try_cast_internal<int32_t>(val);
        }
    }
    return 0;
}

int64_t intarkdb_value_int64(intarkdb_result result, int64_t row, int64_t col) {
    Value val;
    if (intarkdb_value_internal(result, row, col, val)) {
        if (val.GetType() == GS_TYPE_BOOLEAN || val.IsNumeric()) {
            return try_cast_internal<int64_t>(val);
        }
    }
    return 0;
}

uint32_t intarkdb_value_uint32(intarkdb_result result, int64_t row, int64_t col) {
    Value val;
    if (intarkdb_value_internal(result, row, col, val)) {
        if (val.GetType() == GS_TYPE_BOOLEAN || val.IsInteger()) {
            return try_cast_internal<uint32_t>(val);
        }
    }
    return 0;
}

uint64_t intarkdb_value_uint64(intarkdb_result result, int64_t row, int64_t col) {
    Value val;
    if (intarkdb_value_internal(result, row, col, val)) {
        if (val.GetType() == GS_TYPE_BOOLEAN || val.IsInteger()) {
            return try_cast_internal<uint64_t>(val);
        }
    }
    return 0;
}

double intarkdb_value_double(intarkdb_result result, int64_t row, int64_t col) {
    Value val;
    if (intarkdb_value_internal(result, row, col, val)) {
        if (val.IsNumeric()) {
            return try_cast_internal<double>(val);
        }
    }
    return 0;
}

// return format YYYY-MM-DD if column type is DATE
// return format YYYY-MM-DD HH24:MI:SS if column type is TIMESTAMP or DATETIME
// return nullptr if column type is other type
char *intarkdb_value_date(intarkdb_result result, int64_t row, int64_t col) {
    intarkdb_free_value(result);

    Value val;
    if (intarkdb_value_internal(result, row, col, val)) {
        if (!(val.GetType() == GS_TYPE_DATE || val.GetType() == GS_TYPE_TIMESTAMP)) {
            return nullptr;
        }

        std::string date_str = val.ToString();
        if (val.GetType() == GS_TYPE_TIMESTAMP && date_str.length() > DATE_FMT_YYYYMMDDHHMISS_LEN) {
            date_str = date_str.substr(0, DATE_FMT_YYYYMMDDHHMISS_LEN);
        }
        val = ValueFactory::ValueVarchar(date_str);
        return intarkdb_value_ptr(result, val);
    }
    return nullptr;
}

// return format YYYY-MM-DD HH24:MI:SS.FFFFFF if column type is TIMESTAMP
// return nullptr if column type is other type
char *intarkdb_value_timestamp(intarkdb_result result, int64_t row, int64_t col) {
    intarkdb_free_value(result);

    Value val;
    if (intarkdb_value_internal(result, row, col, val)) {
        if (!(val.GetType() == GS_TYPE_TIMESTAMP)) {
            return nullptr;
        }
        return intarkdb_value_ptr(result, val);
    }
    return nullptr;
}

// return milliseconds
int64_t intarkdb_value_timestamp_ms(intarkdb_result result, int64_t row, int64_t col) {
    Value val;
    if (intarkdb_value_internal(result, row, col, val)) {
        if (val.GetType() == GStorDataType::GS_TYPE_TIMESTAMP || val.GetType() == GStorDataType::GS_TYPE_DATE) {
            int64_t ts_1970_us = *((int64_t *)(val.GetRawBuff()));
            int64_t ts_1970_ms = ts_1970_us / MICROSECS_PER_MILLISEC;
            return ts_1970_ms;
        }
    }
    return 0;
}

int64_t intarkdb_value_timestamp_us(intarkdb_result result, int64_t row, int64_t col) {
    Value val;
    if (intarkdb_value_internal(result, row, col, val)) {
        if (val.GetType() == GStorDataType::GS_TYPE_TIMESTAMP || val.GetType() == GStorDataType::GS_TYPE_DATE) {
            int64_t ts_1970_us = *((int64_t *)(val.GetRawBuff()));
            return ts_1970_us;
        }
    }
    return 0;
}

// return blob void* pointer and length
void *intarkdb_value_blob(intarkdb_result result, int64_t row, int64_t col, int32_t *val_len) {
    intarkdb_free_value(result);
    if (!val_len) {
        return nullptr;
    }

    Value val;
    *val_len = 0;
    if (intarkdb_value_internal(result, row, col, val)) {
        if (!(val.GetType() == GS_TYPE_BLOB)) {
            return nullptr;
        }
        *val_len = val.Size();

        auto ptr_byte = std::make_unique<uint8_t[]>(val.Size() + 1);
        memcpy_s(ptr_byte.get(), *val_len, val.GetRawBuff(), *val_len);
        ptr_byte.get()[*val_len] = '\0';
        result->value_ptr = (char *)ptr_byte.release();
        return result->value_ptr;
    }
    return nullptr;
}

// return blob  length
void intarkdb_blob_len(intarkdb_result result, int64_t row, int64_t col, int32_t *val_len) {
    intarkdb_free_value(result);
    Value val;
    *val_len = 0;
    if (intarkdb_value_internal(result, row, col, val)) {
        if (val.GetType() == GStorDataType::GS_TYPE_BLOB) {
            *val_len = val.Size();
        }
    }
}

// -----------------------------------------prepare----------------------------------------------------
intarkdb_prepared_statement intarkdb_init_prepare_stmt(void) {
    return nullptr;
}

intarkdb_state_t intarkdb_prepare(intarkdb_connection connection, const char *query, intarkdb_prepared_statement *out) {
    if (!connection || !query || !out) {
        return SQL_ERROR;
    }
    auto wrapper = new PreparedStatementWrapper();

    try {
        Connection *conn = (Connection *)connection;
        wrapper->statement = conn->Prepare(query);
        *out = (intarkdb_prepared_statement)wrapper;
        return !wrapper->statement->HasError() ? SQL_SUCCESS : SQL_ERROR;
    } catch (const std::exception &ex) {
        delete wrapper;
        wrapper = nullptr;
        GS_LOG_RUN_WAR(ex.what());
        return SQL_ERROR;
    } catch (...) {
        delete wrapper;
        wrapper = nullptr;
        return SQL_ERROR;
    }
}

int64_t intarkdb_prepare_nparam(intarkdb_prepared_statement prepared_statement) {
    if (!prepared_statement) {
        return 0;
    }

    auto wrapper = (PreparedStatementWrapper *)prepared_statement;
    if (!wrapper || !wrapper->statement || wrapper->statement->HasError()) {
        return 0;
    }
    return wrapper->statement->ParamCount();
}

bool intarkdb_prepare_is_select(intarkdb_prepared_statement prepared_statement) {
    if (!prepared_statement) {
        return false;
    }

    auto wrapper = (PreparedStatementWrapper *)prepared_statement;
    if (!wrapper || !wrapper->statement || wrapper->statement->HasError()) {
        return false;
    }
    return wrapper->statement->IsRecordBatchSelect();
}

char *intarkdb_prepare_errmsg(intarkdb_prepared_statement prepared_statement) {
    if (!prepared_statement) {
        return nullptr;
    }

    auto wrapper = (PreparedStatementWrapper *)prepared_statement;
    if (!wrapper || !wrapper->statement) {
        return nullptr;
    }
    return (char *)wrapper->statement->ErrorMsg().c_str();
}

intarkdb_state_t intarkdb_execute_prepared(intarkdb_prepared_statement prepared_statement, intarkdb_result result) {
    intarkdb_free_row(result);

    try {
        auto wrapper = (PreparedStatementWrapper *)prepared_statement;
        if (!wrapper || !wrapper->statement || wrapper->statement->HasError()) {
            return SQL_ERROR;
        }
        if (result) {
            wrapper->statement->SetNeedResultSetEx(result->need_result_ex);
            wrapper->statement->SetLimitRowsEx(result->limit_rows_ex);
            reset_need_result_ex(result);
            reset_limit_rows_ex(result);
        } else {
            wrapper->statement->SetNeedResultSetEx(false);
        }
        auto rb = wrapper->statement->Execute(wrapper->values);
        auto instance = wrapper->statement->GetConnection()->GetStorageInstance();
        return intarkdb_translate_result(instance, rb.release(), result);
    } catch (const std::exception &ex) {
        GS_LOG_RUN_WAR(ex.what());
        return SQL_ERROR;
    } catch (...) {
        return SQL_ERROR;
    }
}

void intarkdb_destroy_prepare(intarkdb_prepared_statement *prepared_statement) {
    void **wrapper = reinterpret_cast<void **>(prepared_statement);

    if (!wrapper) {
        return;
    }

    auto casted = (PreparedStatementWrapper *)*wrapper;
    if (casted) {
        delete casted;
    }
    *wrapper = nullptr;
}

intarkdb_state_t intarkdb_clear_bindings(intarkdb_prepared_statement prepared_statement) {
	auto wrapper = reinterpret_cast<PreparedStatementWrapper *>(prepared_statement);
	if (!wrapper || !wrapper->statement || wrapper->statement->HasError()) {
		return SQL_ERROR;
	}
	wrapper->values.clear();
	return SQL_SUCCESS;
}

static intarkdb_state_t intarkdb_bind_value(intarkdb_prepared_statement prepared_statement, uint32_t param_idx, Value val) {
    auto wrapper = (PreparedStatementWrapper *)prepared_statement;
    if (!wrapper || !wrapper->statement || wrapper->statement->HasError()) {
        return SQL_ERROR;
    }
    if (param_idx == 0 || param_idx > wrapper->statement->ParamCount()) {
        return SQL_ERROR;
    }
    if (param_idx > wrapper->values.size()) {
        wrapper->values.resize(param_idx);
    }
    wrapper->values[param_idx - 1] = val;
    return SQL_SUCCESS;
}

intarkdb_state_t intarkdb_bind_boolean(intarkdb_prepared_statement prepared_statement, uint32_t param_idx, bool val) {
    return intarkdb_bind_value(prepared_statement, param_idx, ValueFactory::ValueBool(val));
}

intarkdb_state_t intarkdb_bind_int8(intarkdb_prepared_statement prepared_statement, uint32_t param_idx, int8_t val) {
    return intarkdb_bind_value(prepared_statement, param_idx, ValueFactory::ValueInt(val));
}

intarkdb_state_t intarkdb_bind_int16(intarkdb_prepared_statement prepared_statement, uint32_t param_idx, int16_t val) {
    return intarkdb_bind_value(prepared_statement, param_idx, ValueFactory::ValueInt(val));
}

intarkdb_state_t intarkdb_bind_int32(intarkdb_prepared_statement prepared_statement, uint32_t param_idx, int32_t val) {
    return intarkdb_bind_value(prepared_statement, param_idx, ValueFactory::ValueInt(val));
}

intarkdb_state_t intarkdb_bind_int64(intarkdb_prepared_statement prepared_statement, uint32_t param_idx, int64_t val) {
    return intarkdb_bind_value(prepared_statement, param_idx, ValueFactory::ValueBigInt(val));
}

intarkdb_state_t intarkdb_bind_uint8(intarkdb_prepared_statement prepared_statement, uint32_t param_idx, uint8_t val) {
    return intarkdb_bind_value(prepared_statement, param_idx, ValueFactory::ValueUnsignInt(val));
}

intarkdb_state_t intarkdb_bind_uint16(intarkdb_prepared_statement prepared_statement, uint32_t param_idx,
                                      uint16_t val) {
    return intarkdb_bind_value(prepared_statement, param_idx, ValueFactory::ValueUnsignInt(val));
}

intarkdb_state_t intarkdb_bind_uint32(intarkdb_prepared_statement prepared_statement, uint32_t param_idx,
                                      uint32_t val) {
    return intarkdb_bind_value(prepared_statement, param_idx, ValueFactory::ValueUnsignInt(val));
}

intarkdb_state_t intarkdb_bind_uint64(intarkdb_prepared_statement prepared_statement, uint32_t param_idx,
                                      uint64_t val) {
    return intarkdb_bind_value(prepared_statement, param_idx, ValueFactory::ValueUnsignBigInt(val));
}

intarkdb_state_t intarkdb_bind_float(intarkdb_prepared_statement prepared_statement, uint32_t param_idx, float val) {
    return intarkdb_bind_value(prepared_statement, param_idx, ValueFactory::ValueDouble(val));
}

intarkdb_state_t intarkdb_bind_double(intarkdb_prepared_statement prepared_statement, uint32_t param_idx, double val) {
    return intarkdb_bind_value(prepared_statement, param_idx, ValueFactory::ValueDouble(val));
}

intarkdb_state_t intarkdb_bind_date(intarkdb_prepared_statement prepared_statement, uint32_t param_idx,
                                    const char *val) {
    try {
        auto value_str = ValueFactory::ValueVarchar(val);
        auto value_date = DataType::GetTypeInstance(GStorDataType::GS_TYPE_TIMESTAMP)->CastValue(value_str);
        return intarkdb_bind_value(prepared_statement, param_idx, value_date);
    } catch (const std::exception &e) {
        return SQL_ERROR;
    }
}

intarkdb_state_t intarkdb_bind_timestamp_ms(intarkdb_prepared_statement prepared_statement, uint32_t param_idx,
                                            int64_t val) {
    try {
        int64_t ts_1970_ms = val;

        struct timestamp_stor_t ts;
        ts.ts = ts_1970_ms * MICROSECS_PER_MILLISEC;
        auto value_timestamp = ValueFactory::ValueTimeStamp(ts);
        return intarkdb_bind_value(prepared_statement, param_idx, value_timestamp);
    } catch (const std::exception &e) {
        return SQL_ERROR;
    }
}

intarkdb_state_t intarkdb_bind_timestamp_us(intarkdb_prepared_statement prepared_statement, uint32_t param_idx,
                                            int64_t val) {
    try {
        struct timestamp_stor_t ts;
        ts.ts = val;  // unix ts us
        auto value_timestamp = ValueFactory::ValueTimeStamp(ts);
        return intarkdb_bind_value(prepared_statement, param_idx, value_timestamp);
    } catch (const std::exception &e) {
        return SQL_ERROR;
    }
}

intarkdb_state_t intarkdb_bind_varchar(intarkdb_prepared_statement prepared_statement, uint32_t param_idx,
                                       const char *val) {
    return intarkdb_bind_value(prepared_statement, param_idx, ValueFactory::ValueVarchar(val));
}

intarkdb_state_t intarkdb_bind_decimal(intarkdb_prepared_statement prepared_statement, uint32_t param_idx,
                                       const char *val) {
    try {
        auto value_str = ValueFactory::ValueVarchar(val);
        auto value_decimal = DataType::GetTypeInstance(GStorDataType::GS_TYPE_DECIMAL)->CastValue(value_str);
        auto value_varchar = DataType::GetTypeInstance(GStorDataType::GS_TYPE_VARCHAR)->CastValue(value_decimal);
        return intarkdb_bind_value(prepared_statement, param_idx, value_varchar);
    } catch (const std::exception &e) {
        return SQL_ERROR;
    }
}

intarkdb_state_t intarkdb_bind_null(intarkdb_prepared_statement prepared_statement, uint32_t param_idx) {
    return intarkdb_bind_value(prepared_statement, param_idx, ValueFactory::ValueNull());
}

intarkdb_state_t intarkdb_bind_blob(intarkdb_prepared_statement prepared_statement, uint32_t param_idx,
                                    const void *data, uint32_t len) {
    return intarkdb_bind_value(prepared_statement, param_idx, ValueFactory::ValueBlob((uint8_t *)data, len));
}

char *intarkdb_expanded_sql(intarkdb_prepared_statement prepared_statement) {
    try {
        auto wrapper = (PreparedStatementWrapper *)prepared_statement;
        if (!wrapper || !wrapper->statement) {
            return nullptr;
        }
        // sqlit3 expanded sql result should be freed by sqlite3_free
        // but we don't have intarkdb_free, so we keep the string in wrapper
        // TODO: return new string buff in sqlite3_expanded_sql , othterwise user use sqlite3_free to free the buff
        // will cause crash
        wrapper->expanded_sql = wrapper->statement->ExpandedSQL();
        return (char *)wrapper->expanded_sql.c_str();
    } catch (...) {
        // do nothing
    }
    return nullptr;
}
EXP_SQL_API const char *intarkdb_sql(intarkdb_prepared_statement prepared_statement)
{
    try {
        auto wrapper = (PreparedStatementWrapper *)prepared_statement;
        if (!wrapper || !wrapper->statement) {
            return nullptr;
        }
        auto &original_sql = wrapper->statement->Original_sql();
        return (const char *)original_sql.c_str();
    } catch (...) {
        // do nothing
    }
    return nullptr;   
}
