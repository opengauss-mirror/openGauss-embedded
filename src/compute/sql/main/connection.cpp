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
 * connection.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/main/connection.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "main/connection.h"

#include <fmt/core.h>
#include <fmt/ranges.h>

#include <iostream>
#include <random>
#include <set>
#include <system_error>

#ifdef ENABLE_PG_QUERY
#include "binder/binder.h"
#endif
#include "binder/expressions/bound_alias.h"
#include "binder/expressions/bound_func_call.h"
#include "binder/expressions/bound_seq_func.h"
#include "binder/statement_type.h"
#include "binder/statement/copy_statement.h"
#include "binder/statement/create_view.h"
#include "binder/statement/ctas_statement.h"
#include "binder/statement/delete_statement.h"
#include "binder/statement/show_statement.h"
#include "catalog/default_view.h"
#include "common/csv_util.h"
#include "common/default_value.h"
#include "common/gstor_exception.h"
#include "common/record_batch.h"
#include "common/string_util.h"
#include "common/exception.h"
#include "function/pragma/pragma_queries.h"
#include "planner/optimizer/optimizer.h"
#include "planner/planner.h"
#include "storage/gstor/zekernel/common/cm_log.h"
#include "type/type_str.h"
#include "common/stat.h"


typedef enum en_status_def {
    SQL_ERROR = -1,
    SQL_SUCCESS = 0,
    SQL_TIMEDOUT = 1,
} intarkdb_state_t;

Connection::Connection(std::shared_ptr<IntarkDB> instance, const UserInfo& user) : instance_(instance), user_(user) {}

Connection::~Connection() {
    if (handle_) {
#ifdef _MSC_VER
        GS_LOG_RUN_INF("[SessionEnd]PID:%lu user:%s IP:%s SID:%d", GetCurrentThreadId(), user_.GetName().c_str(),user_.GetIP().c_str(), user_.GetSID());
#else
        GS_LOG_RUN("[SessionEnd]PID:%lu user:%s IP:%s SID:%d", pthread_self(), user_.GetName().c_str(), user_.GetIP().c_str(), user_.GetSID());
#endif
        auto instance_ptr = instance_.lock();
        if (instance_ptr != nullptr) {
            // 回收handle
            instance_ptr->DestoryHandle(handle_);
        }
        handle_ = nullptr;
    }
}

void Connection::Init() {
    // get the database operate handle
    auto instance_ptr = instance_.lock();
    if (instance_ptr == nullptr) {
        throw intarkdb::IntarkDBInValidException("intarkdb instance is invalid");
    }

    path_ = instance_ptr->GetDbPath();

    int32_t rescode = SQL_ERROR;
    auto ret = instance_ptr->AllocHandle(&handle_);
    if (ret != GS_SUCCESS || handle_ == nullptr) {
        if (ret == GS_FULL_CONN) {
            throw std::runtime_error("The number of connections has reached the upper limit!");
        }
        throw std::runtime_error("get database handle fail");
    }
    
    //verify
    bool login = user_.GetIP() == UserInfo::GetDefaultUser().GetIP() ? 
        true : gstor_verify_user_password(((db_handle_t *)handle_)->handle, user_.GetName().c_str(), user_.GetPassword().c_str());
    rescode =  login ? SQL_SUCCESS : SQL_ERROR;

    if (!login) {
        if (instance_ptr != nullptr) {
            instance_ptr->DestoryHandle(handle_);
        }
        std::string errmsg(cm_get_errormsg(ERR_ACCOUNT_AUTH_FAILED));
        throw intarkdb::Exception(ExceptionType::ACCOUNT_AUTH_FAILED, errmsg);
    }

    // init catalog
    catalog_ = std::make_unique<Catalog>(user_.GetName(), handle_);

    // create default views
    //DefaultViewGenerator::CreateDefaultEntry(this); // 

    //todo add user log 
#ifdef _MSC_VER
    GS_LOG_RUN_INF("[SessionStart]PID:%lu user:%s IP:%s SID:%d", GetCurrentThreadId(), user_.GetName().c_str(),user_.GetIP().c_str(), user_.GetSID());
#else
    GS_LOG_RUN("[SessionStart]PID:%lu user:%s IP:%s SID:%d", pthread_self(), user_.GetName().c_str(), user_.GetIP().c_str(), user_.GetSID());
#endif
}

std::unique_ptr<PreparedStatement> Connection::Prepare(const std::string& query) {
    GS_LOG_RUN_INF("[DB:%s][Prepare SQL]:%s", path_.c_str(), query.c_str());
    try {
        auto statements = ParseStatementsInternal(query);
        if (statements.empty()) {
            throw std::runtime_error("No statement to prepare!");
        }
        if (statements.size() > 1) {
            throw std::runtime_error("Cannot prepare multiple statements at once!");
        }
        auto bound_statement = BindSQLStmt(statements[0], query);
#ifdef ENABLE_PG_QUERY
        parser_.Clear();
#endif
        return CreatePreparedStatement(std::move(bound_statement));
    } catch (std::exception& ex) {
        return std::make_unique<PreparedStatement>(true, std::string(ex.what()));
    }
}

std::vector<ParsedStatement> Connection::ParseStatementsInternal(const std::string& query_input) {
#ifdef ENABLE_PG_QUERY
    auto query_utf8_view = intarkdb::UTF8StringView(query_input);
    if (!query_utf8_view.IsUTF8()) {
        throw intarkdb::Exception(ExceptionType::SYNTAX, "query include invalid utf8 character");
    }
    std::string query = intarkdb::UTF8Util::TrimLeft(query_input);
    parser_.Parse(query);
    if (!parser_.success) {
        int location = parser_.error_location - 1 < 0 ? 0 : parser_.error_location - 1;
        location = (size_t)location >= query.length() ? query.length() - 1 : location;
        throw intarkdb::Exception(ExceptionType::SYNTAX, "[query fail to parse]" + parser_.error_message, location);
    }
    auto tree = parser_.parse_tree;
#endif
    std::vector<ParsedStatement> stmts;
#ifdef ENABLE_PG_QUERY
    if (!tree) {
        return stmts;
    }
    for (auto entry = tree->head; entry != nullptr; entry = entry->next) {
        ParsedStatement stmt;
        stmt.stmt = reinterpret_cast<duckdb_libpgquery::PGNode*>(entry->data.ptr_value);
        stmts.push_back(stmt);
    }
#endif
    return stmts;
}

std::unique_ptr<BoundStatement> Connection::BindSQLStmt(const ParsedStatement& stmt, const std::string& query) {
#ifdef ENABLE_PG_QUERY
    Binder binder = CreateBinder();
    binder.SetUser(user_.GetName());
    auto statement = binder.BindSQLStmt(stmt.stmt);
    statement->n_param = binder.ParamCount();
    statement->query = query;
    return statement;
#else
    return nullptr;
#endif
}

std::unique_ptr<PreparedStatement> Connection::CreatePreparedStatement(std::unique_ptr<BoundStatement> stmt) {
    Planner planner = CreatePlanner();
    intarkdb::Optimizer optimizer;
    PhysicalPlanPtr physical_plan;
    auto& statement = *stmt;
    switch (statement.Type()) {
        case StatementType::SELECT_STATEMENT: {
            auto& select_stmt = dynamic_cast<SelectStatement&>(statement);
            auto logical_plan = planner.PlanSelect(select_stmt);
            logical_plan = optimizer.OptimizeLogicalPlan(logical_plan);
            physical_plan = planner.CreatePhysicalPlan(logical_plan);
            break;
        }
        case StatementType::INSERT_STATEMENT: {
            auto& insert_stmt = dynamic_cast<InsertStatement&>(statement);
            auto logical_plan = planner.PlanInsert(insert_stmt);
            physical_plan = planner.CreatePhysicalPlan(logical_plan);
            break;
        }
        case StatementType::DELETE_STATEMENT: {
            auto& delete_stmt = dynamic_cast<DeleteStatement&>(statement);
            if (TruncateTable(statement.query, delete_stmt, true)) {
                return nullptr;
            }
            auto logical_plan = planner.PlanDelete(delete_stmt);
            logical_plan = optimizer.OptimizeLogicalPlan(logical_plan);
            physical_plan = planner.CreatePhysicalPlan(logical_plan);
            break;
        }
        case StatementType::UPDATE_STATEMENT: {
            auto& update_stmt = dynamic_cast<UpdateStatement&>(statement);
            auto logical_plan = planner.PlanUpdate(update_stmt);
            logical_plan = optimizer.OptimizeLogicalPlan(logical_plan);
            physical_plan = planner.CreatePhysicalPlan(logical_plan);
            break;
        }
        case StatementType::CREATE_STATEMENT:
        case StatementType::INDEX_STATEMENT:
        case StatementType::SEQUENCE_STATEMENT:
        case StatementType::TRANSACTION_STATEMENT:
        case StatementType::SET_STATEMENT:
        case StatementType::ALTER_STATEMENT:
        case StatementType::SHOW_STATEMENT:
        case StatementType::DROP_STATEMENT:
        case StatementType::CTAS_STATEMENT:
        case StatementType::CREATE_VIEW_STATEMENT:
        case StatementType::COPY_STATEMENT:
        case StatementType::CHECKPOINT_STATEMENT:
        case StatementType::EXPLAIN_STATEMENT:
        case StatementType::COMMENT_STATEMENT:
            break;
        default:
            std::invalid_argument("unspported statement type");
    }

    auto r = std::make_unique<PreparedStatement>(statement.query, statement.n_param, this, std::move(stmt),
                                                 std::move(physical_plan));
    r->params = planner.GetPrepareParams();
    if (statement.Type() == StatementType::SELECT_STATEMENT || statement.Type() == StatementType::SHOW_STATEMENT) {
        r->SetIsRecordBatchSelect(true);
    } else {
        r->SetIsRecordBatchSelect(false);
    }
    return r;
}

std::unique_ptr<RecordBatch> Connection::Query(const char* query) {
    GS_LOG_RUN_INF("[DB:%s][Query SQL]:%s", path_.c_str(), query);
    // statistical time
    struct timeval tv_begin;
    cm_gettimeofday(&tv_begin);
    // end statistical time

    std::unique_ptr<RecordBatch> result = std::make_unique<RecordBatch>(Schema());
    try {
        auto statements = ParseStatementsInternal(query);
        if (statements.empty()) {
            throw std::runtime_error("No statement to execute!");
        }
        for (auto& statement : statements) {
            auto bound_statement = BindSQLStmt(statement, query);
            result = ExecuteStatement(query, std::move(bound_statement));
        }
    } catch (const std::exception& e) {
        if (IsAutoCommit()) {
            Rollback();
        }
        result->SetRetCode(-1);
        result->SetRetMsg(e.what());
        GS_LOG_RUN_INF("%s", e.what());
    }
#ifdef ENABLE_PG_QUERY
    parser_.Clear();
#endif
    // statistical time
    struct timeval tv_end;
    cm_gettimeofday(&tv_end);
    auto nsec = tv_end.tv_sec - tv_begin.tv_sec;
    if (nsec > LONG_SQL_TIME) {
        auto nusec = tv_end.tv_usec - tv_begin.tv_usec;
        GS_LOG_RUN_WAR("[SQL_TIME:(%ld秒)(%ld微秒)][Query SQL]:%s", nsec, nusec, query);
    }
    // end statistical time

#ifdef _MSC_VER
    GS_LOG_RUN_INF("[SessionSQL]PID:%lu TableType:%s SID:%d", GetCurrentThreadId(),result->stmt_props.GetStrTableTypes().c_str(), user_.GetSID());
#else
    GS_LOG_RUN_INF("[SessionSQL]PID:%lu TableType:%s SID:%d", pthread_self(), result->stmt_props.GetStrTableTypes().c_str(), user_.GetSID());
#endif
    return result;
}

std::unique_ptr<RecordIterator> Connection::QueryIterator(const char* query) {
    GS_LOG_RUN_INF("[DB:%s][Query SQL]:%s", path_.c_str(), query);
    // statistical time
    struct timeval tv_begin;
    cm_gettimeofday(&tv_begin);
    // end statistical time

    std::unique_ptr<RecordIterator> result = nullptr;
    try {
        auto stmts = ParseStatementsInternal(query);
        if (stmts.empty()) {
            throw std::runtime_error("No statement to execute!");
        }
        for (auto& stmt : stmts) {
            auto bound_stmt = BindSQLStmt(stmt, query);
            if (bound_stmt->Type() == StatementType::SELECT_STATEMENT) {
                result = ExecuteStatementStreaming(std::move(bound_stmt));
            } else {
                result = ExecuteStatement(query, std::move(bound_stmt));
            }
            break;  // not support multi statement
        }
    } catch (const intarkdb::Exception& e) {
        if (IsAutoCommit()) {
            Rollback();
        }
        auto fail_record_batch = std::make_unique<RecordBatch>(Schema());
        fail_record_batch->SetRetCode(-1);
        fail_record_batch->SetRetMsg(e.what());
        fail_record_batch->SetRetLocation(e.GetLocation());
        result = std::move(fail_record_batch);
    } catch (const std::exception& e) {
        if (IsAutoCommit()) {
            Rollback();
        }
        auto fail_record_batch = std::make_unique<RecordBatch>(Schema());
        fail_record_batch->SetRetCode(-1);
        fail_record_batch->SetRetMsg(e.what());
        fail_record_batch->SetRetLocation(-1);
        result = std::move(fail_record_batch);
    }
#ifdef ENABLE_PG_QUERY
    parser_.Clear();
#endif

    // statistical time
    struct timeval tv_end;
    cm_gettimeofday(&tv_end);
    auto nsec = tv_end.tv_sec - tv_begin.tv_sec;
    if (nsec > LONG_SQL_TIME) {
        auto nusec = tv_end.tv_usec - tv_begin.tv_usec;
        GS_LOG_RUN_WAR("[SQL_TIME:(%ld秒)(%ld微秒)][Query SQL]:%s", nsec, nusec, query);
    }
    // end statistical time

#ifdef _MSC_VER
    GS_LOG_RUN_INF("[SessionSQL]PID:%lu TableType:%s SID:%d", GetCurrentThreadId(),result->stmt_props.GetStrTableTypes().c_str(), user_.GetSID());
#else
    GS_LOG_RUN_INF("[SessionSQL]PID:%lu TableType:%s SID:%d", pthread_self(), result->stmt_props.GetStrTableTypes().c_str(), user_.GetSID());
#endif
    return result;
}

std::unique_ptr<RecordStreaming> Connection::ExecuteStatementStreaming(std::unique_ptr<BoundStatement> statement) {
    if (statement && statement->Type() == StatementType::SELECT_STATEMENT) {
        try {
            auto& select_stmt = dynamic_cast<SelectStatement&>(*statement);
            Planner planner = CreatePlanner();
            auto logical_plan = planner.PlanSelect(select_stmt);
            intarkdb::Optimizer optimizer;
            logical_plan = optimizer.OptimizeLogicalPlan(logical_plan);
            auto physical_plan = planner.CreatePhysicalPlan(logical_plan);
            auto record_streaming = std::make_unique<RecordStreaming>(physical_plan->GetSchema(), physical_plan);
            record_streaming->stmt_type = select_stmt.Type();
            return record_streaming;
        } catch (const std::exception& e) {
            auto result = std::make_unique<RecordStreaming>(Schema(), nullptr);
            result->SetRetCode(-1);
            result->SetRetMsg(e.what());
            return result;
        }
    }
    throw std::runtime_error("ExecuteStatementStreaming not support non select statement");
}

std::unique_ptr<RecordBatch> Connection::ExecuteStatement(const std::string& query,
                                                          std::unique_ptr<BoundStatement> statement) {
    std::unique_ptr<RecordBatch> result = std::make_unique<RecordBatch>(Schema());
    intarkdb::Optimizer optimizer;
    switch (statement->Type()) {
        case StatementType::CREATE_STATEMENT: {
            auto& create_stmt = dynamic_cast<CreateStatement&>(*statement);
            // handle create table statement
            auto ret = CreateTable(create_stmt);
            if (ret != GS_SUCCESS && ret != GS_IGNORE_OBJECT_EXISTS) {
                throw std::runtime_error("CreateTable err");
            }
            break;
        }
        case StatementType::INDEX_STATEMENT: {
            auto& index_stmt = dynamic_cast<CreateIndexStatement&>(*statement);
            // handle create index statement
            if (CreateIndex(index_stmt) != GS_SUCCESS) {
                throw std::runtime_error("CreateIndex err");
            }
            break;
        }
        case StatementType::SEQUENCE_STATEMENT: {
            auto& sequence_stmt = dynamic_cast<CreateSequenceStatement&>(*statement);
            // handle create index statement
            if (CreateSequence(sequence_stmt) != GS_SUCCESS) {
                throw std::runtime_error("CreateSequence err");
            }
            break;
        }
        case StatementType::SELECT_STATEMENT: {
            auto& select_stmt = dynamic_cast<SelectStatement&>(*statement);
            Planner planner = CreatePlanner();
            auto logical_plan = planner.PlanSelect(select_stmt);

            logical_plan = optimizer.OptimizeLogicalPlan(logical_plan);

            auto physical_plan = planner.CreatePhysicalPlan(logical_plan);
            result = std::make_unique<RecordBatch>(physical_plan->GetSchema());
            result->SetRecordBatchType(RecordBatchType::Select);
            uint64_t row_count = 0;
            while (true) {
                if (limit_rows_ex > 0 && row_count >= limit_rows_ex) {
                    break;
                }
                auto&& [r, _, eof] = physical_plan->Next();
                if (eof) {
                    break;
                }
                result->AddRecord(std::move(r));
                row_count++;
            }
            break;
        }
        case StatementType::INSERT_STATEMENT: {
            auto& insert_stmt = dynamic_cast<InsertStatement&>(*statement);
            Planner planner = CreatePlanner();
            auto logical_plan = planner.PlanInsert(insert_stmt);
            logical_plan = optimizer.OptimizeLogicalPlan(logical_plan);
            auto physical_plan = planner.CreatePhysicalPlan(logical_plan);
            {
                physical_plan->SetAutoCommit(IsAutoCommit());
                physical_plan->SetNeedResultSetEx(is_need_result_ex);
                physical_plan->Execute(*result);
                result->SetRecordBatchType(RecordBatchType::Insert);
            }
            if (IsAutoCommit()) {
                gstor_commit(((db_handle_t*)handle_)->handle);
            }
            break;
        }
        case StatementType::DELETE_STATEMENT: {
            auto& delete_stmt = dynamic_cast<DeleteStatement&>(*statement);
            // check if truncate statement
            if (TruncateTable(query, delete_stmt, false)) {
                break;
            }

            Planner planner = CreatePlanner();
            auto logical_plan = planner.PlanDelete(delete_stmt);

            logical_plan = optimizer.OptimizeLogicalPlan(logical_plan);

            auto physical_plan = planner.CreatePhysicalPlan(logical_plan);
            result = std::make_unique<RecordBatch>(physical_plan->GetSchema());
            result->SetRecordBatchType(RecordBatchType::Delete);
            {
                auto r = physical_plan->Execute();
                result->SetEffectRow(r.GetEffectRow());
            }
            if (IsAutoCommit()) {
                gstor_commit(((db_handle_t*)handle_)->handle);
            }
            break;
        }
        case StatementType::TRANSACTION_STATEMENT: {
            auto& transaction_stmt = dynamic_cast<TransactionStatement&>(*statement);
            Planner planner = CreatePlanner();
            auto logical_plan = planner.PlanTransaction(handle_, transaction_stmt);
            auto physical_plan = planner.CreatePhysicalPlan(logical_plan);
            auto r = physical_plan->Execute();
            if (r.GetRetCode() != GS_SUCCESS) {
                throw std::runtime_error("transaction err");
            }
            SetBeginTransaction(transaction_stmt.type);
            break;
        }
        case StatementType::SET_STATEMENT: {
            auto& set_stmt = dynamic_cast<SetStatement&>(*statement);
            if (set_stmt.set_name == SetName::SET_NAME_AUTOCOMMIT) {
                is_autocommit_param = set_stmt.set_value.GetCastAs<bool>();
                break;
            }

            Planner planner = CreatePlanner();
            auto logical_plan = planner.PlanSet(set_stmt);
            auto physical_plan = planner.CreatePhysicalPlan(logical_plan);
            physical_plan->Execute();
            break;
        }
        case StatementType::UPDATE_STATEMENT: {
            auto& update_stmt = dynamic_cast<UpdateStatement&>(*statement);
            Planner planner = CreatePlanner();
            auto logical_plan = planner.PlanUpdate(update_stmt);

            logical_plan = optimizer.OptimizeLogicalPlan(logical_plan);

            auto physical_plan = planner.CreatePhysicalPlan(logical_plan);
            *result = RecordBatch(physical_plan->GetSchema());
            {
                auto r = physical_plan->Execute();
                result->SetEffectRow(r.GetEffectRow());
            }
            if (IsAutoCommit()) {
                gstor_commit(((db_handle_t*)handle_)->handle);
            }
            break;
        }
        case StatementType::ALTER_STATEMENT: {
            auto& alter_stmt = dynamic_cast<AlterStatement&>(*statement);
            // handle alter table statement
            if (AlterTable(alter_stmt) != GS_SUCCESS) {
                throw std::runtime_error("AlterTable err");
            }
            break;
        }
        case StatementType::SHOW_STATEMENT: {
            auto& show_stmt = dynamic_cast<ShowStatement&>(*statement);
            if (show_stmt.show_type == ShowType::SHOW_TYPE_CREATE_TABLE) {
                ShowCreateTable(show_stmt, *result);
                break;
            }
            show_stmt.db_path = path_;
            Planner planner = CreatePlanner();
            auto logical_plan = planner.PlanShow(show_stmt);
            if (show_stmt.show_type != ShowType::SHOW_TYPE_ALL &&
                show_stmt.show_type != ShowType::SHOW_TYPE_VARIABLES &&
                show_stmt.show_type != ShowType::SHOW_TYPE_USERS) {
                logical_plan = optimizer.OptimizeLogicalPlan(logical_plan);
            }
            auto physical_plan = planner.CreatePhysicalPlan(logical_plan);
            physical_plan->Execute(*result);
            result->SetRecordBatchType(RecordBatchType::Select);
            break;
        }
        case StatementType::DROP_STATEMENT: {
            auto& drop_stmt = dynamic_cast<DropStatement&>(*statement);
            Planner planner = CreatePlanner();
            auto logical_plan = planner.PlanDrop(drop_stmt);
            auto physical_plan = planner.CreatePhysicalPlan(logical_plan);
            auto r = physical_plan->Execute();
            if (r.GetRetCode() != GS_SUCCESS) {
                throw intarkdb::Exception(ExceptionType::EXECUTOR,r.GetRetMsg());
            }
            break;
        }
        case StatementType::CTAS_STATEMENT: {
            auto& ctas_stmt = dynamic_cast<CtasStatement&>(*statement);
            // handle create table as select statement
            if (CtasTable(ctas_stmt, *result) != GS_SUCCESS) {
                throw std::runtime_error("CtasTable err");
            }
            break;
        }
        case StatementType::CREATE_VIEW_STATEMENT: {
            auto& view_stmt = dynamic_cast<CreateViewStatement&>(*statement);
            auto queryStmt = view_stmt.getBoundSTMT();
            auto& select_stmt = dynamic_cast<SelectStatement&>(*queryStmt);

            // TODO: 有必要Planner吗?
            Planner planner = CreatePlanner();
            auto logical_plan = planner.PlanSelect(select_stmt);
            auto physical_plan = planner.CreatePhysicalPlan(logical_plan);
            // FIXME: 这个columns有什么必要？
            const auto& schema = physical_plan->GetSchema();
            auto columns = schema.GetColumns();
            if (CreateView(view_stmt.getViewName(), columns, query, view_stmt.ignore_conflict) != GS_SUCCESS) {
                throw std::runtime_error("CreateView err");
            }
            break;
        }
        case StatementType::COPY_STATEMENT: {
            auto& copy_stmt = static_cast<CopyStatement&>(*statement);
            if (copy_stmt.info->is_from) {
                CopyFrom(copy_stmt);
            } else {
                int64_t effect_row = 0;
                CopyTo(copy_stmt, effect_row);
                result->SetEffectRow(effect_row);
            }
            break;
        }
        case StatementType::CHECKPOINT_STATEMENT: {
            auto result = catalog_->forceCheckpoint();
            if (result != 0) {
                printf("force checkpoint return error %d !\n", result);
            } else {
                printf("force checkpoint success \n");
            }
            break;
        }
        case StatementType::EXPLAIN_STATEMENT: {
            break;
        }
        case StatementType::COMMENT_STATEMENT: {
            auto& comment_stmt = dynamic_cast<CommentStatement&>(*statement);
            Planner planner = CreatePlanner();
            auto logical_plan = planner.PlanCommentOn(catalog_.get(), comment_stmt);
            auto physical_plan = planner.CreatePhysicalPlan(logical_plan);
            auto r = physical_plan->Execute();
            break;
        }
        case StatementType::SYNONYM_STATEMENT: {
            auto& synonym_stmt = dynamic_cast<SynonymStatement&>(*statement);
            Planner planner = CreatePlanner();
            auto logical_plan = planner.PlanSynonym(synonym_stmt);
            auto physical_plan = planner.CreatePhysicalPlan(logical_plan);
            physical_plan->Execute();
            break;
        }
        default:
            throw intarkdb::Exception(ExceptionType::EXECUTOR, "unspported statement type");
    }
    result->stmt_props = statement->props;
    result->stmt_type = statement->Type();
    return result;
}

#ifdef ENABLE_PG_QUERY
auto Connection::CreateBinder() -> Binder { return Binder(*catalog_, parser_); }
#endif

Planner Connection::CreatePlanner() { return Planner(*catalog_); }

int Connection::CreateTable(const CreateStatement& stmt) {
    auto table_info = catalog_->GetTable(user_.GetName(), stmt.GetTableName());
    if (table_info != nullptr &&
        table_info->GetObjectType() != DIC_TYPE_TABLE) {  // 此处只处理非表名重复的情况，表名重复的情况在catalog中处理
        throw intarkdb::Exception(ExceptionType::CATALOG,
                                  fmt::format("object {} is not a table ", stmt.GetTableName()));
    }
    return catalog_->CreateTable(stmt.GetTableName(), stmt.GetColumns(), stmt);
}

int Connection::CreateIndex(const CreateIndexStatement& stmt) {
    auto ret = catalog_->CreateIndex(stmt.GetTableName(), stmt);
    return ret;
}

int Connection::CreateSequence(const CreateSequenceStatement& stmt) {
    auto ret = catalog_->CreateSequence(stmt);
    return ret;
}

bool Connection::TruncateTable(const std::string& query, const DeleteStatement& stmt, bool is_prepare) {
    std::string sql = query;
    for (size_t i = 0; i < sql.size(); i++) {
        if (sql[i] >= 'a' && sql[i] <= 'z') {
            sql[i] -= 32;
        }
    }

    if (sql.find("TRUNCATE") != std::string::npos) {
        if (is_prepare) {
            return true;
        }
        auto table_name = stmt.target_table->GetBoundTableName();

        // TRUNCATE
        int32 ret = gstor_truncate_table(((db_handle_t*)handle_)->handle, nullptr, (char*)table_name.c_str());
        if (ret != GS_SUCCESS) {
            printf("truncate table %s error!!\n", table_name.c_str());
        } else {
            printf("truncate table %s success!\n", table_name.c_str());
        }
        return true;
    }
    return false;
}

bool Connection::IsAutoCommit() {
    if (is_begin_transaction) {
        return false;
    } else {
        return is_autocommit_param;
    }
    // 兜底自动提交
    return true;
}

void Connection::SetBeginTransaction(TransactionType type) {
    if (type == TransactionType::BEGIN_TRANSACTION) {
        is_begin_transaction = true;
        int32 ret = gstor_set_is_begin_transaction(((db_handle_t*)handle_)->handle, GS_TRUE);
        if (ret != GS_SUCCESS) {
            printf("set session is_begin_transaction error!\n");
        }
    } else if (type == TransactionType::COMMIT || type == TransactionType::ROLLBACK) {
        is_begin_transaction = false;
        int32 ret = gstor_set_is_begin_transaction(((db_handle_t*)handle_)->handle, GS_FALSE);
        if (ret != GS_SUCCESS) {
            printf("set session is_begin_transaction error!\n");
        }
    }
}

int Connection::AlterTable(const AlterStatement& stmt) {
    auto& stmt_ref = const_cast<AlterStatement&>(stmt);

    if (stmt_ref.AlterType() == GsAlterTableType::ALTABLE_ADD_PARTITION) {
        stmt_ref.GetAlterTableInfoDefMutable().part_opt.hiboundval.str =
            const_cast<char*>(stmt_ref.hpartbound_.c_str());
        stmt_ref.GetAlterTableInfoDefMutable().part_opt.hiboundval.len = stmt_ref.hpartbound_.length();
    }

    return catalog_->AlterTable(stmt.GetTableName(), stmt);
}

int Connection::CtasTable(CtasStatement& stmt, RecordBatch& result) {
    Planner planner = CreatePlanner();

    auto select_plan = planner.PlanSelect(*stmt.as_select_);
    auto new_schema =
        Schema::RenameSchemaColumnIfExistSameNameColumns(select_plan->GetSchema());  // 如果有重名的列，会重命名列名
    const auto& col_infos = new_schema.GetColumnInfos();
    std::vector<Column> columns;
    columns.reserve(col_infos.size());
    // 与 SchemaColumnInfoToColumn 有细微差别，TODO整合
    for (const auto& col_info : col_infos) {
        auto def = intarkdb::NewColumnDef(col_info.col_type);
        def.nullable = true;
        def.col_slot = col_info.slot;
        if (!col_info.alias.empty()) {
            columns.emplace_back(col_info.alias, def);
        } else {
            // 忽略列的表名
            columns.emplace_back(col_info.col_name.back(), def);
        }
    }
    stmt.create_stmt_->SetColumns(std::move(columns));
    // create table
    auto ret = CreateTable(*stmt.create_stmt_);
    if (ret != GS_SUCCESS) {
        if (ret == GS_IGNORE_OBJECT_EXISTS) {
            return GS_SUCCESS;
        }
        throw std::runtime_error("Ctas-CreateTable err");
    }

    try {
        // insert data to new table : insert into select
        auto table_info = catalog_->GetTable(user_.GetName(), stmt.tablename_);
        if (!table_info) {
            throw std::invalid_argument(fmt::format("invalid table {}", stmt.tablename_));
        }
        auto table_ref =
            std::make_unique<BoundBaseTable>(user_.GetName(), stmt.tablename_, std::nullopt, std::move(table_info));

        auto logical_plan = planner.PlanInsert(std::move(table_ref), std::move(stmt.create_stmt_), select_plan);
        auto physical_plan = planner.CreatePhysicalPlan(logical_plan);
        physical_plan->Execute(result);
    } catch (const std::runtime_error& e) {
        drop_def_t drop_info = {0};
        drop_info.name = (char*)stmt.tablename_.c_str();
        drop_info.if_exists = (int)false;
        drop_info.type = DROP_TYPE_TABLE;
        auto user = user_.GetName();
        if (gstor_drop(((db_handle_t*)handle_)->handle, (char*)user.c_str(), &drop_info) != GS_SUCCESS) {
            GS_LOG_RUN_ERR("CtasTable runtime_error, drop_table failed");
        }
        Rollback();
        throw std::runtime_error(e.what());
    } catch (...) {
        drop_def_t drop_info = {0};
        drop_info.name = (char*)stmt.tablename_.c_str();
        drop_info.if_exists = (int)false;
        drop_info.type = DROP_TYPE_TABLE;
        auto user = user_.GetName();
        if (gstor_drop(((db_handle_t*)handle_)->handle, (char*)user.c_str(), &drop_info) != GS_SUCCESS) {
            GS_LOG_RUN_ERR("CtasTable exception, drop_table failed");
        }
        Rollback();
        throw std::runtime_error("unknown error.");
    }
    if (IsAutoCommit()) {
        gstor_commit(((db_handle_t*)handle_)->handle);
    }

    return GS_SUCCESS;
}

std::unique_ptr<TableInfo> Connection::GetTableInfo(const std::string& table_name) {
    return catalog_->GetTable(user_.GetName(), table_name);
}

int Connection::CreateView(const std::string& viewName, const std::vector<Column>& columns, const std::string& query,
                           bool ignoreConflict) {
    return catalog_->CreateView(viewName, columns, query, ignoreConflict);
}

void Connection::ShowCreateTable(ShowStatement& stmt, RecordBatch& rb_out) {
    std::vector<SchemaColumnInfo> columns = {
        {{"__show_create_table", "sql"}, "", GS_TYPE_VARCHAR, 0},
    };
    auto schema = Schema(std::move(columns));
    rb_out = RecordBatch(schema);

    std::vector<std::string> v_index_sqls;
    std::string table = stmt.table_name;
    auto sql = ShowCreateTable(table, v_index_sqls) + "\n";
    for (auto item : v_index_sqls) {
        sql += item + "\n";
    }

    std::vector<Value> row_values;
    row_values.push_back(Value(GStorDataType::GS_TYPE_VARCHAR, sql));
    rb_out.AddRecord(Record(std::move(row_values)));
}

static inline void ShowPartitionInfo(const exp_table_meta &table_meta, std::stringstream &sschema, std::stringstream &sErr) {
    sschema << "\n";
    const exp_part_table_t *part_table = &table_meta.part_table;
    if (part_table->desc.parttype ==PART_TYPE_RANGE)
        sschema << "PARTITION BY RANGE(";
    else if (part_table->desc.parttype == PART_TYPE_LIST)
        sschema << "PARTITION BY LIST(";
    else if (part_table->desc.parttype == PART_TYPE_HASH)
        sschema << "PARTITION BY HASH(";
    else {
        sErr << "invalid part type:" << table_meta.part_table.desc.parttype << std::endl;
        return;
    }
    for (uint32 partkey_idx=0; partkey_idx < part_table->desc.partkeys; ++partkey_idx){
        sschema << std::string(table_meta.columns[part_table->keycols[partkey_idx].column_id].name.str);
        if (partkey_idx != part_table->desc.partkeys - 1) sschema << ",";
    }
    sschema << ") TIMESCALE INTERVAL '" << part_table->desc.interval.str << "' ";
    if (table_meta.has_retention) {
        sschema << "RETENTION '";
        if (table_meta.retention.day>0)
            sschema << table_meta.retention.day << "d'";
        else if (table_meta.retention.hour>0)
            sschema << table_meta.retention.hour << "h'";
        else {
            sErr << "invalid part retention" << std::endl;
            return;
        }
    }
    if (part_table->desc.auto_addpart)
        sschema << " AUTOPART";
    if (part_table->desc.is_crosspart)
        sschema << " CROSSPART ";
}

std::string Connection::ShowCreateTable(const std::string& table_name, std::vector<std::string>& index_sqls) {
    auto table = GetTableInfo(table_name);
    if (table == nullptr) throw std::runtime_error("table:" + table_name + " not exist");
    auto table_meta = table->GetTableMetaInfo();
    std::stringstream sschema;
    sschema << "CREATE TABLE " << table_meta.name << "(\n";
    std::vector<uint16> primary_col_ids;
    for (size_t i = 0; i < table_meta.index_count; i++) {
        auto index = table_meta.indexes[i];
        bool bIndexSQL = true;

        if (index.is_primary) {
            for (size_t j = 0; j < index.col_count; j++) {
                primary_col_ids.push_back(index.col_ids[j]);
            }
            bIndexSQL = false;
        }

        // 过滤时序分区表自动创建的分区索引
        if (table_meta.is_timescale && table_meta.parted && index.col_count == table_meta.part_table.desc.partkeys) {
            bIndexSQL = false;
            for (size_t col_idx = 0; col_idx < index.col_count; col_idx++) {
                if (index.col_ids[col_idx] != table_meta.part_table.keycols[col_idx].column_id) {
                    bIndexSQL = true;
                    break;
                }
            }
        }

        if (bIndexSQL) {
            std::string index_sql = "CREATE ";
            if (index.is_unique) index_sql += "UNIQUE ";

            index_sql += "INDEX " + std::string(index.name.str) + " on " + std::string(table_meta.name) + "(";
            for (size_t j = 0; j < index.col_count; j++) {
                index_sql += table_meta.columns[index.col_ids[j]].name.str;
                if (j != index.col_count - 1) index_sql += ", ";
            }
            index_sql += ");";
            index_sqls.push_back(index_sql);
        }
    }

    // COMMENT
    std::string table_comment;
    bool32 is_table_comment = GS_FALSE;
    std::string table_id = std::to_string(table_meta.id);
    std::string sql = "select \"COLUMN#\",\"COMMENT#\" from \"SYS_COMMENT\" where \"TABLE#\" = " + table_id;
    auto rb = Query(sql.c_str());

    for (size_t i = 0; i < table_meta.column_count; i++) {
        auto column = table_meta.columns[i];
        sschema << column.name.str << " ";
        std::string col_type_name = fmt::format("{}", column.col_type);
        if (col_type_name == UNKNOWN_TYPE_NAME) {
            std::stringstream sErr;
            sErr << "not support " << column.name.str << " type:" << column.col_type << std::endl;
            return sErr.str();
        }
        sschema << col_type_name;
        if (column.precision > 0)
            sschema << "(" << column.precision << ", " << column.scale << ")";
        else if (column.col_type == gs_type_t::GS_TYPE_VARCHAR && column.size != COLUMN_VARCHAR_SIZE_DEFAULT)
            sschema << "(" << column.size << ")";

        if (!column.nullable) sschema << " NOT NULL";

        if (column.is_default) {
            DefaultValue* default_value = reinterpret_cast<DefaultValue*>(const_cast<char*>(column.default_val.str));
            auto value = ShowValue(*default_value, column.col_type);
            if (default_value->type == DefaultValueType::DEFAULT_VALUE_TYPE_FUNC) {
                sschema << " DEFAULT " << value.ToString();
            } else {
                sschema << " DEFAULT " << value.ToSQLString();
            }
        }

        if (primary_col_ids.size() == 1 && primary_col_ids[0] == i) sschema << " PRIMARY KEY";

        // COMMENT
        auto col_slot = std::to_string(column.col_slot);
        for (size_t i = 0; i < rb->RowCount(); ++i) {
            auto& row = rb->RowRef(i);
            const auto& val_col_slot = row.Field(0);
            // COLUMN COMMENT
            if (val_col_slot.ToString() == col_slot) {
                auto val_col_comment = row.Field(1).ToSQLString();
                sschema << " COMMENT " << val_col_comment;
                if (is_table_comment) break;
            }
            // TABLE COMMENT
            if (val_col_slot.IsNull()) {
                is_table_comment = GS_TRUE;
                table_comment = row.Field(1).ToSQLString();
            }
        }

        if (i != table_meta.column_count - 1) sschema << ",\n";
    }
    if (primary_col_ids.size() > 1) {
        sschema << ", PRIMARY KEY (";
        auto iter = primary_col_ids.begin();
        while (iter != primary_col_ids.end()) {
            sschema << std::string(table_meta.columns[*iter].name.str);
            if (++iter != primary_col_ids.end()) sschema << ",";
        }
        sschema << ")";
    }
    sschema << ")";
    if (table_meta.is_timescale && table_meta.parted) {
        std::stringstream sErr;
        ShowPartitionInfo(table_meta, sschema, sErr);
        if (!sErr.str().empty()) {
            return sErr.str();
        }
    }
    if (is_table_comment) {
        sschema << " COMMENT " << table_comment;
    }
    sschema << ";";
    return sschema.str();
}

static auto GetDelimiter(const std::unordered_map<std::string, std::vector<Value>>& options) -> char {
    char delimiter = intarkdb::kDefaultCSVDelimiter;
    auto delimiter_it = options.find("delimiter");
    if (delimiter_it != options.end()) {
        if (delimiter_it->second.size() == 0) {
            throw intarkdb::Exception(ExceptionType::PARSER, "'delimiter' expects a single argument as a string value");
        }
        const auto& tmp_delimiter = delimiter_it->second[0].GetCastAs<std::string>();
        if (tmp_delimiter.size() > 1 || tmp_delimiter.size() == 0) {
            throw intarkdb::Exception(ExceptionType::PARSER,
                                      "'delimiter' expects a single argument as a string value with signle character");
        }
        delimiter = tmp_delimiter[0];
    }
    return delimiter;
}

// 构造 insert PreparedStatement for copy from
auto Connection::CopyFromInsertStatement(CopyStatement& stmt) -> std::unique_ptr<PreparedStatement> {
    if (!stmt.info->table_ref) {
        throw intarkdb::Exception(ExceptionType::EXECUTOR, "insert table is null");
    }
    if (stmt.info->select_list.size() <= 0) {
        throw intarkdb::Exception(ExceptionType::EXECUTOR, "insert table field is null");
    }
    std::string sql = "insert into " + stmt.info->table_ref->GetBoundTableName() + "(";
    std::string values = "(";
    auto item_num = stmt.info->select_list.size();
    for (size_t i = 0; i < item_num; ++i) {
        const auto& item = stmt.info->select_list[i];
        if (item->Type() != ExpressionType::COLUMN_REF) {
            throw intarkdb::Exception(ExceptionType::BINDER, "not support non column name in insert values list");
        }
        BoundColumnRef& column_ref = static_cast<BoundColumnRef&>(*item);
        const auto& colname = column_ref.Name().back();
        if (i + 1 < item_num) {
            sql += fmt::format("{}", colname) + ",";
            values += "?,";
        } else {
            sql += fmt::format("{}", colname) + ")";
            values += "?)";
        }
    }
    sql += "values " + values;
    return Prepare(sql);
}

auto Connection::CopyTo(CopyStatement& stmt, int64_t& effect_row) -> int {
    auto planner = CreatePlanner();
    auto logical_plan = planner.PlanSelect(*stmt.select_statement);

    intarkdb::Optimizer optimizer;

    logical_plan = optimizer.OptimizeLogicalPlan(logical_plan);

    auto physical_plan = planner.CreatePhysicalPlan(logical_plan);

    auto delimiter = GetDelimiter(stmt.info->options);
    intarkdb::CSVWriter writer(stmt.info->file_path, delimiter);
    if (!writer.Open()) {
        throw intarkdb::Exception(ExceptionType::EXECUTOR, "open file failed");
    }
    effect_row = 0;
    while (true) {
        const auto& [r, _, eof] = physical_plan->Next();
        if (eof) {
            break;
        }
        writer.WriteRecord(r);
        effect_row++;
    }
    if (IsAutoCommit()) {
        gstor_commit(((db_handle_t*)handle_)->handle);
    }
    return GS_SUCCESS;
};

auto Connection::CopyFrom(CopyStatement& stmt) -> int {
    char delimiter = GetDelimiter(stmt.info->options);
    intarkdb::CSVReader reader(
        stmt.info->file_path,
        Schema(stmt.info->table_ref->GetTableNameOrAlias(), stmt.info->table_ref->GetTableInfo().columns), delimiter);
    if (!reader.Open()) {
        throw intarkdb::Exception(ExceptionType::EXECUTOR, "open file failed");
    }
    auto prepare_insert_stmt = CopyFromInsertStatement(stmt);
    while (true) {
        const auto& [r, eof] = reader.ReadRecord();
        if (eof) {
            break;
        }
        prepare_insert_stmt->Execute(RecordToVector(r));
    }
    if (IsAutoCommit()) {
        gstor_commit(((db_handle_t*)handle_)->handle);
    }
    return GS_SUCCESS;
}

std::string GetColString(std::vector<std::string> col_names) {
    std::string result = " ";
    for (size_t i = 0; i < col_names.size(); i++) {
        result += col_names[i];
        result += " real, ";
    }

    return result;
}

static std::string GetSubstring(const std::string& str) {
    std::string result;
    auto low_str = intarkdb::StringUtil::Lower(str);
    size_t pos = low_str.find("where");
    result = pos == std::string::npos ? str: str.substr(0, pos);
    return result;
}

static std::string GetAggSql(const std::string& sql, const std::string& bucket_field) {
    std::string result;
    auto low_sql = intarkdb::StringUtil::Lower(sql);
    size_t pos = low_sql.find("from");
    size_t pos_start = low_sql.find("where");
    size_t pos_end = low_sql.find(">");
    if ((pos == std::string::npos) || (pos_start == std::string::npos) || (pos_end == std::string::npos)) {
        throw std::runtime_error("illegal aggregation sql!");
    }
    constexpr size_t WHERE_LEN = 5; // "where" length
    std::regex pattern(R"(^\s+|\s+$)");
    if (low_sql.find("cw_scrap_rate")!= std::string::npos 
        || low_sql.find("cw_complete_rate")!= std::string::npos) {
        result = sql;
    } else {
        result = sql.substr(0, pos - 1) + ", max(" +
             std::regex_replace(sql.substr(pos_start + WHERE_LEN, pos_end - pos_start - WHERE_LEN - 1), pattern, "") + ") as " +
             bucket_field + " " + sql.substr(pos);
    }
    return result;
}

static std::string GetTableName(const std::string& sql) {
    std::string table_name;
    auto low_sql = intarkdb::StringUtil::Lower(sql);
    size_t from_len = 4; // "from" length
    size_t pos_start = low_sql.find("from");
    size_t pos_end = low_sql.find("where");
    if ((pos_start == std::string::npos) || (pos_end == std::string::npos)) {
        throw std::runtime_error("illegal aggregation sql, no from or no where!");
    }
    pos_start += from_len;
    pos_end -= 1; // remove the space before where
    std::regex pattern(R"(^\s+|\s+$)");
    table_name = std::regex_replace(sql.substr(pos_start, pos_end - pos_start), pattern, "");
    return table_name;
}

void Connection::Rollback() { gstor_rollback(((db_handle_t*)handle_)->handle); }
