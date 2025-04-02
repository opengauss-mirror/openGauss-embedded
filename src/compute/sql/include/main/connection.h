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
 * connection.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/main/connection.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <iostream>
#include <memory>

#include "binder/statement/transaction_statement.h"
#include "catalog/catalog.h"
#include "common/record_batch.h"
#include "common/record_streaming.h"
#include "main/database.h"
#include "main/prepare_statement.h"
#ifdef ENABLE_PG_QUERY
#include "postgres_parser.hpp"
#endif

constexpr uint32_t LONG_SQL_TIME = 1;

class UserInfo{
public:
    UserInfo(const UserInfo& user) : name(user.name), password(user.password), ip(user.ip), sid(user.sid) {}
    UserInfo(const std::string& name, const std::string& pswd, const std::string& ip, uint32_t sid) :
        name(name), password(pswd), ip(ip), sid(sid) {}

    const std::string& GetName() const { return name; }
    const std::string& GetPassword() const { return password; }
    const std::string& GetIP() const { return ip; }
    uint32_t GetSID() const { return sid; }
    static const UserInfo GetDefaultUser() { return {SYS_USER_NAME, "", "local", 0}; }

private:
    std::string name;
    std::string password;
    std::string ip;
    uint32_t sid; 
};

#ifdef ENABLE_PG_QUERY
class Binder;
#endif
class Planner;
class DeleteStatement;
class CtasStatement;
class ShowStatement;
class CopyStatement;

class Connection {
   public:
    EXPORT_API explicit Connection(std::shared_ptr<IntarkDB> instance, const UserInfo &user = UserInfo::GetDefaultUser());
    EXPORT_API ~Connection();

    EXPORT_API void Init();

    EXPORT_API std::unique_ptr<RecordBatch> Query(const char* query);
    EXPORT_API std::unique_ptr<RecordIterator> QueryIterator(const char* query);

    EXPORT_API std::unique_ptr<PreparedStatement> Prepare(const std::string& query);
    std::vector<ParsedStatement> ParseStatementsInternal(const std::string& query);
    std::unique_ptr<BoundStatement> BindSQLStmt(const ParsedStatement& stmt, const std::string& query);
    std::unique_ptr<PreparedStatement> CreatePreparedStatement(std::unique_ptr<BoundStatement> statement);
    std::unique_ptr<RecordBatch> ExecuteStatement(const std::string& query, std::unique_ptr<BoundStatement> statement);
    std::unique_ptr<RecordStreaming> ExecuteStatementStreaming(std::unique_ptr<BoundStatement> statement);
#ifdef ENABLE_PG_QUERY
    auto CreateBinder() -> Binder;
#endif

    Planner CreatePlanner();

    int CreateTable(const CreateStatement& stmt);

    int CreateIndex(const CreateIndexStatement& stmt);

    int CreateSequence(const CreateSequenceStatement& stmt);

    int AlterTable(const AlterStatement& stmt);

    bool TruncateTable(const std::string& query, const DeleteStatement& stmt, bool is_prepare);

    int CtasTable(CtasStatement& stmt, RecordBatch& result);

    EXPORT_API std::unique_ptr<TableInfo> GetTableInfo(const std::string& table_name);

    int CreateView(const std::string& viewName, const std::vector<Column>& columns, const std::string& query,
                   bool ignoreConflict);

    EXPORT_API std::string ShowCreateTable(const std::string& table_name, std::vector<std::string>& index_sqls);
    void ShowCreateTable(ShowStatement& stmt, RecordBatch& rb_out);

    auto CopyFrom(CopyStatement& stmt) -> int;
    auto CopyTo(CopyStatement& stmt, int64_t& effect_row) -> int;
    auto CopyFromInsertStatement(CopyStatement& stmt) -> std::unique_ptr<PreparedStatement>;
   
    void Rollback();

   public:
    EXPORT_API void* GetStorageHandle() { return handle_; }
    std::weak_ptr<IntarkDB> GetStorageInstance() { return instance_; }
    bool IsAutoCommit();

    void SetNeedResultSetEx(bool need) { is_need_result_ex = need; }

    void SetLimitRowsEx(uint64_t limit) { limit_rows_ex = limit; }

    UserInfo GetUser() const { return user_; }

   private:
    void SetBeginTransaction(TransactionType type);

    std::weak_ptr<IntarkDB> instance_;
    void* handle_{NULL};
    int conn_id_;
#ifdef ENABLE_PG_QUERY
    duckdb::PostgresParser parser_;
#endif
    std::unique_ptr<Catalog> catalog_;
    bool is_autocommit_param = true;
    bool is_begin_transaction = false;

    // if need insert resultset
    bool is_need_result_ex = false;
    // if need limit rows
    uint64_t limit_rows_ex = 0;
    UserInfo user_;

    std::string path_;
};
