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
* bind_show.cpp
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/binder/bind_show.cpp
*
* -------------------------------------------------------------------------
*/
#include <fmt/core.h>
#include <fmt/format.h>

#include "binder/binder.h"
#include "binder/transform_typename.h"
#include "common/exception.h"
#include "common/string_util.h"
#include "common/util.h"
#include "function/pragma/pragma_queries.h"
#include "pg_functions.hpp"

auto Binder::BindShowDataBase() -> std::unique_ptr<ShowStatement> {
    is_system_command_ = true;
    throw intarkdb::Exception(ExceptionType::BINDER, fmt::format("do not support show databases yet"));
}

auto Binder::BindShowTables() -> std::unique_ptr<ShowStatement> {
    is_system_command_ = true;
    auto result = std::make_unique<ShowStatement>();
    result->name = "show_tables";
    result->show_type = ShowType::SHOW_TYPE_TABLES;

    auto user_id = catalog_.GetUserID();
    auto query = PragmaShowTables(user_id, SQL_SPACE_TYPE_USERS);  //  gstor-> SPACE_TYPE_USERS
    duckdb_libpgquery::pg_parser_cleanup();  // bugfix:709996977,多次调用同一个parser.Parse时，需要先释放上一次申请的空间
    parser_.Parse(query);
    if (!parser_.success) {
        throw intarkdb::Exception(ExceptionType::PARSER,
                                  fmt::format("show stmt, query fail to parse:{}", parser_.error_message));
    }
    auto select_stmt = reinterpret_cast<duckdb_libpgquery::PGRawStmt *>(parser_.parse_tree->head->data.ptr_value)->stmt;
    result->stmt = BindSelect(reinterpret_cast<duckdb_libpgquery::PGSelectStmt *>(select_stmt));
    return result;
}

auto Binder::BindShowSpecificTable(const std::string &lname) -> std::unique_ptr<ShowStatement> {
    is_system_command_ = true;
    auto result = std::make_unique<ShowStatement>();
    result->name = "show";
    result->show_type = ShowType::SHOW_TYPE_SPECIFIC_TABLE;
    static constexpr size_t DOUBLE_QUOTE_WIDTH = 2;
    result->table_name =
        lname.substr(SHOWTABLE_NAME_START_IDX, lname.length() - DOUBLE_QUOTE_WIDTH);  // "tablename"  -> tablename
    result->table_info = catalog_.GetTable(user_, result->table_name);
    if (result->table_info == NULL) {
        GS_LOG_RUN_INF("%s", fmt::format("table {} not exists!", result->table_name).c_str());
        throw intarkdb::Exception(ExceptionType::CATALOG, fmt::format("table {} not found", result->table_name));
    }

    // user id
    auto user_id = catalog_.GetUserID();
    auto query = PragmaDescribeTable(user_id, result->table_info->GetTableId());
    duckdb_libpgquery::pg_parser_cleanup();  // bugfix:709996977,多次调用同一个parser.Parse时，需要先释放上一次申请的空间
    parser_.Parse(query);
    if (!parser_.success) {
        throw intarkdb::Exception(ExceptionType::PARSER,
                                  fmt::format("show stmt, query fail to parse:{}", parser_.error_message));
    }
    auto select_stmt = reinterpret_cast<duckdb_libpgquery::PGRawStmt *>(parser_.parse_tree->head->data.ptr_value)->stmt;
    result->stmt = BindSelect(reinterpret_cast<duckdb_libpgquery::PGSelectStmt *>(select_stmt));
    return result;
}

auto Binder::BindShowAll(const std::string &lname) -> std::unique_ptr<ShowStatement> {
    auto result = std::make_unique<ShowStatement>();
    result->name = "show_all";
    result->show_type = ShowType::SHOW_TYPE_ALL;
    return result;
}

auto Binder::BindShowCreateTable(const std::string &lname) -> std::unique_ptr<ShowStatement> {
    auto result = std::make_unique<ShowStatement>();
    result->name = "show create";
    result->show_type = ShowType::SHOW_TYPE_CREATE_TABLE;
    static constexpr size_t DOUBLE_QUOTE_WIDTH = 2;
    result->table_name =
        lname.substr(SHOWTABLE_NAME_START_IDX, lname.length() - DOUBLE_QUOTE_WIDTH);
    result->table_info = catalog_.GetTable(user_, result->table_name);
    if (result->table_info == NULL) {
        GS_LOG_RUN_INF("%s", fmt::format("Table {} not exists!", result->table_name).c_str());
        throw intarkdb::Exception(ExceptionType::CATALOG, fmt::format("table {} not found", result->table_name));
    }
    return result;
}

auto Binder::BindShowVariables(const std::string &variable_name) -> std::unique_ptr<ShowStatement> {
    auto result = std::make_unique<ShowStatement>();
    result->name = "show variables";
    result->show_type = ShowType::SHOW_TYPE_VARIABLES;
    result->variable_name = variable_name;
    return result;
}

auto BindShowUser() -> std::unique_ptr<ShowStatement> {
    auto result = std::make_unique<ShowStatement>();
    result->name = "__show_current_user";
    result->show_type = ShowType::SHOW_TYPE_USERS;
    result->variable_name = result->name;
    return result;
}

auto BindShowAllUsers() -> std::unique_ptr<ShowStatement> {
    auto result = std::make_unique<ShowStatement>();
    result->name = "__show_all_users";
    result->show_type = ShowType::SHOW_TYPE_USERS;
    result->variable_name = result->name;
    return result;
}

constexpr int32_t SHOW_CREATE_TABLE_FLAG = 2;
auto Binder::BindVariableShow(duckdb_libpgquery::PGVariableShowStmt *stmt) -> std::unique_ptr<ShowStatement> {
    auto result = std::make_unique<ShowStatement>();
    result->SetPragma(GS_TRUE);

    auto lname = intarkdb::StringUtil::Lower(stmt->name);
    std::string new_query;
    if (lname == "\"databases\"") {
        return BindShowDataBase();
    }
    if (lname == "\"tables\"") {
        return BindShowTables();
    }
    if (lname == "__show_tables_expanded") {
        return BindShowAll(lname);
    }
    if (lname == "__show_current_user") {
        return BindShowUser();
    }
    if (lname == "__show_all_users") {
        return BindShowAllUsers();
    }
    if (lname == "__variables_show") {
        auto variable_name = intarkdb::StringUtil::Lower(stmt->variable_name);
        return BindShowVariables(variable_name);
    }
    if (stmt->is_summary == SHOW_CREATE_TABLE_FLAG) {
        return BindShowCreateTable(lname);
    }
    // show one specific table
    return BindShowSpecificTable(lname);
}
