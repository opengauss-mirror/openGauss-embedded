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
 * bind_ctas.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/binder/bind_ctas.cpp
 *
 * -------------------------------------------------------------------------
 */
// ctas: create table as select

#include <fmt/core.h>
#include <fmt/format.h>

#include "binder/binder.h"
#include "binder/expressions/bound_alias.h"
#include "common/exception.h"
#include "common/util.h"
#include "planner/expression_iterator.h"

using duckdb_libpgquery::PGCreateTableAsStmt;

static auto CheckUnSupportedFeature(PGCreateTableAsStmt &ctas_stmt) -> void {
    if (ctas_stmt.onconflict == duckdb_libpgquery::PG_REPLACE_ON_CONFLICT) {
        throw intarkdb::Exception(ExceptionType::BINDER,"Replace expr is not supported in create-table-as-select sql!");
    }
    if (ctas_stmt.relkind == duckdb_libpgquery::PG_OBJECT_MATVIEW) {
        throw intarkdb::Exception(ExceptionType::BINDER, "Unsupport Materialized view in create-table-as-select sql!");
    }
    if (ctas_stmt.is_select_into || ctas_stmt.into->colNames || ctas_stmt.into->options) {
        throw intarkdb::Exception(ExceptionType::BINDER, "Unsupport features in create-table-as-select sql!");
    }
    
    if (ctas_stmt.query->type != duckdb_libpgquery::T_PGSelectStmt) {
        throw intarkdb::Exception(ExceptionType::BINDER, "Only select statement is supported in create-table-as-select sql!");
    }
}

auto Binder::BindCtas(PGCreateTableAsStmt *stmt) -> std::unique_ptr<CtasStatement> {
    CheckUnSupportedFeature(*stmt);

    if (!stmt->into->rel->relname) {
        throw intarkdb::Exception(ExceptionType::BINDER, "missing schema name");
    }
    std::string table_name = stmt->into->rel->relname;

    if (table_name.length() >= GS_NAME_BUFFER_SIZE) {
        throw intarkdb::Exception(ExceptionType::BINDER,
                                  fmt::format("table name is too long, max length:{}", GS_NAME_BUFFER_SIZE - 1));
    }

    auto select_statement = BindSelectStmt(reinterpret_cast<duckdb_libpgquery::PGSelectStmt *>(stmt->query));

    // TODO: 目前先暂时不支持param 存在于 select list 中[ctas 特殊情况]
    for (auto &expr : select_statement->select_expr_list) {
        if (expr->HasParameter()) {
            throw intarkdb::Exception(ExceptionType::BINDER, "Not supported parameter in select list");
        }
    }

    auto create_statement = std::make_unique<CreateStatement>(table_name);
#ifdef ENABLE_PG_QUERY
    create_statement->SetConflictOpt(stmt->onconflict);
#endif
    auto ctas_stmt =
        std::make_unique<CtasStatement>(table_name, std::move(select_statement), std::move(create_statement));
    return ctas_stmt;
}
