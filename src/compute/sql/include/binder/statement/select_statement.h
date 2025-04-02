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
 * select_statement.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/binder/statement/select_statement.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <memory>
#include <set>
#include <vector>

#include "binder/bound_expression.h"
#include "binder/bound_sort.h"
#include "binder/bound_statement.h"
#include "binder/bound_query_source.h"
#include "common/set_operation_type.h"
#ifdef ENABLE_PG_QUERY
#include "nodes/lockoptions.hpp"
#endif

struct LimitClause {
    std::unique_ptr<BoundExpression> limit;
    std::unique_ptr<BoundExpression> offset;
};

struct ReturnItem {
    std::vector<std::string> col_name;
    LogicalType col_type;
};

struct ValueClaluse {
    std::vector<SchemaColumnInfo> columns;
    std::vector<std::vector<std::unique_ptr<BoundExpression>>> values;
};

#ifdef ENABLE_PG_QUERY
struct LockClause {
    explicit LockClause(std::set<std::string> table_rels,
        duckdb_libpgquery::PGLockClauseStrength strength,
        gstor_rowmark_type_t wait_policy,
        uint32_t wait_n)
        : table_rels_(table_rels),
        strength_(strength),
        wait_policy_(wait_policy),
        wait_n_(wait_n) {}
   public:
    std::set<std::string> table_rels_;
    duckdb_libpgquery::PGLockClauseStrength strength_;
    gstor_rowmark_type_t wait_policy_;
    uint32_t wait_n_;
};
#endif

class SelectStatement : public BoundStatement {
   public:
    SelectStatement() : BoundStatement(StatementType::SELECT_STATEMENT) {}

    SetOperationType set_operation_type{SetOperationType::NONE};

    // select 对外返回的列信息
    std::vector<ReturnItem> return_list;

    std::unique_ptr<SelectStatement> larg;
    std::unique_ptr<SelectStatement> rarg;

    std::unique_ptr<BoundQuerySource> table_ref;

    std::vector<std::unique_ptr<BoundExpression>> select_expr_list;

    std::unique_ptr<BoundExpression> where_clause;

    std::vector<std::unique_ptr<BoundExpression>> group_by_clause;

    std::unique_ptr<BoundExpression> having_clause;

    std::unique_ptr<LimitClause> limit_clause;

    std::vector<std::unique_ptr<BoundSortItem>> sort_items;

    std::vector<std::unique_ptr<BoundExpression>> distinct_on_list;
    bool is_distinct;

    ValueClaluse value_clause;

#ifdef ENABLE_PG_QUERY
    std::unique_ptr<LockClause> lock_clause;
#endif
};
