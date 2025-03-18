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
 * expression_iterator.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/planner/expression_iterator.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "planner/expression_iterator.h"

#include "binder/bound_expression.h"
#include "binder/expressions/bound_agg_call.h"
#include "binder/expressions/bound_alias.h"
#include "binder/expressions/bound_binary_op.h"
#include "binder/expressions/bound_case.h"
#include "binder/expressions/bound_cast.h"
#include "binder/expressions/bound_conjunctive.h"
#include "binder/expressions/bound_func_call.h"
#include "binder/expressions/bound_in_expr.h"
#include "binder/expressions/bound_like_op.h"
#include "binder/expressions/bound_null_test.h"
#include "binder/expressions/bound_seq_func.h"
#include "binder/expressions/bound_sub_query.h"
#include "binder/expressions/bound_unary_op.h"
#include "binder/table_ref/bound_base_table.h"
#include "binder/table_ref/bound_join.h"
#include "binder/table_ref/bound_subquery.h"
#include "binder/table_ref/bound_value_clause.h"

void ExpressionIterator::EnumerateChildren(
    BoundExpression &expr, const std::function<void(std::unique_ptr<BoundExpression> &child)> &callback) {
    switch (expr.Type()) {
        case ExpressionType::AGG_CALL: {
            auto &aggr_expr = (BoundAggCall &)expr;
            for (auto &child : aggr_expr.args_) {
                callback(child);
            }
            break;
        }
        case ExpressionType::BINARY_OP: {
            auto &binary_expr = (BoundBinaryOp &)expr;
            callback(binary_expr.LeftPtr());
            callback(binary_expr.RightPtr());
            break;
        }
        case ExpressionType::UNARY_OP: {
            auto &unary_expr = (BoundUnaryOp &)expr;
            callback(unary_expr.ChildPtr());
            break;
        }
        case ExpressionType::SUBQUERY: {
            auto &subquery_expr = (BoundSubqueryExpr &)expr;
            if (subquery_expr.child) {
                callback(subquery_expr.child);
            }
            break;
        }
        case ExpressionType::ALIAS: {
            auto &alias_expr = (BoundAlias &)expr;
            callback(alias_expr.child_);
            break;
        }
        case ExpressionType::NULL_TEST: {
            auto &null_expr = (BoundNullTest &)expr;
            callback(null_expr.child);
            break;
        }
        case ExpressionType::TYPE_CAST: {
            auto &cast_expr = (BoundCast &)expr;
            callback(cast_expr.child);
            break;
        }
        case ExpressionType::FUNC_CALL: {
            auto &fun_call_expr = (BoundFuncCall &)expr;
            for (auto &arg : fun_call_expr.args_) {
                callback(arg);
            }
            break;
        }
        case ExpressionType::SEQ_FUNC: {
            auto &seq_func_expr = static_cast<BoundSequenceFunction &>(expr);
            callback(seq_func_expr.arg);
            break;
        }
        case ExpressionType::LIKE_OP: {
            auto &like_expr = static_cast<BoundLikeOp &>(expr);
            for (auto &arg : like_expr.Arguments()) {
                callback(arg);
            }
            break;
        }
        case ExpressionType::CONJUNCTIVE: {
            auto &conjunctive_expr = static_cast<BoundConjunctive &>(expr);
            for (auto &arg : conjunctive_expr.items) {
                callback(arg);
            }
            break;
        }
        case ExpressionType::IN_EXPR: {
            auto &in_expr = static_cast<BoundInExpr &>(expr);
            callback(in_expr.in_ref_expr);
            for (auto &arg : in_expr.in_list) {
                callback(arg);
            }
            break;
        }
        case ExpressionType::CASE: {
            auto &case_expr = static_cast<BoundCase &>(expr);
            for (auto &check : case_expr.bound_case_checks) {
                callback(check.when_expr);
                callback(check.then_expr);
            }
            if (case_expr.else_expr) {
                callback(case_expr.else_expr);
            }
            break;
        }

        case ExpressionType::COLUMN_REF:
        case ExpressionType::LITERAL:
        case ExpressionType::STAR:
        // these node types have no children
        case ExpressionType::BOUND_PARAM:
        case ExpressionType::POSITION_REF:
        case ExpressionType::INTERVAL:
        case ExpressionType::WINDOW_FUNC_CALL:
            break;
        default:
            throw std::runtime_error(fmt::format("ExpressionIterator used on unbound expression type={}", expr.Type()));
    }
}

void ExpressionIterator::EnumerateChildren(BoundExpression &expr,
                                           const std::function<void(BoundExpression &child)> &callback) {
    ExpressionIterator::EnumerateChildren(expr, [&](std::unique_ptr<BoundExpression> &child) { callback(*child); });
}

void ExpressionIterator::EnumerateExpression(
    std::unique_ptr<BoundExpression> &expr,
    const std::function<void(std::unique_ptr<BoundExpression> &child)> &callback) {
    callback(expr);
    ExpressionIterator::EnumerateChildren(
        *expr, [&](std::unique_ptr<BoundExpression> &child) { EnumerateExpression(child, callback); });
}

void ExpressionIterator::EnumerateExpression(BoundExpression &expr,
                                             const std::function<void(BoundExpression &child)> &callback) {
    callback(expr);
    ExpressionIterator::EnumerateChildren(
        expr, [&](std::unique_ptr<BoundExpression> &child) { EnumerateExpression(*child, callback); });
}

void ExpressionIterator::EnumerateTableRefChildren(BoundTableRef &ref,
                                                   const std::function<void(BoundExpression &child)> &callback) {
    switch (ref.Type()) {
        case DataSourceType::JOIN_RESULT: {
            auto &bound_join = (BoundJoin &)ref;
            if (bound_join.on_condition) {
                EnumerateExpression(*bound_join.on_condition, callback);
            }
            EnumerateTableRefChildren(*bound_join.left, callback);
            EnumerateTableRefChildren(*bound_join.right, callback);
            break;
        }
        case DataSourceType::SUBQUERY_RESULT: {
            auto &bound_subquery = (BoundSubquery &)ref;
            EnumerateSelectStatement(*bound_subquery.subquery, callback);
            break;
        }
        case DataSourceType::VALUE_CLAUSE: {
            auto &bound_value_clause = (BoundValueClause &)ref;
            for (auto &exprs : bound_value_clause.values) {
                for (auto &expr : exprs) {
                    EnumerateExpression(*expr, callback);
                }
            }
            break;
        }
        case DataSourceType::DUAL:
        case DataSourceType::BASE_TABLE:
            break;
        default:
            throw std::runtime_error(fmt::format("Unimplemented table reference type in ExpressionIterator {}",
                                                 DataSourceTypeToString(ref.Type())));
    }
}

void ExpressionIterator::EnumerateSelectStatement(SelectStatement &stmt,
                                                  const std::function<void(BoundExpression &child)> &callback) {
    for (auto &expr : stmt.select_expr_list) {
        EnumerateExpression(*expr, callback);
    }

    if (stmt.where_clause) {
        EnumerateExpression(*stmt.where_clause, callback);
    }
    for (auto &expr : stmt.group_by_clause) {
        EnumerateExpression(*expr, callback);
    }
    if (stmt.having_clause) {
        EnumerateExpression(*stmt.having_clause, callback);
    }

    if (stmt.table_ref) {
        EnumerateTableRefChildren(*stmt.table_ref, callback);
    }
}

void TableRefIterator::EnumerateTableRef(SelectStatement &stmt,
                                         const std::function<void(BoundBaseTable &tbl)> &callback) {
    if (stmt.table_ref) {
        EnumerateTableRef(*stmt.table_ref, callback);
    } else {
        if (stmt.larg) EnumerateTableRef(*stmt.larg, callback);
        if (stmt.rarg) EnumerateTableRef(*stmt.rarg, callback);
    }
}

void TableRefIterator::EnumerateTableRef(BoundTableRef &ref, const std::function<void(BoundBaseTable &tbl)> &callback) {
    switch (ref.Type()) {
        case DataSourceType::BASE_TABLE: {
            callback(static_cast<BoundBaseTable &>(ref));
            break;
        }
        case DataSourceType::JOIN_RESULT: {
            auto &join_ref = static_cast<BoundJoin &>(ref);
            if (join_ref.left) {
                EnumerateTableRef(*join_ref.left, callback);
            }
            if (join_ref.right) {
                EnumerateTableRef(*join_ref.right, callback);
            }
            break;
        }
        case DataSourceType::SUBQUERY_RESULT: {
            auto &subquery_ref = static_cast<BoundSubquery &>(ref);
            if (subquery_ref.subquery && subquery_ref.subquery->table_ref) {
                EnumerateTableRef(*subquery_ref.subquery->table_ref, callback);
            }
            break;
        }
        case DataSourceType::VALUE_CLAUSE:
        case DataSourceType::DUAL:
        case DataSourceType::INVALID:
            break;
        default:
            throw std::runtime_error(fmt::format("unkown table reference type {}", DataSourceTypeToString(ref.Type())));
    }
}
