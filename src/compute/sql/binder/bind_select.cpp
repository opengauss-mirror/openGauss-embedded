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
 * bind_select.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/binder/bind_select.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "binder/binder.h"
#include "binder/bound_sort.h"
#include "binder/expressions/bound_alias.h"
#include "binder/expressions/bound_binary_op.h"
#include "binder/expressions/bound_conjunctive.h"
#include "binder/expressions/bound_constant.h"
#include "binder/expressions/bound_func_expr.h"
#include "binder/expressions/bound_in_expr.h"
#include "binder/expressions/bound_like_op.h"
#include "binder/expressions/bound_position_ref_expr.h"
#include "binder/expressions/bound_unary_op.h"
#include "common/null_check_ptr.h"
#include "nodes/parsenodes.hpp"
#include "planner/expression_iterator.h"

using duckdb_libpgquery::PGSelectStmt;

static auto CheckUnSupportedFeature(PGSelectStmt &pg_stmt) {
    if (pg_stmt.intoClause) {
        // 不支持 select info , e.g: select * into new_table from old_table;
        throw intarkdb::Exception(ExceptionType::NOT_IMPLEMENTED, "unsupported select into");
    }
    if (pg_stmt.withClause) {
        throw intarkdb::Exception(ExceptionType::NOT_IMPLEMENTED, "unsupported with sytax");
    }
    if (pg_stmt.windowClause) {
        throw intarkdb::Exception(ExceptionType::NOT_IMPLEMENTED, "unsupported windows function");
    }
    if (pg_stmt.pivot) {
        throw intarkdb::Exception(ExceptionType::NOT_IMPLEMENTED, "unsupported pivot");
    }
    if (pg_stmt.sampleOptions) {
        throw intarkdb::Exception(ExceptionType::NOT_IMPLEMENTED, "unsupported sample");
    }
    if (pg_stmt.qualifyClause) {
        throw intarkdb::Exception(ExceptionType::NOT_IMPLEMENTED, "unsupported qualify");
    }
}

auto Binder::BindDistinctOnList(SelectStatement &stmt, duckdb_libpgquery::PGList *distinct_clause) -> void {
    stmt.is_distinct = false;
    if (distinct_clause != nullptr && distinct_clause->head != nullptr) {
        auto target = NullCheckPtrCast<duckdb_libpgquery::PGNode>(distinct_clause->head->data.ptr_value);
        if (target != nullptr) {
            // 支持 Distinct ON
            stmt.distinct_on_list = BindExpressionList(distinct_clause, 1);
        }
        stmt.is_distinct = true;
    }
}

auto Binder::BindSelectNoSetOp(duckdb_libpgquery::PGSelectStmt *pg_stmt) -> std::unique_ptr<SelectStatement> {
    auto select_stmt = std::make_unique<SelectStatement>();

    if (pg_stmt->valuesLists) {
        select_stmt->table_ref = BindValueList(pg_stmt->valuesLists, select_stmt->select_expr_list);
    } else {
        // 绑定 from 子句
        select_stmt->table_ref = BindFromClause(pg_stmt->fromClause);
        if (!pg_stmt->targetList) {
            throw intarkdb::Exception(ExceptionType::BINDER, "no select list");
        }
        // 绑定 select 列
        select_stmt->select_expr_list = BindSelectListExprs(pg_stmt->targetList);
    }

    BindDistinctOnList(*select_stmt, pg_stmt->distinctClause);

    ctx.BuildProjectionAndAlias(select_stmt->select_expr_list);

    // init return list
    for (auto &expr : select_stmt->select_expr_list) {
        ReturnItem item;
        item.col_name = expr->GetName();
        item.col_type = expr->ReturnType();
        select_stmt->return_list.push_back(item);
    }

    // 绑定 where 子句
    select_stmt->where_clause = BindWhereClause(pg_stmt->whereClause);
    // group by
    select_stmt->group_by_clause = BindGroupByClause(pg_stmt->groupClause);
    // Bind HAVING clause.
    select_stmt->having_clause = BindHavingClause(pg_stmt->havingClause);
    // Bind LIMIT clause.
    select_stmt->limit_clause = BindLimitClause(pg_stmt->limitCount, pg_stmt->limitOffset);
    // Bind ORDER BY clause.
    select_stmt->sort_items = BindSortItems(pg_stmt->sortClause, select_stmt->select_expr_list);
    // Bind LockingClause
#ifdef ENABLE_PG_QUERY
    select_stmt->lock_clause = BindLockingClause(pg_stmt->lockingClause);
#endif
    return select_stmt;
}

static auto MergeAliasBinding(std::unordered_map<std::string, AliasIdx> &src,
                              std::unordered_map<std::string, AliasIdx> &dst,
                              const std::vector<std::unique_ptr<BoundExpression>> &result_list) -> void {
    for (auto &item : src) {
        auto iter = dst.find(item.first);
        if (iter == dst.end()) {  // 只有第一个命中的别名会被加入
            item.second.type = result_list[item.second.idx]->ReturnType();
            dst.insert(item);
        }
    }
}

static auto MergeReturnListToAliasBinding(const std::vector<ReturnItem> &return_list,
                                          std::unordered_map<std::string, AliasIdx> &alias_binding,
                                          const std::vector<std::unique_ptr<BoundExpression>> &result_list) -> void {
    int return_size = return_list.size();
    for (int i = 0; i < return_size; ++i) {
        const auto &item = return_list[i];
        const auto &result_type = result_list[i]->ReturnType();
        auto iter = alias_binding.find(item.col_name.back());
        if (iter == alias_binding.end()) {
            alias_binding.insert(std::make_pair(item.col_name.back(), AliasIdx{i, result_type}));
        }
    }
}

static auto AddSelectOriginName(const std::vector<std::unique_ptr<BoundExpression>> &select_expr_list,
                                std::unordered_map<std::string, AliasIdx> &alias_binding,
                                const std::vector<std::unique_ptr<BoundExpression>> &result_list) -> void {
    int list_size = select_expr_list.size();
    for (int i = 0; i < list_size; ++i) {
        const auto &expr = select_expr_list[i];
        if (expr->Type() == ExpressionType::ALIAS) {
            const auto &result_type = result_list[i]->ReturnType();
            const auto &alias_expr = static_cast<const BoundAlias &>(*expr);
            auto origin_name = alias_expr.child_->GetName().back();
            auto iter = alias_binding.find(origin_name);
            if (iter == alias_binding.end()) {
                alias_binding.insert(std::make_pair(origin_name, AliasIdx{i, result_type}));
            }
        }
    }
}

static auto BuildUnionAliasMap(SelectStatement &left_stmt, SelectStatement &right_stmt, BinderContext &ctx,
                               const std::vector<std::unique_ptr<BoundExpression>> &result_list) -> void {
    MergeReturnListToAliasBinding(left_stmt.return_list, ctx.alias_binding, result_list);
    MergeReturnListToAliasBinding(right_stmt.return_list, ctx.alias_binding, result_list);
    AddSelectOriginName(left_stmt.select_expr_list, ctx.alias_binding, result_list);
    AddSelectOriginName(right_stmt.select_expr_list, ctx.alias_binding, result_list);
}

auto Binder::BindSelectSetOp(duckdb_libpgquery::PGSelectStmt *pg_stmt) -> std::unique_ptr<SelectStatement> {
    auto select_stmt = std::make_unique<SelectStatement>();
    Binder left_binder(this);
    Binder right_binder(this);
    select_stmt->larg = left_binder.BindSelectStmt(pg_stmt->larg);
    select_stmt->rarg = right_binder.BindSelectStmt(pg_stmt->rarg);

    if (select_stmt->larg->select_expr_list.size() != select_stmt->rarg->select_expr_list.size()) {
        throw intarkdb::Exception(ExceptionType::BINDER,
                                  "set operation must have an equal number of expressions in target lists");
    }

    for (size_t i = 0; i < select_stmt->larg->select_expr_list.size(); ++i) {
        auto type = intarkdb::GetCompatibleType(select_stmt->larg->select_expr_list[i]->ReturnType(),
                                                select_stmt->rarg->select_expr_list[i]->ReturnType());
        if (type.TypeId() == GS_TYPE_NULL) {
            type = LogicalType::Integer();
        }
        select_stmt->select_expr_list.push_back(
            std::make_unique<BoundColumnRef>(select_stmt->larg->select_expr_list[i]->GetName(), type));
    }

    BuildUnionAliasMap(*select_stmt->larg, *select_stmt->rarg, ctx, select_stmt->select_expr_list);
    MergeAliasBinding(left_binder.ctx.alias_binding, ctx.alias_binding, select_stmt->select_expr_list);
    MergeAliasBinding(right_binder.ctx.alias_binding, ctx.alias_binding, select_stmt->select_expr_list);

    for (size_t i = 0; i < select_stmt->select_expr_list.size(); ++i) {
        select_stmt->return_list.emplace_back(
            ReturnItem{select_stmt->select_expr_list[i]->GetName(), select_stmt->select_expr_list[i]->ReturnType()});
    }

    select_stmt->table_ref = std::make_unique<BoundQuerySource>(DataSourceType::DUAL);

    // 关闭检查列存在性
    check_column_exist_ = false;
    select_stmt->sort_items = BindSortItems(pg_stmt->sortClause, select_stmt->select_expr_list);
    check_column_exist_ = true;

    // check sort items show in select list ?
    for (size_t i = 0; i < select_stmt->sort_items.size(); ++i) {
        auto &sort_item = select_stmt->sort_items[i];
        if (sort_item->sort_expr->Type() != ExpressionType::POSITION_REF) {
            auto iter = ctx.alias_binding.find(sort_item->sort_expr->GetName().back());
            if (iter != ctx.alias_binding.end()) {
                sort_item->sort_expr = std::make_unique<BoundPositionRef>(iter->second.idx, iter->second.type);
            } else {
                throw intarkdb::Exception(ExceptionType::BINDER, "sort item not in select list");
            }
        }
    }

    select_stmt->limit_clause = BindLimitClause(pg_stmt->limitCount, pg_stmt->limitOffset);

    switch (pg_stmt->op) {
        case duckdb_libpgquery::PG_SETOP_UNION: {
            select_stmt->set_operation_type = SetOperationType::UNION;
            break;
        }
        case duckdb_libpgquery::PG_SETOP_EXCEPT: {
            select_stmt->set_operation_type = SetOperationType::EXCEPT;
            break;
        }
        case duckdb_libpgquery::PG_SETOP_INTERSECT: {
            select_stmt->set_operation_type = SetOperationType::INTERSECT;
            break;
        }
        // ignore union all by name
        default:
            throw intarkdb::Exception(ExceptionType::BINDER, "invalid set operation");
    }
    select_stmt->is_distinct = !pg_stmt->all;
    return select_stmt;
}

auto Binder::BindSelectStmt(duckdb_libpgquery::PGSelectStmt *pg_stmt) -> std::unique_ptr<SelectStatement>
{
    // 检查不支持的语法元素
    CheckUnSupportedFeature(*pg_stmt);

    switch (pg_stmt->op) {
        case duckdb_libpgquery::PG_SETOP_NONE: {
            return BindSelectNoSetOp(pg_stmt);
        }
        case duckdb_libpgquery::PG_SETOP_UNION:
        case duckdb_libpgquery::PG_SETOP_EXCEPT:
        case duckdb_libpgquery::PG_SETOP_INTERSECT: {
            return BindSelectSetOp(pg_stmt);
        }
        default:
            throw intarkdb::Exception(ExceptionType::NOT_IMPLEMENTED, "unsupported select type");
    }
}

static auto TransformSortType(duckdb_libpgquery::PGSortByDir sortby_dir) -> SortType {
    switch (sortby_dir) {
        case duckdb_libpgquery::PG_SORTBY_DEFAULT:
        case duckdb_libpgquery::PG_SORTBY_ASC:
            return SortType::ASC;
        case duckdb_libpgquery::PG_SORTBY_DESC:
            return SortType::DESC;
        default:
            throw intarkdb::Exception(ExceptionType::BINDER, "unsupported sort type");
    }
}

auto Binder::BindSortItems(duckdb_libpgquery::PGList *list,
                           const std::vector<std::unique_ptr<BoundExpression>> &select_list)
    -> std::vector<std::unique_ptr<BoundSortItem>> {
    auto sort_items = std::vector<std::unique_ptr<BoundSortItem>>{};
    if (list) {
        for (auto node = list->head; node != nullptr; node = node->next) {
            auto item = NullCheckPtrCast<duckdb_libpgquery::PGNode>(node->data.ptr_value);
            if (item->type == duckdb_libpgquery::T_PGSortBy) {
                auto sort_item = NullCheckPtrCast<duckdb_libpgquery::PGSortBy>(item.get());
                auto type = TransformSortType(sort_item->sortby_dir);
                auto is_null_first = sort_item->sortby_nulls == duckdb_libpgquery::PG_SORTBY_NULLS_FIRST;
                auto order_expr = BindSortExpression(sort_item->node, select_list);
                if (order_expr) { // 当expr为null时，表示排序列不影响排序，不会加入到排序列表，e.g order by 'abc'
                    sort_items.emplace_back(
                        std::make_unique<BoundSortItem>(type, is_null_first, std::move(order_expr)));
                }
            } else {
                throw intarkdb::Exception(ExceptionType::NOT_IMPLEMENTED, "unsupported order by node");
            }
        }
    }
    return sort_items;
}

auto Binder::BindLimitClause(duckdb_libpgquery::PGNode *limit, duckdb_libpgquery::PGNode *offset)
    -> std::unique_ptr<LimitClause>
{
    std::unique_ptr<LimitClause> limit_clause = nullptr;
    if (limit || offset) {
        limit_clause = std::make_unique<LimitClause>();
    }
    if (limit) {
        limit_clause->limit = BindLimitCount(limit);
    }
    if (offset) {
        limit_clause->offset = BindLimitOffset(offset);
    }
    return limit_clause;
}

auto Binder::BindLimitCount(duckdb_libpgquery::PGNode *root) -> std::unique_ptr<BoundExpression> {
    auto expr = BindExpression(root, 1);
    if (expr && expr->Type() != ExpressionType::LITERAL && expr->Type() != ExpressionType::BOUND_PARAM) {
        throw intarkdb::Exception(ExceptionType::BINDER, "limit count must be constant");
    }
    return expr;
}

auto Binder::BindLimitOffset(duckdb_libpgquery::PGNode *root) -> std::unique_ptr<BoundExpression> {
    auto expr = BindExpression(root, 1);
    if (expr && expr->Type() != ExpressionType::LITERAL && expr->Type() != ExpressionType::BOUND_PARAM) {
        throw intarkdb::Exception(ExceptionType::BINDER, "limit count must be constant");
    }
    return expr;
}

auto Binder::BindHavingClause(duckdb_libpgquery::PGNode *root) -> std::unique_ptr<BoundExpression>
{
    return BindExpression(root, 1);
}

auto Binder::BindGroupByClause(duckdb_libpgquery::PGList *list) -> std::vector<std::unique_ptr<BoundExpression>>
{
    std::vector<std::unique_ptr<BoundExpression>> group_by_list;
    if (list) {
        for (auto node = list->head; node != nullptr; node = lnext(node)) {
            auto target = reinterpret_cast<duckdb_libpgquery::PGNode *>(node->data.ptr_value);
            auto expr = BindGroupByExpression(target);
            if (expr) {
                group_by_list.emplace_back(std::move(expr));
            }
        }
    }
    return group_by_list;
}

auto Binder::BindGroupByExpression(duckdb_libpgquery::PGNode *node) -> std::unique_ptr<BoundExpression> {
    auto expr = BindExpression(node, 1);
    if (expr && expr->Type() == ExpressionType::LITERAL) {
        auto &constant_expr = static_cast<BoundConstant &>(*expr);
        const auto &val = constant_expr.ValRef();
        if (val.IsInteger()) {
            auto idx = val.GetCastAs<int32_t>();
            expr = std::make_unique<BoundPositionRef>(idx - 1, GS_TYPE_UNKNOWN);
        } else {
            expr = nullptr;  // groupby 非整数常量，等于无效groupby 忽略
        }
    }
    if (expr && expr->HasSubQuery()) {
        throw intarkdb::Exception(ExceptionType::BINDER, "group by not support subquery");
    }
    return expr;
}

auto Binder::BindWhereClause(duckdb_libpgquery::PGNode *root) -> std::unique_ptr<BoundExpression>
{
    auto expr = BindExpression(root, 1);
    if (expr) {
        // where 表达式中含有*，这种情况pg的语法解释不能过滤掉
        ExpressionIterator::EnumerateChildren(*expr, [&](std::unique_ptr<BoundExpression> &child) {
            if (child->Type() == ExpressionType::STAR) {
                throw intarkdb::Exception(ExceptionType::BINDER, "not supported * expression in whereClause");
            }
        });
    }
    return expr;
}

#ifdef ENABLE_PG_QUERY
auto Binder::BindLockingClause(duckdb_libpgquery::PGList *list) -> std::unique_ptr<LockClause> {
    std::unique_ptr<LockClause> lock_clause = nullptr;
    if (list) {
        for (auto node = list->head; node != nullptr; node = node->next) {
            auto item = NullCheckPtrCast<duckdb_libpgquery::PGNode>(node->data.ptr_value);
            if (item->type == duckdb_libpgquery::T_PGLockingClause) {
                auto lock_item = NullCheckPtrCast<duckdb_libpgquery::PGLockingClause>(item.get());

                auto locked_rels = lock_item->lockedRels;
                std::set<std::string> table_rels;
                if (locked_rels) {
                    for (auto node_rels = locked_rels->head; node_rels != nullptr; node_rels = node_rels->next) {
                        auto range_var = NullCheckPtrCast<duckdb_libpgquery::PGRangeVar>(node_rels->data.ptr_value);
                        table_rels.insert(range_var->relname);
                    }
                }

                auto strength = lock_item->strength;
                gstor_rowmark_type_t wait_policy = GSTOR_ROWMARK_WAIT_BLOCK;
                switch (lock_item->waitPolicy) {
                    case duckdb_libpgquery::PGLockWaitPolicy::PGLockWaitBlock:
                        wait_policy = GSTOR_ROWMARK_WAIT_BLOCK;
                        break;
                    case duckdb_libpgquery::PGLockWaitPolicy::PGLockWaitSkip:
                        wait_policy = GSTOR_ROWMARK_SKIP_LOCKED;
                        break;
                    case duckdb_libpgquery::PGLockWaitPolicy::LockWaitError:
                        wait_policy = GSTOR_ROWMARK_NOWAIT;
                        break;
                    case duckdb_libpgquery::PGLockWaitPolicy::PGLockWaitBlock_n:
                        wait_policy = GSTOR_ROWMARK_WAIT_SECOND;
                        break;
                    default:
                        throw intarkdb::Exception(ExceptionType::NOT_IMPLEMENTED, "bad lock wait_policy");
                }
                auto wait_n = lock_item->wait_n;
                if (strength != duckdb_libpgquery::PGLockClauseStrength::LCS_FORUPDATE) {
                    throw intarkdb::Exception(
                        ExceptionType::BINDER,
                        fmt::format("unsupported LockClauseStrength type, must be LCS_FORUPDATE"));
                }
                lock_clause = std::make_unique<LockClause>(table_rels, strength, wait_policy, wait_n);
            } else {
                throw intarkdb::Exception(ExceptionType::NOT_IMPLEMENTED, "unsupported locking clause node");
            }
        }
    }

    return lock_clause;
}
#endif
