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
 * planner.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/planner/planner.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "planner/planner.h"

#include <fmt/core.h>

#include <set>
#include <stdexcept>
#include <utility>

#include "binder/bound_expression.h"
#include "binder/bound_table_ref.h"
#include "binder/expressions/bound_agg_call.h"
#include "binder/expressions/bound_alias.h"
#include "binder/expressions/bound_binary_op.h"
#include "binder/expressions/bound_case.h"
#include "binder/expressions/bound_cast.h"
#include "binder/expressions/bound_column_def.h"
#include "binder/expressions/bound_conjunctive.h"
#include "binder/expressions/bound_constant.h"
#include "binder/expressions/bound_func_call.h"
#include "binder/expressions/bound_in_expr.h"
#include "binder/expressions/bound_like_op.h"
#include "binder/expressions/bound_null_test.h"
#include "binder/expressions/bound_parameter.h"
#include "binder/expressions/bound_position_ref_expr.h"
#include "binder/expressions/bound_seq_func.h"
#include "binder/expressions/bound_sub_query.h"
#include "binder/expressions/bound_unary_op.h"
#include "binder/expressions/window_function.h"
#include "binder/table_ref/bound_value_clause.h"
#include "common/compare_type.h"
#include "common/exception.h"
#include "common/expression_util.h"
#include "planner/expression_iterator.h"
#include "planner/expressions/case_expression.h"
#include "planner/expressions/cast_expression.h"
#include "planner/expressions/column_param_expression.h"
#include "planner/expressions/column_value_expression.h"
#include "planner/expressions/compare_expression.h"
#include "planner/expressions/conj_expression.h"
#include "planner/expressions/constant_expression.h"
#include "planner/expressions/function_expression.h"
#include "planner/expressions/in_expression.h"
#include "planner/expressions/like_expression.h"
#include "planner/expressions/logic_op_expression.h"
#include "planner/expressions/math_op_expression.h"
#include "planner/expressions/nulltest_expression.h"
#include "planner/expressions/sequence_expression.h"
#include "planner/expressions/string_op_expression.h"
#include "planner/expressions/subquery_expression.h"
#include "planner/logical_plan/aggregate_plan.h"
#include "planner/logical_plan/apply_plan.h"
#include "planner/logical_plan/comment_on_plan.h"
#include "planner/logical_plan/copyTo_plan.h"
#include "planner/logical_plan/delete_plan.h"
#include "planner/logical_plan/distinct_plan.h"
#include "planner/logical_plan/drop_plan.h"
#include "planner/logical_plan/empty_source_plan.h"
#include "planner/logical_plan/filter_plan.h"
#include "planner/logical_plan/insert_plan.h"
#include "planner/logical_plan/limit_plan.h"
#include "planner/logical_plan/logical_plan.h"
#include "planner/logical_plan/nested_loop_join_plan.h"
#include "planner/logical_plan/projection_plan.h"
#include "planner/logical_plan/role_plan.h"
#include "planner/logical_plan/scan_plan.h"
#include "planner/logical_plan/set_plan.h"
#include "planner/logical_plan/show_plan.h"
#include "planner/logical_plan/sort_plan.h"
#include "planner/logical_plan/synonym_plan.h"
#include "planner/logical_plan/transaction_plan.h"
#include "planner/logical_plan/union_plan.h"
#include "planner/logical_plan/update_plan.h"
#include "planner/logical_plan/values_plan.h"
#include "planner/logical_plan/window_plan.h"
#include "planner/optimizer/optimizer.h"
#include "planner/physical_plan/aggregate_exec.h"
#include "planner/physical_plan/comment_on_exec.h"
#include "planner/physical_plan/delete_exec.h"
#include "planner/physical_plan/distinct_exec.h"
#include "planner/physical_plan/drop_exec.h"
#include "planner/physical_plan/empty_source_exec.h"
#include "planner/physical_plan/fast_scan_exec.h"
#include "planner/physical_plan/filter_exec.h"
#include "planner/physical_plan/insert_exec.h"
#include "planner/physical_plan/join/nested_loop_join_exec.h"
#include "planner/physical_plan/limit_exec.h"
#include "planner/physical_plan/projection_exec.h"
#include "planner/physical_plan/role_exec.h"
#include "planner/physical_plan/seq_scan_exec.h"
#include "planner/physical_plan/set_exec.h"
#include "planner/physical_plan/show_exec.h"
#include "planner/physical_plan/sort_exec.h"
#include "planner/physical_plan/synonym_exec.h"
#include "planner/physical_plan/transaction_exec.h"
#include "planner/physical_plan/union_exec.h"
#include "planner/physical_plan/update_exec.h"
#include "planner/physical_plan/values_exec.h"
#include "planner/physical_plan/window_exec.h"
#include "storage/gstor/gstor_executor.h"

using intarkdb::LogicOpType;

void Planner::PlanQuery(BoundStatement& statement) {}

auto Planner::PlanSubqueryTableRef(BoundSubquery& subquery_ref, scan_action_t action) -> LogicalPlanPtr {
    std::unordered_map<int, ReturnItem> subquery_alias_col;
    // prepare subquery alias
    for (size_t i = 0; i < subquery_ref.subquery->select_expr_list.size(); ++i) {
        auto& col_expr = subquery_ref.subquery->select_expr_list[i];
        if (col_expr->Type() == ExpressionType::ALIAS) {
            // 记录别名列的映射关系
            subquery_alias_col.insert({i, ReturnItem{col_expr->GetName(), col_expr->ReturnType()}});
        }
    }
    auto select_node = PlanSelect(*subquery_ref.subquery);
    const auto& schema = select_node->GetSchema();
    auto columns = schema.GetColumnInfos();
    std::vector<std::unique_ptr<BoundExpression>> new_select_list;
    // 重命名列名
    for (size_t i = 0; i < columns.size(); ++i) {
        auto iter = subquery_alias_col.find(i);
        if (iter != subquery_alias_col.end()) {
            new_select_list.push_back(std::make_unique<BoundPositionRef>(i, iter->second.col_type));
            columns[i].col_name = std::vector{subquery_ref.alias, iter->second.col_name.back()};
        } else {
            new_select_list.push_back(std::make_unique<BoundPositionRef>(i, columns[i].col_type));
            columns[i].col_name = std::vector{subquery_ref.alias, columns[i].GetColNameWithoutTableName()};
        }
    }
    Schema new_schema = Schema(std::move(columns));
    new_schema = Schema::RenameSchemaColumnIfExistSameNameColumns(new_schema);
    // 使用projection plan 重命名列名
    return std::make_shared<ProjectionPlan>(std::move(new_schema), std::move(new_select_list), select_node);
}

auto Planner::PlanJoinTableRef(SelectStatement& statement, BoundJoin& join_ref, scan_action_t action)
    -> LogicalPlanPtr {
    auto left = PlanTableRef(statement, std::move(join_ref.left), action);
    auto right = PlanTableRef(statement, std::move(join_ref.right), action);
    PlanSubqueries(&join_ref.on_condition);
    return std::make_shared<NestedLoopJoinPlan>(left, right, std::move(join_ref.on_condition), join_ref.join_type);
}

auto Planner::PlanTableRef(SelectStatement& statement, std::unique_ptr<BoundTableRef> table, scan_action_t action)
    -> LogicalPlanPtr {
    switch (table->Type()) {
        case DataSourceType::DUAL: {
            return std::make_shared<EmptySourcePlan>(Schema(), 1);
        }
        case DataSourceType::BASE_TABLE: {
            auto table_ref_ptr = std::unique_ptr<BoundBaseTable>(static_cast<BoundBaseTable*>(table.release()));
            std::string lock_table = table_ref_ptr->GetBoundTableName();
            auto scan_plan = std::make_shared<ScanPlan>(
                catalog_.CreateTableDataSource(std::move(table_ref_ptr), action, table_idx_++),
                std::vector<std::string>{});
#ifdef ENABLE_PG_QUERY
            if (statement.lock_clause) {
                auto it = statement.lock_clause->table_rels_.find(lock_table);
                if (statement.lock_clause->table_rels_.size() <= 0 || it != statement.lock_clause->table_rels_.end()) {
                    scan_plan->source->lock_clause_.is_select_for_update = GS_TRUE;
                    scan_plan->source->lock_clause_.wait_policy = statement.lock_clause->wait_policy_;
                    scan_plan->source->lock_clause_.wait_n = statement.lock_clause->wait_n_;
                } else {
                    scan_plan->source->lock_clause_.is_select_for_update = GS_FALSE;
                }
            } else {
                scan_plan->source->lock_clause_.is_select_for_update = GS_FALSE;
            }
#endif
            return scan_plan;
        }
        case DataSourceType::JOIN_RESULT: {
            auto& join_ref = static_cast<BoundJoin&>(*table);
            return PlanJoinTableRef(statement, join_ref, action);
        }
        case DataSourceType::SUBQUERY_RESULT: {
            auto& subquery_ref = static_cast<BoundSubquery&>(*table);
            return PlanSubqueryTableRef(subquery_ref, action);
        }
        case DataSourceType::VALUE_CLAUSE: {
            auto& value_clause = static_cast<BoundValueClause&>(*table);
            for (auto& exprs : value_clause.values) {
                for (auto& expr : exprs) {
                    PlanSubqueries(&expr);
                }
            }
            return std::make_shared<ValuesPlan>(Schema(std::move(value_clause.col_infos)),
                                                std::move(value_clause.values));
        }
        default:
            break;
    }
    throw std::runtime_error(fmt::format("unsupported DataSourceType {}\n", DataSourceTypeToString(table->Type())));
}

auto Planner::PlanSetOpSelect(SelectStatement& statement) -> LogicalPlanPtr {
    auto left = PlanSelect(*statement.larg);
    auto right = PlanSelect(*statement.rarg);

    LogicalPlanPtr plan = std::make_shared<UnionPlan>(std::move(statement.select_expr_list), left, right,
                                                      statement.set_operation_type, statement.is_distinct);
    if (statement.sort_items.size() > 0) {
        plan = std::make_shared<SortPlan>(plan, std::move(statement.sort_items));
    }
    if (statement.limit_clause) {
        if (statement.limit_clause->limit && (statement.limit_clause->limit->Type() != ExpressionType::LITERAL &&
                                              statement.limit_clause->limit->Type() != ExpressionType::BOUND_PARAM)) {
            throw std::runtime_error("LIMIT count must be an integer constant.");
        }
        plan = std::make_shared<LimitPlan>(std::move(statement.limit_clause->limit),
                                           std::move(statement.limit_clause->offset), plan);
    }
    return plan;
}

auto Planner::PlanValues(SelectStatement& statement) -> LogicalPlanPtr {
    auto values_plan = std::make_shared<ValuesPlan>(Schema(std::move(statement.value_clause.columns)),
                                                    std::move(statement.value_clause.values));
    for (size_t i = 0; i < values_plan->values_list.size(); ++i) {
        auto& row = values_plan->values_list[i];
        for (size_t j = 0; j < row.size(); ++j) {
            PlanSubqueries(&row[j]);
        }
    }
    return values_plan;
}

auto Planner::PlanSubqueryExprs(std::vector<std::unique_ptr<BoundExpression>>& exprs) -> void {
    for (auto& expr : exprs) {
        PlanSubqueries(&expr);
    }
}

static auto HasAggExpr(const std::vector<std::unique_ptr<BoundExpression>>& exprs) -> bool {
    for (auto& expr : exprs) {
        if (expr->HasAggregation()) {
            return true;
        }
    }
    return false;
}

static auto HasAggClause(const SelectStatement& statement) -> bool {
    if (HasAggExpr(statement.select_expr_list)) {
        return true;
    }
    if (!statement.group_by_clause.empty()) {
        return true;
    }
    if (statement.having_clause) {
        return true;
    }
    return false;
}

static auto HasWindowFunction(const SelectStatement& statement) -> bool {
    for (auto& expr : statement.select_expr_list) {
        if (expr->HasWindow()) {
            return true;
        }
    }
    return false;
}

static auto ReplacePositionRef(std::unique_ptr<BoundExpression>& expr,
                               const std::vector<std::unique_ptr<BoundExpression>>& select_expr_list) -> void {
    ExpressionIterator::EnumerateExpression(expr, [&](std::unique_ptr<BoundExpression>& expr) {
        if (expr->Type() == ExpressionType::POSITION_REF) {
            auto& position_ref = static_cast<BoundPositionRef&>(*expr);
            int index = position_ref.GetIndex();
            if (index < 0 || (size_t)index >= select_expr_list.size()) {
                throw intarkdb::Exception(
                    ExceptionType::PLANNER,
                    fmt::format("out of range - should be between 1 and {}", select_expr_list.size()));
            }
            expr = select_expr_list[index]->Copy();
        }
    });
}

auto Planner::PlanWhereClause(SelectStatement& statement, LogicalPlanPtr& plan) -> LogicalPlanPtr {
    if (statement.where_clause) {
        // 遍历where表达式,如果有子查询,则将子查询转换为plan
        PlanSubqueries(&statement.where_clause);
        ReplacePositionRef(statement.where_clause, statement.select_expr_list);
        plan = std::make_shared<FilterPlan>(std::move(statement.where_clause), plan);
    }
    return plan;
}

static auto ReplaceGroupClauseAgg(std::unordered_set<hash_t>& func_set,
                                  std::unordered_map<hash_t, const BoundExpression&>& group_by_map,
                                  std::vector<std::unique_ptr<BoundExpression>>& agg_exprs,
                                  std::unique_ptr<BoundExpression>& expr) -> void {
    std::string group_by_item_name;
    bool group_by_item_or_agg = true;
    bool is_agg = false;
    ExpressionIterator::EnumerateExpression(expr, [&](std::unique_ptr<BoundExpression>& expr) {
        auto hash_value = expr->Hash();
        if (expr->Type() == ExpressionType::AGG_CALL) {
            if (func_set.find(hash_value) == func_set.end()) {
                agg_exprs.push_back(expr->Copy());
                func_set.insert(hash_value);
            }
            is_agg = true;
        } else {
            // 非 AGG 是否出现在 group by 中
            auto group_by_item = group_by_map.find(hash_value);
            if (group_by_item != group_by_map.end()) {
                // 修改了expr，会影响  ExpressionIterator 的遍历
                expr = std::make_unique<BoundColumnRef>(group_by_item->second.GetName(),
                                                        group_by_item->second.ReturnType());
            } else {
                // 是列引用 && 不是 聚合函数的参数
                group_by_item_or_agg =
                    expr->Type() == ExpressionType::COLUMN_REF && !is_agg ? false : group_by_item_or_agg;
                if (!group_by_item_or_agg) {
                    group_by_item_name = expr->ToString();
                }
            }
        }
    });

    if (!group_by_item_or_agg) {
        throw intarkdb::Exception(ExceptionType::PLANNER,
                                  fmt::format("column {} not appear in group by clause", group_by_item_name));
    }
}

static auto ReplaceGroupClausePositionRef(std::vector<std::unique_ptr<BoundExpression>>& group_by_clause,
                                          const std::vector<std::unique_ptr<BoundExpression>>& select_expr_list)
    -> void {
    for (auto& expr : group_by_clause) {
        ReplacePositionRef(expr, select_expr_list);
    }
}

auto Planner::PlanWindowClause(SelectStatement& statement, LogicalPlanPtr& plan) -> LogicalPlanPtr {
    bool has_window = false;
    int slot = 0;
    for (size_t i = 0; i < statement.select_expr_list.size(); ++i) {
        auto& expr = statement.select_expr_list[i];
        if (expr->HasWindow()) {
            if (has_window) {
                // not support multiple window function
                throw intarkdb::Exception(ExceptionType::PLANNER, "not support multiple window function");
            }
            has_window = true;
            slot = i;
        }
    }
    auto expr = std::move(statement.select_expr_list[slot]);  // 从select list中移除
    if (expr->Type() != ExpressionType::WINDOW_FUNC_CALL) {
        throw intarkdb::Exception(ExceptionType::PLANNER, "not support windows function with other expression");
    }
    // cast bound expression to window function expression
    std::unique_ptr<intarkdb::WindowFunction> window_func_expr =
        std::unique_ptr<intarkdb::WindowFunction>(static_cast<intarkdb::WindowFunction*>(expr.release()));
    // use alias to rename the column name
    auto new_expr = std::make_unique<BoundAlias>(window_func_expr->GetFunctionName(),
                                                 std::make_unique<BoundConstant>(ValueFactory::ValueBigInt(1)));
    // modfiy the windows function to constant
    statement.select_expr_list[slot] = std::move(new_expr);
    std::vector<std::unique_ptr<BoundExpression>> select_list;
    select_list.reserve(statement.select_expr_list.size());
    for (auto& select_expr : statement.select_expr_list) {
        select_list.push_back(select_expr->Copy());
    }
    plan = std::make_shared<ProjectionPlan>(LogicalPlanType::Projection, std::move(select_list), plan);
    return std::make_shared<intarkdb::WindowPlan>(std::move(window_func_expr), slot, plan);
}

auto Planner::PlanGroupByClause(SelectStatement& statement, int origin_select_size, LogicalPlanPtr& plan)
    -> LogicalPlanPtr {
    std::vector<std::unique_ptr<BoundExpression>> agg_exprs;

    std::unordered_set<hash_t> func_set;
    std::unordered_map<hash_t, const BoundExpression&> group_by_map;  // hash to expression string
    std::unordered_set<hash_t> select_item_set;

    // 替换group by中的position ref
    ReplaceGroupClausePositionRef(statement.group_by_clause, statement.select_expr_list);
    // 有group by,所有字段都需要时存在于 group by,或者是在group by的字段上再进行计算
    for (auto& expr : statement.group_by_clause) {
        group_by_map.insert({expr->Hash(), *expr});
    }

    for (auto& expr : statement.select_expr_list) {
        select_item_set.insert(expr->Hash());
    }

    for (auto& expr : statement.select_expr_list) {
        ReplaceGroupClauseAgg(func_set, group_by_map, agg_exprs, expr);
    }

    // having clause
    if (statement.having_clause) {
        ReplaceGroupClauseAgg(func_set, group_by_map, agg_exprs, statement.having_clause);
    }

    for (auto& sort_item : statement.sort_items) {
        PlanSubqueries(&sort_item->sort_expr);

        ReplaceGroupClauseAgg(func_set, group_by_map, agg_exprs, sort_item->sort_expr);

        auto expr_hash = sort_item->sort_expr->Hash();
        // sort expr not in select list , 放在转换为colume ref 之后
        if (select_item_set.find(expr_hash) == select_item_set.end()) {
            select_item_set.insert(expr_hash);
            statement.select_expr_list.push_back(sort_item->sort_expr->Copy());
        }
    }

    std::vector<bool> distincts;
    for (size_t i = 0; i < agg_exprs.size(); ++i) {
        auto& expr = agg_exprs[i];
        auto& agg_call_expr = static_cast<BoundAggCall&>(*expr);
        distincts.push_back(agg_call_expr.is_distinct_);
        if (agg_call_expr.func_name_ == "top" || agg_call_expr.func_name_ == "bottom") {
            if (agg_call_expr.args_.size() != 2) {
                throw intarkdb::Exception(ExceptionType::PLANNER,
                                          fmt::format("{} must with two args", agg_call_expr.func_name_));
            }
            continue;
        }
        if (agg_call_expr.args_.size() > 1) {
            throw intarkdb::Exception(
                ExceptionType::PLANNER,
                fmt::format("{} with multiple args is not implemented yet", agg_call_expr.func_name_));
        }
    }

    plan = std::make_shared<AggregatePlan>(std::move(statement.group_by_clause), std::move(agg_exprs), distincts, plan);
    if (statement.having_clause) {
        plan = std::make_shared<FilterPlan>(std::move(statement.having_clause), plan);
    }
    return plan;
}

auto Planner::PlanSelect(SelectStatement& statement) -> LogicalPlanPtr {
    // TODO: 优化掉 PlanValues
    if (statement.value_clause.values.size() > 0) {  // values in select_statement
        return PlanValues(statement);
    }

    if (statement.set_operation_type != SetOperationType::NONE) {
        return PlanSetOpSelect(statement);
    }

    bool is_values_clause = statement.table_ref->Type() == DataSourceType::VALUE_CLAUSE;
    LogicalPlanPtr plan = PlanTableRef(statement, std::move(statement.table_ref), GSTOR_CURSOR_ACTION_SELECT);
    if (is_values_clause) {
        return plan;
    }
    // only support SCALAR Subquery in select list
    plan = PlanScalarSubqueryExprs(statement.select_expr_list, plan);
    // handle correlated subquery
    PlanSubqueryExprs(statement.select_expr_list);

    if (statement.where_clause) {
        plan = PlanWhereClause(statement, plan);
    }

    auto origin_column_size = statement.select_expr_list.size();

    if (statement.is_distinct) {
        // 添加item 到 select_list
        for (size_t i = 0; i < statement.distinct_on_list.size(); ++i) {
            statement.select_expr_list.push_back(statement.distinct_on_list[i]->Copy());
        }
    }
    // projection
    if (HasAggClause(statement)) {
        if (statement.having_clause) {
            PlanSubqueries(&statement.having_clause);
        }
        plan = PlanGroupByClause(statement, origin_column_size, plan);

        if (HasWindowFunction(statement)) {
            throw intarkdb::Exception(ExceptionType::PLANNER, "window function not support in group by clause");
            // plan = PlanWindowClause(statement, plan);
        }
    } else {
        if (HasWindowFunction(statement)) {
            plan = PlanWindowClause(statement, plan);
        }
        // normal select list
        if (!statement.sort_items.empty()) {
            std::unordered_set<hash_t> expr_set;
            for (auto& expr : statement.select_expr_list) {
                expr_set.insert(expr->Hash());
            }
            // 判断order by的字段是否存在于select list中
            for (auto& sort : statement.sort_items) {
                PlanSubqueries(&sort->sort_expr);
                auto& sort_expr = *sort->sort_expr;
                if (sort_expr.Type() == ExpressionType::POSITION_REF) {
                    continue;
                }
                if (expr_set.find(sort_expr.Hash()) == expr_set.end()) {
                    if (sort_expr.Type() == ExpressionType::ALIAS) {
                        const BoundAlias& alias_expr = static_cast<const BoundAlias&>(sort_expr);
                        statement.select_expr_list.push_back(alias_expr.child_->Copy());
                    } else {
                        statement.select_expr_list.push_back(sort_expr.Copy());
                    }
                }
            }
        }
    }

    std::vector<std::unique_ptr<BoundExpression>> prune_select_list;
    auto need_prune = statement.select_expr_list.size() > (size_t)origin_column_size;
    if (need_prune) {
        prune_select_list.reserve(origin_column_size);
        for (size_t i = 0; i < origin_column_size; i++) {
            // 引用上层的列
            prune_select_list.push_back(
                std::make_unique<BoundPositionRef>((int64_t)i, statement.select_expr_list[i]->ReturnType()));
        }
    }

    if (!statement.sort_items.empty()) {
        // 不是子查询中 || (是子查询，但limit不为空) 都无法忽略 sort clause
        if (!IsInSubqueryPlanning() || statement.limit_clause) {
            // sort expr 不会出现新表达式，直接转换为column ref
            std::vector<std::unique_ptr<BoundSortItem>> sort_items;
            // TODO: 优化代码结构,避免copy
            for (size_t i = 0; i < statement.sort_items.size(); ++i) {
                auto& sort_item = statement.sort_items[i];
                const auto& sort_expr = *sort_item->sort_expr;
                if (sort_expr.Type() == ExpressionType::COLUMN_REF) {
                    sort_items.emplace_back(std::make_unique<BoundSortItem>(
                        sort_item->GetSortType(), sort_item->IsNullFirst(), sort_expr.Copy()));
                } else if (sort_expr.Type() == ExpressionType::POSITION_REF) {
                    auto& position_ref = static_cast<const BoundPositionRef&>(sort_expr);
                    auto& position_ref_column = statement.select_expr_list[position_ref.GetIndex()];
                    auto column_ref = std::make_unique<BoundColumnRef>(position_ref_column->GetName(),
                                                                       position_ref_column->ReturnType());
                    sort_items.emplace_back(std::make_unique<BoundSortItem>(
                        sort_item->GetSortType(), sort_item->IsNullFirst(), std::move(column_ref)));
                } else {
                    auto new_sort_expr = std::make_unique<BoundColumnRef>(sort_expr.GetName(), sort_expr.ReturnType());
                    sort_items.emplace_back(std::make_unique<BoundSortItem>(
                        sort_item->GetSortType(), sort_item->IsNullFirst(), std::move(new_sort_expr)));
                }
            }
            plan = std::make_shared<ProjectionPlan>(LogicalPlanType::Projection, std::move(statement.select_expr_list),
                                                    plan);
            plan = std::make_shared<SortPlan>(plan, std::move(sort_items));
        } else {
            plan = std::make_shared<ProjectionPlan>(LogicalPlanType::Projection, std::move(statement.select_expr_list),
                                                    plan);
        }
    } else {
        plan =
            std::make_shared<ProjectionPlan>(LogicalPlanType::Projection, std::move(statement.select_expr_list), plan);
    }

    if (statement.is_distinct) {
        plan = std::make_shared<DistinctPlan>(plan, std::move(statement.distinct_on_list));
    }

    if (statement.limit_clause) {
        if (statement.limit_clause->limit && (statement.limit_clause->limit->Type() != ExpressionType::LITERAL &&
                                              statement.limit_clause->limit->Type() != ExpressionType::BOUND_PARAM)) {
            throw std::runtime_error("LIMIT count must be an integer constant.");
        }
        if (statement.limit_clause->offset && statement.limit_clause->offset->Type() != ExpressionType::LITERAL &&
            statement.limit_clause->offset->Type() != ExpressionType::BOUND_PARAM) {
            throw std::runtime_error("OFFSET must be an integer constant.");
        }
        plan = std::make_shared<LimitPlan>(std::move(statement.limit_clause->limit),
                                           std::move(statement.limit_clause->offset), plan);
    }

    if (need_prune) {
        // add a projection layer
        plan = std::make_shared<ProjectionPlan>(LogicalPlanType::Projection, std::move(prune_select_list), plan);
    }
    return plan;
}

// assume subquery are scalar subquery( one column one row)
auto Planner::PlanScalarSubqueryExpr(std::unique_ptr<BoundExpression>& expr,
                                     std::vector<std::unique_ptr<BoundExpression>>& pullup_predicate)
    -> LogicalPlanPtr {
    auto& subquery_expr = static_cast<BoundSubqueryExpr&>(*expr);
    if (subquery_expr.select_statement->return_list.size() > 1) {
        throw intarkdb::Exception(ExceptionType::PLANNER, "not support more than 1 column in scalar subquery");
    }
    std::vector<std::string> col_name{subquery_expr.GetSubqueryName(),
                                      subquery_expr.select_statement->return_list[0].col_name.back()};
    auto& col_type = subquery_expr.select_statement->return_list[0].col_type;
    std::unique_ptr<BoundColumnRef> column_ref = std::make_unique<BoundColumnRef>(col_name, col_type);

    EnterSbuqueryPlanning();
    auto plan = PlanSelect(*subquery_expr.select_statement);
    auto& ctx = subquery_planner_ctxs_.back();
    if (ctx.where_clause) {
        pullup_predicate.emplace_back(std::move(ctx.where_clause));
    }
    ExitSubqueryPlanning();

    // 获取 PlanSelect 之后的列名而不是直接copy expr , 避免 子查询中还有子查询时，出现的expr依然为subquery
    auto old_column_info = plan->GetSchema().GetColumnInfoByIdx(0);
    std::vector<std::unique_ptr<BoundExpression>> origins;
    origins.emplace_back(std::make_unique<BoundColumnRef>(old_column_info.col_name, old_column_info.col_type));

    // TODO: 先添加limit，保证只返回一行，后续再确认，返回多行时，是否需要报错
    plan = std::make_shared<LimitPlan>(std::make_unique<BoundConstant>(ValueFactory::ValueInt(1)), nullptr, plan);
    // add projection , rename the column from origin table to subquery name
    std::vector<SchemaColumnInfo> columns;
    columns.emplace_back(column_ref->GetName(), "", column_ref->ReturnType(), 0);
    plan = std::make_shared<ProjectionPlan>(Schema{std::move(columns)}, std::move(origins), plan);
    expr = std::move(column_ref);
    return plan;
}

// rewrite subquery expr in select list and values clause
auto Planner::PlanScalarSubqueryExprs(std::vector<std::unique_ptr<BoundExpression>>& exprs, LogicalPlanPtr plan)
    -> LogicalPlanPtr {
    std::vector<LogicalPlanPtr> subquery_plans;
    std::vector<std::unique_ptr<BoundExpression>> pullup_predicate;
    for (auto& expr : exprs) {
        if (expr->HasSubQuery()) {
            // TODO: 可以先假设只有一个相关子查询,后续再考虑多个相关子查询
            ExpressionIterator::EnumerateExpression(expr, [&](std::unique_ptr<BoundExpression>& expr) {
                if (expr->Type() == ExpressionType::SUBQUERY) {
                    auto& subquery_expr = static_cast<BoundSubqueryExpr&>(*expr);
                    if (!subquery_expr.IsCorrelated() && subquery_expr.subquery_type == SubqueryType::SCALAR) {
                        subquery_plans.emplace_back(PlanScalarSubqueryExpr(expr, pullup_predicate));
                    }
                }
            });
        }
    }
    // build new plan
    if (subquery_plans.empty()) {
        return plan;
    }
    auto& new_plan = subquery_plans[0];
    for (size_t i = 1; i < subquery_plans.size(); ++i) {
        // note: 使用 left join , 保证右表即使是空集, 至少会返回一个null，符合子查询定义
        new_plan = std::make_shared<NestedLoopJoinPlan>(new_plan, subquery_plans[i], nullptr, JoinType::LeftJoin);
    }
    std::unique_ptr<BoundExpression> predicate = nullptr;
    if (pullup_predicate.size() > 0) {
        predicate = std::move(pullup_predicate[0]);
    }
    return std::make_shared<NestedLoopJoinPlan>(plan, new_plan, std::move(predicate), JoinType::LeftJoin);
}

void Planner::PlanSubqueries(std::unique_ptr<BoundExpression>* expr_ptr) {
    if (!expr_ptr || !*expr_ptr) {
        return;
    }
    auto& expr = **expr_ptr;
    // 遍历表达式树，处理子查询
    ExpressionIterator::EnumerateChildren(expr,
                                          [&](std::unique_ptr<BoundExpression>& child) { PlanSubqueries(&child); });
    if (expr.Type() == ExpressionType::SUBQUERY) {
        BoundSubqueryExpr& bound_sub_query = static_cast<BoundSubqueryExpr&>(expr);
        *expr_ptr = PlanSubquery(bound_sub_query);
    }
}

auto Planner::PlanSubquery(BoundSubqueryExpr& expr) -> std::unique_ptr<BoundExpression> {
    EnterSbuqueryPlanning();

    if (expr.select_statement->select_expr_list.size() == 0) {
        throw std::runtime_error("subquery select list should not be empty");
    }

    std::unique_ptr<BoundExpression> result_expression;
    if (!expr.IsCorrelated()) {
        auto sub_plan = PlanSelect(*expr.select_statement);
        // 优化器
        intarkdb::Optimizer optimizer;
        sub_plan = optimizer.OptimizeLogicalPlan(sub_plan);

        // 处理非关联子查询
        // recreate subquery expression
        result_expression =
            std::make_unique<BoundSubqueryExpr>(expr.subquery_type, sub_plan, std::move(expr.correlated_columns),
                                                std::move(expr.child), expr.op_name, expr.subquery_id);
    } else {
        // 支持关联子查询
        auto sub_plan = PlanSelect(*expr.select_statement);
        result_expression =
            std::make_unique<BoundSubqueryExpr>(expr.subquery_type, sub_plan, std::move(expr.correlated_columns),
                                                std::move(expr.child), expr.op_name, expr.subquery_id);
    }
    ExitSubqueryPlanning();
    return result_expression;
}

auto Planner::PlanInsert(std::unique_ptr<BoundTableRef> table_ref, std::unique_ptr<CreateStatement> create_stmt,
                         LogicalPlanPtr& plan) -> LogicalPlanPtr {
    auto table_ref_ptr = std::unique_ptr<BoundBaseTable>(static_cast<BoundBaseTable*>(table_ref.release()));

    return std::make_shared<InsertPlan>(
        LogicalPlanType::Insert,
        catalog_.CreateTableDataSource(std::move(table_ref_ptr), GSTOR_CURSOR_ACTION_INSERT, table_idx_++),
        // for create as select
        create_stmt->GetColumns(), std::vector<Column>{}, std::vector<Column>{}, ONCONFLICT_ALIAS_NONE, plan);
}

auto Planner::PlanInsert(InsertStatement& statement) -> LogicalPlanPtr {
    auto table_ref_ptr = std::unique_ptr<BoundBaseTable>(static_cast<BoundBaseTable*>(statement.table_.release()));

    // select statement
    SelectStatement& select_stmt = *(statement.select_.get());
    auto select_plan = PlanSelect(select_stmt);

    auto insert_plan = std::make_shared<InsertPlan>(
        LogicalPlanType::Insert,
        catalog_.CreateTableDataSource(std::move(table_ref_ptr), GSTOR_CURSOR_ACTION_INSERT, table_idx_++),
        statement.bound_columns_, statement.unbound_defaults_, statement.bound_defaults_,
        statement.opt_or_action_, select_plan);
    return insert_plan;
}

auto Planner::PlanDelete(DeleteStatement& statement) -> LogicalPlanPtr {
    auto table_ref_ptr =
        std::unique_ptr<BoundBaseTable>(static_cast<BoundBaseTable*>(statement.target_table.release()));

    std::shared_ptr<ScanPlan> scan_plan = std::make_shared<ScanPlan>(
        catalog_.CreateTableDataSource(std::move(table_ref_ptr), GSTOR_CURSOR_ACTION_DELETE, table_idx_++),
        std::vector<std::string>{});

    LogicalPlanPtr plan = scan_plan;

    if (statement.condition) {
        PlanSubqueries(&statement.condition);
        plan = std::make_shared<FilterPlan>(std::move(statement.condition), plan);
    }

    plan = std::make_shared<DeletePlan>(LogicalPlanType::Delete, *scan_plan->source, plan);

    return plan;
}

static auto GetBestIndexId(std::shared_ptr<ScanPlan> scan_plan) -> std::tuple<uint16_t, size_t> {
    const TableInfo& table_info = scan_plan->source->GetTableRef().GetTableInfo();
    size_t loggest_match_size = 0;
    uint16_t use_index_id = GS_INVALID_ID16;
    for (size_t i = 0; i < table_info.GetIndexCount(); ++i) {
        const auto& index = table_info.GetIndexBySlot(i);  // 遍历索引
        const exp_index_def_t& index_def = index.GetIndexDef();
        GS_LOG_RUN_INF("index name = %s,is_primary=%d, slot=%d \n", index_def.name.str, index_def.is_primary,
                       index_def.index_slot);
        size_t col_idx = 0;
        for (col_idx = 0; col_idx < index_def.col_count; ++col_idx) {  // 遍历索引列
            // 索引不知道列在表中的序号，只能通过列定义中的ID来寻找列信息
            const auto& column = table_info.columns[index_def.col_ids[col_idx]];
            const exp_column_def_t& column_def = column.GetRaw();

            bool found = false;
            for (size_t expr_idx = 0; expr_idx < scan_plan->bound_expressions.size(); ++expr_idx) {
                auto& expr = scan_plan->bound_expressions[expr_idx];  // 查找与索引列匹配的表达式
                if (expr->Type() == ExpressionType::BINARY_OP) {
                    auto& binary_op = static_cast<BoundBinaryOp&>(*expr);
                    if (!binary_op.IsCompareOp()) {
                        continue;
                    }
                    auto& left = binary_op.LeftPtr();
                    auto& right = binary_op.RightPtr();
                    if (left->Type() == ExpressionType::COLUMN_REF &&
                        (right->Type() == ExpressionType::LITERAL || right->Type() == ExpressionType::BOUND_PARAM)) {
                        if (strcmp(left->ToString().c_str(), column_def.name.str) == 0) {
                            found = true;
                            break;
                        }
                    } else if (right->Type() == ExpressionType::COLUMN_REF &&
                               (left->Type() == ExpressionType::LITERAL ||
                                left->Type() == ExpressionType::BOUND_PARAM)) {
                        if (strcmp(right->ToString().c_str(), column_def.name.str) == 0) {
                            found = true;
                            break;
                        }
                    }
                }
            }
            if (!found) {
                break;
            }
        }
        if (col_idx > loggest_match_size) {
            use_index_id = index_def.index_slot;
            loggest_match_size = col_idx;
        }
    }
    GS_LOG_RUN_INF("is use index = %d , index slot=%u\n", !(use_index_id == GS_INVALID_ID16), use_index_id);
    return std::tuple{use_index_id, loggest_match_size};
}

static PhysicalPlanPtr CreateSeqScanExec(Planner& planner, std::shared_ptr<ScanPlan> scan_plan) {
    if (scan_plan->IsOnlyCount()) {
        return std::make_unique<FastScanExec>(scan_plan->GetSchema(), std::move(scan_plan->source));
    }
    auto [best_index_id, loggest_match] = GetBestIndexId(scan_plan);
    std::vector<std::unique_ptr<Expression>> predicates;
    IndexMatchInfo index_match_info;
    if (best_index_id != GS_INVALID_ID16) {
        const TableInfo& table_info = scan_plan->source->GetTableRef().GetTableInfo();
        const auto& index = table_info.GetIndexBySlot(best_index_id);
        const exp_index_def_t& index_def = index.GetIndexDef();
        std::map<int, IndexMatchColumnInfo> index_match_columns_map;
        index_match_info.use_index = true;
        index_match_info.index_slot = best_index_id;
        index_match_info.total_index_column = index_def.col_count;
        // TODO: 优化代码结构
        for (size_t i = 0; i < scan_plan->bound_expressions.size(); ++i) {
            auto& expr = scan_plan->bound_expressions[i];
            if (expr->Type() == ExpressionType::BINARY_OP) {
                auto& binary_op = static_cast<BoundBinaryOp&>(*expr);
                // 不支持 != 操作符
                if (!(binary_op.OpName() == ">" || binary_op.OpName() == ">=" || binary_op.OpName() == "<" ||
                      binary_op.OpName() == "<=" || binary_op.OpName() == "=" || binary_op.OpName() == "==")) {
                    predicates.push_back(planner.CreatePhysicalExpression(*expr, scan_plan));
                    continue;
                }
                auto op_type = intarkdb::ToComparisonType(binary_op.OpName());
                auto& left = binary_op.LeftPtr();
                auto& right = binary_op.RightPtr();
                if (left->Type() == ExpressionType::COLUMN_REF &&
                    (right->Type() == ExpressionType::LITERAL || right->Type() == ExpressionType::BOUND_PARAM)) {
                    // 不存在 左边是常量 右边是列引用的情况
                    bool is_index_condition = false;
                    for (size_t col_idx = 0; col_idx < index_def.col_count; ++col_idx) {
                        const auto& column = table_info.columns[index_def.col_ids[col_idx]];
                        const exp_column_def_t& column_def = column.GetRaw();
                        if (strcmp(left->ToString().c_str(), column_def.name.str) == 0) {
                            auto right_expr = planner.CreatePhysicalExpression(*right, scan_plan);
                            auto left_expr = planner.CreatePhysicalExpression(*left, scan_plan);
                            index_match_columns_map[col_idx].col_id = column_def.col_slot;
                            index_match_columns_map[col_idx].data_type = column_def.col_type;
                            if (op_type == intarkdb::ComparisonType::LessThan ||
                                op_type == intarkdb::ComparisonType::LessThanOrEqual) {
                                index_match_columns_map[col_idx].upper = right_expr.get();
                            } else if (op_type == intarkdb::ComparisonType::GreaterThan ||
                                       op_type == intarkdb::ComparisonType::GreaterThanOrEqual) {
                                index_match_columns_map[col_idx].lower = right_expr.get();
                            } else if (op_type == intarkdb::ComparisonType::Equal) {
                                index_match_columns_map[col_idx].lower = right_expr.get();
                                index_match_columns_map[col_idx].upper = right_expr.get();
                            }

                            predicates.push_back(BinaryOpFactory(binary_op.OpName(), std::move(left_expr),
                                                                 std::move(right_expr), binary_op.FuncInfo()));
                            is_index_condition = true;
                            break;
                        }
                    }
                    if (!is_index_condition) {
                        predicates.push_back(planner.CreatePhysicalExpression(*expr, scan_plan));
                    }
                } else {
                    predicates.push_back(planner.CreatePhysicalExpression(*expr, scan_plan));
                }
            }
        }
        // NOTE: 这里的index_columns是按照索引列的顺序排列的
        for (auto& iter : index_match_columns_map) {
            index_match_info.index_columns.push_back(iter.second);
            // 只传递符合最长匹配的索引条件的列
            if (index_match_info.index_columns.size() >= loggest_match) {
                break;
            }
        }
        scan_plan->source->SetIndexInfo(index_match_info);
    } else {  // no valid index
        for (auto& expr : scan_plan->bound_expressions) {
            predicates.push_back(planner.CreatePhysicalExpression(*expr, scan_plan));
        }
    }
    return std::make_shared<SeqScanExec>(std::move(scan_plan->source), scan_plan->Projection(), std::move(predicates));
}

auto Planner::CreatePhysicalExpression(BoundExpression& logical_expr, const LogicalPlanPtr& plan)
    -> std::unique_ptr<Expression> {
    switch (logical_expr.Type()) {
        case ExpressionType::COLUMN_REF: {
            auto& column_ref = static_cast<BoundColumnRef&>(logical_expr);
            if (column_ref.IsOuter() && !outer_plans.empty()) {
                // 获取列在schema中的相对位置
                auto idx =
                    outer_plans.top()->GetSchema().GetIdxByName(column_ref.GetName());  // 获取列在schema中的相对位置
                auto param_expr =
                    std::make_unique<ColumnParamExpression>(column_ref.Name(), idx, column_ref.ReturnType());
                params_cols.push_back(param_expr.get());
                return param_expr;
            }
            auto idx = plan->GetSchema().GetIdxByName(column_ref.GetName());  // 获取列在schema中的相对位置
            return std::make_unique<ColumnValueExpression>(column_ref.Name(), idx, column_ref.ReturnType());
        }
        case ExpressionType::POSITION_REF: {
            const auto& position_ref = static_cast<BoundPositionRef&>(logical_expr);
            const auto& column_info = plan->GetSchema().GetColumnInfoByIdx(position_ref.GetIndex());
            return std::make_unique<ColumnValueExpression>(column_info.col_name, position_ref.GetIndex(),
                                                           position_ref.ReturnType());
        }
        case ExpressionType::CONJUNCTIVE: {
            auto& conjunctive = static_cast<BoundConjunctive&>(logical_expr);
            std::vector<std::unique_ptr<Expression>> exprs;
            for (auto& item : conjunctive.items) {
                exprs.push_back(CreatePhysicalExpression(*item, plan));
            }
            return std::make_unique<ConjunctiveExpression>(std::move(exprs));
        }
        case ExpressionType::ALIAS: {
            auto& alias = static_cast<BoundAlias&>(logical_expr);
            return CreatePhysicalExpression(*alias.child_, plan);
        }
        case ExpressionType::LITERAL: {
            auto& constant = static_cast<BoundConstant&>(logical_expr);
            return std::make_unique<ConstantExpression>(std::move(constant.MoveableVal()));
        }
        case ExpressionType::BINARY_OP: {
            auto& binary_op = static_cast<BoundBinaryOp&>(logical_expr);
            auto left = CreatePhysicalExpression(binary_op.Left(), plan);
            auto right = CreatePhysicalExpression(binary_op.Right(), plan);
            return BinaryOpFactory(binary_op.OpName(), std::move(left), std::move(right), binary_op.FuncInfo());
        }
        case ExpressionType::UNARY_OP: {
            auto& unary_op = static_cast<BoundUnaryOp&>(logical_expr);
            auto expr = CreatePhysicalExpression(unary_op.Child(), plan);
            return UnaryOpFactory(unary_op.OpName(), std::move(expr));
        }
        case ExpressionType::NULL_TEST: {
            auto& null_test_op = static_cast<BoundNullTest&>(logical_expr);
            auto expr = CreatePhysicalExpression(*null_test_op.child, plan);
            return std::make_unique<NullTestExpression>(std::move(expr), null_test_op.null_test_type);
        }
        case ExpressionType::AGG_CALL: {
            auto& agg_call_expr = static_cast<BoundAggCall&>(logical_expr);
            auto idx = plan->GetSchema().GetIdxByName(agg_call_expr.GetName());  // 获取列在schema中的相对位置
            return std::make_unique<ColumnValueExpression>(std::vector<std::string>{agg_call_expr.ToString()}, idx,
                                                           agg_call_expr.ReturnType());
        }
        case ExpressionType::LIKE_OP: {
            auto& like_op = static_cast<BoundLikeOp&>(logical_expr);
            auto left = CreatePhysicalExpression(like_op.Left(), plan);
            auto right = CreatePhysicalExpression(like_op.Right(), plan);
            auto escape = CreatePhysicalExpression(like_op.Escape(), plan);
            return LikeOpFactory(like_op.OpName(), std::move(left), std::move(right), std::move(escape));
        }
        case ExpressionType::FUNC_CALL: {
            auto& func_call_expr = static_cast<BoundFuncCall&>(logical_expr);
            std::vector<std::unique_ptr<Expression>> args;
            for (auto& arg : func_call_expr.args_) {
                args.push_back(CreatePhysicalExpression(*arg, plan));
            }
            return FunctionFactory(func_call_expr.func_name_, std::move(args));
        }
        case ExpressionType::SEQ_FUNC: {
            auto& seq_func_expr = static_cast<BoundSequenceFunction&>(logical_expr);
            auto arg = CreatePhysicalExpression(*seq_func_expr.arg, plan);
            return std::make_unique<SequenceExpression>(seq_func_expr.funcname, std::move(arg), seq_func_expr.catalog);
        }
        case ExpressionType::SUBQUERY: {
            outer_plans.push(plan);
            auto& subquery_expr = static_cast<BoundSubqueryExpr&>(logical_expr);
            std::unique_ptr<Expression> expr;
            if (subquery_expr.child) {
                expr = CreatePhysicalExpression(*subquery_expr.child, plan);
            }
            if (subquery_expr.correlated_columns.size() > 0) {
                auto physical_plan = CreatePhysicalPlan(subquery_expr.plan_ptr);
                auto subexpr = SubQueryFactory(subquery_expr.subquery_type, std::move(physical_plan), std::move(expr),
                                               subquery_expr.op_name);
                outer_plans.pop();
                return std::make_unique<SubQueryCorrelatedExpression>(params_cols, std::move(subexpr));
            } else {
                auto physical_plan = CreatePhysicalPlan(subquery_expr.plan_ptr);
                auto subexpr = SubQueryFactory(subquery_expr.subquery_type, std::move(physical_plan), std::move(expr),
                                               subquery_expr.op_name);
                outer_plans.pop();
                return subexpr;
            }
        }
        case ExpressionType::TYPE_CAST: {
            auto& cast_expr = static_cast<BoundCast&>(logical_expr);
            auto expr = CreatePhysicalExpression(cast_expr.Child(), plan);
            return std::make_unique<CastExpression>(cast_expr.target_type, std::move(expr), cast_expr.TryCast());
        }
        case ExpressionType::BOUND_PARAM: {
            auto& param_expr = static_cast<BoundParameter&>(logical_expr);
            auto expr = std::make_unique<ColumnParamExpression>(std::vector<std::string>{}, param_expr.parameter_nr);
            prepare_params_cols_.push_back(expr.get());
            return expr;
        }
        case ExpressionType::IN_EXPR: {
            auto& in_expr = static_cast<BoundInExpr&>(logical_expr);
            auto in_ref_expr = CreatePhysicalExpression(*in_expr.in_ref_expr, plan);
            std::vector<std::unique_ptr<Expression>> list;
            for (auto& expr : in_expr.in_list) {
                list.push_back(CreatePhysicalExpression(*expr, plan));
            }
            return std::make_unique<InExpression>(std::move(in_ref_expr), std::move(list), in_expr.is_not_in);
        }
        case ExpressionType::CASE: {
            auto& case_expr = static_cast<BoundCase&>(logical_expr);
            std::vector<CaseCheck> case_checks;
            for (auto& bound_case_check : case_expr.bound_case_checks) {
                CaseCheck case_check_one;
                case_check_one.when_expr = CreatePhysicalExpression(*(bound_case_check.when_expr), plan);
                case_check_one.then_expr = CreatePhysicalExpression(*(bound_case_check.then_expr), plan);
                case_checks.push_back(std::move(case_check_one));
            }

            std::unique_ptr<Expression> else_expr = nullptr;
            if (case_expr.else_expr) {
                else_expr = CreatePhysicalExpression(*case_expr.else_expr, plan);
            }
            return std::make_unique<CaseExpression>(std::move(case_checks), std::move(else_expr));
        }
        default:
            break;
    }
    throw intarkdb::Exception(
        ExceptionType::PLANNER,
        fmt::format("unsupported bound expression type={} in create physical expression", logical_expr.Type()));
}

auto BinaryOpFactory(const std::string& op_name, std::unique_ptr<Expression> left, std::unique_ptr<Expression> right,
                     const std::optional<intarkdb::Function>& func) -> std::unique_ptr<Expression> {
    if (intarkdb::IsCompareOp(op_name)) {
        if (!func.has_value()) {
            throw intarkdb::Exception(
                ExceptionType::PLANNER,
                fmt::format("unsupported {} {} {}", left->GetLogicalType(), op_name, right->GetLogicalType()));
        }
        auto type = intarkdb::ToComparisonType(op_name);
        return std::make_unique<ComparisonExpression>(type, std::move(left), std::move(right), func.value());
    }
    if (intarkdb::IsBinaryLogicalOp(op_name)) {
        auto type = intarkdb::ToLogicOpType(op_name);
        if (type == LogicOpType::And) {
            return std::make_unique<LogicBinaryOpExpression>(type, std::move(left), std::move(right));
        }
        if (type == LogicOpType::Or) {
            return std::make_unique<LogicBinaryOpExpression>(type, std::move(left), std::move(right));
        }
        throw intarkdb::Exception(ExceptionType::PLANNER, fmt::format("unsupported op {} ", op_name));
    }
    if (op_name == "||") {  // string concat 特殊处理
        return std::make_unique<StringOpExpression>(StringOpType::Concat, std::move(left), std::move(right),
                                                    func.value());
    }
    return std::make_unique<MathBinaryOpExpression>(ToMathOpType(op_name), std::move(left), std::move(right),
                                                    func.value());
}

auto LikeOpFactory(const std::string& op_name, std::unique_ptr<Expression> left, std::unique_ptr<Expression> right,
                   std::unique_ptr<Expression> escape) -> std::unique_ptr<Expression> {
    auto type = LikeTypeFromString(op_name);
    return std::make_unique<LikeExpression>(type, std::move(left), std::move(right), std::move(escape));
}

auto UnaryOpFactory(const std::string& op_name, std::unique_ptr<Expression> expr) -> std::unique_ptr<Expression> {
    if (op_name == "!" || op_name == "not") {
        return std::make_unique<LogicUnaryOpExpression>(LogicOpType::Not, std::move(expr));
    } else if (op_name == "-") {
        const auto& func = intarkdb::FunctionContext::GetFunction("-", {expr->GetLogicalType()});
        if (func) {
            return std::make_unique<MathUnaryOpExpression>(MathOpType::Minus, std::move(expr), func.value());
        }
        throw intarkdb::Exception(ExceptionType::PLANNER, fmt::format("unsupported - with {}", expr->GetLogicalType()));
    }
    throw std::invalid_argument(fmt::format("unsupport this unary op {}", op_name));
}

auto FunctionFactory(const std::string& funcname, std::vector<std::unique_ptr<Expression>> args)
    -> std::unique_ptr<Expression> {
    auto f = SQLFunction::FUNC_MAP.find(funcname);
    if (f == SQLFunction::FUNC_MAP.end()) {
        throw std::invalid_argument(fmt::format("unsupport this function {}", funcname));
    }
    return std::make_unique<FunctionExpression>(funcname, f->second.func, std::move(args), f->second.return_type);
}

auto Planner::CreateProjectionExec(std::shared_ptr<ProjectionPlan> project_plan) -> PhysicalPlanPtr {
    std::vector<std::unique_ptr<Expression>> exprs;
    const auto& logical_exprs = project_plan->Exprs();
    std::vector<SchemaColumnInfo> cols;
    int idx = 0;
    for (const auto& logical_expr : logical_exprs) {
        if (logical_expr->Type() == ExpressionType::COLUMN_REF) {
            const auto& column_ref = static_cast<const BoundColumnRef&>(*logical_expr);
            if (column_ref.IsOuter() && !outer_plans.empty()) {
                // 获取列在schema中的相对位置
                auto idx =
                    outer_plans.top()->GetSchema().GetIdxByName(column_ref.GetName());  // 获取列在schema中的相对位置
                auto param_expr =
                    std::make_unique<ColumnParamExpression>(column_ref.Name(), idx, column_ref.ReturnType());
                params_cols.push_back(param_expr.get());
                exprs.push_back(std::move(param_expr));
                cols.emplace_back(BoundExpressionToSchemaColumnInfo(project_plan->GetLastPlan(), *logical_expr, idx++));
                continue;
            }
        }
        cols.emplace_back(BoundExpressionToSchemaColumnInfo(project_plan->GetLastPlan(), *logical_expr, idx++));
        exprs.push_back(CreatePhysicalExpression(*logical_expr, project_plan->GetLastPlan()));
    }
    // 前面需要访问source,sub会迁移source的所有权，所以把sub的创建往后放
    auto sub = CreatePhysicalPlan(project_plan->GetLastPlan());
    return std::make_shared<ProjectionExec>(sub, Schema(std::move(cols)), std::move(exprs));
}

auto Planner::CreatePhysicalPlan(const LogicalPlanPtr& plan) -> PhysicalPlanPtr {
    switch (plan->Type()) {
        case LogicalPlanType::EmptySource: {
            std::shared_ptr<EmptySourcePlan> empty_plan = std::dynamic_pointer_cast<EmptySourcePlan>(plan);
            return std::make_shared<EmptySourceExec>(empty_plan->GetSchema(), empty_plan->GetReturnCount());
        }
        case LogicalPlanType::Scan: {
            // exec scan
            std::shared_ptr<ScanPlan> scan_plan = std::dynamic_pointer_cast<ScanPlan>(plan);
            return CreateSeqScanExec(*this, scan_plan);
        }
        case LogicalPlanType::Projection: {
            // exec
            std::shared_ptr<ProjectionPlan> project_plan = std::dynamic_pointer_cast<ProjectionPlan>(plan);
            return CreateProjectionExec(project_plan);
        }
        case LogicalPlanType::Filter: {
            std::shared_ptr<FilterPlan> filter = std::dynamic_pointer_cast<FilterPlan>(plan);
            auto expr = CreatePhysicalExpression(*filter->expr, filter);
            auto sub = CreatePhysicalPlan(filter->GetLastPlan());
            return std::make_shared<FilterExec>(sub, std::move(expr));
        }
        case LogicalPlanType::Insert: {
            std::shared_ptr<InsertPlan> insert = std::dynamic_pointer_cast<InsertPlan>(plan);
            auto sub = CreatePhysicalPlan(insert->GetLastPlan());
            return std::make_shared<InsertExec>(sub, std::move(insert->source), insert->bound_columns_,
                                                insert->unbound_defaults_, insert->bound_defaults_,
                                                insert->opt_or_action_);
        }
        case LogicalPlanType::Values: {
            std::shared_ptr<ValuesPlan> value_plan = std::dynamic_pointer_cast<ValuesPlan>(plan);
            std::vector<std::vector<std::unique_ptr<Expression>>> insert_exprs_array;
            for (const auto& values : value_plan->values_list) {
                std::vector<std::unique_ptr<Expression>> insert_exprs;
                for (const auto& value : values) {
                    insert_exprs.emplace_back(CreatePhysicalExpression(*value, value_plan));
                }
                insert_exprs_array.push_back(std::move(insert_exprs));
            }
            return std::make_shared<ValuesExec>(value_plan->GetSchema(), std::move(insert_exprs_array));
        }
        case LogicalPlanType::Delete: {
            std::shared_ptr<DeletePlan> deletePlan = std::dynamic_pointer_cast<DeletePlan>(plan);
            auto sub = CreatePhysicalPlan(deletePlan->child_);
            return std::make_shared<DeleteExec>(deletePlan->sc_, sub);
        }
        case LogicalPlanType::Update: {
            std::shared_ptr<UpdatePlan> updatePlan = std::dynamic_pointer_cast<UpdatePlan>(plan);
            auto sub = CreatePhysicalPlan(updatePlan->child_);
            return std::make_shared<UpdateExec>(updatePlan->sc_, std::vector<PhysicalPlanPtr>({sub}),
                                                updatePlan->target_list_);
        }
        case LogicalPlanType::Transaction: {
            std::shared_ptr<TransactionPlan> values = std::dynamic_pointer_cast<TransactionPlan>(plan);
            return std::make_shared<TransactionExec>(values->handle, values->type);
        }
        case LogicalPlanType::Distinct: {
            std::shared_ptr<DistinctPlan> distinct = std::dynamic_pointer_cast<DistinctPlan>(plan);
            std::vector<std::unique_ptr<Expression>> exprs;
            for (const auto& expr : distinct->distinct_on_list) {
                exprs.push_back(CreatePhysicalExpression(*expr, distinct->GetLastPlan()));
            }
            auto child = CreatePhysicalPlan(distinct->GetLastPlan());
            return std::make_shared<DistinctExec>(child, std::move(exprs));
        }
        case LogicalPlanType::Limit: {
            std::shared_ptr<LimitPlan> limit_plan = std::dynamic_pointer_cast<LimitPlan>(plan);
            std::unique_ptr<Expression> limit = nullptr;
            std::unique_ptr<Expression> offset = nullptr;
            if (limit_plan->limit && !limit_plan->limit->IsInvalid()) {
                limit = CreatePhysicalExpression(*limit_plan->limit, limit_plan->GetLastPlan());
            }
            if (limit_plan->offset && !limit_plan->offset->IsInvalid()) {
                offset = CreatePhysicalExpression(*limit_plan->offset, limit_plan->GetLastPlan());
            }
            auto child = CreatePhysicalPlan(limit_plan->GetLastPlan());
            return std::make_shared<LimitExec>(child, std::move(limit), std::move(offset));
        }
        case LogicalPlanType::Sort: {
            std::shared_ptr<SortPlan> sort_plan = std::dynamic_pointer_cast<SortPlan>(plan);
            std::vector<std::unique_ptr<Expression>> exprs;
            std::vector<OrderByInfo> order_infos;
            for (const auto& order : sort_plan->order_by_) {
                order_infos.push_back({order->GetSortType(), order->IsNullFirst()});
                exprs.push_back(CreatePhysicalExpression(*order->sort_expr, sort_plan));
            }
            auto child = CreatePhysicalPlan(sort_plan->GetLastPlan());
            return std::make_shared<SortExec>(child, std::move(exprs), std::move(order_infos));
        }
        case LogicalPlanType::Drop: {
            std::shared_ptr<DropPlan> values = std::dynamic_pointer_cast<DropPlan>(plan);
            return std::make_shared<DropExec>(catalog_, values->name, values->if_exists, values->type);
        }
        case LogicalPlanType::NestedLoopJoin: {
            std::shared_ptr<NestedLoopJoinPlan> join_ref = std::dynamic_pointer_cast<NestedLoopJoinPlan>(plan);
            return CreateJoinExec(join_ref);
        }
        case LogicalPlanType::Aggregation: {
            std::shared_ptr<AggregatePlan> agg_plan = std::dynamic_pointer_cast<AggregatePlan>(plan);

            std::vector<std::unique_ptr<Expression>> group_exprs;

            for (size_t i = 0; i < agg_plan->group_by_.size(); ++i) {
                group_exprs.push_back(CreatePhysicalExpression(*agg_plan->group_by_[i], agg_plan->GetLastPlan()));
            }
            std::vector<std::string> ops;
            std::vector<std::unique_ptr<Expression>> agg_exprs;
            for (size_t i = 0; i < agg_plan->aggregates_.size(); ++i) {
                const auto& agg_expr = agg_plan->aggregates_[i];
                if (agg_expr->Type() != ExpressionType::AGG_CALL) {
                    throw std::runtime_error("not agg call");
                }
                const auto& agg_call = static_cast<BoundAggCall&>(*agg_expr);
                ops.push_back(agg_call.func_name_);
                if (agg_call.func_name_ == "count_star") {
                    agg_exprs.push_back(nullptr);
                } else {
                    if (agg_call.args_.size() == 0) {
                        throw std::runtime_error("No function matches the given name and argument types");
                    } else if (agg_call.args_.size() == 1) {
                        agg_exprs.push_back(CreatePhysicalExpression(*agg_call.args_[0], agg_plan->GetLastPlan()));
                    } else if (agg_call.args_.size() == 2) {
                        agg_exprs.push_back(CreatePhysicalExpression(*agg_call.args_[0], agg_plan->GetLastPlan()));
                        agg_exprs.push_back(CreatePhysicalExpression(*agg_call.args_[1], agg_plan->GetLastPlan()));
                    } else {
                        agg_exprs.push_back(CreatePhysicalExpression(*agg_call.args_[0], agg_plan->GetLastPlan()));
                    }
                }
            }
            std::vector<SchemaColumnInfo> cols;
            int idx = 0;
            for (const auto& group_expr : agg_plan->group_by_) {
                auto col = BoundExpressionToSchemaColumnInfo(agg_plan->GetLastPlan(), *group_expr, idx++);
                cols.emplace_back(col);
            }
            for (const auto& agg_expr : agg_plan->aggregates_) {
                auto col = BoundExpressionToSchemaColumnInfo(agg_plan->GetLastPlan(), *agg_expr, idx++);
                cols.emplace_back(col);
            }
            auto schema = agg_plan->GetSchema();
            const auto& columns = schema.GetColumnInfos();
            for (size_t i = idx; i < columns.size(); ++i) {
                cols.emplace_back(columns[i]);
            }
            auto child = CreatePhysicalPlan(agg_plan->GetLastPlan());
            return std::make_shared<AggregateExec>(std::move(group_exprs), ops, std::move(agg_exprs),
                                                   agg_plan->distincts, Schema(std::move(cols)), child);
        }
        case LogicalPlanType::Apply: {
            std::shared_ptr<ApplyPlan> apply_plan = std::dynamic_pointer_cast<ApplyPlan>(plan);
            const auto& children = apply_plan->Children();
            auto left = CreatePhysicalPlan(children[0]);
            auto right = CreatePhysicalPlan(children[1]);
            std::unique_ptr<Expression> pred = nullptr;
            if (apply_plan->Pred()) {
                pred = CreatePhysicalExpression(*apply_plan->Pred(), plan);
            }
            return std::make_shared<NestedLoopJoinExec>(apply_plan->GetSchema(), left, right, std::move(pred),
                                                        apply_plan->GetJoinType());
        }
        case LogicalPlanType::Union: {
            std::shared_ptr<UnionPlan> union_plan = std::dynamic_pointer_cast<UnionPlan>(plan);
            auto schema = union_plan->GetSchema();
            auto left = CreatePhysicalPlan(union_plan->left);
            auto right = CreatePhysicalPlan(union_plan->right);
            return std::make_shared<UnionExec>(schema, left, right, union_plan->set_op_type, union_plan->is_distinct);
        }
        case LogicalPlanType::Comment_On: {
            std::shared_ptr<CommentOnPlan> comment_on_plan = std::dynamic_pointer_cast<CommentOnPlan>(plan);
            return std::make_shared<CommentOnExec>(comment_on_plan->catalog_, comment_on_plan->object_type_,
                                                   comment_on_plan->user_name, comment_on_plan->table_name,
                                                   comment_on_plan->column_name, comment_on_plan->comment);
        }
        case LogicalPlanType::Show: {
            std::shared_ptr<ShowPlan> show_plan = std::dynamic_pointer_cast<ShowPlan>(plan);
            if (show_plan->show_type != ShowType::SHOW_TYPE_ALL &&
                show_plan->show_type != ShowType::SHOW_TYPE_VARIABLES &&
                show_plan->show_type != ShowType::SHOW_TYPE_USERS) {
                auto select_exec = CreatePhysicalPlan(show_plan->GetLastPlan());
                return std::make_shared<ShowExec>(select_exec, catalog_, show_plan->show_type);
            } else {
                auto show_exe = std::make_shared<ShowExec>(nullptr, catalog_, show_plan->show_type);
                show_exe->db_path = show_plan->db_path;
                show_exe->variable_name_ = show_plan->variable_name;
                return show_exe;
            }
        }
        case LogicalPlanType::Set: {
            std::shared_ptr<SetPlan> set_plan = std::dynamic_pointer_cast<SetPlan>(plan);
            auto set_exe = std::make_shared<SetExec>(nullptr, catalog_, set_plan->set_kind_, set_plan->set_name_,
                                                     set_plan->set_value_);
            return set_exe;
        }
        case LogicalPlanType::CreateRole: {
            std::shared_ptr<CreateRolePlan> create_role_plan = std::dynamic_pointer_cast<CreateRolePlan>(plan);
            auto create_role_exe =
                std::make_shared<CreateRoleExec>(nullptr, catalog_, create_role_plan->role_stmt_type_,
                                                 create_role_plan->name_, create_role_plan->password_);
            return create_role_exe;
        }
        case LogicalPlanType::AlterRole: {
            std::shared_ptr<AlterRolePlan> alter_role_plan = std::dynamic_pointer_cast<AlterRolePlan>(plan);
            auto alter_role_exe =
                std::make_shared<AlterRoleExec>(nullptr, catalog_, alter_role_plan->alter_role_statement);
            return alter_role_exe;
        }
        case LogicalPlanType::DropRole: {
            std::shared_ptr<DropRolePlan> create_role_plan = std::dynamic_pointer_cast<DropRolePlan>(plan);
            auto drop_role_exe = std::make_shared<DropRoleExec>(nullptr, catalog_, create_role_plan->role_stmt_type_,
                                                                create_role_plan->roles_);
            return drop_role_exe;
        }
        case LogicalPlanType::GrantRole: {
            std::shared_ptr<GrantRolePlan> grant_role_plan = std::dynamic_pointer_cast<GrantRolePlan>(plan);
            auto grant_role_exe =
                std::make_shared<GrantRoleExec>(nullptr, catalog_, grant_role_plan->grant_role_statement);
            return grant_role_exe;
        }
        case LogicalPlanType::Grant: {
            std::shared_ptr<GrantPlan> grant_plan = std::dynamic_pointer_cast<GrantPlan>(plan);
            auto grant_exe = std::make_shared<GrantExec>(nullptr, catalog_, grant_plan->grant_statement);
            return grant_exe;
        }
        case LogicalPlanType::Synonym: {
            std::shared_ptr<SynonymPlan> synonym_plan = std::dynamic_pointer_cast<SynonymPlan>(plan);
            auto synonym_exe = std::make_shared<SynonymExec>(nullptr, catalog_, synonym_plan->synonym_statement);
            return synonym_exe;
        }
        case LogicalPlanType::Window: {
            const intarkdb::WindowPlan& window_plan = static_cast<const intarkdb::WindowPlan&>(*plan);
            auto child = CreatePhysicalPlan(window_plan.Children()[0]);
            std::vector<size_t> partition_idxs;
            std::vector<intarkdb::WindowSortItem> order_idxs;
            const auto& partition_exprs = window_plan.window_func_->GetOverClause().partition_by;
            const auto& order_exprs = window_plan.window_func_->GetOverClause().order_by;
            const auto& schema = child->GetSchema();
            for (const auto& expr : partition_exprs) {
                partition_idxs.push_back(schema.GetIdxByName(expr->GetName()));
            }
            for (const auto& sort_item : order_exprs) {
                const auto& expr = sort_item->sort_expr;
                intarkdb::WindowSortItem window_sort_item;
                window_sort_item.idx = schema.GetIdxByName(expr->GetName());
                window_sort_item.is_asc = sort_item->GetSortType() == SortType::ASC;
                window_sort_item.is_null_first = sort_item->IsNullFirst();
                order_idxs.push_back(window_sort_item);
            }
            return std::make_shared<intarkdb::WindowExec>(window_plan.func_idx_, partition_idxs, order_idxs, child);
        }
        default:
            break;
    }
    throw std::invalid_argument(fmt::format("not support this logical plan type={}", plan->Type()));
}

auto Planner::PlanTransaction(void* handle, TransactionStatement& statement) -> LogicalPlanPtr {
    LogicalPlanPtr plan = std::make_shared<TransactionPlan>(LogicalPlanType::Transaction, handle, statement.type);

    return plan;
}

auto Planner::PlanUpdate(UpdateStatement& statement) -> LogicalPlanPtr {
    auto table_ref_ptr = std::unique_ptr<BoundBaseTable>(static_cast<BoundBaseTable*>(statement.table.release()));

    std::shared_ptr<ScanPlan> scan_plan = std::make_shared<ScanPlan>(
        catalog_.CreateTableDataSource(std::move(table_ref_ptr), GSTOR_CURSOR_ACTION_UPDATE, table_idx_++),
        std::vector<std::string>{});

    LogicalPlanPtr plan = scan_plan;

    if (statement.condition) {
        PlanSubqueries(&statement.condition);
        plan = std::make_shared<FilterPlan>(std::move(statement.condition), plan);
    }

    std::vector<std::pair<Column, std::unique_ptr<Expression>>> target_exprs;
    for (auto& [col, target_expr] : statement.update_items) {
        PlanSubqueries(&target_expr);
        auto target_expr_physi = CreatePhysicalExpression(*target_expr, plan);
        target_exprs.emplace_back(std::make_pair(std::move(col), std::move(target_expr_physi)));
    }
    return std::make_shared<UpdatePlan>(LogicalPlanType::Update, *scan_plan->source, std::move(plan),
                                        std::move(target_exprs));
}

auto Planner::PlanDrop(DropStatement& statement) -> LogicalPlanPtr {
    LogicalPlanPtr plan =
        std::make_shared<DropPlan>(LogicalPlanType::Drop, statement.name, statement.if_exists, statement.type);

    return plan;
}

auto Planner::PlanCommentOn(Catalog* catalog, CommentStatement& statement) -> LogicalPlanPtr {
    LogicalPlanPtr plan =
        std::make_shared<CommentOnPlan>(catalog, statement.object_type_, statement.user_name, statement.table_name,
                                        statement.column_name, statement.comment);

    return plan;
}

auto Planner::PlanShow(ShowStatement& statement) -> LogicalPlanPtr {
    if (statement.show_type == ShowType::SHOW_TYPE_ALL || statement.show_type == ShowType::SHOW_TYPE_VARIABLES ||
        statement.show_type == ShowType::SHOW_TYPE_USERS) {
        LogicalPlanPtr plan = std::make_shared<ShowPlan>(LogicalPlanType::Show, statement.show_type, nullptr);
        auto show_plan = static_cast<ShowPlan*>(plan.get());
        show_plan->db_path = statement.db_path;
        show_plan->variable_name = statement.variable_name;
        return plan;
    } else {
        SelectStatement& select_column_stmt = *(statement.stmt);
        auto select_plan = PlanSelect(select_column_stmt);
        LogicalPlanPtr plan = std::make_shared<ShowPlan>(LogicalPlanType::Show, statement.show_type, select_plan);
        return plan;
    }
}

auto Planner::PlanSet(SetStatement& statement) -> LogicalPlanPtr {
    LogicalPlanPtr plan = std::make_shared<SetPlan>(LogicalPlanType::Set, statement.set_kind, statement.set_name,
                                                    statement.set_value, nullptr);
    return plan;
}

auto Planner::PlanCreateRole(CreateRoleStatement& statement) -> LogicalPlanPtr {
    LogicalPlanPtr plan = std::make_shared<CreateRolePlan>(LogicalPlanType::CreateRole, statement.role_stmt_type,
                                                           statement.name, statement.password, nullptr);
    return plan;
}

auto Planner::PlanAlterRole(AlterRoleStatement& statement) -> LogicalPlanPtr {
    LogicalPlanPtr plan = std::make_shared<AlterRolePlan>(LogicalPlanType::AlterRole, statement, nullptr);
    return plan;
}

auto Planner::PlanDropRole(DropRoleStatement& statement) -> LogicalPlanPtr {
    LogicalPlanPtr plan = std::make_shared<DropRolePlan>(LogicalPlanType::DropRole, statement.role_stmt_type,
                                                         std::move(statement.roles), nullptr);
    return plan;
}

auto Planner::PlanGrantRole(GrantRoleStatement& statement) -> LogicalPlanPtr {
    LogicalPlanPtr plan = std::make_shared<GrantRolePlan>(LogicalPlanType::GrantRole, statement, nullptr);
    return plan;
}

auto Planner::PlanGrant(GrantStatement& statement) -> LogicalPlanPtr {
    LogicalPlanPtr plan = std::make_shared<GrantPlan>(LogicalPlanType::Grant, statement, nullptr);
    return plan;
}

auto Planner::PlanSynonym(SynonymStatement& statement) -> LogicalPlanPtr {
    LogicalPlanPtr plan = std::make_shared<SynonymPlan>(LogicalPlanType::Synonym, statement, nullptr);
    return plan;
}
