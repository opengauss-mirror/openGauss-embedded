//===----------------------------------------------------------------------===//
// Copyright 2018-2023 Stichting DuckDB Foundation
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice (including the next paragraph)
// shall be included in all copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.
//===----------------------------------------------------------------------===//
#include "planner/optimizer/filter_pushdown.h"

#include "binder/expressions/bound_column_def.h"
#include "binder/expressions/bound_conjunctive.h"
#include "binder/expressions/bound_in_expr.h"
#include "catalog/table_info.h"
#include "planner/logical_plan/empty_source_plan.h"
#include "planner/logical_plan/filter_plan.h"
#include "planner/logical_plan/scan_plan.h"
#include "storage/gstor/zekernel/common/cm_log.h"

namespace intarkdb {

FilterPushdown::FilterPushdown(Optimizer &optimizer) : optimizer(optimizer) {}

// 获取
LogicalPlanPtr FilterPushdown::Rewrite(LogicalPlanPtr &op) {
    GS_LOG_RUN_INF("op type = %d \n", static_cast<int>(op->Type()));
    switch (op->Type()) {
        case LogicalPlanType::Filter:
            return PushdownFilter(op);
        case LogicalPlanType::Scan:
            return PushdownGet(op);
        case LogicalPlanType::NestedLoopJoin:
            return PushdownJoin(op);
        default:
            return FinishPushdown(op);
    }
}

LogicalPlanPtr FilterPushdown::FinishPushdown(LogicalPlanPtr &op) {
    std::vector<LogicalPlanPtr> opChildren = op->Children();
    // 无法处理的类型，处理其子节点
    std::vector<LogicalPlanPtr> new_children;
    for (auto &child : opChildren) {
        // 改写子树
        FilterPushdown filter_pushdown(optimizer);
        new_children.push_back(filter_pushdown.Rewrite(child));
    }
    if (new_children.size() > 0) {
        op->SetChildren(new_children);  // 重新设置优化过的子节点
    }
    return PushFinalFilter(op);
}

LogicalPlanPtr FilterPushdown::PushFinalFilter(LogicalPlanPtr &op) {
    if (predicates.empty()) {
        // no filters to push
        return op;
    }
    // predicates 不为空，filter未完全下推 重新构造一个filter plan
    auto filter = std::make_unique<BoundConjunctive>(std::move(predicates));

    auto filter_plan = std::make_shared<FilterPlan>(std::move(filter), op);
    return filter_plan;
}

auto FilterPushdown::FillCombiner() -> void {
    for (auto &pred : predicates) {
        combiner.AddFilter(std::move(pred));
    }
    predicates.clear();
}

auto FilterPushdown::GenerateFromCombiner() -> void {
    if (predicates.empty()) {
        combiner.GenerateFilters(
            [&](std::unique_ptr<BoundExpression> filter) { predicates.push_back(std::move(filter)); });
    }
}

//! Push down a LogicalGet op
LogicalPlanPtr FilterPushdown::PushdownFilter(LogicalPlanPtr &op) {
    GS_LOG_RUN_INF("op content = %s \n", op->ToString().c_str());

    FilterPlan &filterPlan = static_cast<FilterPlan &>(*op);

    FillCombiner();

    if (AddFilterToCombiner(std::move(filterPlan.expr)) == FilterResult::UNSTATISFIED) {
        return std::make_shared<EmptySourcePlan>(op->GetSchema());  // 返回空数据源节点
    }

    GenerateFromCombiner();  // 从 combiner -> filters
    return Rewrite(op->Children()[0]);
}

static auto IsPushdownAble(BoundBinaryOp &binary_expr) -> bool {
    auto &left = binary_expr.LeftPtr();
    auto &right = binary_expr.RightPtr();
    if (left->Type() == ExpressionType::COLUMN_REF &&
        (right->Type() == ExpressionType::LITERAL || right->Type() == ExpressionType::BOUND_PARAM)) {
        return true;
    }
    if (right->Type() == ExpressionType::COLUMN_REF &&
        (left->Type() == ExpressionType::LITERAL || left->Type() == ExpressionType::BOUND_PARAM)) {
        return true;
    }
    return false;
}

static auto PushdownJoinColumn(std::unique_ptr<BoundExpression> predicate, const Schema &left_schema,
                               const Schema &right_schema,
                               std::vector<std::unique_ptr<BoundExpression>> &left_predicates,
                               std::vector<std::unique_ptr<BoundExpression>> &right_predicates,
                               std::vector<std::unique_ptr<BoundExpression>> &can_not_push_down_predicates) -> void {
    if (predicate->Type() != ExpressionType::BINARY_OP) {
        can_not_push_down_predicates.push_back(std::move(predicate));
        return;
    }
    auto &binary_expr = static_cast<BoundBinaryOp &>(*predicate);
    if (!binary_expr.IsCompareOp()) {
        can_not_push_down_predicates.push_back(std::move(predicate));
        return;
    }
    auto &left = binary_expr.LeftPtr();
    auto &right = binary_expr.RightPtr();
    if (left->Type() == ExpressionType::COLUMN_REF) {
        if (right->Type() == ExpressionType::COLUMN_REF) {
            auto &left_col = static_cast<BoundColumnRef &>(*left);
            auto &right_col = static_cast<BoundColumnRef &>(*right);
            auto left_col_idx_from_left = left_schema.GetIdxByNameWithoutException(left_col.Name());
            auto right_col_idx_from_left = left_schema.GetIdxByNameWithoutException(right_col.Name());
            if (left_col_idx_from_left != INVALID_COLUMN_INDEX && right_col_idx_from_left != INVALID_COLUMN_INDEX) {
                left_predicates.push_back(std::move(predicate));
            } else {
                auto left_col_idx_from_right = right_schema.GetIdxByNameWithoutException(left_col.Name());
                auto right_col_idx_from_right = right_schema.GetIdxByNameWithoutException(right_col.Name());
                if (left_col_idx_from_right != INVALID_COLUMN_INDEX &&
                    right_col_idx_from_right != INVALID_COLUMN_INDEX) {
                    right_predicates.push_back(std::move(predicate));
                } else {
                    can_not_push_down_predicates.push_back(std::move(predicate));
                }
            }
        } else if (right->Type() == ExpressionType::LITERAL || right->Type() == ExpressionType::BOUND_PARAM) {
            auto &left_col = static_cast<BoundColumnRef &>(*left);
            auto left_idx = left_schema.GetIdxByNameWithoutException(left_col.Name());
            auto right_idx = right_schema.GetIdxByNameWithoutException(left_col.Name());
            if (left_idx != INVALID_COLUMN_INDEX && right_idx != INVALID_COLUMN_INDEX) {
                throw Exception(ExceptionType::OPTIMIZER, "dupliacte Column found in both join schema");
            }
            if (left_idx != INVALID_COLUMN_INDEX) {
                left_predicates.push_back(std::move(predicate));
            } else if (right_idx != INVALID_COLUMN_INDEX) {
                right_predicates.push_back(std::move(predicate));
            } else {
                throw Exception(ExceptionType::OPTIMIZER, "Column not found in join schema");
            }
        } else {
            can_not_push_down_predicates.push_back(std::move(predicate));
        }
    } else if (right->Type() == ExpressionType::COLUMN_REF) {
        if (left->Type() == ExpressionType::LITERAL || left->Type() == ExpressionType::BOUND_PARAM) {
            auto &right_col = static_cast<BoundColumnRef &>(*right);
            auto left_idx = left_schema.GetIdxByNameWithoutException(right_col.Name());
            auto right_idx = right_schema.GetIdxByNameWithoutException(right_col.Name());
            if (left_idx != INVALID_COLUMN_INDEX && right_idx != INVALID_COLUMN_INDEX) {
                throw Exception(ExceptionType::OPTIMIZER, "dupliacte Column found in both join schema");
            }
            if (left_idx != INVALID_COLUMN_INDEX) {
                left_predicates.push_back(std::move(predicate));
            } else if (right_idx != INVALID_COLUMN_INDEX) {
                right_predicates.push_back(std::move(predicate));
            } else {
                throw Exception(ExceptionType::OPTIMIZER, "Column not found in join schema");
            }
        } else {
            can_not_push_down_predicates.push_back(std::move(predicate));
        }
    } else {
        can_not_push_down_predicates.push_back(std::move(predicate));
    }
}

auto FilterPushdown::ExtractJoinPredicates(NestedLoopJoinPlan &plan,
                                           std::vector<std::unique_ptr<BoundExpression>> &left_predicates,
                                           std::vector<std::unique_ptr<BoundExpression>> &right_predicates) -> void {
    const auto &left_schema = plan.LeftPtr()->GetSchema();
    const auto &right_schema = plan.RightPtr()->GetSchema();
    std::vector<std::unique_ptr<BoundExpression>> can_not_push_down_predicates;
    for (auto &predicate : predicates) {
        if (predicate->Type() != ExpressionType::BINARY_OP) {
            can_not_push_down_predicates.push_back(std::move(predicate));
            continue;
        }
        BoundBinaryOp &binary_expr = static_cast<BoundBinaryOp &>(*predicate);
        if (!binary_expr.IsCompareOp()) {
            can_not_push_down_predicates.push_back(std::move(predicate));
            continue;
        }

        PushdownJoinColumn(std::move(predicate), left_schema, right_schema, left_predicates, right_predicates,
                           can_not_push_down_predicates);
    }
    predicates = std::move(can_not_push_down_predicates);
}

LogicalPlanPtr FilterPushdown::PushdownJoin(LogicalPlanPtr &op) {
    NestedLoopJoinPlan &join_plan = static_cast<NestedLoopJoinPlan &>(*op);

    if (join_plan.join_type_ != JoinType::CrossJoin) {  // 先只支持CrossJoin的下推
        return FinishPushdown(op);
    }

    FillCombiner();
    if (join_plan.pred_ != nullptr) {
        AddFilterToCombiner(std::move(join_plan.pred_));
    }
    GenerateFromCombiner();

    FilterPushdown left_filter_pushdown(optimizer);
    FilterPushdown right_filter_pushdown(optimizer);

    std::vector<std::unique_ptr<BoundExpression>> left_predicates;
    std::vector<std::unique_ptr<BoundExpression>> right_predicates;

    ExtractJoinPredicates(join_plan, left_predicates, right_predicates);

    left_filter_pushdown.predicates = std::move(left_predicates);
    right_filter_pushdown.predicates = std::move(right_predicates);

    std::vector<LogicalPlanPtr> new_children;
    join_plan.pred_ = std::make_unique<BoundConjunctive>(std::move(predicates));

    std::vector<LogicalPlanPtr> children;
    auto left_child = join_plan.LeftPtr();
    auto right_child = join_plan.RightPtr();
    children.push_back(left_filter_pushdown.Rewrite(left_child));
    children.push_back(right_filter_pushdown.Rewrite(right_child));
    op->SetChildren(children);
    return op;
}

//! Push down a LogicalGet op
LogicalPlanPtr FilterPushdown::PushdownGet(LogicalPlanPtr &op) {
    ScanPlan &scan_plan = static_cast<ScanPlan &>(*op);

    std::vector<std::unique_ptr<BoundExpression>> push_down_predicates;
    std::vector<std::unique_ptr<BoundExpression>> can_not_push_down_predicates;
    for (size_t i = 0; i < predicates.size(); ++i) {
        auto &pred = predicates[i];
        if (pred->Type() == ExpressionType::BINARY_OP) {
            BoundBinaryOp &binary_expr = static_cast<BoundBinaryOp &>(*pred);
            if (!binary_expr.IsCompareOp()) {  // 不下推非比较操作符
                can_not_push_down_predicates.push_back(std::move(pred));
            } else {
                // 支持 column op literal 或者 literal op column
                if (IsPushdownAble(binary_expr)) {
                    push_down_predicates.push_back(std::move(pred));
                } else {
                    can_not_push_down_predicates.push_back(std::move(pred));
                }
            }
        } else {
            can_not_push_down_predicates.push_back(std::move(pred));
        }
    }

    predicates.clear();

    if (AddFiltersToCombiner(std::move(push_down_predicates)) == FilterResult::UNSTATISFIED) {
        return std::make_shared<EmptySourcePlan>(op->GetSchema());
    }

    for (auto &pred : can_not_push_down_predicates) {
        if (pred->Type() == ExpressionType::IN_EXPR) {
            BoundInExpr &in_expr = static_cast<BoundInExpr &>(*pred);
            if (in_expr.is_not_in) {  // 不支持 not in
                continue;
            }
            if (in_expr.in_ref_expr->Type() == ExpressionType::COLUMN_REF) {
                BoundColumnRef &col_ref = static_cast<BoundColumnRef &>(*in_expr.in_ref_expr);
                const auto &exprs = in_expr.in_list;
                std::vector<Value> values;
                bool all_literal = true;
                bool has_null = false;
                for (auto &expr : exprs) {
                    if (expr->Type() == ExpressionType::LITERAL) {
                        BoundConstant &literal = static_cast<BoundConstant &>(*expr);
                        if (literal.ValRef().IsNull()) {
                            has_null = true;
                            break;
                        }
                        values.push_back(literal.ValRef());
                    } else {
                        all_literal = false;
                        break;
                    }
                }
                if (!all_literal || has_null) {
                    break;
                }
                if (values.size() == 1) {
                    std::unique_ptr<BoundConstant> equal_bound = std::make_unique<BoundConstant>(values.front());
                    if (combiner.AddFilter(std::make_unique<BoundBinaryOp>(
                            "=", col_ref.Copy(), std::move(equal_bound))) == FilterResult::UNSTATISFIED) {
                        return std::make_shared<EmptySourcePlan>(op->GetSchema());
                    }
                } else {
                    sort(values.begin(), values.end(),
                         [](const Value &a, const Value &b) { return a.LessThan(b) == Trivalent::TRI_TRUE; });
                    std::unique_ptr<BoundConstant> lower_bound = std::make_unique<BoundConstant>(values.front());
                    std::unique_ptr<BoundConstant> upper_bound = std::make_unique<BoundConstant>(values.back());
                    if (combiner.AddFilter(std::make_unique<BoundBinaryOp>(
                            ">=", col_ref.Copy(), std::move(lower_bound))) == FilterResult::UNSTATISFIED) {
                        return std::make_shared<EmptySourcePlan>(op->GetSchema());
                    }
                    if (combiner.AddFilter(std::make_unique<BoundBinaryOp>(
                            "<=", col_ref.Copy(), std::move(upper_bound))) == FilterResult::UNSTATISFIED) {
                        return std::make_shared<EmptySourcePlan>(op->GetSchema());
                    }
                }
            }
        }
    }

    GenerateFromCombiner();
    scan_plan.bound_expressions = std::move(predicates);
    predicates = std::move(can_not_push_down_predicates);
    return FinishPushdown(op);
}

FilterResult FilterPushdown::AddFiltersToCombiner(std::vector<std::unique_ptr<BoundExpression>> &&predicates) {
    for (auto &pred : predicates) {
        auto result = combiner.AddFilter(std::move(pred));
        if (result != FilterResult::SUCCESS) {
            return result;
        }
    }
    return FilterResult::SUCCESS;
}

FilterResult FilterPushdown::AddFilterToCombiner(std::unique_ptr<BoundExpression> expr) {
    std::vector<std::unique_ptr<BoundExpression>> exprs;

    if (expr->Type() == ExpressionType::CONJUNCTIVE) {
        BoundConjunctive &conjunctive = static_cast<BoundConjunctive &>(*expr);
        exprs = std::move(conjunctive.items);
    } else {
        exprs.push_back(std::move(expr));
    }

    // 把分解后的表达式添加到 combiner 中
    for (auto &child_expr : exprs) {
        if (combiner.AddFilter(std::move(child_expr)) == FilterResult::UNSTATISFIED) {
            return FilterResult::UNSTATISFIED;
        }
    }
    return FilterResult::SUCCESS;
}

}  // namespace intarkdb
