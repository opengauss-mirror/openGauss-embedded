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
 * create_join_exec.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/planner/create_join_exec.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "binder/expressions/bound_conjunctive.h"
#include "common/expression_util.h"
#include "planner/logical_plan/nested_loop_join_plan.h"
#include "planner/physical_plan/filter_exec.h"
#include "planner/physical_plan/join/hash_join_exec.h"
#include "planner/physical_plan/join/nested_inner_join_exec.h"
#include "planner/physical_plan/join/nested_loop_join_exec.h"
#include "planner/physical_plan/union_exec.h"
#include "planner/planner.h"

static auto ExtractEqualConditionColumnIdx(const Schema& left_schema, const Schema& right_schema,
                                           std::unique_ptr<BoundExpression> expr, std::vector<size_t>& left_idxs,
                                           std::vector<size_t>& right_idxs, std::vector<LogicalType>& types)
    -> std::unique_ptr<BoundExpression> {
    if (ExpressionUtil::IsEqualExpr(*expr)) {
        auto& binary_op = static_cast<BoundBinaryOp&>(*expr);
        if (binary_op.Left().Type() == ExpressionType::COLUMN_REF &&
            binary_op.Right().Type() == ExpressionType::COLUMN_REF) {
            const auto& left_column_ref = static_cast<const BoundColumnRef&>(binary_op.Left());
            const auto& right_column_ref = static_cast<const BoundColumnRef&>(binary_op.Right());
            auto left_idx = left_schema.GetIdxByNameWithoutException(left_column_ref.GetName());
            auto right_idx = right_schema.GetIdxByNameWithoutException(right_column_ref.GetName());
            if (left_idx == INVALID_COLUMN_INDEX && right_idx == INVALID_COLUMN_INDEX) {
                left_idx = right_schema.GetIdxByNameWithoutException(left_column_ref.GetName());
                right_idx = left_schema.GetIdxByNameWithoutException(right_column_ref.GetName());
                std::swap(left_idx, right_idx);
            }
            if (left_idx != INVALID_COLUMN_INDEX && right_idx != INVALID_COLUMN_INDEX) {
                left_idxs.push_back(left_idx);
                right_idxs.push_back(right_idx);
                types.push_back(intarkdb::GetCompatibleType(left_schema.GetColumnInfos()[left_idx].col_type,
                                                            right_schema.GetColumnInfos()[right_idx].col_type));
                return nullptr;
            }
        }
        return expr;
    } else if (expr->Type() == ExpressionType::CONJUNCTIVE) {
        std::vector<std::unique_ptr<BoundExpression>> new_conditions;
        auto& conjunctive = static_cast<BoundConjunctive&>(*expr);
        for (auto& item : conjunctive.items) {
            auto tmp_exp = ExtractEqualConditionColumnIdx(left_schema, right_schema, std::move(item), left_idxs,
                                                          right_idxs, types);
            if (tmp_exp) {
                new_conditions.push_back(std::move(tmp_exp));
            }
        }
        if (new_conditions.empty()) {
            return nullptr;
        }
        return std::make_unique<BoundConjunctive>(std::move(new_conditions));
    }
    return expr;
}

auto Planner::CreateJoinExec(std::shared_ptr<NestedLoopJoinPlan>& plan) -> PhysicalPlanPtr {
    std::shared_ptr<NestedLoopJoinPlan> join_ref = std::dynamic_pointer_cast<NestedLoopJoinPlan>(plan);
    auto children = join_ref->Children();

    std::unique_ptr<Expression> pred = nullptr;
    if (join_ref->join_type_ == JoinType::CrossJoin || join_ref->join_type_ == JoinType::LeftJoin ||
        join_ref->join_type_ == JoinType::RightJoin) {
        std::vector<size_t> left_idxs;
        std::vector<size_t> right_idxs;
        std::vector<LogicalType> idxs_types;
        if (join_ref->pred_) {
            auto new_pred =
                ExtractEqualConditionColumnIdx(children[0]->GetSchema(), children[1]->GetSchema(),
                                               std::move(join_ref->pred_), left_idxs, right_idxs, idxs_types);
            if (new_pred) {
                pred = CreatePhysicalExpression(*new_pred, join_ref);
            }
        }
        auto left = CreatePhysicalPlan(children[0]);
        auto right = CreatePhysicalPlan(children[1]);
        if (left_idxs.size() > 0 && right_idxs.size() > 0) {  // with JOIN ON condition
            // use hash join
            // FIXME: JOIN ON 与 WHERE其实并不等价
            return std::make_shared<HashJoinExec>(join_ref->GetSchema(), join_ref->join_type_, left, right,
                                                  std::move(right_idxs), std::move(left_idxs), std::move(idxs_types),
                                                  std::move(pred));
        }
        // without JOIN ON 中没有等值条件
        auto join_exec =
            std::make_shared<InnerJoinExec>(join_ref->GetSchema(), join_ref->join_type_, left, right, std::move(pred));
        return join_exec;
    } else if (join_ref->join_type_ == JoinType::FullJoin) {
        std::unique_ptr<Expression> pred_copy = nullptr;
        std::vector<size_t> left_idxs;
        std::vector<size_t> right_idxs;
        std::vector<LogicalType> idxs_types;
        if (join_ref->pred_) {
            auto new_pred =
                ExtractEqualConditionColumnIdx(children[0]->GetSchema(), children[1]->GetSchema(),
                                               std::move(join_ref->pred_), left_idxs, right_idxs, idxs_types);
            if (new_pred) {
                pred = CreatePhysicalExpression(*new_pred->Copy(), join_ref);
                pred_copy = CreatePhysicalExpression(*new_pred, join_ref);
            }
        }
        auto left = CreatePhysicalPlan(children[0]);
        auto right = CreatePhysicalPlan(children[1]);
        if (left_idxs.size() > 0 && right_idxs.size() > 0) {  // with JOIN ON condition
            auto copy_right_idxs = right_idxs;
            auto copy_left_idxs = left_idxs;
            auto copy_idxs_types = idxs_types;
            auto left_join_exec = std::make_shared<HashJoinExec>(join_ref->GetSchema(), JoinType::LeftJoin, left, right,
                                                                 std::move(copy_right_idxs), std::move(copy_left_idxs),
                                                                 std::move(copy_idxs_types), std::move(pred_copy));
            auto right_join_exec = std::make_shared<HashJoinExec>(join_ref->GetSchema(), JoinType::RightJoin, left,
                                                                  right, std::move(right_idxs), std::move(left_idxs),
                                                                  std::move(idxs_types), std::move(pred));
            return std::make_shared<UnionJoinExec>(join_ref->GetSchema(), left_join_exec, right_join_exec);
        }
        auto left_join_exec =
            std::make_shared<InnerJoinExec>(join_ref->GetSchema(), JoinType::LeftJoin, left, right, std::move(pred));
        auto right_join_exec = std::make_shared<InnerJoinExec>(join_ref->GetSchema(), JoinType::RightJoin, left, right,
                                                               std::move(pred_copy));
        return std::make_shared<UnionJoinExec>(join_ref->GetSchema(), left_join_exec, right_join_exec);
    }
    throw intarkdb::Exception(ExceptionType::FATAL, "Unsupported join type");
}
