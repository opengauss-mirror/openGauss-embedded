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
* join_plan.cpp
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/planner/logical_plan/join_plan.cpp
*
* -------------------------------------------------------------------------
*/
#include "binder/expressions/bound_column_def.h"
#include "common/expression_util.h"
#include "planner/logical_plan/nested_loop_join_plan.h"

auto NestedLoopJoinPlan::ExtracOnCondition(std::vector<std::unique_ptr<BoundExpression>>& conditions,
                                           std::vector<std::unique_ptr<BoundExpression>>& equal_conditions,
                                           std::vector<std::unique_ptr<BoundExpression>>& left_coditions,
                                           std::vector<std::unique_ptr<BoundExpression>>& right_coditions,
                                           std::vector<std::unique_ptr<BoundExpression>>& other_coditions) -> void {
    for (auto& condition : conditions) {
        // TODO: 处理 condition 中包含 in 的情况
        if (ExpressionUtil::IsEqualExpr(*condition)) {
            auto& binary_expr = static_cast<BoundBinaryOp&>(*condition);
            auto& left_op = binary_expr.Left();
            auto& right_op = binary_expr.Right();
            if (left_op.Type() == ExpressionType::COLUMN_REF && right_op.Type() == ExpressionType::COLUMN_REF) {
                auto& left_column = static_cast<BoundColumnRef&>(left_op);
                auto& right_column = static_cast<BoundColumnRef&>(right_op);
                auto left_idx = left_->GetSchema().GetIdxByNameWithoutException(left_column.Name());
                auto right_idx = right_->GetSchema().GetIdxByNameWithoutException(right_column.Name());
                if (left_idx == INVALID_COLUMN_INDEX && right_idx == INVALID_COLUMN_INDEX) {
                    left_idx = left_->GetSchema().GetIdxByNameWithoutException(right_column.Name());
                    right_idx = right_->GetSchema().GetIdxByNameWithoutException(left_column.Name());
                    std::swap(left_idx, right_idx);
                }
                if (left_idx != INVALID_COLUMN_INDEX && right_idx != INVALID_COLUMN_INDEX) {
                    equal_conditions.emplace_back(reinterpret_cast<BoundBinaryOp*>(condition.release()));
                }
                continue;
            }
        }
        const auto& columns = ExpressionUtil::GetRefColumns(*condition);  // 获取相关的列信息
        if (columns.empty()) {
            // TODO 可能是  外部查询列 op 常量, 先不处理
            other_coditions.emplace_back(std::move(condition));
            continue;
        }

        bool all_left = true;
        bool all_right = true;
        for (const auto& col : columns) {
            if (left_->GetSchema().GetIdxByNameWithoutException(col.col_name) == INVALID_COLUMN_INDEX) {
                all_left = false;
            }
            if (right_->GetSchema().GetIdxByNameWithoutException(col.col_name) == INVALID_COLUMN_INDEX) {
                all_right = false;
            }
        }
        if (all_left) {
            left_coditions.emplace_back(std::move(condition));
        } else if (all_right) {
            right_coditions.emplace_back(std::move(condition));
        } else {
            // TODO: 可以进一步处理
            other_coditions.emplace_back(std::move(condition));
        }
    }
}
