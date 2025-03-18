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
 * nested_loop_join_plan.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/planner/logical_plan/nested_loop_join_plan.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include "binder/bound_expression.h"
#include "common/join_type.h"
#include "planner/logical_plan/logical_plan.h"

class NestedLoopJoinPlan : public LogicalPlan {
   public:
    NestedLoopJoinPlan(LogicalPlanPtr left, LogicalPlanPtr right, std::unique_ptr<BoundExpression> pred,
                       JoinType join_type)
        : LogicalPlan(LogicalPlanType::NestedLoopJoin),
          left_(left),
          right_(right),
          pred_(std::move(pred)),
          join_type_(join_type) {
        std::vector<SchemaColumnInfo> columns;
        const auto& left_schema = left_->GetSchema();
        const auto& right_schema = right_->GetSchema();
        const auto& left_columns = left_schema.GetColumnInfos();
        const auto& right_columns = right_schema.GetColumnInfos();
        columns.reserve(left_columns.size() + right_columns.size());
        int slot = 0;
        for (auto& col : left_columns) {
            columns.push_back(col);
            columns.back().slot = slot++;
        }
        for (auto& col : right_columns) {
            columns.push_back(col);
            columns.back().slot = slot++;
        }
        schema_ = Schema(std::move(columns));
    }

    virtual const Schema& GetSchema() const override { return schema_; }

    virtual std::vector<LogicalPlanPtr> Children() const override { return {left_, right_}; }
    virtual void SetChildren(const std::vector<LogicalPlanPtr>& children) override {
        left_ = children[0];
        right_ = children[1];
    }

    LogicalPlanPtr LeftPtr() const { return left_; }
    LogicalPlanPtr RightPtr() const { return right_; }

    virtual std::string ToString() const override {
        if (pred_ == nullptr) {
            return fmt::format("NestedLoopJoinPlanNode {} pred=null", join_type_);
        } else {
            return fmt::format("NestedLoopJoinPlanNode {} pred={}", join_type_, pred_);
        }
    }

   private:
    auto ExtracOnCondition(std::vector<std::unique_ptr<BoundExpression>>& conditions,
                           std::vector<std::unique_ptr<BoundExpression>>& equal_conditions,
                           std::vector<std::unique_ptr<BoundExpression>>& left_coditions,
                           std::vector<std::unique_ptr<BoundExpression>>& right_coditions,
                           std::vector<std::unique_ptr<BoundExpression>>& other_coditions) -> void;

   private:
    LogicalPlanPtr left_;
    LogicalPlanPtr right_;
    Schema schema_;

   public:
    std::unique_ptr<BoundExpression> pred_;
    JoinType join_type_;
};
