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
 * aggregate_plan.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/planner/logical_plan/aggregate_plan.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <algorithm>
#include <memory>

#include "binder/bound_expression.h"
#include "binder/expressions/bound_agg_call.h"
#include "planner/logical_plan/logical_plan.h"

class AggregatePlan : public LogicalPlan {
   public:
    AggregatePlan(std::vector<std::unique_ptr<BoundExpression>> group_by,
                  std::vector<std::unique_ptr<BoundExpression>> aggregates, const std::vector<bool>& distincts,
                  LogicalPlanPtr child)
        : LogicalPlan(LogicalPlanType::Aggregation),
          group_by_(std::move(group_by)),
          aggregates_(std::move(aggregates)),
          distincts(distincts),
          child_(child) {
        // 中间层，包括了聚合列 和 group by 列
        std::vector<SchemaColumnInfo> cols;
        cols.reserve(group_by_.size() + aggregates_.size());
        int slot = 0;
        for (auto& col : group_by_) {
            cols.push_back(BoundExpressionToSchemaColumnInfo(child_,*col, slot++));
        }
        for (auto& agg : aggregates_) {
            cols.push_back(BoundExpressionToSchemaColumnInfo(child_,*agg, slot++));
        }
        schema_ = Schema(std::move(cols));
    }

    virtual const Schema& GetSchema() const override { return schema_; }

    virtual std::vector<LogicalPlanPtr> Children() const override { return {child_}; }

    virtual void SetChildren(const std::vector<LogicalPlanPtr>& children) override { child_ = children[0]; }

    virtual std::string ToString() const override {
        std::vector<std::string> to_strings;
        std::transform(group_by_.cbegin(), group_by_.cend(), std::back_inserter(to_strings),
                       [](const std::unique_ptr<BoundExpression>& expr) { return expr->ToString(); });

        std::transform(aggregates_.cbegin(), aggregates_.cend(), std::back_inserter(to_strings),
                       [](const std::unique_ptr<BoundExpression>& expr) { return expr->ToString(); });
        return fmt::format("Aggregation: {}", fmt::join(to_strings, ", "));
    }

    LogicalPlanPtr GetLastPlan() const { return child_; }

    bool IsOnlyCountStar() const {
        bool result = false;
        if (group_by_.size() == 0 && aggregates_.size() == 1 && aggregates_[0]->Type() == ExpressionType::AGG_CALL) {
            const auto& agg_call = static_cast<BoundAggCall&>(*aggregates_[0]);
            result = agg_call.func_name_ == "count_star";
        }
        return result;
    }

    std::vector<std::unique_ptr<BoundExpression>> group_by_;
    std::vector<std::unique_ptr<BoundExpression>> aggregates_;
    std::vector<bool> distincts;
    LogicalPlanPtr child_;
    Schema schema_;
};
