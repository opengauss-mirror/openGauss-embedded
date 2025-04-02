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
* apply_plan.h
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/include/planner/logical_plan/apply_plan.h
*
* -------------------------------------------------------------------------
*/
#pragma once

#include "binder/bound_expression.h"
#include "common/join_type.h"
#include "planner/logical_plan/logical_plan.h"

class ApplyPlan : public LogicalPlan {
   public:
    explicit ApplyPlan(const Schema& schema, LogicalPlanPtr left, LogicalPlanPtr right,
                       std::unique_ptr<BoundExpression> pred, JoinType join_type)
        : LogicalPlan(LogicalPlanType::Apply),
          schema_(schema),
          left_(std::move(left)),
          right_(std::move(right)),
          pred_(std::move(pred)),
          join_type_(join_type) {}

    virtual const Schema& GetSchema() const override { return schema_; }

    virtual std::vector<LogicalPlanPtr> Children() const override { return {left_, right_}; }
    virtual void SetChildren(const std::vector<LogicalPlanPtr>& children) override {
        left_ = children[0];
        right_ = children[1];
    }

    virtual std::string ToString() const override { return "ApplyPlan"; }

    std::unique_ptr<BoundExpression>& Pred() { return pred_; }

    JoinType GetJoinType() { return join_type_; }

   private:
    Schema schema_;
    LogicalPlanPtr left_;
    LogicalPlanPtr right_;
    std::unique_ptr<BoundExpression> pred_{nullptr};
    JoinType join_type_;
};
