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
* limit_plan.h
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/include/planner/logical_plan/limit_plan.h
*
* -------------------------------------------------------------------------
*/
#pragma once

#include <optional>

#include "binder/bound_expression.h"
#include "planner/logical_plan/logical_plan.h"

class LimitPlan : public LogicalPlan {
   public:
    LimitPlan(std::unique_ptr<BoundExpression> limit, std::unique_ptr<BoundExpression> offset, LogicalPlanPtr plan)
        : LogicalPlan(LogicalPlanType::Limit), limit(std::move(limit)), offset(std::move(offset)), child_(plan) {}
    virtual const Schema& GetSchema() const override { return child_->GetSchema(); }
    virtual std::vector<LogicalPlanPtr> Children() const override { return {child_}; }
    virtual void SetChildren(const std::vector<LogicalPlanPtr>& children) override { child_ = children[0]; }
    virtual std::string ToString() const override { return "LimitPlan"; }
    LogicalPlanPtr GetLastPlan() const { return child_; }

    std::unique_ptr<BoundExpression> limit;
    std::unique_ptr<BoundExpression> offset;

   private:
    LogicalPlanPtr child_;
};
