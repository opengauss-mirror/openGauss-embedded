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
* distinct_plan.h
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/include/planner/logical_plan/distinct_plan.h
*
* -------------------------------------------------------------------------
*/
#pragma once

#include <memory>

#include "binder/bound_expression.h"
#include "planner/logical_plan/logical_plan.h"

class DistinctPlan : public LogicalPlan {
   public:
    DistinctPlan(LogicalPlanPtr child, std::vector<std::unique_ptr<BoundExpression>> distinct_on_list)
        : LogicalPlan(LogicalPlanType::Distinct), child_(child), distinct_on_list(std::move(distinct_on_list)) {}

    virtual const Schema& GetSchema() const override;

    virtual std::vector<LogicalPlanPtr> Children() const override;
    virtual void SetChildren(const std::vector<LogicalPlanPtr>& children) override { child_ = children[0]; }

    virtual std::string ToString() const override { return "DistinctPlan"; }

    LogicalPlanPtr GetLastPlan() const { return child_; }

   private:
    LogicalPlanPtr child_;

   public:
    std::vector<std::unique_ptr<BoundExpression>> distinct_on_list;
};
