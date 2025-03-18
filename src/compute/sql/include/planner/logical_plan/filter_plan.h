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
 * filter_plan.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/planner/logical_plan/filter_plan.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <memory>
#include <string>
#include <vector>

#include "binder/bound_expression.h"
#include "catalog/schema.h"
#include "planner/logical_plan/logical_plan.h"

class FilterPlan : public LogicalPlan {
   public:
    FilterPlan(std::unique_ptr<BoundExpression> expr, LogicalPlanPtr plan);
    virtual const Schema& GetSchema() const override;

    virtual std::vector<LogicalPlanPtr> Children() const override;
    virtual void SetChildren(const std::vector<LogicalPlanPtr>& children) override { child_ = children[0]; }

    virtual std::string ToString() const override;


    LogicalPlanPtr GetLastPlan() const;

    std::unique_ptr<BoundExpression> expr;

   private:
    LogicalPlanPtr child_;
};
