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
 * filter_plan.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/planner/logical_plan/filter_plan.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "planner/logical_plan/filter_plan.h"

#include <fmt/format.h>

FilterPlan::FilterPlan(std::unique_ptr<BoundExpression> expr, LogicalPlanPtr plan)
    : LogicalPlan(LogicalPlanType::Filter), expr(std::move(expr)), child_(plan) {}

const Schema& FilterPlan::GetSchema() const { return child_->GetSchema(); }

std::vector<LogicalPlanPtr> FilterPlan::Children() const { return {child_}; }

std::string FilterPlan::ToString() const { return fmt::format("Filter: {}", expr->ToString()); }


LogicalPlanPtr FilterPlan::GetLastPlan() const { return child_; }
