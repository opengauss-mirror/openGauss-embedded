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
* distinct_plan.cpp
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/planner/logical_plan/distinct_plan.cpp
*
* -------------------------------------------------------------------------
*/
#include "planner/logical_plan/distinct_plan.h"

const Schema& DistinctPlan::GetSchema() const { return child_->GetSchema(); }

std::vector<LogicalPlanPtr> DistinctPlan::Children() const { return {child_}; }
