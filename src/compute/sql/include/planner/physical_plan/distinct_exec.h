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
 * distinct_exec.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/planner/physical_plan/distinct_exec.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <vector>

#include "cm_dec4.h"
#include "common/distinct_keys.h"
#include "planner/expressions/expression.h"
#include "planner/physical_plan/physical_plan.h"

class DistinctExec : public PhysicalPlan {
   public:
    DistinctExec(PhysicalPlanPtr child, std::vector<std::unique_ptr<Expression>> distinct_on_list)
        : child_(child), schema_(child->GetSchema()), exprs_(std::move(distinct_on_list)) {}
    ~DistinctExec() {}

    virtual Schema GetSchema() const override;

    // depercated
    virtual auto Execute() const -> RecordBatch override { return RecordBatch({}); }

    virtual std::vector<PhysicalPlanPtr> Children() const override { return {child_}; }

    virtual std::string ToString() const override { return "Distinct"; }

    virtual auto Next() -> std::tuple<Record, knl_cursor_t*, bool> override;

    virtual void ResetNext() override {
        set_.clear();
        child_->ResetNext();
    }

   private:
    DistinctKeySet set_;
    PhysicalPlanPtr child_;
    Schema schema_;
    std::vector<std::unique_ptr<Expression>> exprs_;
};
