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
 * projection_exec.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/planner/physical_plan/projection_exec.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include "planner/expressions/expression.h"
#include "planner/physical_plan/physical_plan.h"

class ProjectionExec : public PhysicalPlan {
   public:
    ProjectionExec(PhysicalPlanPtr child, const Schema& schema, std::vector<std::unique_ptr<Expression>> exprs);
    virtual Schema GetSchema() const override;

    virtual std::vector<PhysicalPlanPtr> Children() const override;

    virtual std::string ToString() const override;

    virtual auto Next() -> std::tuple<Record, knl_cursor_t*, bool> override;

    // depercated
    virtual auto Execute() const -> RecordBatch override { return RecordBatch({}); }

    virtual void ResetNext() override {
        child_->ResetNext();
        for (auto& expr : exprs_) {
            expr->Reset();
        }
    }

   private:
    PhysicalPlanPtr child_;
    Schema schema_;
    std::vector<std::unique_ptr<Expression>> exprs_;
};
