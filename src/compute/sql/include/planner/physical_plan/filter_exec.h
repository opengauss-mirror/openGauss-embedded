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
 * filter_exec.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/planner/physical_plan/filter_exec.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include "planner/expressions/expression.h"
#include "planner/physical_plan/physical_plan.h"

class FilterExec : public PhysicalPlan {
   public:
    FilterExec(PhysicalPlanPtr child, std::unique_ptr<Expression> expr);
    ~FilterExec() {}
    virtual Schema GetSchema() const override;

    // depercated
    virtual auto Execute() const -> RecordBatch override { return RecordBatch({}); }

    virtual std::vector<PhysicalPlanPtr> Children() const override;

    virtual std::string ToString() const override;

    virtual auto Next() -> std::tuple<Record, knl_cursor_t*, bool> override;

    virtual auto ResetNext() -> void override {
        child_->ResetNext();
        if (expr_) {
            expr_->Reset();
        }
    }

   private:
    PhysicalPlanPtr child_;
    std::unique_ptr<Expression> expr_;
};
