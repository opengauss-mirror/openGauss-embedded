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
 * values_exec.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/planner/physical_plan/values_exec.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include "planner/expressions/expression.h"
#include "planner/physical_plan/physical_plan.h"

class ValuesExec : public PhysicalPlan {
   public:
    ValuesExec(Schema schema, std::vector<std::vector<std::unique_ptr<Expression>>> &&values_list)
        : schema_(std::move(schema)), insert_values_(std::move(values_list)) {}
    virtual Schema GetSchema() const override;

    virtual auto Execute() const -> RecordBatch override;
    void Execute(RecordBatch &rb_out) override;

    virtual std::vector<PhysicalPlanPtr> Children() const override;

    virtual std::string ToString() const override;

    virtual auto Next() -> std::tuple<Record, knl_cursor_t *, bool> override;

    void ResetNext() override {
        idx_ = 0;
        if (child_) child_->ResetNext();
        for (auto &values : insert_values_) {
            for (auto &expr : values) {
                expr->Reset();
            }
        }
    }

   private:
    Schema schema_;
    PhysicalPlanPtr child_;
    std::vector<std::vector<std::unique_ptr<Expression>>> insert_values_;
    uint32_t idx_{0};
};
