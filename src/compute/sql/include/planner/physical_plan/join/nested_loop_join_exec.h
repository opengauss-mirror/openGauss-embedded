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
 * nested_loop_join_exec.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/planner/physical_plan/nested_loop_join_exec.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include "binder/table_ref/bound_join.h"
#include "planner/physical_plan/physical_plan.h"
#include "planner/physical_plan/seq_scan_exec.h"

class NestedLoopJoinExec : public PhysicalPlan {
   public:
    NestedLoopJoinExec(Schema schema, PhysicalPlanPtr left, PhysicalPlanPtr right, std::unique_ptr<Expression> pred,
                       JoinType join_type)
        : schema_(schema), left_(left), right_(right), pred_(std::move(pred)), join_type_(join_type) {}
    virtual Schema GetSchema() const override { return schema_; }

    virtual auto Execute() const -> RecordBatch override { return RecordBatch({}); }

    virtual std::vector<PhysicalPlanPtr> Children() const override { return {left_, right_}; }

    virtual std::string ToString() const override { return "NestedLoopJoinExec"; }

    virtual auto Next() -> std::tuple<Record, knl_cursor_t*, bool> override;

    virtual void ResetNext() override;

   private:
    void Init();

   private:
    Schema schema_;
    PhysicalPlanPtr left_;
    PhysicalPlanPtr right_;
    std::unique_ptr<Expression> pred_;
    std::vector<Record> records_;
    std::vector<Record> right_records_;
    JoinType join_type_;
    uint32_t idx_{0};
    bool init_{false};
};
