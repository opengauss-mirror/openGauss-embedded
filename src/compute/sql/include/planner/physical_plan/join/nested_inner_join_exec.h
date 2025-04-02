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
 * nested_inner_join_exec.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/planner/physical_plan/nested_inner_join_exec.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once
#include "binder/table_ref/bound_join.h"
#include "planner/physical_plan/physical_plan.h"
#include "planner/physical_plan/seq_scan_exec.h"

class InnerJoinExec : public PhysicalPlan {
   public:
    InnerJoinExec(const Schema& schema, JoinType type, PhysicalPlanPtr left, PhysicalPlanPtr right,std::unique_ptr<Expression> pred)
        : schema_(schema), join_type_(type),left_(left), right_(right),pred_(std::move(pred)) {}

    virtual Schema GetSchema() const override { return schema_; }

    virtual auto Execute() const -> RecordBatch override { return RecordBatch({}); }

    virtual std::vector<PhysicalPlanPtr> Children() const override { return {left_, right_}; }

    virtual std::string ToString() const override { return "InnerJoinExec"; }

    virtual auto Next() -> std::tuple<Record, knl_cursor_t*, bool> override;
    auto CrossJoinNext() -> std::tuple<Record, knl_cursor_t*, bool>;
    auto LeftJoinNext() -> std::tuple<Record, knl_cursor_t*, bool>;
    auto RightJoinNext() -> std::tuple<Record, knl_cursor_t*, bool>;

    virtual void ResetNext() override;

    auto Init(PhysicalPlanPtr plan) -> void;
    auto LeftInit() -> void;
    auto RightInit() -> void;

   private:
    Schema schema_;
    JoinType join_type_;
    PhysicalPlanPtr left_;  // probe_table
    PhysicalPlanPtr right_;
    std::unique_ptr<Expression> pred_;
    bool left_eof_{false};
    bool right_eof_{true};
    Record curr_record_;
    uint32_t idx_{0};
    std::vector<Record,intarkdb::Allocator<Record>> records_; // cache result
    bool init_{false};
    size_t padding_column_size_;
};
