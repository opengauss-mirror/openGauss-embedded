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
 * union_exec.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/planner/physical_plan/union_exec.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <list>
#include <memory>

#include "common/set_operation_type.h"
#include "planner/physical_plan/distinct_exec.h"
#include "planner/physical_plan/physical_plan.h"

class UnionExec : public PhysicalPlan {
   public:
    UnionExec(const Schema& schema, PhysicalPlanPtr left, PhysicalPlanPtr right, SetOperationType set_op_type,
              bool is_distinct)
        : schema_(std::move(schema)),
          left_(std::move(left)),
          right_(std::move(right)),
          set_op_type_(set_op_type),
          is_distinct_(is_distinct) {}

    void Init();
    auto IntersectInit() -> void;
    auto ExceptInit() -> void;
    virtual Schema GetSchema() const override { return schema_; }

    virtual std::vector<PhysicalPlanPtr> Children() const override { return {left_, right_}; }

    virtual std::string ToString() const override { return "UnionExec"; }

    virtual auto Next() -> std::tuple<Record, knl_cursor_t*, bool> override;

    // depercated
    virtual auto Execute() const -> RecordBatch override { return RecordBatch({}); }

    virtual void ResetNext() override {
        init_ = false;
        left_->ResetNext();
        right_->ResetNext();
        left_eof_ = false;
        right_eof_ = false;
        distinct_keys_.clear();
        idx_ = distinct_keys_.begin();
    }

    auto UnionNext() -> std::tuple<Record, knl_cursor_t*, bool>;
    auto IntersectNext() -> std::tuple<Record, knl_cursor_t*, bool>;
    auto ExceptNext() -> std::tuple<Record, knl_cursor_t*, bool>;

   private:
    Schema schema_;
    PhysicalPlanPtr left_;
    PhysicalPlanPtr right_;
    SetOperationType set_op_type_;
    bool is_distinct_;
    bool init_{false};
    bool left_eof_{false};
    bool right_eof_{false};
    std::unordered_multiset<DistinctKey> distinct_keys_;
    std::unordered_multiset<DistinctKey>::iterator idx_;
};

class UnionJoinExec : public PhysicalPlan {
   public:
    UnionJoinExec(const Schema& schema, PhysicalPlanPtr left, PhysicalPlanPtr right)
        : schema_(std::move(schema)), left_(std::move(left)), right_(std::move(right)) {}

    virtual Schema GetSchema() const override { return schema_; }

    virtual std::vector<PhysicalPlanPtr> Children() const override { return {left_, right_}; }

    virtual std::string ToString() const override { return "UnionJoinExec"; }

    virtual auto Next() -> std::tuple<Record, knl_cursor_t*, bool> override;

    // depercated
    virtual auto Execute() const -> RecordBatch override { return RecordBatch({}); }

    virtual void ResetNext() override {
        left_->ResetNext();
        right_->ResetNext();
        left_eof_ = false;
        right_eof_ = false;
        distinct_keys_.clear();
    }

   private:
    Schema schema_;
    PhysicalPlanPtr left_;
    PhysicalPlanPtr right_;
    bool left_eof_{false};
    bool right_eof_{false};
    std::unordered_multiset<DistinctKey> distinct_keys_;
};
