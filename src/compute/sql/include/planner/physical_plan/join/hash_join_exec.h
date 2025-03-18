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
 * hash_join_exec.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/planner/physical_plan/join/hash_join_exec.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include "common/distinct_keys.h"
#include "common/join_type.h"
#include "planner/expressions/expression.h"
#include "planner/physical_plan/physical_plan.h"

class HashJoinExec : public PhysicalPlan {
   public:
    explicit HashJoinExec(const Schema& schema, JoinType join_type, PhysicalPlanPtr left, PhysicalPlanPtr right,
                          std::vector<size_t>&& inner_key_idxs, std::vector<size_t>&& outer_key_idxs,
                          std::vector<LogicalType>&& idxs_types, std::unique_ptr<Expression> pred)
        : join_type_(join_type),
          schema_(schema),
          left_(left),
          right_(right),
          inner_key_idxs_(std::move(inner_key_idxs)),
          outer_key_idxs_(std::move(outer_key_idxs)),
          idxs_types_(std::move(idxs_types)),
          pred_(std::move(pred)) {
    }

    virtual auto GetSchema() const -> Schema override { return schema_; }

    virtual auto Execute() const -> RecordBatch override { return RecordBatch({}); }

    virtual auto Children() const -> std::vector<PhysicalPlanPtr> override { return {left_, right_}; }

    virtual auto ToString() const -> std::string override { return "HashJoinExec"; }

    virtual auto Next() -> std::tuple<Record, knl_cursor_t*, bool> override;

    virtual void ResetNext() override;

    auto CrossJoinNext() -> std::tuple<Record, knl_cursor_t*, bool>;
    auto LeftJoinNext() -> std::tuple<Record, knl_cursor_t*, bool>;
    auto RightJoinNext() -> std::tuple<Record, knl_cursor_t*, bool>;

   private:
    auto LeftInit() -> void;
    auto RightInit() -> void;
    auto Init(PhysicalPlanPtr plan, const std::vector<size_t>& idxs) -> void;

    struct HashIdx {
        int idx = -1;
        DistinctKeyMap<std::vector<Record>>::iterator iter = {};
        void Clear() { idx = -1; }
    };

   private:
    JoinType join_type_;
    bool init_{false};
    Schema schema_;
    PhysicalPlanPtr left_;
    PhysicalPlanPtr right_;
    std::vector<size_t> inner_key_idxs_;
    std::vector<size_t> outer_key_idxs_;
    std::vector<LogicalType> idxs_types_;
    Record curr_record_;
    DistinctKeyMap<std::vector<Record>> hash_table_;
    HashIdx hash_idx_;
    std::unique_ptr<Expression> pred_;
    size_t padding_column_size_;
};
