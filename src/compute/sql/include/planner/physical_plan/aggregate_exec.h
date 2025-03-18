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
 * aggregate_exec.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/planner/physical_plan/aggregate_exec.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <map>
#include <set>
#include <vector>

#include "common/hash_util.h"
#include "common/memory/memory_manager.h"
#include "planner/expressions/column_value_expression.h"
#include "planner/physical_plan/distinct_exec.h"
#include "planner/physical_plan/physical_plan.h"

class AggregateExec : public PhysicalPlan {
   public:
    AggregateExec(std::vector<std::unique_ptr<Expression>> groupby, std::vector<std::string> ops,
                  std::vector<std::unique_ptr<Expression>> be_group, const std::vector<bool>& distincts, Schema schema,
                  PhysicalPlanPtr child);

    virtual Schema GetSchema() const override;

    virtual std::vector<PhysicalPlanPtr> Children() const override { return {child_}; }

    virtual std::string ToString() const override { return "AggreateExec"; }

    void Init();
    virtual auto Next() -> std::tuple<Record, knl_cursor_t*, bool> override;

    // depercated
    virtual auto Execute() const -> RecordBatch override { return RecordBatch({}); }

    virtual void ResetNext() override {
        init_ = false;
        child_->ResetNext();
        for (auto& group : groups_) {
            if (group) {
                group->Reset();
            }
        }
        for (auto& be_group : be_groups_) {
            if (be_group) {
                be_group->Reset();
            }
        }
    }

   private:
    Schema schema_;
    PhysicalPlanPtr child_;
    std::vector<std::unique_ptr<Expression>> groups_;     // 聚合条件
    std::vector<std::string> ops_;                        // 聚合函数字符串
    std::vector<std::unique_ptr<Expression>> be_groups_;  // 聚合函数表达式
    std::vector<Record> results_;
    size_t idx_{0};
    bool init_{false};
    std::vector<bool> distincts;
};
