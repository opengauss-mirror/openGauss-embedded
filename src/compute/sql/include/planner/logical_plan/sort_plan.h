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
 * sort_plan.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/planner/logical_plan/sort_plan.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include "binder/bound_sort.h"
#include "planner/logical_plan/logical_plan.h"

class SortPlan : public LogicalPlan {
   public:
    SortPlan(LogicalPlanPtr plan, std::vector<std::unique_ptr<BoundSortItem>> order_by)
        : LogicalPlan(LogicalPlanType::Sort), order_by_(std::move(order_by)), child_(plan) {}

    virtual const Schema& GetSchema() const override { return child_->GetSchema(); }

    virtual std::vector<LogicalPlanPtr> Children() const override { return {child_}; }
    virtual void SetChildren(const std::vector<LogicalPlanPtr>& children) override { child_ = children[0]; }

    virtual std::string ToString() const override {
        std::string str = "SortPlan: OrderBy ";
        for (auto& item : order_by_) {
            str += item->ToString() + " ";
        }
        return str;
    }

    LogicalPlanPtr GetLastPlan() const { return child_; }

    std::vector<std::unique_ptr<BoundSortItem>> order_by_;

   private:
    LogicalPlanPtr child_;
};
