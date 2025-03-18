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
* window_plan.h
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/include/planner/logical_plan/window_plan.h
*
* -------------------------------------------------------------------------
*/
#pragma once

#include "planner/logical_plan/logical_plan.h"
#include "binder/expressions/window_function.h"

namespace intarkdb {
class WindowPlan : public LogicalPlan {
public:
    WindowPlan(std::unique_ptr<WindowFunction> expr,int64_t func_idx,LogicalPlanPtr child) : LogicalPlan(LogicalPlanType::Window){
        window_func_ = std::move(expr);
        children_.push_back(child);
        func_idx_ = func_idx;
    }
    virtual const Schema &GetSchema() const {
        return children_[0]->GetSchema();
    };

    virtual std::vector<LogicalPlanPtr> Children() const {
        return children_;
    }

    virtual void SetChildren(const std::vector<LogicalPlanPtr> &children) {
        children_ = children;
    }

    virtual std::string ToString() const { return "LogicalWindow"; }

    std::unique_ptr<WindowFunction> window_func_;
    std::vector<LogicalPlanPtr> children_;
    int64_t func_idx_;
};
} // namespace intarkdb
