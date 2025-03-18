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
* show_plan.h
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/include/planner/logical_plan/show_plan.h
*
* -------------------------------------------------------------------------
*/
#pragma once

#include <memory>
#include <vector>

#include "binder/statement/show_statement.h"
#include "planner/logical_plan/logical_plan.h"

class ShowPlan : public LogicalPlan {
   public:
    ShowPlan(LogicalPlanType type, ShowType show_type, LogicalPlanPtr child)
        : LogicalPlan(LogicalPlanType::Show), show_type(show_type), child_(child) {}

    virtual const Schema& GetSchema() const override;

    virtual std::vector<LogicalPlanPtr> Children() const override { return {child_}; }
    virtual void SetChildren(const std::vector<LogicalPlanPtr>& children) override {}

    LogicalPlanPtr GetLastPlan() const { return child_; }

    virtual std::string ToString() const override {
        { return "ShowPlan"; }
    }

    ShowType show_type;
    std::string db_path;
    std::string variable_name;

   private:
    LogicalPlanPtr child_{nullptr};
    static inline Schema empty_schema = Schema{};
};
