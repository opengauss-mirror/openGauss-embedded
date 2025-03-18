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
 * update_plan.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/planner/logical_plan/update_plan.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <memory>

#include "datasource/table_datasource.h"
#include "planner/logical_plan/filter_plan.h"
#include "planner/logical_plan/logical_plan.h"

class UpdatePlan : public LogicalPlan {
   public:
    UpdatePlan(LogicalPlanType type, TableDataSource& sc, LogicalPlanPtr child,
               std::vector<std::pair<Column, std::unique_ptr<Expression>>> target_list)
        : LogicalPlan(type), sc_(sc), child_(std::move(child)), target_list_(std::move(target_list)) {}

    const Schema& GetSchema() const override { return empty_schema; }

    virtual std::vector<LogicalPlanPtr> Children() const override { return {child_}; }
    virtual void SetChildren(const std::vector<LogicalPlanPtr>& children) override { child_ = children[0]; }

    LogicalPlanPtr GetLastPlan() const { return child_; }

    // private:
    TableDataSource& sc_;
    LogicalPlanPtr child_;
    std::vector<std::pair<Column, std::unique_ptr<Expression>>> target_list_;

   private:
    static inline Schema empty_schema = Schema{};
};
