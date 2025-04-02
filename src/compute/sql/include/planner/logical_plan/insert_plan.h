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
* insert_plan.h
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/include/planner/logical_plan/insert_plan.h
*
* -------------------------------------------------------------------------
*/
#pragma once

#include <fmt/core.h>
#include <fmt/format.h>
#include <fmt/ranges.h>

#include <memory>
#include <vector>

#include "binder/statement/insert_statement.h"
#include "datasource/table_datasource.h"
#include "planner/logical_plan/logical_plan.h"

class InsertPlan : public LogicalPlan {
   public:
    InsertPlan(LogicalPlanType type, std::unique_ptr<TableDataSource> data_source,
               const std::vector<Column>& bound_columns,
               const std::vector<Column>& unbound_defaults,
               const std::vector<Column>& bound_defaults,
               InsertOnConflictActionAlias opt_or_action,
               LogicalPlanPtr child)
        : LogicalPlan(type),
          source(std::move(data_source)),
          bound_columns_(bound_columns),
          unbound_defaults_(unbound_defaults),
          bound_defaults_(bound_defaults),
          opt_or_action_(opt_or_action),
          child_(child) {}

    virtual const Schema& GetSchema() const override;

    virtual std::vector<LogicalPlanPtr> Children() const override { return {child_}; }
    virtual void SetChildren(const std::vector<LogicalPlanPtr>& children) override { child_ = children[0]; }

    virtual std::string ToString() const override {
        { return "InsertPlan"; }
    }

    LogicalPlanPtr GetLastPlan() const { return child_; }

    std::unique_ptr<TableDataSource> source;

    std::vector<Column> bound_columns_;
    std::vector<Column> unbound_defaults_;
    std::vector<Column> bound_defaults_;

    InsertOnConflictActionAlias opt_or_action_;

   private:
    LogicalPlanPtr child_;
    static inline Schema empty_schema = Schema{};
};
