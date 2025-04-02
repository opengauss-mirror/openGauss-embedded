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
* scan_plan.h
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/include/planner/logical_plan/scan_plan.h
*
* -------------------------------------------------------------------------
*/
#pragma once

#include <fmt/core.h>
#include <fmt/format.h>
#include <fmt/ranges.h>

#include <memory>
#include <optional>
#include <vector>

#include "binder/bound_expression.h"
#include "binder/statement/select_statement.h"
#include "datasource/table_datasource.h"
#include "planner/logical_plan/logical_plan.h"

class ScanPlan : public LogicalPlan {
   public:
    ScanPlan(std::unique_ptr<TableDataSource> sc, const std::vector<std::string>& projection)
        : LogicalPlan(LogicalPlanType::Scan), source(std::move(sc)), projection_(projection) {}

    virtual const Schema& GetSchema() const override;

    virtual std::vector<LogicalPlanPtr> Children() const override { return children_; }
    virtual void SetChildren(const std::vector<LogicalPlanPtr>& children) override { children_ = children; }

    virtual std::string ToString() const override {
        std::string filter_str = "";
        for (auto& expr : bound_expressions) {
            filter_str += expr->ToString() + " ";
        }
        if (!projection_.empty()) {
            return fmt::format("Scan table={} projection={} filter={}", source->GetTableName(), projection_, filter_str);
        } else {
            return fmt::format("Scan table={} projection=None filter={}", source->GetTableName(), filter_str);
        }
    }

    auto IsOnlyCount() const -> bool { return only_count_rows_; }
    auto SetOnlyCount(bool only_count) -> void { only_count_rows_ = only_count; }

    const std::vector<std::string>& Projection() const { return projection_; }

    std::unique_ptr<TableDataSource> source;
    std::vector<std::unique_ptr<BoundExpression>> bound_expressions;

   private:
    std::vector<std::string> projection_;
    std::vector<LogicalPlanPtr> children_;
    mutable Schema schema_;
    bool only_count_rows_{false}; // just return the rows number of datasource
};
