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
 * projection_plan.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/planner/logical_plan/projection_plan.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "planner/logical_plan/projection_plan.h"

#include <fmt/core.h>
#include <fmt/format.h>

#include <algorithm>

#include "binder/bound_expression.h"
#include "binder/expressions/bound_alias.h"

std::vector<LogicalPlanPtr> ProjectionPlan::Children() const { return {child_}; }

std::string ProjectionPlan::ToString() const {
    std::vector<std::string> to_strings;
    const Schema& schema = GetSchema();
    const auto& columns = schema.GetColumnInfos();
    to_strings.reserve(columns.size());
    for (size_t i = 0; i < columns.size(); ++i) {
        to_strings.push_back(columns[i].GetColNameWithoutTableName());
    }
    return fmt::format("Projection: {}", fmt::join(to_strings, ", "));
}

void ProjectionPlan::InitSchema() const {
    if (!schema_.has_value()) {
        std::vector<SchemaColumnInfo> columns;
        columns.reserve(exprs_.size());
        int slot = 0;
        for (size_t i = 0; i < exprs_.size(); ++i) {
            columns.emplace_back(BoundExpressionToSchemaColumnInfo(child_, *exprs_[i], slot++));
        }
        schema_ = Schema(std::move(columns));
    }
}

const Schema& ProjectionPlan::GetSchema() const {
    if (!schema_.has_value()) {
        InitSchema();
    }
    return schema_.value();
}
