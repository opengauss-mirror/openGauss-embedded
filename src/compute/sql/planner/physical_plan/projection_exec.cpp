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
 * projection_exec.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/planner/physical_plan/projection_exec.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "planner/physical_plan/projection_exec.h"

ProjectionExec::ProjectionExec(PhysicalPlanPtr child, const Schema& schema,
                               std::vector<std::unique_ptr<Expression>> exprs)
    : child_(child), schema_(schema), exprs_(std::move(exprs)) {}

auto ProjectionExec::GetSchema() const -> Schema { return schema_; }

auto ProjectionExec::Children() const -> std::vector<PhysicalPlanPtr> { return {child_}; }

auto ProjectionExec::ToString() const -> std::string {
    // TODO
    return "Projection";
}

auto ProjectionExec::Next() -> std::tuple<Record, knl_cursor_t*, bool> {
    const auto& headers = schema_.GetColumnInfos();
    auto&& [r, cur, eof] = child_->Next();
    if (eof) {
        return {{}, nullptr, true};
    }
    std::vector<Value> values;
    values.reserve(headers.size());
    for (size_t i = 0; i < headers.size(); ++i) {
        values.push_back(exprs_[i]->Evaluate(r));
    }
    return std::make_tuple(std::move(values), cur, eof);
}
