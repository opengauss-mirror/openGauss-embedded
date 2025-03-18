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
 * filter_exec.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/planner/physical_plan/filter_exec.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "planner/physical_plan/filter_exec.h"

#include <stdexcept>

FilterExec::FilterExec(PhysicalPlanPtr child, std::unique_ptr<Expression> expr)
    : child_(child), expr_(std::move(expr)) {}

Schema FilterExec::GetSchema() const { return child_->GetSchema(); }

std::vector<PhysicalPlanPtr> FilterExec::Children() const { return {child_}; }

std::string FilterExec::ToString() const { return "Filter"; }

auto FilterExec::Next() -> std::tuple<Record, knl_cursor_t*, bool> {
    while (true) {
        auto&& [r, cur, eof] = child_->Next();
        if (eof) {
            break;
        }
        auto expr_value = expr_->Evaluate(r);
        if (expr_value.IsNull()) {
            continue;
        }
        if (expr_value.GetCastAs<Trivalent>() != Trivalent::TRI_TRUE) {
            continue;
        }
        return {std::move(r), cur, eof};
    }
    return {{}, nullptr, true};
}
