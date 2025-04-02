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
 * compare_expression.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/planner/expressions/compare_expression.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "planner/expressions/compare_expression.h"

#include "type/compare_operator.h"
#include "type/type_system.h"

auto ComparisonExpression::Evaluate(const Record& record) const -> Value {
    auto left_value = lexp_->Evaluate(record);
    auto right_value = rexp_->Evaluate(record);
    if (left_value.IsNull() || right_value.IsNull()) {
        return Value(GStorDataType::GS_TYPE_BOOLEAN, Trivalent::UNKNOWN);
    }
    return func_info_.func({left_value, right_value});
}

