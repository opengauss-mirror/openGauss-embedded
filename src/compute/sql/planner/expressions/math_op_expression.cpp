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
 * math_op_expression.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/planner/expressions/math_op_expression.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "planner/expressions/math_op_expression.h"

#include "function/function.h"
#include "type/type_id.h"

auto MathBinaryOpExpression::Evaluate(const Record& record) const -> Value {
    auto left = left_->Evaluate(record);
    auto right = right_->Evaluate(record);
    if (left.IsNull() || right.IsNull()) {
        return ValueFactory::ValueNull();
    }
    return func_info_.func({left, right});
}

auto MathUnaryOpExpression::Evaluate(const Record& record) const -> Value {
    auto val = child_->Evaluate(record);
    if (val.IsNull()) {
        return val;
    }

    switch (type_) {
        case MathOpType::Minus: {
            auto func = intarkdb::FunctionContext::GetFunction("-", {val.GetLogicalType()});
            if (func.has_value()) {
                val = func.value().func({val});
            } else {
                throw intarkdb::Exception(ExceptionType::EXECUTOR,
                                          fmt::format("unsupported - with {}", val.GetLogicalType()));
            }
            break;
        }
        default:
            throw intarkdb::Exception(ExceptionType::EXECUTOR, fmt::format("unknown unary math op type: {}", type_));
    }
    return val;
}
