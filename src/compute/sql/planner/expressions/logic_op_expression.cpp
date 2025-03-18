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
* logic_op_expression.cpp
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/planner/expressions/logic_op_expression.cpp
*
* -------------------------------------------------------------------------
*/
#include "planner/expressions/logic_op_expression.h"

using intarkdb::LogicOpType;

auto LogicBinaryOpExpression::Evaluate(const Record& record) const -> Value {
    auto left_value = left_->Evaluate(record);
    auto right_value = right_->Evaluate(record);
    if (left_value.IsNull()) {
        left_value = ValueFactory::ValueBool(Trivalent::UNKNOWN);
    }
    if (right_value.IsNull()) {
        right_value = ValueFactory::ValueBool(Trivalent::UNKNOWN);
    }
    if (left_value.GetType() != GStorDataType::GS_TYPE_BOOLEAN) {
        left_value = DataType::GetTypeInstance(GStorDataType::GS_TYPE_BOOLEAN)->CastValue(left_value);
    }
    if (right_value.GetType() != GStorDataType::GS_TYPE_BOOLEAN) {
        right_value = DataType::GetTypeInstance(GStorDataType::GS_TYPE_BOOLEAN)->CastValue(right_value);
    }
    auto bool_type = static_cast<const BooleanType*>(DataType::GetTypeInstance(GStorDataType::GS_TYPE_BOOLEAN));
    switch (type_) {
        case LogicOpType::And: {
            return bool_type->And(left_value, right_value);
        }
        case LogicOpType::Or: {
            return bool_type->Or(left_value, right_value);
        }
        default:
            break;
    }
    throw std::runtime_error(fmt::format("unkown type {}", int(type_)));
}

auto LogicUnaryOpExpression::EvaluateInternal(Value& r) const -> Value {
    if (r.IsNull()) {
        r = ValueFactory::ValueBool(Trivalent::UNKNOWN);
    }
    switch (type_) {
        case LogicOpType::Not: {
            if (r.GetType() != GStorDataType::GS_TYPE_BOOLEAN) {
                r = DataType::GetTypeInstance(GStorDataType::GS_TYPE_BOOLEAN)->CastValue(r);
            }
            auto b = static_cast<const BooleanType*>(DataType::GetTypeInstance(r.GetType()));
            return b->Not(r);
        }
        default:
            break;
    }
    throw std::runtime_error(fmt::format("unknown the logic op type {}", int(type_)));
}

auto LogicUnaryOpExpression::ReEvaluate(const Record& record) const -> Value {
    auto r = expr_->ReEvaluate(record);
    return EvaluateInternal(r);
}

auto LogicUnaryOpExpression::Evaluate(const Record& record) const -> Value {
    auto r = expr_->Evaluate(record);
    return EvaluateInternal(r);
}
