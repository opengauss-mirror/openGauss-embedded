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
* like_expression.cpp
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/planner/expressions/like_expression.cpp
*
* -------------------------------------------------------------------------
*/
#include "planner/expressions/like_expression.h"

#include <memory>

#include "common/string_util.h"
#include "type/operator/like_operator.h"
#include "type/type_system.h"

auto LikeExpression::Evaluate(const Record& record) const -> Value {
    auto left_value = lexp_->Evaluate(record);
    auto right_value = rexp_->Evaluate(record);
    auto escape = escape_->Evaluate(record);
    return LikeOperator(type_, left_value, right_value, escape);
}

LikeType LikeTypeFromString(const std::string& op_name) {
    LikeType type = LikeType::Invalid;
    if (op_name == "~~") {
        type = LikeType::Like;
    } else if (op_name == "!~~") {
        type = LikeType::NotLike;
    } else if (op_name == "~~~") {
        type = LikeType::Glob;
    } else if (op_name == "~~*") {
        type = LikeType::ILike;
    } else if (op_name == "!~~*") {
        type = LikeType::NotILike;
    } else if (op_name == "like_escape") {
        type = LikeType::LikeEscape;
    } else if (op_name == "not_like_escape") {
        type = LikeType::NotLikeEscape;
    } else if (op_name == "ilike_escape") {
        type = LikeType::ILikeEscape;
    } else if (op_name == "not_ilike_escape") {
        type = LikeType::NotILikeEscape;
    } else {
        throw std::invalid_argument(fmt::format("unsupport this binaray op {}", op_name));
    }
    return type;
}
