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
 * string_op_expression.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/planner/expressions/string_op_expression.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <fmt/core.h>
#include <fmt/format.h>

#include <memory>

#include "function/function.h"
#include "planner/expressions/expression.h"

enum class StringOpType : uint8_t { Concat };

class StringOpExpression : public Expression {
   public:
    StringOpExpression(StringOpType type, std::unique_ptr<Expression> left, std::unique_ptr<Expression> right,
                       const intarkdb::Function& func_info)
        : Expression(GS_TYPE_VARCHAR),
          type_{type},
          left_(std::move(left)),
          right_(std::move(right)),
          func_info_(func_info) {}

    virtual auto Evaluate(const Record& record) const -> Value;

    virtual auto ToString() const -> std::string {
        return fmt::format("{} {} {}", left_->ToString(), type_, right_->ToString());
    }

    virtual auto Reset() -> void {
        left_->Reset();
        right_->Reset();
    }

   private:
    StringOpType type_;
    std::unique_ptr<Expression> left_;
    std::unique_ptr<Expression> right_;
    intarkdb::Function func_info_;
};

template <>
struct fmt::formatter<StringOpType> : formatter<std::string_view> {
    template <typename FormatContext>
    auto format(StringOpType c, FormatContext& ctx) const {
        std::string_view name;
        switch (c) {
            case StringOpType::Concat: {
                name = "||";
                break;
            }
            default:
                throw intarkdb::Exception(ExceptionType::EXECUTOR, fmt::format("unknown math op type: {}", c));
        }
        return formatter<std::string_view>::format(name, ctx);
    }
};
