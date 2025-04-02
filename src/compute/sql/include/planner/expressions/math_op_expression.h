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
 * math_op_expression.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/planner/expressions/math_op_expression.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <fmt/core.h>
#include <fmt/format.h>

#include <memory>

#include "common/math_op_type.h"
#include "function/function.h"
#include "planner/expressions/expression.h"

class MathBinaryOpExpression : public Expression {
   public:
    MathBinaryOpExpression(MathOpType type, std::unique_ptr<Expression> left, std::unique_ptr<Expression> right,
                           const intarkdb::Function& func_info)
        : Expression(func_info.sig.return_type),
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
    MathOpType type_;
    std::unique_ptr<Expression> left_;
    std::unique_ptr<Expression> right_;
    intarkdb::Function func_info_;
};

class MathUnaryOpExpression : public Expression {
   public:
    MathUnaryOpExpression(MathOpType type, std::unique_ptr<Expression> child, const intarkdb::Function func_info)
        : Expression(func_info.sig.return_type), type_{type}, child_(std::move(child)), func_info_(func_info) {}

    virtual auto Evaluate(const Record& record) const -> Value;

    virtual auto ToString() const -> std::string { return fmt::format("{}{}", type_, child_->ToString()); }

   private:
    MathOpType type_;
    std::unique_ptr<Expression> child_;
    intarkdb::Function func_info_;
};