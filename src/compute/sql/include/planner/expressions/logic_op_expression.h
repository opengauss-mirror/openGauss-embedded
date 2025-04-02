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
 * logic_op_expression.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/planner/expressions/logic_op_expression.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <memory>

#include "common/logic_op_type.h"
#include "planner/expressions/expression.h"

class LogicBinaryOpExpression : public Expression {
   public:
    LogicBinaryOpExpression(intarkdb::LogicOpType type, std::unique_ptr<Expression> left,
                            std::unique_ptr<Expression> right)
        : Expression(GS_TYPE_BOOLEAN), type_{type}, left_(std::move(left)), right_(std::move(right)) {}

    virtual auto Evaluate(const Record& record) const -> Value;

    virtual auto ToString() const -> std::string {
        return fmt::format("{} {} {}", left_->ToString(), type_, right_->ToString());
    }

   private:
    intarkdb::LogicOpType type_;
    std::unique_ptr<Expression> left_;
    std::unique_ptr<Expression> right_;
};

class LogicUnaryOpExpression : public Expression {
   public:
    LogicUnaryOpExpression(intarkdb::LogicOpType type, std::unique_ptr<Expression> expr)
        : Expression(GS_TYPE_BOOLEAN), type_(type), expr_(std::move(expr)) {}

    virtual auto Evaluate(const Record& record) const -> Value;

    virtual auto ReEvaluate(const Record& record) const -> Value;

    virtual auto ToString() const -> std::string { return fmt::format("{} {}", type_, expr_->ToString()); };

    virtual auto Reset() -> void { expr_->Reset(); }

   private:
    virtual auto EvaluateInternal(Value& v) const -> Value;

   private:
    intarkdb::LogicOpType type_;
    std::unique_ptr<Expression> expr_;
};
