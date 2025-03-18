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
 * in_expression.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/planner/expressions/in_expression.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include "planner/expressions/expression.h"

class InExpression : public Expression {
   public:
    InExpression(std::unique_ptr<Expression> in_ref, std::vector<std::unique_ptr<Expression>>&& in_list, bool is_not_in)
        : Expression(GS_TYPE_BOOLEAN),in_ref_expr(std::move(in_ref)), in_list(std::move(in_list)), is_not_in(is_not_in) {}

    virtual auto Evaluate(const Record& record) const -> Value {
        auto in_ref = in_ref_expr->Evaluate(record);
        if (in_ref.IsNull()) {
            return Value(GS_TYPE_BOOLEAN);
        }
        bool has_null = false;
        for (const auto& expr : in_list) {
            const auto& val = expr->Evaluate(record);
            if (val.IsNull()) {
                has_null = true;
                continue;
            }
            auto ret = in_ref.Equal(val);
            if (!is_not_in && ret == Trivalent::TRI_TRUE) {
                return ValueFactory::ValueBool(true);
            }
            if (is_not_in && ret == Trivalent::TRI_TRUE) {
                return ValueFactory::ValueBool(false);
            }
        }
        if (has_null) {
            return Value(GS_TYPE_BOOLEAN);
        }
        return is_not_in ? ValueFactory::ValueBool(true) : ValueFactory::ValueBool(false);
    }

    virtual auto ReEvaluate(const Record& record) const -> Value { return Evaluate(record); }

    virtual auto ToString() const -> std::string { return "InExpression"; }

    virtual auto Reset() -> void {
        in_ref_expr->Reset();
        for (const auto& expr : in_list) {
            expr->Reset();
        }
    }

    std::unique_ptr<Expression> in_ref_expr;
    std::vector<std::unique_ptr<Expression>> in_list;
    bool is_not_in = false;
};
