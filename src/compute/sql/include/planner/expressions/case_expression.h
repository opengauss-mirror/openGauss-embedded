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
 * case_expression.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/planner/expressions/case_expression.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include "planner/expressions/expression.h"

struct CaseCheck {
    std::unique_ptr<Expression> when_expr;
    std::unique_ptr<Expression> then_expr;
};

class CaseExpression : public Expression {
   public:
	CaseExpression(std::vector<CaseCheck> case_checks, std::unique_ptr<Expression> else_expr)
        : Expression(GS_TYPE_VARCHAR), case_checks_(std::move(case_checks)), else_expr_(std::move(else_expr)) {}

    virtual auto Evaluate(const Record& record) const -> Value {
        for (auto & case_check_one : case_checks_) {
            auto when_val = case_check_one.when_expr->Evaluate(record);
            auto then_val = case_check_one.then_expr->Evaluate(record);
            if (!when_val.IsNull() && when_val.GetCastAs<bool>()) {
                return then_val;
            }
        }

        // if has else expr
        if (else_expr_) {
            return else_expr_->Evaluate(record);
        }
        return Value();
    }

    virtual auto ReEvaluate(const Record& record) const -> Value { return Evaluate(record); }

    virtual auto ToString() const -> std::string { return "CaseExpression"; }

    std::vector<CaseCheck> case_checks_;
	std::unique_ptr<Expression> else_expr_;
}; 
