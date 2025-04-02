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
* nulltest_expression.h
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/include/planner/expressions/nulltest_expression.h
*
* -------------------------------------------------------------------------
*/
#pragma once

#include "binder/expressions/bound_null_test.h"
#include "planner/expressions/expression.h"

class NullTestExpression : public Expression {
   public:
    NullTestExpression(std::unique_ptr<Expression> child, NullTest type)
        : Expression(GStorDataType::GS_TYPE_BOOLEAN), child_(std::move(child)), null_test_type_(type) {}

    virtual auto Evaluate(const Record& record) const -> Value override {
        auto child_eval = child_->Evaluate(record).IsNull();
        if (null_test_type_ == NullTest::IS_NOT_NULL) {
            child_eval = !child_eval;
        }
        return ValueFactory::ValueBool(child_eval);
    }

    virtual auto ToString() const -> std::string override { return "NullTest"; }

    virtual auto Reset() -> void override { child_->Reset(); }

   private:
    std::unique_ptr<Expression> child_;
    NullTest null_test_type_;
};
