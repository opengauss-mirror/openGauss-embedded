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
 * compare_expression.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/planner/expressions/compare_expression.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include "common/compare_type.h"
#include "function/function.h"
#include "planner/expressions/expression.h"
#include "type/value.h"

class ComparisonExpression : public Expression {
   public:
    ComparisonExpression(intarkdb::ComparisonType type, std::unique_ptr<Expression> lexp,
                         std::unique_ptr<Expression> rexp, const intarkdb::Function& func_info)
        : Expression(GS_TYPE_BOOLEAN),
          type_(type),
          lexp_(std::move(lexp)),
          rexp_(std::move(rexp)),
          func_info_(func_info) {}

    virtual auto Evaluate(const Record& record) const -> Value;

    virtual auto ToString() const -> std::string {
        return fmt::format("{} {} {}", lexp_->ToString(), type_, rexp_->ToString());
    };

    virtual auto Reset() -> void {
        lexp_->Reset();
        rexp_->Reset();
    }

   private:
    intarkdb::ComparisonType type_;
    std::unique_ptr<Expression> lexp_;
    std::unique_ptr<Expression> rexp_;
    intarkdb::Function func_info_;
};
