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
 * cast_expression.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/planner/expressions/cast_expression.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <memory>

#include "planner/expressions/expression.h"
#include "type/type_str.h"

class CastExpression : public Expression {
   public:
    CastExpression(LogicalType target_type, std::unique_ptr<Expression> child, bool try_cast)
        : Expression(target_type), target_type_(target_type), child_(std::move(child)), try_cast_(try_cast) {}
    virtual auto Evaluate(const Record& record) const -> Value override;

    /** @return the string representation of the plan node and its children */
    virtual auto ToString() const -> std::string override {
        if (try_cast_) {
            return fmt::format("TRY_CAST ( {} AS {} )", child_->ToString(), target_type_);
        }
        return fmt::format("CAST ( {} AS {} )", child_->ToString(), target_type_);
    }

    virtual auto Reset() -> void override { child_->Reset(); }

   private:
    LogicalType target_type_;
    std::unique_ptr<Expression> child_;
    bool try_cast_{false};
};
