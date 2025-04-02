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
 * conj_expression.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/planner/expressions/conj_expression.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <string>

#include "common/record_batch.h"
#include "planner/expressions/expression.h"
#include "type/value.h"

class ConjunctiveExpression : public Expression {
   public:
    explicit ConjunctiveExpression(std::vector<std::unique_ptr<Expression>>&& exprs)
        : Expression(GS_TYPE_BOOLEAN), items(std::move(exprs)) {}

    auto Evaluate(const Record& record) const -> Value override {
        Trivalent result = Trivalent::TRI_TRUE;
        for (auto& item : items) {
            auto&& val = item->Evaluate(record);
            auto curr = val.IsNull() ? Trivalent::UNKNOWN : val.GetCastAs<Trivalent>();
            result = TrivalentOper::And(result, curr);
            if (result == Trivalent::TRI_FALSE) {
                return ValueFactory::ValueBool(Trivalent::TRI_FALSE);
            }
        }
        return ValueFactory::ValueBool(result);
    }

    auto ToString() const -> std::string override { return "ConjunctiveExpression"; }

    auto Reset() -> void override {
        for (auto& item : items) {
            item->Reset();
        }
    }

   private:
    std::vector<std::unique_ptr<Expression>> items;
};
