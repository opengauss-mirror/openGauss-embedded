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
* constant_expression.h
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/include/planner/expressions/constant_expression.h
*
* -------------------------------------------------------------------------
*/
#pragma once

#include <string>

#include "common/record_batch.h"
#include "planner/expressions/expression.h"
#include "type/value.h"

class ConstantExpression : public Expression {
   public:
    explicit ConstantExpression(const Value& val) : Expression(val.GetLogicalType()), val_(val) {}
    explicit ConstantExpression(Value&& val) : Expression(val.GetLogicalType()), val_(std::move(val)) {}

    auto Evaluate(const Record&) const -> Value override { return val_; }

    auto ToString() const -> std::string override { return val_.ToString(); }

   private:
    Value val_;
};
