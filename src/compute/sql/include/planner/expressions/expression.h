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
 * expression.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/planner/expressions/expression.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <memory>
#include <vector>

#include "catalog/schema.h"
#include "common/record_batch.h"
#include "type/type_system.h"

class Expression;
using ExpressionPtr = std::shared_ptr<Expression>;

class Expression {
   public:
    Expression(LogicalType type) : type_{type} {}

    virtual ~Expression() = default;

    virtual auto Evaluate(const Record& record) const -> Value = 0;

    virtual auto ReEvaluate(const Record& record) const -> Value { return Evaluate(record); }

    virtual auto GetLogicalType() const -> LogicalType { return type_; }

    virtual auto ToString() const -> std::string { return "<unknown>"; }
    
    virtual auto Reset() -> void {}

   private:
    LogicalType type_;
};
