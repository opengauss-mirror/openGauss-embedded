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
 * column_param_expression.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/planner/expressions/column_param_expression.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <fmt/core.h>
#include <fmt/format.h>

#include <memory>
#include <string>
#include <vector>

#include "binder/expressions/bound_column_def.h"
#include "catalog/schema.h"
#include "planner/expressions/constant_expression.h"
#include "planner/expressions/expression.h"
#include "type/type_system.h"

// an expression present for column
class ColumnParamExpression : public Expression {
   public:
    ColumnParamExpression(const std::vector<std::string>& column_name, int col_slot,
                          LogicalType col_type = GStorDataType::GS_TYPE_PARAM)
        : Expression(col_type), column_name_(column_name), col_slot_(col_slot) {}

    virtual auto Evaluate(const Record& record) const -> Value override { return val_; }

    auto ToString() const -> std::string override { return val_.ToString(); }

    void InitParam(const Record& rc) const { val_ = rc.Field(col_slot_); }
    void InitParam(const std::vector<Value>& values) const { val_ = values[col_slot_]; }

    std::vector<std::string> column_name_;
    int col_slot_;
    mutable Value val_;
};
