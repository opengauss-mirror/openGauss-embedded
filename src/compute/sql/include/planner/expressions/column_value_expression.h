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
* column_value_expression.h
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/include/planner/expressions/column_value_expression.h
*
* -------------------------------------------------------------------------
*/
#pragma once

#include <fmt/core.h>
#include <fmt/format.h>

#include <memory>
#include <string>
#include <vector>

#include "catalog/schema.h"
#include "planner/expressions/expression.h"
#include "type/type_system.h"

// an expression present for column
class ColumnValueExpression : public Expression {
   public:
    ColumnValueExpression(const std::vector<std::string>& column_name, int col_slot, LogicalType type)
        : Expression(type), column_name_(column_name), col_slot_(col_slot) {}

    virtual auto Evaluate(const Record& record) const -> Value override { return record.Field(col_slot_); }

    auto ToString() const -> std::string override { return fmt::format("#{}", col_slot_); }

    auto GetName() const -> const std::vector<std::string>& { return column_name_; }

    auto GetColIdx() const -> uint32_t { return col_slot_; }

   private:
    std::vector<std::string> column_name_;  // tablename.columnname
    uint32_t col_slot_;                     // col idx in the table
};
