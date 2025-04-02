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
* expression_util.h
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/include/common/expression_util.h
*
* -------------------------------------------------------------------------
*/
#pragma once

#include "binder/bound_expression.h"
#include "binder/expressions/bound_binary_op.h"
#include "binder/expressions/bound_column_def.h"
#include "planner/expression_iterator.h"

class ExpressionUtil {
   public:
    static auto IsEqualFunctionName(const std::string& name) -> bool { return name == "=" || name == "=="; }

    static auto IsEqualExpr(const BoundExpression& expr) -> bool {
        if (expr.Type() == ExpressionType::BINARY_OP) {
            auto& binary_expr = static_cast<const BoundBinaryOp&>(expr);
            if (IsEqualFunctionName(binary_expr.OpName())) {
                return true;
            }
        }
        return false;
    }

    static auto GetRefColumns(BoundExpression& expr) -> std::vector<SchemaColumnInfo> {
        std::vector<SchemaColumnInfo> columns;
        ExpressionIterator::EnumerateExpression(expr, [&](const BoundExpression& expr) {
            if (expr.Type() == ExpressionType::COLUMN_REF) {
                auto& column = static_cast<const BoundColumnRef&>(expr);
                SchemaColumnInfo col_info;
                col_info.col_name = column.Name();
                col_info.col_type = column.ReturnType();
                columns.push_back(col_info);
            }
        });
        return columns;
    }
};
