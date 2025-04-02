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
* bound_expression.cpp
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/binder/bound_expression.cpp
*
* -------------------------------------------------------------------------
*/

#include "binder/bound_expression.h"

#include "binder/expressions/bound_alias.h"
#include "binder/expressions/bound_position_ref_expr.h"

auto BoundExpressionToSchemaColumnInfo(const LogicalPlanPtr &plan, const BoundExpression &expr, uint32_t slot)
    -> SchemaColumnInfo {
    SchemaColumnInfo col_info;
    col_info.slot = slot;
    if (expr.Type() == ExpressionType::ALIAS) {
        const auto &alias_expr = static_cast<const BoundAlias &>(expr);
        col_info.alias = alias_expr.alias_;
        col_info.col_name = alias_expr.child_->GetName();
    } else if (expr.Type() == ExpressionType::POSITION_REF) {
        const auto &pos_expr = static_cast<const BoundPositionRef &>(expr);
        const auto& column_info = plan->GetSchema().GetColumnInfoByIdx(pos_expr.GetIndex());
        col_info.alias = column_info.alias;
        col_info.col_name = column_info.col_name;
    } else {
        col_info.col_name = expr.GetName();
    }
    col_info.col_type = expr.ReturnType();
    return col_info;
}
