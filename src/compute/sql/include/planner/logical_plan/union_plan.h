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
 * union_plan.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/planner/logical_plan/union_plan.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <memory>
#include <optional>

#include "binder/bound_expression.h"
#include "common/set_operation_type.h"
#include "planner/logical_plan/logical_plan.h"

class UnionPlan : public LogicalPlan {
   public:
    UnionPlan(std::vector<std::unique_ptr<BoundExpression>> select_list, LogicalPlanPtr left, LogicalPlanPtr right,
              SetOperationType type, bool is_distinct)
        : LogicalPlan(LogicalPlanType::Union),
          select_list(std::move(select_list)),
          left(std::move(left)),
          right(std::move(right)),
          set_op_type(type),
          is_distinct(is_distinct) {}

    virtual const Schema& GetSchema() const override {
        if (!schema_.has_value()) {
            InitSchema();
        }
        return schema_.value();
    }

    virtual std::vector<LogicalPlanPtr> Children() const override { return {left, right}; }
    virtual void SetChildren(const std::vector<LogicalPlanPtr>& children) override {
        left = children[0];
        right = children[1];
    }

    virtual std::string ToString() const override {
        std::vector<std::string> to_strings;
        const Schema& schema = GetSchema();
        const auto& columns = schema.GetColumnInfos();
        for (size_t i = 0; i < columns.size(); ++i) {
            to_strings.push_back(columns[i].GetColNameWithoutTableName());
        }
        return fmt::format("Union: {}", fmt::join(to_strings, ", "));
    }

    std::vector<std::unique_ptr<BoundExpression>> select_list;
    LogicalPlanPtr left;
    LogicalPlanPtr right;
    SetOperationType set_op_type;
    bool is_distinct;

   private:
    void InitSchema() const {
        if (!schema_.has_value()) {
            std::vector<SchemaColumnInfo> columns;
            columns.reserve(select_list.size());
            for (size_t i = 0; i < select_list.size(); ++i) {
                if (i < left->GetSchema().GetColumnInfos().size()) {
                    columns.push_back(BoundExpressionToSchemaColumnInfo(left, *select_list[i], i));
                } else {
                    columns.push_back(BoundExpressionToSchemaColumnInfo(right, *select_list[i], i));
                }
            }
            schema_ = Schema(std::move(columns));
        }
    }

   private:
    mutable std::optional<Schema> schema_;
};
