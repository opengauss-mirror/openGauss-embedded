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
 * projection_plan.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/planner/logical_plan/projection_plan.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <memory>
#include <optional>
#include <vector>

#include "binder/bound_expression.h"
#include "planner/logical_plan/logical_plan.h"

class ProjectionPlan : public LogicalPlan {
   public:
    ProjectionPlan(LogicalPlanType type, std::vector<std::unique_ptr<BoundExpression>>&& exprs, LogicalPlanPtr plan)
        : LogicalPlan(type), exprs_(std::move(exprs)), child_{plan} {}
    ProjectionPlan(Schema schema, std::vector<std::unique_ptr<BoundExpression>>&& exprs, LogicalPlanPtr plan)
        : LogicalPlan(LogicalPlanType::Projection), exprs_(std::move(exprs)), child_(plan), schema_(schema) {}
    virtual const Schema& GetSchema() const override;
    virtual std::vector<LogicalPlanPtr> Children() const override;
    virtual void SetChildren(const std::vector<LogicalPlanPtr>& children) override { child_ = children[0]; }
    virtual std::string ToString() const override;

    LogicalPlanPtr GetLastPlan() const { return child_; }

    std::vector<std::unique_ptr<BoundExpression>>& Exprs() { return exprs_; }

    // 返回一个可修改的引用
    Schema& GetSchemaRef() {
        if (!schema_.has_value()) {
            InitSchema();
        }
        return schema_.value();
    }

   private:
    void InitSchema() const;

   private:
    std::vector<std::unique_ptr<BoundExpression>> exprs_;
    LogicalPlanPtr child_;
    mutable std::optional<Schema> schema_;
};
