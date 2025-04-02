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
 * values_plan.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/planner/logical_plan/values_plan.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <fmt/core.h>
#include <fmt/format.h>
#include <fmt/ranges.h>

#include <memory>
#include <optional>
#include <vector>

#include "binder/bound_expression.h"
#include "planner/logical_plan/logical_plan.h"

class ValuesPlan : public LogicalPlan {
   public:
    ValuesPlan(const Schema&& schema, std::vector<std::vector<std::unique_ptr<BoundExpression>>>&& values_list)
        : LogicalPlan(LogicalPlanType::Values), values_list(std::move(values_list)), schema_(std::move(schema)) {}

    virtual const Schema& GetSchema() const override;

    virtual std::vector<LogicalPlanPtr> Children() const override { return {}; }
    virtual void SetChildren(const std::vector<LogicalPlanPtr>& children) override {}

    virtual std::string ToString() const override {
        { return "ValuesPlan"; }
    }

    std::vector<std::vector<std::unique_ptr<BoundExpression>>> values_list;

   private:
    Schema schema_;
};
