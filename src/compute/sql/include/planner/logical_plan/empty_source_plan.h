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
 * empty_source_plan.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/planner/logical_plan/empty_source_plan.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include "planner/logical_plan/logical_plan.h"

class EmptySourcePlan : public LogicalPlan {
   public:
    EmptySourcePlan(Schema schema, int return_count = 0)
        : LogicalPlan(LogicalPlanType::EmptySource), schema_(std::move(schema)), return_count_(return_count) {}

    ~EmptySourcePlan() {}
    virtual const Schema& GetSchema() const override { return schema_; }

    virtual std::vector<LogicalPlanPtr> Children() const override { return {}; }
    virtual void SetChildren(const std::vector<LogicalPlanPtr>& children) override {}

    virtual std::string ToString() const override { return "EmptySource"; }

    int GetReturnCount() const { return return_count_; }

   private:
    Schema schema_;
    int return_count_ = 0;
};
