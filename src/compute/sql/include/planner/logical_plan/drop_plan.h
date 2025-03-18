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
* drop_plan.h
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/include/planner/logical_plan/drop_plan.h
*
* -------------------------------------------------------------------------
*/
#pragma once

#include "planner/logical_plan/logical_plan.h"
#include "type/type_system.h"

class DropPlan : public LogicalPlan {
   public:
    DropPlan(LogicalPlanType ltype, std::string name, bool if_exists, ObjectType type)
        : LogicalPlan(ltype), name(name), if_exists(if_exists), type(type) {}

    virtual const Schema& GetSchema() const override { return {schema_}; };

    virtual std::vector<LogicalPlanPtr> Children() const override { return {child_}; }
    virtual void SetChildren(const std::vector<LogicalPlanPtr>& children) override { child_ = children[0]; }

    virtual std::string ToString() const override {
        { return "DropPlan"; }
    }

    std::string name;
    bool if_exists = false;
    ObjectType type;

   private:
    LogicalPlanPtr child_;
    Schema schema_;
};
