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
* delete_plan.h
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/include/planner/logical_plan/delete_plan.h
*
* -------------------------------------------------------------------------
*/
#pragma once

#include "datasource/table_datasource.h"
#include "planner/logical_plan/logical_plan.h"

class DeletePlan : public LogicalPlan {
   public:
    DeletePlan(LogicalPlanType type, TableDataSource& sc, LogicalPlanPtr child)
        : LogicalPlan(type), sc_(sc), child_(std::move(child)) {}

    virtual const Schema& GetSchema() const override;
    virtual std::vector<LogicalPlanPtr> Children() const override;
    virtual void SetChildren(const std::vector<LogicalPlanPtr>& children) override { child_ = children[0]; }
    virtual std::string ToString() const override;

    TableDataSource& sc_;
    LogicalPlanPtr child_;

   private:
    static inline Schema empty_schema = Schema{};
};
