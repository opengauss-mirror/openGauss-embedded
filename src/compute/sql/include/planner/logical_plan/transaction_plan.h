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
* transaction_plan.h
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/include/planner/logical_plan/transaction_plan.h
*
* -------------------------------------------------------------------------
*/
#pragma once

#include "binder/statement/transaction_statement.h"
#include "planner/logical_plan/logical_plan.h"

class TransactionPlan : public LogicalPlan {
   public:
    TransactionPlan(LogicalPlanType ltype, void* handle, TransactionType type)
        : LogicalPlan(ltype), handle(handle), type(type) {}

    virtual const Schema& GetSchema() const override { return schema_; };

    virtual std::vector<LogicalPlanPtr> Children() const override { return {child_}; }
    virtual void SetChildren(const std::vector<LogicalPlanPtr>& children) override { child_ = children[0]; }

    virtual std::string ToString() const override {
        { return "TransactionPlan"; }
    }

    void* handle;
    TransactionType type;

   private:
    LogicalPlanPtr child_;
    Schema schema_ = Schema();
};
