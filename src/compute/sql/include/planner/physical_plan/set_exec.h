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
* set_exec.h
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/include/planner/physical_plan/set_exec.h
*
* -------------------------------------------------------------------------
*/
#pragma once

#include "binder/statement/set_statement.h"
#include "catalog/catalog.h"
#include "planner/expressions/expression.h"
#include "planner/physical_plan/physical_plan.h"

class SetExec : public PhysicalPlan {
   public:
    SetExec(PhysicalPlanPtr child, const Catalog & catalog, SetKind setkind, SetName setname, Value setvalue)
        : child_(child),
        catalog_(const_cast<Catalog &>(catalog)),
        set_kind_(setkind),
        set_name_(setname),
        set_value_(setvalue) {}
    virtual Schema GetSchema() const override { return child_->GetSchema(); }

    auto Execute() const -> RecordBatch override;

    virtual std::vector<PhysicalPlanPtr> Children() const override { return {child_}; }

    virtual std::string ToString() const override { return "SetExec"; }

    virtual auto Next() -> std::tuple<Record, knl_cursor_t *, bool> override { return {}; }

    //
    void SetMaxConnections() const;
    void SetSynchronousCommit() const;

   private:
    PhysicalPlanPtr child_;
    Catalog & catalog_;
    SetKind set_kind_;
    SetName set_name_;
    Value set_value_;
};