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
* show_exec.h
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/include/planner/physical_plan/show_exec.h
*
* -------------------------------------------------------------------------
*/
#pragma once

#include "binder/statement/show_statement.h"
#include "catalog/catalog.h"
#include "planner/expressions/expression.h"
#include "planner/physical_plan/physical_plan.h"

class ShowExec : public PhysicalPlan {
   public:
    ShowExec(PhysicalPlanPtr child, const Catalog & catalog, ShowType showtype)
        : child_(child),
        catalog_(const_cast<Catalog &>(catalog)),
        show_type_(showtype) {}
    virtual Schema GetSchema() const override { return child_->GetSchema(); }

    auto Execute() const -> RecordBatch override;
    void Execute(RecordBatch &rb_out) override;

    virtual std::vector<PhysicalPlanPtr> Children() const override { return {child_}; }

    virtual std::string ToString() const override { return "ShowExec"; }

    virtual auto Next() -> std::tuple<Record, knl_cursor_t *, bool> override { return {}; }

    //
    void ShowTables(RecordBatch &rb_out);
    void DescribeTable(RecordBatch &rb_out);
    void ShowAll(RecordBatch &rb_out);
    void ShowVariables(RecordBatch &rb_out);
    void ShowUsers(RecordBatch &rb_out);

    std::string db_path;
    std::string variable_name_;

   private:
    PhysicalPlanPtr child_;
    Catalog & catalog_;
    ShowType show_type_;
};
