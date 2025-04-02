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
* synonym_exec.h
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/include/planner/physical_plan/synonym_exec.h
*
* -------------------------------------------------------------------------
*/
#pragma once

#include "binder/statement/synonym_statement.h"
#include "catalog/catalog.h"
#include "planner/expressions/expression.h"
#include "planner/physical_plan/physical_plan.h"

class SynonymExec : public PhysicalPlan {
   public:
    SynonymExec(PhysicalPlanPtr child,
                    const Catalog & catalog,
                    SynonymStatement &statement)
        : child_(child),
        catalog_(catalog),
        synonym_statement(statement) {}
    virtual Schema GetSchema() const override { return Schema(); }

    auto Execute() const -> RecordBatch override;

    virtual std::vector<PhysicalPlanPtr> Children() const override { return {child_}; }

    virtual std::string ToString() const override { return "SynonymExec"; }

    virtual auto Next() -> std::tuple<Record, knl_cursor_t *, bool> override { return {}; }

   private:
    PhysicalPlanPtr child_;
    const Catalog & catalog_;

    SynonymStatement &synonym_statement;
};
