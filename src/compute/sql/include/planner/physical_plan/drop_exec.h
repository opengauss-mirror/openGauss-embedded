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
* drop_exec.h
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/include/planner/physical_plan/drop_exec.h
*
* -------------------------------------------------------------------------
*/
#pragma once

#include <vector>

#include "catalog/catalog.h"
#include "planner/physical_plan/physical_plan.h"
#include "type/type_system.h"

class DropExec : public PhysicalPlan {
   public:
    DropExec(const Catalog & catalog, std::string name, bool if_exists, ObjectType type)
        : catalog_(const_cast<Catalog &>(catalog)),
        name(name),
        if_exists(if_exists),
        type(type) {}

    virtual Schema GetSchema() const override { return {schema_}; };

    virtual auto Execute() const -> RecordBatch override;

    virtual std::vector<PhysicalPlanPtr> Children() const override { return {}; }

    virtual std::string ToString() const override {
        { return "DropExec"; }
    }

    virtual auto Next() -> std::tuple<Record, knl_cursor_t*, bool> override { return {}; }

    // storage
    auto DropSynonym() const -> RecordBatch;

   private:
    Catalog & catalog_;
    std::string name;
    bool if_exists = false;
    ObjectType type;
    Schema schema_;
};