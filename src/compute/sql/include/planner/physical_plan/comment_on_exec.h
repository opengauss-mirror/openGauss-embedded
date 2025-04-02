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
* comment_on_exec.h
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/include/planner/physical_plan/comment_on_exec.h
*
* -------------------------------------------------------------------------
*/
#pragma once

#include "catalog/catalog.h"
#include "planner/physical_plan/physical_plan.h"

class CommentOnExec : public PhysicalPlan {
   public:
    CommentOnExec(Catalog *catalog, ObjectType obj_type, const std::string &user,
        const std::string &table, const std::string &column, const std::string &comment_str)
        : catalog_(catalog),
        object_type_(obj_type),
        user_name(user),
        table_name(table),
        column_name(column),
        comment(comment_str) {}

    virtual Schema GetSchema() const override { return schema_; };

    virtual auto Execute() const -> RecordBatch override;

    virtual std::vector<PhysicalPlanPtr> Children() const override { return {}; }

    virtual std::string ToString() const override { return "CommentOnExec"; }

    virtual auto Next() -> std::tuple<Record, knl_cursor_t*, bool> override { return {}; }


   private:
    Catalog *catalog_;
    ObjectType object_type_;

    std::string user_name;
    std::string table_name;
    std::string column_name;
    std::string comment;

    Schema schema_;
};
