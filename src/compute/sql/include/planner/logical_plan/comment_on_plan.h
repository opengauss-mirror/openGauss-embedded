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
* comment_on_plan.h
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/include/planner/logical_plan/comment_on_plan.h
*
* -------------------------------------------------------------------------
*/
#pragma once

#include "binder/bound_statement.h"
#include "catalog/catalog.h"
#include "planner/logical_plan/logical_plan.h"

class CommentOnPlan : public LogicalPlan {
   public:
    CommentOnPlan(Catalog *catalog, PGObjectType obj_type, const std::string &user, const std::string &table,
                  const std::string &column, const std::string &comment_str)
        : LogicalPlan(LogicalPlanType::Comment_On),
          catalog_(catalog),
          object_type_(obj_type),
          user_name(user),
          table_name(table),
          column_name(column),
          comment(comment_str) {}

    virtual const Schema &GetSchema() const override { return empty_schema_; };
    virtual std::vector<LogicalPlanPtr> Children() const override { return {}; }
    virtual void SetChildren(const std::vector<LogicalPlanPtr> &children) override {}

    virtual std::string ToString() const override {
        { return "CommentOnPlan"; }
    }

   public:
    Catalog *catalog_;
    PGObjectType object_type_;

    std::string user_name;
    std::string table_name;
    std::string column_name;
    std::string comment;

   private:
    static inline Schema empty_schema_ = Schema{};
};
