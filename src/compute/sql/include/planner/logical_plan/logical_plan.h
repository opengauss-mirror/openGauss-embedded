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
* logical_plan.h
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/include/planner/logical_plan/logical_plan.h
*
* -------------------------------------------------------------------------
*/
#pragma once

#include <fmt/core.h>
#include <fmt/format.h>

#include <memory>
#include <string>
#include <vector>

#include "catalog/schema.h"

enum class LogicalPlanType : uint8_t {
    Scan,
    Projection,
    Filter,
    Insert,
    Values,
    Update,
    Delete,
    Transaction,
    Aggregation,
    Distinct,
    Limit,
    Sort,
    Drop,
    NestedLoopJoin,
    Apply,
    Copy_To,
    Union,
    Comment_On,
    EmptySource,
    Show,
    Set,
    CreateRole,
    AlterRole,
    DropRole,
    GrantRole,
    Grant,
    Synonym,
    Window,
};

class LogicalPlan;
using LogicalPlanPtr = std::shared_ptr<LogicalPlan>;

/**
 * logical plan represents a data transformation or action that returns a relation( a set of tuples)
 */
class LogicalPlan {
   public:
    LogicalPlan(LogicalPlanType type) : type_(type) {}
    virtual const Schema &GetSchema() const = 0;

    virtual std::vector<LogicalPlanPtr> Children() const = 0;
    virtual void SetChildren(const std::vector<LogicalPlanPtr> &children) = 0;

    virtual std::string ToString() const { return "LogicalPlan"; }

    friend std::string format(const LogicalPlan &, int);
    std::string Print() const { return format(*this, 0); }

    LogicalPlanType Type() const { return type_; }

   private:
    LogicalPlanType type_;
};

template <>
struct fmt::formatter<LogicalPlanType> : formatter<std::string_view> {
    template <typename FormatContext>
    auto format(LogicalPlanType c, FormatContext &ctx) const {
        std::string_view name;
        switch (c) {
            case LogicalPlanType::Scan:
                name = "Logical_Scan";
                break;
            case LogicalPlanType::Projection:
                name = "Logical_Projecton";
                break;
            case LogicalPlanType::Filter:
                name = "Logical_Filter";
                break;
            case LogicalPlanType::Insert:
                name = "Logical_Insert";
                break;
            case LogicalPlanType::Values:
                name = "Logical_Values";
                break;
            case LogicalPlanType::Update:
                name = "Logical_Update";
                break;
            case LogicalPlanType::Delete:
                name = "Logical_Delete";
                break;
            case LogicalPlanType::Transaction:
                name = "Logical_Transaction";
                break;
            case LogicalPlanType::Aggregation:
                name = "Logical_Aggregation";
                break;
            case LogicalPlanType::Distinct:
                name = "Logical_Distinct";
                break;
            case LogicalPlanType::Limit:
                name = "Logical_Limit";
                break;
            case LogicalPlanType::Sort:
                name = "Logical_Sort";
                break;
            case LogicalPlanType::Drop:
                name = "Logical_Drop";
                break;
            case LogicalPlanType::NestedLoopJoin:
                name = "Logical_NestedLoopJoin";
                break;
            case LogicalPlanType::Apply:
                name = "Logical_Apply";
                break;
            case LogicalPlanType::Copy_To:
                name = "Logical_CopyTo";
                break;
            case LogicalPlanType::Union:
                name = "Logical_Union";
                break;
            case LogicalPlanType::Comment_On:
                name = "Logical_CommentOn";
                break;
            case LogicalPlanType::EmptySource:
                name = "Logical_EmptySource";
                break;
            case LogicalPlanType::Show:
                name = "Logical_Show";
                break;
            case LogicalPlanType::Set:
                name = "Logical_Set";
                break;
            case LogicalPlanType::CreateRole:
                name = "Logical_CreateRole";
                break;
            case LogicalPlanType::AlterRole:
                name = "Logical_AlterRole";
                break;
            case LogicalPlanType::DropRole:
                name = "Logical_DropRole";
                break;
            case LogicalPlanType::GrantRole:
                name = "Logical_GrantRole";
                break;
            case LogicalPlanType::Grant:
                name = "Logical_Grant";
                break;
            case LogicalPlanType::Synonym:
                name = "Logical_Synonym";
                break;
            case LogicalPlanType::Window:
                name = "Logical_Window";
                break;
        }
        return formatter<std::string_view>::format(name, ctx);
    }
};
