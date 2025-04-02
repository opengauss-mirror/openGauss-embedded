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
* create_role_plan.h
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/include/planner/logical_plan/create_role_plan.h
*
* -------------------------------------------------------------------------
*/
#pragma once

#include <memory>
#include <vector>

#include "binder/statement/role_statement.h"
#include "planner/logical_plan/logical_plan.h"

// create user/role
class CreateRolePlan : public LogicalPlan {
   public:
    CreateRolePlan(LogicalPlanType type,
                    RoleStmtType role_stmt_type,
                    const std::string& name,
                    const std::string& password,
                    LogicalPlanPtr child)
        : LogicalPlan(LogicalPlanType::CreateRole),
        role_stmt_type_(role_stmt_type),
        name_(name),
        password_(password),
        child_(child) {}

    virtual const Schema& GetSchema() const override { return empty_schema; }

    virtual std::vector<LogicalPlanPtr> Children() const override { return {child_}; }
    virtual void SetChildren(const std::vector<LogicalPlanPtr>& children) override {}

    LogicalPlanPtr GetLastPlan() const { return child_; }

    virtual std::string ToString() const override {
        return "CreateRolePlan";
    }

    RoleStmtType role_stmt_type_;
    std::string name_;
    std::string password_;

   private:
    LogicalPlanPtr child_{nullptr};
    static inline Schema empty_schema = Schema{};
};

// alter user/role
class AlterRolePlan : public LogicalPlan {
   public:
    AlterRolePlan(LogicalPlanType type,
                    AlterRoleStatement &statement,
                    LogicalPlanPtr child)
        : LogicalPlan(LogicalPlanType::AlterRole),
        alter_role_statement(statement),
        child_(child) {}

    virtual const Schema& GetSchema() const override { return empty_schema; }

    virtual std::vector<LogicalPlanPtr> Children() const override { return {child_}; }
    virtual void SetChildren(const std::vector<LogicalPlanPtr>& children) override {}

    LogicalPlanPtr GetLastPlan() const { return child_; }

    virtual std::string ToString() const override {
        return "AlterRolePlan";
    }

    AlterRoleStatement &alter_role_statement;

   private:
    LogicalPlanPtr child_{nullptr};
    static inline Schema empty_schema = Schema{};
};

// drop user/role
class DropRolePlan : public LogicalPlan {
   public:
    DropRolePlan(LogicalPlanType type, RoleStmtType role_stmt_type, const std::vector<std::string>& roles, LogicalPlanPtr child)
        : LogicalPlan(LogicalPlanType::DropRole),
        role_stmt_type_(role_stmt_type),
        roles_(roles),
        child_(child) {}

    virtual const Schema& GetSchema() const override { return empty_schema; }

    virtual std::vector<LogicalPlanPtr> Children() const override { return {child_}; }
    virtual void SetChildren(const std::vector<LogicalPlanPtr>& children) override {}

    LogicalPlanPtr GetLastPlan() const { return child_; }

    virtual std::string ToString() const override {
        return "DropRolePlan";
    }

    RoleStmtType role_stmt_type_;
    std::vector<std::string> roles_;

   private:
    LogicalPlanPtr child_{nullptr};
    static inline Schema empty_schema = Schema{};
};

// grant role to user
class GrantRolePlan : public LogicalPlan {
   public:
    GrantRolePlan(LogicalPlanType type,
                    GrantRoleStatement &statement,
                    LogicalPlanPtr child)
        : LogicalPlan(LogicalPlanType::GrantRole),
        grant_role_statement(statement),
        child_(child) {}

    virtual const Schema& GetSchema() const override { return empty_schema; }

    virtual std::vector<LogicalPlanPtr> Children() const override { return {child_}; }
    virtual void SetChildren(const std::vector<LogicalPlanPtr>& children) override {}

    LogicalPlanPtr GetLastPlan() const { return child_; }

    virtual std::string ToString() const override {
        return "GrantRolePlan";
    }

    GrantRoleStatement &grant_role_statement;

   private:
    LogicalPlanPtr child_{nullptr};
    static inline Schema empty_schema = Schema{};
};

// grant privileges to user
class GrantPlan : public LogicalPlan {
   public:
    GrantPlan(LogicalPlanType type,
                    GrantStatement &statement,
                    LogicalPlanPtr child)
        : LogicalPlan(LogicalPlanType::Grant),
        grant_statement(statement),
        child_(child) {}

    virtual const Schema& GetSchema() const override { return empty_schema; }

    virtual std::vector<LogicalPlanPtr> Children() const override { return {child_}; }
    virtual void SetChildren(const std::vector<LogicalPlanPtr>& children) override {}

    LogicalPlanPtr GetLastPlan() const { return child_; }

    virtual std::string ToString() const override {
        return "GrantPlan";
    }

    GrantStatement &grant_statement;

   private:
    LogicalPlanPtr child_{nullptr};
    static inline Schema empty_schema = Schema{};
};
