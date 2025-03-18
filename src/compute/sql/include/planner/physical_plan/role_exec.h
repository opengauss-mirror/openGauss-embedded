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
* create_role_exec.h
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/include/planner/physical_plan/create_role_exec.h
*
* -------------------------------------------------------------------------
*/
#pragma once

#include "binder/statement/role_statement.h"
#include "catalog/catalog.h"
#include "planner/expressions/expression.h"
#include "planner/physical_plan/physical_plan.h"

class CreateRoleExec : public PhysicalPlan {
   public:
    CreateRoleExec(PhysicalPlanPtr child,
                    const Catalog & catalog,
                    RoleStmtType role_stmt_type,
                    const std::string& name,
                    const std::string& password)
        : child_(child),
        catalog_(catalog),
        role_stmt_type_(role_stmt_type),
        name_(name),
        password_(password) {}
    virtual Schema GetSchema() const override { return Schema(); }

    auto Execute() const -> RecordBatch override;

    virtual std::vector<PhysicalPlanPtr> Children() const override { return {child_}; }

    virtual std::string ToString() const override { return "CreateRoleExec"; }

    virtual auto Next() -> std::tuple<Record, knl_cursor_t *, bool> override { return {}; }

    // Perform relevant operations on the storage engine
    void CreateRole() const;
    void CreateUser() const;

   private:
    PhysicalPlanPtr child_;
    const Catalog & catalog_;

    RoleStmtType role_stmt_type_;
    std::string name_;
    std::string password_;
};

class AlterRoleExec : public PhysicalPlan {
   public:
    AlterRoleExec(PhysicalPlanPtr child,
                    const Catalog & catalog,
                    AlterRoleStatement &statement)
        : child_(child),
        catalog_(const_cast<Catalog &>(catalog)),
        alter_role_statement(statement) {}
    virtual Schema GetSchema() const override { return Schema(); }

    auto Execute() const -> RecordBatch override;

    virtual std::vector<PhysicalPlanPtr> Children() const override { return {child_}; }

    virtual std::string ToString() const override { return "AlterRoleExec"; }

    virtual auto Next() -> std::tuple<Record, knl_cursor_t *, bool> override { return {}; }

   private:
    PhysicalPlanPtr child_;
    Catalog & catalog_;

    AlterRoleStatement &alter_role_statement;
};

class DropRoleExec : public PhysicalPlan {
   public:
    DropRoleExec(PhysicalPlanPtr child, const Catalog & catalog, RoleStmtType role_stmt_type,
                std::vector<std::string> roles)
        : child_(child),
        catalog_(catalog),
        role_stmt_type_(role_stmt_type),
        roles_(roles) {}
    virtual Schema GetSchema() const override { return Schema(); }

    auto Execute() const -> RecordBatch override;

    virtual std::vector<PhysicalPlanPtr> Children() const override { return {child_}; }

    virtual std::string ToString() const override { return "DropRoleExec"; }

    virtual auto Next() -> std::tuple<Record, knl_cursor_t *, bool> override { return {}; }

    // Perform relevant operations on the storage engine
    void DropRole() const;
    void DropUser() const;

   private:
    PhysicalPlanPtr child_;
    const Catalog & catalog_;

    RoleStmtType role_stmt_type_;
    std::vector<std::string> roles_;
};

class GrantRoleExec : public PhysicalPlan {
   public:
    GrantRoleExec(PhysicalPlanPtr child,
                    const Catalog & catalog,
                    GrantRoleStatement &statement)
        : child_(child),
        catalog_(catalog),
        grant_role_statement(statement) {}
    virtual Schema GetSchema() const override { return Schema(); }

    auto Execute() const -> RecordBatch override;

    virtual std::vector<PhysicalPlanPtr> Children() const override { return {child_}; }

    virtual std::string ToString() const override { return "GrantRoleExec"; }

    virtual auto Next() -> std::tuple<Record, knl_cursor_t *, bool> override { return {}; }

    // Perform relevant operations on the storage engine
    void GrantRole() const;
    void RevokeRole() const;

   private:
    PhysicalPlanPtr child_;
    const Catalog & catalog_;

    GrantRoleStatement &grant_role_statement;
};

class GrantExec : public PhysicalPlan {
   public:
    GrantExec(PhysicalPlanPtr child,
                    const Catalog & catalog,
                    GrantStatement &statement)
        : child_(child),
        catalog_(catalog),
        grant_statement(statement) {}
    virtual Schema GetSchema() const override { return Schema(); }

    auto Execute() const -> RecordBatch override;

    virtual std::vector<PhysicalPlanPtr> Children() const override { return {child_}; }

    virtual std::string ToString() const override { return "GrantExec"; }

    virtual auto Next() -> std::tuple<Record, knl_cursor_t *, bool> override { return {}; }

    uint32_t GetPrivIdByName(std::string name) const;
    object_type_t TransObjectType(duckdb_libpgquery::PGObjectType obj_type) const;
    priv_type_def TransPrivType(object_type_t obj_type) const;

    // Perform relevant operations on the storage engine
    void GrantPrivileges() const;
    void RevokePrivileges() const;

   private:
    PhysicalPlanPtr child_;
    const Catalog & catalog_;

    GrantStatement &grant_statement;
};
