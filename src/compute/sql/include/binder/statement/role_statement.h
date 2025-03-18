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
* create_role_statement.h
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/include/binder/statement/create_role_statement.h
*
* -------------------------------------------------------------------------
*/
#pragma once

#include "binder/bound_statement.h"
#include "nodes/parsenodes.hpp"

enum class RoleStmtType : int8_t {
    ROLE_STMT_ROLE = 0,      // ROLE
    ROLE_STMT_USER = 1,      // USER
    ROLE_STMT_GROUP = 2,     // GROUP
};

class CreateRoleStatement : public BoundStatement {
   public:
    explicit CreateRoleStatement() : BoundStatement(StatementType::CREATE_ROLE_STATEMENT) {}

   public:
    RoleStmtType role_stmt_type;
    std::string name;
    std::string password;
};

class AlterRoleStatement : public BoundStatement {
   public:
    explicit AlterRoleStatement() : BoundStatement(StatementType::ALTER_ROLE_STATEMENT) {}

   public:
    RoleStmtType role_stmt_type;
    std::string name;
    std::string password;
};

template <>
struct fmt::formatter<RoleStmtType> : formatter<std::string_view> {
    template <typename FormatContext>
    auto format(RoleStmtType c, FormatContext &ctx) const {
        std::string_view name;
        switch (c) {
            case RoleStmtType::ROLE_STMT_ROLE:
                name = "ROLE_STMT_ROLE";
                break;
            case RoleStmtType::ROLE_STMT_USER:
                name = "ROLE_STMT_USER";
                break;
            case RoleStmtType::ROLE_STMT_GROUP:
                name = "ROLE_STMT_GROUP";
                break;
            default:
                name = "ROLE_STMT_UNKNOWN";
                break;
        }
        return formatter<std::string_view>::format(name, ctx);
    }
};

class DropRoleStatement : public BoundStatement {
   public:
    explicit DropRoleStatement() : BoundStatement(StatementType::DROP_ROLE_STATEMENT) {}

   public:
    RoleStmtType role_stmt_type;
    std::vector<std::string> roles;
};

// ------------------------------grant role---------------------------//
class GrantRoleStatement : public BoundStatement {
   public:
    explicit GrantRoleStatement() : BoundStatement(StatementType::GRANT_ROLE_STATEMENT) {}

   public:
    RoleStmtType role_stmt_type;
    bool is_grant;
    std::vector<std::string> granted_roles;
    std::vector<std::string> grantee_roles;
    std::string grantor;
};

// ------------------------------grant-------------------------------//
class GrantStatement : public BoundStatement {
   public:
    explicit GrantStatement() : BoundStatement(StatementType::GRANT_STATEMENT) {}

   public:
    RoleStmtType role_stmt_type;
    bool is_grant;
    duckdb_libpgquery::PGGrantTargetType target_type;
    duckdb_libpgquery::PGObjectType obj_type;
    std::vector<std::string> schemas;
    std::vector<std::string> objects;
    std::vector<std::string> privileges;
    bool is_all_privileges{false};
    std::vector<std::string> grantees;
    bool grant_option;
    std::string grantor;
};
