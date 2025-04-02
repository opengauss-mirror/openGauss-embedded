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
* bind_role.cpp
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/binder/bind_role.cpp
*
* -------------------------------------------------------------------------
*/
#include "binder/binder.h"
#include "binder/transform_typename.h"
#include "common/exception.h"
#include "common/null_check_ptr.h"
#include "common/string_util.h"
#include "common/util.h"
#include "common/grant_type.h"
#include "common/object_type.h"

// -------------------------------------------BindCreateRole-----------------------------------------//
auto BindCreateRoleInternal(duckdb_libpgquery::PGCreateRoleStmt *stmt) -> std::unique_ptr<CreateRoleStatement> {
    auto result = std::make_unique<CreateRoleStatement>();

    if (!stmt->role) {
        throw intarkdb::Exception(ExceptionType::BINDER, "Role name is null");
    } else if (strlen(stmt->role) >= GS_NAME_BUFFER_SIZE) {
        throw intarkdb::Exception(ExceptionType::BINDER, "Role name too long");
    }
    result->role_stmt_type = RoleStmtType::ROLE_STMT_ROLE;
    result->name = stmt->role;

    auto list = stmt->options;
    if (list) {
        for (auto node = list->head; node != nullptr; node = node->next) {
            auto item = NullCheckPtrCast<duckdb_libpgquery::PGNode>(node->data.ptr_value);
            if (item->type == duckdb_libpgquery::T_PGDefElem) {
                auto role_def = NullCheckPtrCast<duckdb_libpgquery::PGDefElem>(item.get());
                std::string defname = role_def->defname;
                if (defname == "password") {
                    auto target = NullCheckPtrCast<duckdb_libpgquery::PGValue>(role_def->arg);
                    result->password = target->val.str;
                }

                if (role_def->defaction != duckdb_libpgquery::PGDefElemAction::PG_DEFELEM_UNSPEC) {
                    throw intarkdb::Exception(ExceptionType::BINDER, "bad defaction");
                }
            } else {
                throw intarkdb::Exception(ExceptionType::NOT_IMPLEMENTED,
                                            fmt::format("item->type {}", Binder::ConvertNodeTagToString(item->type)));
            }
        }
    }

    return result;
}

auto BindCreateUserInternal(duckdb_libpgquery::PGCreateRoleStmt *stmt) -> std::unique_ptr<CreateRoleStatement> {
    auto result = std::make_unique<CreateRoleStatement>();

    if (!stmt->role) {
        throw intarkdb::Exception(ExceptionType::BINDER, "User name is null");
    } else if (strlen(stmt->role) >= GS_NAME_BUFFER_SIZE) {
        throw intarkdb::Exception(ExceptionType::BINDER, "User name too long");
    }
    result->role_stmt_type = RoleStmtType::ROLE_STMT_USER;
    result->name = stmt->role;

    auto list = stmt->options;
    bool is_password_set = false;
    if (list) {
        for (auto node = list->head; node != nullptr; node = node->next) {
            auto item = NullCheckPtrCast<duckdb_libpgquery::PGNode>(node->data.ptr_value);

            if (item->type == duckdb_libpgquery::T_PGDefElem) {
                auto user_def = NullCheckPtrCast<duckdb_libpgquery::PGDefElem>(item.get());
                std::string defname = user_def->defname;
                if (defname == "password") {
                    auto target = NullCheckPtrCast<duckdb_libpgquery::PGValue>(user_def->arg);
                    result->password = target->val.str;
                    is_password_set = true;
                } else {
                    throw intarkdb::Exception(ExceptionType::BINDER, fmt::format("{} not support", defname));
                }

                if (user_def->defaction != duckdb_libpgquery::PGDefElemAction::PG_DEFELEM_UNSPEC) {
                    throw intarkdb::Exception(ExceptionType::BINDER, "bad defaction");
                }
            } else {
                throw intarkdb::Exception(ExceptionType::NOT_IMPLEMENTED,
                                            fmt::format("item->type {}", Binder::ConvertNodeTagToString(item->type)));
            }
        }
    }
    if (!is_password_set) {
        throw intarkdb::Exception(ExceptionType::BINDER, "create user must set password");
    }

    return result;
}

auto BindCreateGroupInternal(duckdb_libpgquery::PGCreateRoleStmt *stmt) -> std::unique_ptr<CreateRoleStatement> {
    throw intarkdb::Exception(ExceptionType::BINDER, "Not support create group");
}

auto Binder::BindCreateRole(duckdb_libpgquery::PGCreateRoleStmt *stmt) -> std::unique_ptr<CreateRoleStatement> {
    auto result = std::make_unique<CreateRoleStatement>();

    switch (stmt->stmt_type) {
        case duckdb_libpgquery::PGRoleStmtType::ROLESTMT_ROLE:
            // CheckSysPrivilege
            if (catalog_.CheckSysPrivilege(CREATE_ROLE) != GS_TRUE) {
                throw intarkdb::Exception(ExceptionType::BINDER,
                                        fmt::format("user {} create role permission denied!", user_));
            }
            return BindCreateRoleInternal(stmt);
        case duckdb_libpgquery::PGRoleStmtType::ROLESTMT_USER:
            if (catalog_.CheckSysPrivilege(CREATE_USER) != GS_TRUE) {
                throw intarkdb::Exception(ExceptionType::BINDER,
                                        fmt::format("user {} create user permission denied!", user_));
            }
            return BindCreateUserInternal(stmt);
        case duckdb_libpgquery::PGRoleStmtType::ROLESTMT_GROUP:
            return BindCreateGroupInternal(stmt);
        default:
            throw intarkdb::Exception(ExceptionType::BINDER, "Bad role stmt type");
    }

    return result;
}

// --------------------------------------------BindAlterRole------------------------------------------//
auto Binder::BindAlterRole(duckdb_libpgquery::PGAlterRoleStmt *stmt) -> std::unique_ptr<AlterRoleStatement> {
    auto result = std::make_unique<AlterRoleStatement>();

    if (stmt->role) {
        auto role_spec = NullCheckPtrCast<duckdb_libpgquery::PGRoleSpec>(stmt->role);
        if (role_spec->roletype != duckdb_libpgquery::PGRoleSpecType::ROLESPEC_CSTRING) {
            throw intarkdb::Exception(ExceptionType::BINDER, "bad role type");
        }
        result->name = role_spec->rolename;
    }

    // CheckSysPrivilege
    if (catalog_.CheckSysPrivilege(ALTER_ANY_ROLE)) {
        // do not thing
    } else if (catalog_.CheckSysPrivilege(ALTER_USER)) {
        // check if is current user
        if (result->name != user_) {
            throw intarkdb::Exception(ExceptionType::BINDER,
                                    fmt::format("user {} alter other user permission denied!", user_));
        }
    } else {
        throw intarkdb::Exception(ExceptionType::BINDER,
                                    fmt::format("user {} alter user/role permission denied!", user_));
    }

    auto list = stmt->options;
    bool is_password_set = false;
    if (list) {
        for (auto node = list->head; node != nullptr; node = node->next) {
            auto item = NullCheckPtrCast<duckdb_libpgquery::PGNode>(node->data.ptr_value);

            if (item->type == duckdb_libpgquery::T_PGDefElem) {
                auto user_def = NullCheckPtrCast<duckdb_libpgquery::PGDefElem>(item.get());
                std::string defname = user_def->defname;
                if (defname == "password") {
                    if (user_def->arg) {
                        auto target = NullCheckPtrCast<duckdb_libpgquery::PGValue>(user_def->arg);
                        result->password = target->val.str;
                        is_password_set = true;
                    } else {
                        throw intarkdb::Exception(ExceptionType::BINDER, "password can't be null");
                    }
                } else {
                    throw intarkdb::Exception(ExceptionType::BINDER, fmt::format("{} not support", defname));
                }

                if (user_def->defaction != duckdb_libpgquery::PGDefElemAction::PG_DEFELEM_UNSPEC) {
                    throw intarkdb::Exception(ExceptionType::BINDER, "bad defaction");
                }
            } else {
                throw intarkdb::Exception(ExceptionType::NOT_IMPLEMENTED,
                                            fmt::format("item->type {}", Binder::ConvertNodeTagToString(item->type)));
            }
        }
    }
    if (!is_password_set) {
        throw intarkdb::Exception(ExceptionType::BINDER, "alter user only supports changing password");
    }
    return result;
}

// --------------------------------------------BindDropRole------------------------------------------//
auto Binder::BindDropRole(duckdb_libpgquery::PGDropRoleStmt *stmt) -> std::unique_ptr<DropRoleStatement> {
    auto result = std::make_unique<DropRoleStatement>();

    switch (stmt->stmt_type) {
        case duckdb_libpgquery::PGRoleStmtType::ROLESTMT_ROLE:
            // CheckSysPrivilege
            if (catalog_.CheckSysPrivilege(DROP_ANY_ROLE) != GS_TRUE) {
                throw intarkdb::Exception(ExceptionType::BINDER,
                                        fmt::format("user {} drop role permission denied!", user_));
            }
            result->role_stmt_type = RoleStmtType::ROLE_STMT_ROLE;
            break;
        case duckdb_libpgquery::PGRoleStmtType::ROLESTMT_USER:
            if (catalog_.CheckSysPrivilege(DROP_USER) != GS_TRUE) {
                throw intarkdb::Exception(ExceptionType::BINDER,
                                        fmt::format("user {} drop user permission denied!", user_));
            }
            result->role_stmt_type = RoleStmtType::ROLE_STMT_USER;
            break;
        case duckdb_libpgquery::PGRoleStmtType::ROLESTMT_GROUP:
            result->role_stmt_type = RoleStmtType::ROLE_STMT_GROUP;
            break;
        default:
            throw intarkdb::Exception(ExceptionType::BINDER, "Bad role stmt type");
    }

    if (!stmt->roles) {
        throw intarkdb::Exception(ExceptionType::BINDER, "Role list is null");
    }

    auto list = stmt->roles;
    if (list) {
        for (auto node = list->head; node != nullptr; node = node->next) {
            auto item = NullCheckPtrCast<duckdb_libpgquery::PGNode>(node->data.ptr_value);

            if (item->type == duckdb_libpgquery::T_PGRoleSpec) {
                auto role_spec = NullCheckPtrCast<duckdb_libpgquery::PGRoleSpec>(item.get());

                if (role_spec->roletype != duckdb_libpgquery::PGRoleSpecType::ROLESPEC_CSTRING) {
                    throw intarkdb::Exception(ExceptionType::BINDER, "bad role type");
                }
                result->roles.push_back(role_spec->rolename);
            } else {
                throw intarkdb::Exception(ExceptionType::NOT_IMPLEMENTED,
                                            fmt::format("role spec roletype {}", ConvertNodeTagToString(item->type)));
            }
        }
    }

    return result;
}

// -----------------------------------------BindGrantRole---------------------------------------//
auto Binder::BindGrantRole(duckdb_libpgquery::PGGrantRoleStmt *stmt) -> std::unique_ptr<GrantRoleStatement> {
    // CheckSysPrivilege
    if (catalog_.CheckSysPrivilege(GRANT_ANY_ROLE) != GS_TRUE) {
        throw intarkdb::Exception(ExceptionType::BINDER,
                                fmt::format("user {} grant/revoke role permission denied!", user_));
    }

    auto result = std::make_unique<GrantRoleStatement>();

    // is grant(true : grant, false : revoke)
    result->is_grant = stmt->is_grant;

    // granted_roles
    auto granted_roles_list = stmt->granted_roles;
    if (granted_roles_list) {
        for (auto node = granted_roles_list->head; node != nullptr; node = node->next) {
            auto item = NullCheckPtrCast<duckdb_libpgquery::PGNode>(node->data.ptr_value);
            if (item->type == duckdb_libpgquery::T_PGAccessPriv) {
                auto priv = NullCheckPtrCast<duckdb_libpgquery::PGAccessPriv>(item.get());
                result->granted_roles.push_back(priv->priv_name);
            } else {
                throw intarkdb::Exception(ExceptionType::NOT_IMPLEMENTED,
                                            fmt::format("role spec roletype {}", ConvertNodeTagToString(item->type)));
            }
        }
    }

    // grantee_roles
    auto grantee_roles_list = stmt->grantee_roles;
    if (grantee_roles_list) {
        for (auto node = grantee_roles_list->head; node != nullptr; node = node->next) {
            auto item = NullCheckPtrCast<duckdb_libpgquery::PGNode>(node->data.ptr_value);
            if (item->type == duckdb_libpgquery::T_PGRoleSpec) {
                auto role_spec = NullCheckPtrCast<duckdb_libpgquery::PGRoleSpec>(item.get());
                result->grantee_roles.push_back(role_spec->rolename);
                if (role_spec->roletype != duckdb_libpgquery::PGRoleSpecType::ROLESPEC_CSTRING) {
                    throw intarkdb::Exception(ExceptionType::BINDER, "bad role type");
                }
            } else {
                throw intarkdb::Exception(ExceptionType::NOT_IMPLEMENTED,
                                            fmt::format("role spec roletype {}", ConvertNodeTagToString(item->type)));
            }
        }
    }

    // grantor
    auto grantor = NullCheckPtrCast<duckdb_libpgquery::PGRoleSpec>(stmt->grantor);
    if (grantor) {
        if (grantor->roletype != duckdb_libpgquery::PGRoleSpecType::ROLESPEC_CSTRING) {
            throw intarkdb::Exception(ExceptionType::BINDER, "bad role type");
        }
        result->grantor = grantor->rolename;
    }

    return result;
}

// -----------------------------------------BindGrant---------------------------------------//
auto Binder::BindGrant(duckdb_libpgquery::PGGrantStmt *stmt) -> std::unique_ptr<GrantStatement> {
    // CheckSysPrivilege
    if (catalog_.CheckSysPrivilege(GRANT_ANY_OBJECT_PRIVILEGE) != GS_TRUE) {
        throw intarkdb::Exception(ExceptionType::BINDER,
                                fmt::format("user {} grant/revoke object permission denied!", user_));
    }

    auto result = std::make_unique<GrantStatement>();

    // true = GRANT, false = REVOKE
    result->is_grant = stmt->is_grant;
    result->target_type = stmt->targtype;
    switch (result->target_type) {
        case duckdb_libpgquery::PGGrantTargetType::ACL_TARGET_OBJECT:
        case duckdb_libpgquery::PGGrantTargetType::ACL_TARGET_ALL_IN_SCHEMA:
            break;
        default:
            throw intarkdb::Exception(ExceptionType::NOT_IMPLEMENTED,
                            fmt::format("target type {} not support, must be (some object or all)", result->target_type));
            break;
    }
    result->obj_type = stmt->objtype;
    switch (result->obj_type) {
        case duckdb_libpgquery::PGObjectType::PG_OBJECT_TABLE:
        case duckdb_libpgquery::PGObjectType::PG_OBJECT_SCHEMA:
            break;
        default:
            throw intarkdb::Exception(ExceptionType::NOT_IMPLEMENTED,
                            fmt::format("objects type {} not support, must be (table/view or schema)", result->obj_type));
            break;
    }

    // objects
    auto objects_list = stmt->objects;
    if (objects_list) {
        for (auto node = objects_list->head; node != nullptr; node = node->next) {
            auto item = NullCheckPtrCast<duckdb_libpgquery::PGNode>(node->data.ptr_value);
            if (item->type == duckdb_libpgquery::T_PGRangeVar) {
                auto var = NullCheckPtrCast<duckdb_libpgquery::PGRangeVar>(item.get());
                auto table_ref = BindRangeVarTableRef((const duckdb_libpgquery::PGRangeVar &)(*var), false);

                std::string schema = var->schemaname ? var->schemaname : user_;
                result->schemas.push_back(schema);
                result->objects.push_back(var->relname);
            } else if (item->type == duckdb_libpgquery::T_PGString) {
                auto var = NullCheckPtrCast<duckdb_libpgquery::PGValue>(item.get());
                result->schemas.push_back(var->val.str);
                result->objects.push_back(var->val.str);    // in db_grant_userpriv_to_user objname is user
            } else {
                throw intarkdb::Exception(ExceptionType::NOT_IMPLEMENTED,
                                            fmt::format("revoke objects type {} not support", ConvertNodeTagToString(item->type)));
            }
        }
    }

    // privileges
    auto privileges_list = stmt->privileges;
    if (privileges_list) {
        for (auto node = privileges_list->head; node != nullptr; node = node->next) {
            auto item = NullCheckPtrCast<duckdb_libpgquery::PGNode>(node->data.ptr_value);
            if (item->type == duckdb_libpgquery::T_PGAccessPriv) {
                auto priv = NullCheckPtrCast<duckdb_libpgquery::PGAccessPriv>(item.get());
                auto upper_name = intarkdb::StringUtil::Upper(priv->priv_name);
                result->privileges.push_back(upper_name);
            } else {
                throw intarkdb::Exception(ExceptionType::NOT_IMPLEMENTED,
                                            fmt::format("revoke privileges type {}", ConvertNodeTagToString(item->type)));
            }
        }
    } else {
        // privileges == NIL denotes ALL PRIVILEGES
        result->is_all_privileges = true;
    }

    // grantees
    auto grantees_list = stmt->grantees;
    if (grantees_list) {
        for (auto node = grantees_list->head; node != nullptr; node = node->next) {
            auto item = NullCheckPtrCast<duckdb_libpgquery::PGNode>(node->data.ptr_value);
            if (item->type == duckdb_libpgquery::T_PGRoleSpec) {
                auto role_spec = NullCheckPtrCast<duckdb_libpgquery::PGRoleSpec>(item.get());

                if (role_spec->roletype != duckdb_libpgquery::PGRoleSpecType::ROLESPEC_CSTRING) {
                    throw intarkdb::Exception(ExceptionType::BINDER, "bad role type");
                }
                result->grantees.push_back(role_spec->rolename);
            } else {
                throw intarkdb::Exception(ExceptionType::NOT_IMPLEMENTED,
                                            fmt::format("role spec roletype {}", ConvertNodeTagToString(item->type)));
            }
        }
    }

    // grant_option
    result->grant_option = stmt->grant_option;
    if (result->grant_option) {
        throw intarkdb::Exception(ExceptionType::BINDER, "Not support grant option");
    }

    // grantor
    if (stmt->grantor) {
        auto role_spec = NullCheckPtrCast<duckdb_libpgquery::PGRoleSpec>(stmt->grantor);
        if (role_spec->roletype != duckdb_libpgquery::PGRoleSpecType::ROLESPEC_CSTRING) {
            throw intarkdb::Exception(ExceptionType::BINDER, "bad grantor format");
        }
        result->grantor = role_spec->rolename;
    } else {
        result->grantor = user_;
    }

    return result;
}
