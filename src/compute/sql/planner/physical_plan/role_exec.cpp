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
* create_role_exec.cpp
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/planner/physical_plan/create_role_exec.cpp
*
* -------------------------------------------------------------------------
*/
#include "planner/physical_plan/role_exec.h"

#include <fmt/core.h>
#include <fmt/ranges.h>

#include "common/string_util.h"
#include "common/object_type.h"
#include "storage/gstor/zekernel/kernel/knl_database.h"
#include "../dependency/GmSSL/include/gmssl/sha2.h"

auto CreateRoleExec::Execute() const -> RecordBatch {
    switch (role_stmt_type_) {
        case RoleStmtType::ROLE_STMT_ROLE: {
            CreateRole();
            break;
        }
        case RoleStmtType::ROLE_STMT_USER: {
            CreateUser();
            break;
        }
        default: {
            throw intarkdb::Exception(ExceptionType::EXECUTOR,
                                fmt::format("RoleStmtType {} not supported", role_stmt_type_));
        }
    }
    return RecordBatch(Schema());
}

void CreateRoleExec::CreateRole() const {
    auto storage = catalog_.GetStorageHandle();

    knl_role_def_t def;
    memset(&def, 0, sizeof(knl_role_def_t));
    def.owner_uid = catalog_.GetUserID();
    sprintf_s(def.name, GS_NAME_BUFFER_SIZE - 1, "%s", name_.c_str());
    def.name[GS_NAME_BUFFER_SIZE - 1] = '\0';
    sprintf_s(def.password, GS_PASSWORD_BUFFER_SIZE - 1, "%s", password_.c_str());
    def.password[GS_PASSWORD_BUFFER_SIZE - 1] = '\0';
    const auto& user = catalog_.GetUser();
    text_t owner = { (char *)user.c_str(), (uint32_t)user.length() };
    def.owner = owner;
    def.is_encrypt = GS_FALSE;

    // storage
    if (gstor_create_role(storage->handle, &def) != GS_SUCCESS) {
        int32_t err_code;
        const char *message = nullptr;
        cm_get_error(&err_code, &message, NULL);
        std::string err_msg = fmt::format("create role fail!! errno = {}, message = {}",
                                err_code, message);
        cm_reset_error();
        GS_LOG_RUN_INF("%s", err_msg.c_str());
        throw std::runtime_error(err_msg);
    }
}

bool EncryptPassword(const std::string password, uint8_t *dgst, size_t dgstlen) {
    if (dgstlen < SHA256_DIGEST_SIZE)
        return false;

    sha256_digest((unsigned char *)password.c_str(), password.size(), dgst);
    return true;
}

void CreateRoleExec::CreateUser() const {
    auto storage = catalog_.GetStorageHandle();

    knl_user_def_t def;
    memset(&def, 0, sizeof(knl_user_def_t));
    sprintf_s(def.name, GS_NAME_BUFFER_SIZE - 1, "%s", name_.c_str());
    def.name[GS_NAME_BUFFER_SIZE - 1] = '\0';
    sha256_digest((unsigned char *)password_.c_str(), password_.size(), (uint8_t *)def.password);
    def.password[SHA256_DIGEST_SIZE] = '\0';
    def.is_sys = GS_FALSE;
    def.is_readonly = GS_FALSE;
    def.is_permanent = GS_TRUE;
    def.is_encrypt = GS_TRUE;
    def.tenant_id = SYS_TENANTROOT_ID;

    // storage
    if (gstor_create_user(storage->handle, &def) != GS_SUCCESS) {
        int32_t err_code;
        const char *message = nullptr;
        cm_get_error(&err_code, &message, NULL);
        std::string err_msg = fmt::format("create user fail!! errno = {}, message = {}",
                                err_code, message);
        cm_reset_error();
        GS_LOG_RUN_INF("%s", err_msg.c_str());
        throw std::runtime_error(err_msg);
    }
}

//-------------------------------------------Alter--------------------------------------//
// can only support change password
auto AlterRoleExec::Execute() const -> RecordBatch {
    knl_session_t *session = EC_SESSION(catalog_.GetStorageHandle()->handle);

    knl_user_def_t def;
    memset(&def, 0, sizeof(knl_user_def_t));
    sprintf_s(def.name, GS_NAME_BUFFER_SIZE - 1, "%s", alter_role_statement.name.c_str());
    def.name[GS_NAME_BUFFER_SIZE - 1] = '\0';
    sha256_digest((unsigned char *)alter_role_statement.password.c_str(), alter_role_statement.password.size(), (uint8_t *)def.password);
    def.password[SHA256_DIGEST_SIZE] = '\0';
    def.mask = GS_GET_MASK(ALTER_USER_FIELD_PASSWORD);
    def.is_encrypt = GS_TRUE;

    if (knl_alter_user(session, &def) != GS_SUCCESS) {
        int32_t err_code;
        const char *message = nullptr;
        cm_get_error(&err_code, &message, NULL);
        std::string err_msg = fmt::format("ALTER user fail!! errno = {}, message = {}",
                                err_code, message);
        cm_reset_error();
        GS_LOG_RUN_INF("%s", err_msg.c_str());
        throw std::runtime_error(err_msg);
    }

    return RecordBatch(Schema());
}

//-------------------------------------------Drop---------------------------------------//
auto DropRoleExec::Execute() const -> RecordBatch {
    switch (role_stmt_type_) {
        case RoleStmtType::ROLE_STMT_ROLE: {
            DropRole();
            break;
        }
        case RoleStmtType::ROLE_STMT_USER: {
            DropUser();
            break;
        }
        default: {
            throw intarkdb::Exception(ExceptionType::EXECUTOR,
                                fmt::format("RoleStmtType {} not supported", role_stmt_type_));
        }
    }
    return RecordBatch(Schema());
}

void DropRoleExec::DropRole() const {
    auto storage = catalog_.GetStorageHandle();

    knl_drop_def_t def;
    memset(&def, 0, sizeof(knl_drop_def_t));
    const auto& user = catalog_.GetUser();
    text_t owner = { (char *)user.c_str(), static_cast<uint32_t>(user.length()) };
    def.owner = owner;
    def.purge = GS_TRUE;
    def.temp = GS_FALSE;

    for (auto &role_name : roles_) {
        def.name = { (char *)role_name.c_str(), (uint32_t)role_name.length() };

        // storage
        if (gstor_drop_role(storage->handle, &def) != GS_SUCCESS) {
            int32_t err_code;
            const char *message = nullptr;
            cm_get_error(&err_code, &message, NULL);
            std::string err_msg = fmt::format("drop role fail!! errno = {}, message = {}",
                                    err_code, message);
            cm_reset_error();
            GS_LOG_RUN_INF("%s", err_msg.c_str());
            throw std::runtime_error(err_msg);
        }
    }
}

void DropRoleExec::DropUser() const {
    auto storage = catalog_.GetStorageHandle();

    knl_drop_user_t def;
    memset(&def, 0, sizeof(knl_drop_user_t));

    def.purge = GS_TRUE;
    for (auto &role_name : roles_) {
        def.owner = { (char *)role_name.c_str(), (uint32_t)role_name.length() };
        // storage
        if (gstor_drop_user(storage->handle, &def) != GS_SUCCESS) {
            int32_t err_code;
            const char *message = nullptr;
            cm_get_error(&err_code, &message, NULL);
            std::string err_msg = fmt::format("drop user fail!! errno = {}, message = {}",
                                    err_code, message);
            cm_reset_error();
            GS_LOG_RUN_INF("%s", err_msg.c_str());
            throw std::runtime_error(err_msg);
        }
    }
}

//-------------------------------------------Grant Role------------------------------------//
auto GrantRoleExec::Execute() const -> RecordBatch {
    if (grant_role_statement.is_grant) {
        GrantRole();
    } else {
        RevokeRole();
    }
    return RecordBatch(Schema());
}

void GrantRoleExec::GrantRole() const {
    auto storage = catalog_.GetStorageHandle();
    auto grant_def = std::make_unique<knl_grant_def_t>();

    // priv_type
    grant_def->priv_type = PRIV_TYPE_ROLE;

    knl_session_t *session = EC_SESSION(storage->handle);
    CM_SAVE_STACK(session->stack);

    // privs
    cm_galist_init(&grant_def->privs, session->stack, cm_stack_alloc);
    for (auto &role : grant_role_statement.granted_roles) {
        knl_priv_def_t *priv_def = nullptr;
        if (cm_galist_new(&grant_def->privs, sizeof(knl_priv_def_t), (pointer_t *)&priv_def) != GS_SUCCESS) {
            throw std::runtime_error("cm_galist_new error");
        }
        if (memset_s(priv_def, sizeof(knl_priv_def_t), 0, sizeof(knl_priv_def_t)) != EOK) {
            throw std::runtime_error("memset_s error");
        }
        priv_def->priv_type = PRIV_TYPE_ROLE;
        text_t t_role = { (char *)role.c_str(), static_cast<uint32_t>(role.length()) };
        priv_def->priv_name = t_role;
    }

    // grantees
    cm_galist_init(&grant_def->grantees, session->stack, cm_stack_alloc);
    for (auto &user : grant_role_statement.grantee_roles) {
        knl_holders_def_t *grantee_def = nullptr;
        if (cm_galist_new(&grant_def->grantees, sizeof(knl_holders_def_t), (pointer_t *)&grantee_def) != GS_SUCCESS) {
            throw std::runtime_error("cm_galist_new error");
        }
        if (memset_s(grantee_def, sizeof(knl_holders_def_t), 0, sizeof(knl_holders_def_t)) != EOK) {
            throw std::runtime_error("memset_s error");
        }
        grantee_def->type = TYPE_USER;
        text_t t_user = { (char *)user.c_str(), static_cast<uint32_t>(user.length()) };
        grantee_def->name = t_user;
    }

    // privs_list
    cm_galist_init(&grant_def->privs_list, session->stack, cm_stack_alloc);
    // grantee_list
    cm_galist_init(&grant_def->grantee_list, session->stack, cm_stack_alloc);

    // grant_uid
    grant_def->grant_uid = catalog_.GetUserID();

    // storage
    if (gstor_grant_privilege(storage->handle, grant_def.get()) != GS_SUCCESS) {
        int32_t err_code;
        const char *message = nullptr;
        cm_get_error(&err_code, &message, NULL);
        std::string err_msg = fmt::format("grant role fail!! errno = {}, message = {}", err_code, message);
        cm_reset_error();
        GS_LOG_RUN_INF("%s", err_msg.c_str());
        throw std::runtime_error(err_msg);
    }

    CM_RESTORE_STACK(session->stack);
}

void GrantRoleExec::RevokeRole() const {
    auto storage = catalog_.GetStorageHandle();
    auto revoke_def = std::make_unique<knl_revoke_def_t>();

    revoke_def->priv_type = PRIV_TYPE_ROLE;

    knl_session_t *session = EC_SESSION(storage->handle);
    CM_SAVE_STACK(session->stack);

    // privs
    cm_galist_init(&revoke_def->privs, session->stack, cm_stack_alloc);
    for (auto &role : grant_role_statement.granted_roles) {
        knl_priv_def_t *priv_def = nullptr;
        if (cm_galist_new(&revoke_def->privs, sizeof(knl_priv_def_t), (pointer_t *)&priv_def) != GS_SUCCESS) {
            throw std::runtime_error("cm_galist_new error");
        }
        if (memset_s(priv_def, sizeof(knl_priv_def_t), 0, sizeof(knl_priv_def_t)) != EOK) {
            throw std::runtime_error("memset_s error");
        }
        priv_def->priv_type = PRIV_TYPE_ROLE;
        text_t t_role = { (char *)role.c_str(), static_cast<uint32_t>(role.length()) };
        priv_def->priv_name = t_role;
    }

    // revokees
    cm_galist_init(&revoke_def->revokees, session->stack, cm_stack_alloc);
    for (auto &user : grant_role_statement.grantee_roles) {
        knl_holders_def_t *revokee_def = nullptr;
        if (cm_galist_new(&revoke_def->revokees, sizeof(knl_holders_def_t), (pointer_t *)&revokee_def) != GS_SUCCESS) {
            throw std::runtime_error("cm_galist_new error");
        }
        if (memset_s(revokee_def, sizeof(knl_holders_def_t), 0, sizeof(knl_holders_def_t)) != EOK) {
            throw std::runtime_error("memset_s error");
        }
        revokee_def->type = TYPE_USER;
        text_t t_user = { (char *)user.c_str(), static_cast<uint32_t>(user.length()) };
        revokee_def->name = t_user;
    }

    // privs_list
    cm_galist_init(&revoke_def->privs_list, session->stack, cm_stack_alloc);
    // revokee_list
    cm_galist_init(&revoke_def->revokee_list, session->stack, cm_stack_alloc);

    // storage
    if (gstor_revoke_privilege(storage->handle, revoke_def.get()) != GS_SUCCESS) {
        int32_t err_code;
        const char *message = nullptr;
        cm_get_error(&err_code, &message, NULL);
        std::string err_msg = fmt::format("revoke role fail!! errno = {}, message = {}", err_code, message);
        cm_reset_error();
        GS_LOG_RUN_INF("%s", err_msg.c_str());
        throw std::runtime_error(err_msg);
    }

    CM_RESTORE_STACK(session->stack);
}

//-------------------------------------------Grant------------------------------------//
auto GrantExec::Execute() const -> RecordBatch {
    if (grant_statement.is_grant) {
        GrantPrivileges();
    } else {
        RevokePrivileges();
    }
    return RecordBatch(Schema());
}

void GrantExec::GrantPrivileges() const {
    auto storage = catalog_.GetStorageHandle();
    auto grant_def = std::make_unique<knl_grant_def_t>();

    // objtype
    grant_def->objtype = TransObjectType(grant_statement.obj_type);

    // priv_type
    grant_def->priv_type = TransPrivType(grant_def->objtype);

    knl_session_t *session = EC_SESSION(storage->handle);
    CM_SAVE_STACK(session->stack);

    // privs
    cm_galist_init(&grant_def->privs, session->stack, cm_stack_alloc);
    if (grant_statement.is_all_privileges) {
        knl_priv_def_t *priv_def = nullptr;
        if (cm_galist_new(&grant_def->privs, sizeof(knl_priv_def_t), (pointer_t *)&priv_def) != GS_SUCCESS) {
            throw std::runtime_error("cm_galist_new error");
        }
        if (memset_s(priv_def, sizeof(knl_priv_def_t), 0, sizeof(knl_priv_def_t)) != EOK) {
            throw std::runtime_error("memset_s error");
        }
        priv_def->priv_type = PRIV_TYPE_ALL_PRIV;
    } else {
        for (auto &privilege : grant_statement.privileges) {
            knl_priv_def_t *priv_def = nullptr;
            if (cm_galist_new(&grant_def->privs, sizeof(knl_priv_def_t), (pointer_t *)&priv_def) != GS_SUCCESS) {
                throw std::runtime_error("cm_galist_new error");
            }
            if (memset_s(priv_def, sizeof(knl_priv_def_t), 0, sizeof(knl_priv_def_t)) != EOK) {
                throw std::runtime_error("memset_s error");
            }
            priv_def->priv_type = grant_def->priv_type;
            text_t priv_name = { (char *)privilege.c_str(), static_cast<uint32_t>(privilege.length()) };
            priv_def->priv_name = priv_name;
            priv_def->priv_id = GetPrivIdByName(privilege);
        }
    }

    // grantees
    cm_galist_init(&grant_def->grantees, session->stack, cm_stack_alloc);
    for (auto &grantee : grant_statement.grantees) {
        knl_holders_def_t *grantee_def = nullptr;
        if (cm_galist_new(&grant_def->grantees, sizeof(knl_holders_def_t), (pointer_t *)&grantee_def) != GS_SUCCESS) {
            throw std::runtime_error("cm_galist_new error");
        }
        if (memset_s(grantee_def, sizeof(knl_holders_def_t), 0, sizeof(knl_holders_def_t)) != EOK) {
            throw std::runtime_error("memset_s error");
        }
        // Users and roles cannot have the same name. So if it is not a user, it is a role
        if (catalog_.IsRole(grantee) == GS_TRUE) {
            grantee_def->type = TYPE_ROLE;
        } else {
            grantee_def->type = TYPE_USER;
        }
        text_t t_user_or_role = { (char *)grantee.c_str(), static_cast<uint32_t>(grantee.length()) };
        grantee_def->name = t_user_or_role;
    }

    // privs_list
    cm_galist_init(&grant_def->privs_list, session->stack, cm_stack_alloc);
    // revokee_list
    cm_galist_init(&grant_def->grantee_list, session->stack, cm_stack_alloc);

    // grant_uid
    grant_def->grant_uid = catalog_.GetUserID(grant_statement.grantor);

    // storage
    uint32_t obj_i = 0;
    for (auto &object : grant_statement.objects) {
        // objname
        text_t obj_name = { (char *)object.c_str(), (uint32_t)object.length() };
        grant_def->objname = obj_name;

        // schema
        auto &schema = grant_statement.schemas[obj_i];
        text_t schema_name = { (char *)schema.c_str(), (uint32_t)schema.length() };
        grant_def->schema = schema_name;
        obj_i++;

        // objowner
        grant_def->objowner = catalog_.GetUserID(schema);

        if (gstor_grant_privilege(storage->handle, grant_def.get()) != GS_SUCCESS) {
            int32_t err_code;
            const char *message = nullptr;
            cm_get_error(&err_code, &message, NULL);
            std::string err_msg = fmt::format("grant fail!! errno = {}, message = {}", err_code, message);
            cm_reset_error();
            GS_LOG_RUN_INF("%s", err_msg.c_str());
            throw std::runtime_error(err_msg);
        }
    }

    CM_RESTORE_STACK(session->stack);
}

void GrantExec::RevokePrivileges() const {
    auto storage = catalog_.GetStorageHandle();
    auto revoke_def = std::make_unique<knl_revoke_def_t>();

    // objtype
    revoke_def->objtype = TransObjectType(grant_statement.obj_type);

    // priv_type
    revoke_def->priv_type = TransPrivType(revoke_def->objtype);

    knl_session_t *session = EC_SESSION(storage->handle);
    CM_SAVE_STACK(session->stack);

    // privs
    cm_galist_init(&revoke_def->privs, session->stack, cm_stack_alloc);
    if (grant_statement.is_all_privileges) {
        knl_priv_def_t *priv_def = nullptr;
        if (cm_galist_new(&revoke_def->privs, sizeof(knl_priv_def_t), (pointer_t *)&priv_def) != GS_SUCCESS) {
            throw std::runtime_error("cm_galist_new error");
        }
        if (memset_s(priv_def, sizeof(knl_priv_def_t), 0, sizeof(knl_priv_def_t)) != EOK) {
            throw std::runtime_error("memset_s error");
        }
        priv_def->priv_type = PRIV_TYPE_ALL_PRIV;
    } else {
        for (auto &privilege : grant_statement.privileges) {
            knl_priv_def_t *priv_def = nullptr;
            if (cm_galist_new(&revoke_def->privs, sizeof(knl_priv_def_t), (pointer_t *)&priv_def) != GS_SUCCESS) {
                throw std::runtime_error("cm_galist_new error");
            }
            if (memset_s(priv_def, sizeof(knl_priv_def_t), 0, sizeof(knl_priv_def_t)) != EOK) {
                throw std::runtime_error("memset_s error");
            }
            priv_def->priv_type = revoke_def->priv_type;
            auto upper_name = intarkdb::StringUtil::Upper(privilege);
            text_t priv_name = { (char *)upper_name.c_str(), (uint32_t)upper_name.length() };
            priv_def->priv_name = priv_name;
            priv_def->priv_id = GetPrivIdByName(upper_name);
        }
    }

    // revokees
    cm_galist_init(&revoke_def->revokees, session->stack, cm_stack_alloc);
    for (auto &grantee : grant_statement.grantees) {
        knl_holders_def_t *grantee_def = nullptr;
        if (cm_galist_new(&revoke_def->revokees, sizeof(knl_holders_def_t), (pointer_t *)&grantee_def) != GS_SUCCESS) {
            throw std::runtime_error("cm_galist_new error");
        }
        if (memset_s(grantee_def, sizeof(knl_holders_def_t), 0, sizeof(knl_holders_def_t)) != EOK) {
            throw std::runtime_error("memset_s error");
        }
        if (catalog_.IsRole(grantee) == GS_TRUE) {
            grantee_def->type = TYPE_ROLE;
        } else {
            grantee_def->type = TYPE_USER;
        }
        text_t t_user = { (char *)grantee.c_str(), static_cast<uint32_t>(grantee.length()) };
        grantee_def->name = t_user;
    }

    // privs_list
    cm_galist_init(&revoke_def->privs_list, session->stack, cm_stack_alloc);
    // revokee_list
    cm_galist_init(&revoke_def->revokee_list, session->stack, cm_stack_alloc);

    // storage
    uint32_t obj_i = 0;
    for (auto &object : grant_statement.objects) {
        // objname
        text_t obj_name = { (char *)object.c_str(), static_cast<uint32_t>(object.length()) };
        revoke_def->objname = obj_name;

        // schema
        auto &schema = grant_statement.schemas[obj_i];
        text_t schema_name = { (char *)schema.c_str(), static_cast<uint32_t>(schema.length()) };
        revoke_def->schema = schema_name;
        obj_i++;

        // objowner
        revoke_def->objowner = catalog_.GetUserID(schema);

        if (gstor_revoke_privilege(storage->handle, revoke_def.get()) != GS_SUCCESS) {
            int32_t err_code;
            const char *message = nullptr;
            cm_get_error(&err_code, &message, NULL);
            std::string err_msg = fmt::format("revoke fail!! errno = {}, message = {}", err_code, message);
            cm_reset_error();
            GS_LOG_RUN_INF("%s", err_msg.c_str());
            throw std::runtime_error(err_msg);
        }
    }

    CM_RESTORE_STACK(session->stack);
}

// from g_obj_privs_def
uint32_t GrantExec::GetPrivIdByName(std::string name) const {
    if (name == "ALTER") {
        return GS_PRIV_ALTER;
    } else if (name == "DELETE") {
        return GS_PRIV_DELETE;
    } else if (name == "INSERT") {
        return GS_PRIV_INSERT;
    } else if (name == "SELECT") {
        return GS_PRIV_SELECT;
    } else if (name == "UPDATE") {
        return GS_PRIV_UPDATE;
    } else {
        throw intarkdb::Exception(ExceptionType::EXECUTOR,
                                fmt::format("privilege name {} not support", name));
    }
}

object_type_t GrantExec::TransObjectType(duckdb_libpgquery::PGObjectType obj_type) const {
    object_type_t new_obj_type = OBJ_TYPE_INVALID;
    switch (obj_type) {
        case duckdb_libpgquery::PGObjectType::PG_OBJECT_SCHEMA:
            new_obj_type = OBJ_TYPE_USER;
            break;
        case duckdb_libpgquery::PGObjectType::PG_OBJECT_TABLE:
            new_obj_type = OBJ_TYPE_TABLE;
            break;
        case duckdb_libpgquery::PGObjectType::PG_OBJECT_VIEW:
            new_obj_type = OBJ_TYPE_VIEW;
            break;
        case duckdb_libpgquery::PGObjectType::PG_OBJECT_SEQUENCE:
            new_obj_type = OBJ_TYPE_SEQUENCE;
            break;
        case duckdb_libpgquery::PGObjectType::PG_OBJECT_INDEX:
            new_obj_type = OBJ_TYPE_INDEX;
            break;
        default:
            throw intarkdb::Exception(ExceptionType::EXECUTOR,
                                fmt::format("object type {} not support", obj_type));
            break;
    }

    return new_obj_type;
}

priv_type_def GrantExec::TransPrivType(object_type_t obj_type) const {
    switch (grant_statement.target_type) {
        case duckdb_libpgquery::PGGrantTargetType::ACL_TARGET_OBJECT:
            switch (obj_type) {
                case OBJ_TYPE_USER:
                    return PRIV_TYPE_USER_PRIV;
                    break;
                case OBJ_TYPE_TABLE:
                case OBJ_TYPE_VIEW:
                case OBJ_TYPE_SEQUENCE:
                case OBJ_TYPE_INDEX:
                    return PRIV_TYPE_OBJ_PRIV;
                    break;
                default:
                    throw intarkdb::Exception(ExceptionType::EXECUTOR, "bad object type");
                    break;
            }
        case duckdb_libpgquery::PGGrantTargetType::ACL_TARGET_ALL_IN_SCHEMA:
            switch (obj_type) {
                case OBJ_TYPE_TABLE:
                case OBJ_TYPE_VIEW:
                case OBJ_TYPE_SEQUENCE:
                    return PRIV_TYPE_ALL_PRIV;
                    break;
                default:
                    throw intarkdb::Exception(ExceptionType::EXECUTOR, "bad object type");
                    break;
            }
        default:
            throw std::runtime_error("bad target type, must be object or schema privileges");
            break;
    }
}
