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
* bind_set.cpp
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/binder/bind_set.cpp
*
* -------------------------------------------------------------------------
*/
#include <fmt/core.h>
#include <fmt/format.h>

#include "binder/binder.h"
#include "binder/transform_typename.h"
#include "common/exception.h"
#include "common/string_util.h"
#include "common/util.h"

static auto BindAutoCommit(duckdb_libpgquery::PGVariableSetStmt *stmt) -> std::unique_ptr<SetStatement> {
    std::string name = std::string(stmt->name);
    name = intarkdb::StringUtil::Lower(name);

    if (stmt->scope != duckdb_libpgquery::VariableSetScope::VAR_SET_SCOPE_DEFAULT
        && stmt->scope != duckdb_libpgquery::VariableSetScope::VAR_SET_SCOPE_SESSION) {
        throw std::invalid_argument(fmt::format("Variable 'auto_commit' only support set in session"));
    }
    if (stmt->args->length != 1) {
        throw std::invalid_argument(fmt::format("SET needs a single scalar value parameter"));
    }
    if (!stmt->args->head || !stmt->args->head->data.ptr_value) {
        throw std::invalid_argument(fmt::format("args is empty."));
    }
    if (((duckdb_libpgquery::PGNode *)stmt->args->head->data.ptr_value)->type != duckdb_libpgquery::T_PGAConst) {
        throw std::invalid_argument(fmt::format("only support T_PGAConst."));
    }

    if (((duckdb_libpgquery::PGAConst *)stmt->args->head->data.ptr_value)->val.type != duckdb_libpgquery::T_PGString) {
        throw std::invalid_argument(fmt::format("SET value type not support."));
    }

    std::string value = std::string(((duckdb_libpgquery::PGAConst *)stmt->args->head->data.ptr_value)->val.val.str);
    if (value == "true") {
        return std::make_unique<SetStatement>(SetKind::SET, SetName::SET_NAME_AUTOCOMMIT, ValueFactory::ValueBool(true));
    } else if (value == "false") {
        return std::make_unique<SetStatement>(SetKind::SET, SetName::SET_NAME_AUTOCOMMIT, ValueFactory::ValueBool(false));
    } else {
        throw std::invalid_argument(fmt::format("The value set must be true or false."));
    }
}

static auto BindMaxConnection(duckdb_libpgquery::PGVariableSetStmt *stmt) -> std::unique_ptr<SetStatement> {
    std::string name = std::string(stmt->name);
    name = intarkdb::StringUtil::Lower(name);

    if (stmt->scope != duckdb_libpgquery::VariableSetScope::VAR_SET_SCOPE_GLOBAL) {
        throw std::invalid_argument(
                    fmt::format("Variable 'max_connections' is a GLOBAL variable and should be set with SET GLOBAL"));
    }
    if (stmt->args->length != 1) {
        throw std::invalid_argument(fmt::format("SET needs a single scalar value parameter"));
    }
    if (!stmt->args->head || !stmt->args->head->data.ptr_value) {
        throw std::invalid_argument(fmt::format("args is empty."));
    }
    if (((duckdb_libpgquery::PGNode *)stmt->args->head->data.ptr_value)->type != duckdb_libpgquery::T_PGAConst) {
        throw std::invalid_argument(fmt::format("only support T_PGAConst."));
    }

    if (((duckdb_libpgquery::PGAConst *)stmt->args->head->data.ptr_value)->val.type != duckdb_libpgquery::T_PGInteger) {
        throw std::invalid_argument(fmt::format("SET value type not support, must be integer!"));
    }

    auto value = ((duckdb_libpgquery::PGAConst *)stmt->args->head->data.ptr_value)->val.val.ival;
    if (value < GS_MIN_CONN_NUM || value > GS_MAX_CONN_NUM) {
        throw intarkdb::Exception(ExceptionType::BINDER,
                        fmt::format("set value out of range[{}~{}]", GS_MIN_CONN_NUM, GS_MAX_CONN_NUM));
    }
    return std::make_unique<SetStatement>(SetKind::SET, SetName::SET_NAME_MAXCONNECTIONS, ValueFactory::ValueInt(value));
}

auto BindSynchronousCommit(duckdb_libpgquery::PGVariableSetStmt *stmt) -> std::unique_ptr<SetStatement> {
    std::string name = std::string(stmt->name);
    name = intarkdb::StringUtil::Lower(name);

    if (stmt->args->length != 1) {
        throw std::invalid_argument(fmt::format("SET needs a single scalar value parameter"));
    }
    if (!stmt->args->head || !stmt->args->head->data.ptr_value) {
        throw std::invalid_argument(fmt::format("args is empty."));
    }
    if (((duckdb_libpgquery::PGNode *)stmt->args->head->data.ptr_value)->type != duckdb_libpgquery::T_PGAConst) {
        throw std::invalid_argument(fmt::format("only support T_PGAConst."));
    }

    if (((duckdb_libpgquery::PGAConst *)stmt->args->head->data.ptr_value)->val.type != duckdb_libpgquery::T_PGString) {
        throw std::invalid_argument(fmt::format("SET value type not support, must be string!"));
    }

    std::string set_val = ((duckdb_libpgquery::PGAConst *)stmt->args->head->data.ptr_value)->val.val.str;
    set_val = intarkdb::StringUtil::Lower(set_val);
    if (set_val != "off" && set_val != "on") {
        throw intarkdb::Exception(ExceptionType::BINDER, "bad set value, must be off or on");
    }

    return std::make_unique<SetStatement>(
            SetKind::SET, SetName::SET_NAME_SYNCHRONOUS_COMMIT, ValueFactory::ValueVarchar(set_val));
}

auto Binder::BindVariableSet(duckdb_libpgquery::PGVariableSetStmt *stmt) -> std::unique_ptr<SetStatement> {
    if (stmt->kind != duckdb_libpgquery::VariableSetKind::VAR_SET_VALUE) {
        throw std::invalid_argument(fmt::format("VariableSetKind is not implemented."));
    }

    std::string name = std::string(stmt->name);
    name = intarkdb::StringUtil::Lower(name);
    if (name.empty()) {
        throw std::invalid_argument(fmt::format("set key is empty."));
    }
    if (name == "auto_commit") {
        return BindAutoCommit(stmt);
    } else if (name == "max_connections") {
        return BindMaxConnection(stmt);
    } else if (name == "synchronous_commit") {
        return BindSynchronousCommit(stmt);
    } else {
        throw intarkdb::Exception(ExceptionType::BINDER, fmt::format("set name {} is not supported", name));
    }
}
