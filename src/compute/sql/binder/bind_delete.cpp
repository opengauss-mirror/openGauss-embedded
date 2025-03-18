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
 * bind_delete.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/binder/bind_delete.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "binder/binder.h"
#include "binder/expressions/bound_constant.h"
#include "common/null_check_ptr.h"

auto Binder::BindDelete(duckdb_libpgquery::PGDeleteStmt *stmt) -> std::unique_ptr<DeleteStatement> {
    auto result = std::make_unique<DeleteStatement>();
    // relation
    std::string schema_name = stmt->relation->schemaname ? std::string(stmt->relation->schemaname) : user_;
    if (stmt->relation->alias != nullptr) {
        result->target_table =
            BindBaseTableRef(schema_name, stmt->relation->relname,
                            std::make_optional(stmt->relation->alias->aliasname));
    } else {
        result->target_table = BindBaseTableRef(schema_name, stmt->relation->relname, std::nullopt);
    }

    if(result->target_table == nullptr) {
        throw intarkdb::Exception(ExceptionType::BINDER, "Not found Table: " + std::string(stmt->relation->relname));
    }

    // check privileges
    std::string obj_user = result->target_table->GetSchema();
    std::string obj_name = result->target_table->GetBoundTableName();
    if (catalog_.CheckPrivilege(obj_user, obj_name, OBJ_TYPE_TABLE, GS_PRIV_DELETE) != GS_TRUE) {
        throw intarkdb::Exception(ExceptionType::BINDER,
                                fmt::format("{}.{} delete permission denied!", obj_user, obj_name));
    }
    // check privileges again (for example : synonym)
    const auto &table_info = result->target_table->GetTableInfo();
    std::string real_obj_name = std::string(table_info.GetTableName());
    if (real_obj_name != obj_name) {
        auto uid = table_info.user_id;
        std::string real_obj_user = catalog_.GetUserName(uid);
        if (catalog_.CheckPrivilege(real_obj_user, real_obj_name, OBJ_TYPE_TABLE, GS_PRIV_DELETE) != GS_TRUE) {
            throw intarkdb::Exception(ExceptionType::BINDER,
                                    fmt::format("{}.{} delete permission denied!", real_obj_user, real_obj_name));
        }
    }

    if (result->target_table->GetObjectType() != DIC_TYPE_TABLE) {
        throw intarkdb::Exception(ExceptionType::BINDER, "delete object is not a table");
    }

    ctx.AddTableBinding(result->target_table->GetTableNameOrAlias(), table_info.columns);

    AddRelatedTables(std::string(table_info.GetTableName()), table_info.IsTimeScale(), true);

    if (result->target_table->GetSpaceId() != SQL_SPACE_TYPE_USERS) {
        throw intarkdb::Exception(ExceptionType::BINDER, fmt::format("Cannot delete from system table : {}",
                                                                     result->target_table->GetBoundTableName()));
    }

    if (result->target_table->Type() != DataSourceType::BASE_TABLE) {
        throw intarkdb::Exception(ExceptionType::BINDER, "Can only delete from base tables!");
    }

    // using
    if (stmt->usingClause) {
        throw intarkdb::Exception(ExceptionType::NOT_IMPLEMENTED, "DELETE with USING is not supported");
    }

    // where
    result->condition = BindWhere(stmt->whereClause);

    return result;
}
