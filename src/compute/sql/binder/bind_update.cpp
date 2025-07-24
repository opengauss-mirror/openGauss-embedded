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
 * bind_update.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/binder/bind_update.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <memory>

#include "binder/binder.h"
#include "binder/expressions/bound_constant.h"
#include "common/string_util.h"

auto CheckUpdateOption(duckdb_libpgquery::PGUpdateStmt *stmt) -> void {
    if (stmt->withClause) {
        throw std::invalid_argument("update with clause not supported ");
    }

    if (stmt->fromClause) {
        throw std::invalid_argument("update from clause not supported ");
    }
}

auto CheckTablePrivilege(const TableInfo &table_info) -> void {
    if (table_info.GetObjectType() != DIC_TYPE_TABLE) {
        throw intarkdb::Exception(ExceptionType::BINDER, "Can't update, entry type not support!");
    }
    if (table_info.IsTimeScale()) {
        throw intarkdb::Exception(ExceptionType::BINDER, "time series table cannot update!");
    }
    if (table_info.GetSpaceId() != SQL_SPACE_TYPE_USERS) {
        throw intarkdb::Exception(ExceptionType::BINDER,
                                  "Cannot update system table : " + std::string(table_info.GetTableName()));
    }
}

auto Binder::BindUpdateItems(duckdb_libpgquery::PGList *target_list, const BoundBaseTable &table)
    -> std::vector<UpdateSetItem> {
    bool found = false;
    std::vector<UpdateSetItem> update_items;
    for (auto cell = target_list->head; cell != nullptr; cell = cell->next) {
        found = false;
        auto target = reinterpret_cast<duckdb_libpgquery::PGResTarget *>(cell->data.ptr_value);
        for (auto &column : table.GetTableInfo().columns) {
            if (intarkdb::StringUtil::IsEqualIgnoreCase(column.Name(), target->name)) {
                update_items.emplace_back(std::make_pair(column, BindExpression(target->val, 1)));
                found = true;
                break;
            }
        }
        if (!found) {
            throw intarkdb::Exception(ExceptionType::BINDER, fmt::format("{} column is not defined in the table {}",
                                                                         target->name, table.GetBoundTableName()));
        }
    }
    return update_items;
}

auto Binder::BindUpdate(duckdb_libpgquery::PGUpdateStmt *stmt) -> std::unique_ptr<UpdateStatement> {
    // 检查是否支持更新选项
    CheckUpdateOption(stmt);

    std::string schema_name = stmt->relation->schemaname ? std::string(stmt->relation->schemaname) : user_;
    auto table =
        BindBaseTableRef(schema_name, stmt->relation->relname,
                         stmt->relation->alias ? std::make_optional(stmt->relation->alias->aliasname) : std::nullopt);
    if (table == nullptr) {
        throw intarkdb::Exception(ExceptionType::BINDER, "Table not found");
    }
    const auto &table_info = table->GetTableInfo();

    // 检查表是否可以被更新
    CheckTablePrivilege(table_info);

    // check privileges
    std::string obj_user = table->GetSchema();
    std::string obj_name = table->GetBoundTableName();
    if (catalog_.CheckPrivilege(obj_user, obj_name, OBJ_TYPE_TABLE, GS_PRIV_UPDATE) != GS_TRUE) {
        throw intarkdb::Exception(ExceptionType::BINDER,
                                fmt::format("{}.{} update permission denied!", obj_user, obj_name));
    }
    // check privileges again (for example : synonym)
    std::string real_obj_name = std::string(table_info.GetTableName());
    if (real_obj_name != obj_name) {
        auto uid = table_info.user_id;
        std::string real_obj_user = catalog_.GetUserName(uid);
        if (catalog_.CheckPrivilege(real_obj_user, real_obj_name, OBJ_TYPE_TABLE, GS_PRIV_UPDATE) != GS_TRUE) {
            throw intarkdb::Exception(ExceptionType::BINDER,
                                    fmt::format("{}.{} update permission denied!", real_obj_user, real_obj_name));
        }
    }

    ctx.AddTableBinding(table->GetTableNameOrAlias(), table->GetTableInfo().columns);

    AddRelatedTables(std::string(table_info.GetTableName()), table_info.IsTimeScale(), true);

    // set pairs
    auto update_items = BindUpdateItems(stmt->targetList, *table);
    auto condition = BindWhere(stmt->whereClause);
    auto update_statement = std::make_unique<UpdateStatement>();
    update_statement->table = std::move(table);
    update_statement->update_items = std::move(update_items);
    update_statement->condition = std::move(condition);
    return update_statement;
}
