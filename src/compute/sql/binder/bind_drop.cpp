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
* bind_drop.cpp
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/binder/bind_drop.cpp
*
* -------------------------------------------------------------------------
*/
#include "binder/binder.h"
#include "common/exception.h"
#include "common/null_check_ptr.h"
#include "common/string_util.h"
#include "common/util.h"

auto Binder::BindDropStmt(duckdb_libpgquery::PGDropStmt *stmt) -> std::unique_ptr<DropStatement>
{
    if (stmt->objects->length != 1) {
        throw intarkdb::Exception(ExceptionType::BINDER, "Unsupport drop multiple objects");
    }
    ObjectType type;
    switch (stmt->removeType) {
        case duckdb_libpgquery::PG_OBJECT_TABLE:
            type = ObjectType::TABLE;
            break;
        case duckdb_libpgquery::PG_OBJECT_INDEX:
            type = ObjectType::INDEX;
            break;
        case duckdb_libpgquery::PG_OBJECT_SEQUENCE:
            type = ObjectType::SEQUENCE;
            break;
        case duckdb_libpgquery::PG_OBJECT_VIEW:
            type = ObjectType::VIEW;
            break;
        case duckdb_libpgquery::PG_OBJECT_SYNONYM:
            type = ObjectType::SYNONYM;
            break;
        default:
            throw intarkdb::Exception(ExceptionType::BINDER, "Unsupport drop type");
    }

    auto view_list = (duckdb_libpgquery::PGList *)stmt->objects->head->data.ptr_value;
    std::string name;
    if (view_list->length == 1) {
        name = ((duckdb_libpgquery::PGValue *)view_list->head->data.ptr_value)->val.str;
    } else {
        throw std::invalid_argument(fmt::format("This format is not currently supported."));
    }
    name = intarkdb::StringUtil::Lower(name);

    if (type == ObjectType::TABLE) {
        auto table = BindBaseTableRef(user_, name, std::nullopt, true);
        if (table) {
            const auto &table_info = table->GetTableInfo();
            const auto &metaInfo = table_info.GetTableMetaInfo();
            if (metaInfo.space_id != SQL_SPACE_TYPE_USERS) {
                throw std::runtime_error("Cannot drop system table : " + std::string(metaInfo.name));
            }
            AddRelatedTables(name, table_info.IsTimeScale(), true);
        }
    }

    return std::make_unique<DropStatement>(name, stmt->missing_ok, type);
}
