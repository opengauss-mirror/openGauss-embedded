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
 * bind_create_index.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/binder/bind_create_index.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <cstdlib>

#include "binder/binder.h"
#include "common/null_check_ptr.h"
#include "common/string_util.h"

using namespace duckdb_libpgquery;

static void CheckUnsupportedFeature(PGIndexElem& index_element) {
    if (index_element.collation) {
        throw intarkdb::Exception(ExceptionType::NOT_IMPLEMENTED, "Index with collation not supported yet!");
    }
    if (index_element.opclass) {
        throw intarkdb::Exception(ExceptionType::NOT_IMPLEMENTED, "Index with opclass not supported yet!");
    }

    if (index_element.name == nullptr) {
        // parse the index expression
        throw intarkdb::Exception(ExceptionType::NOT_IMPLEMENTED, "Index expression not supported yet!");
    }
}

static void CheckParamValid(PGIndexStmt& pg_stmt) {
    if (pg_stmt.idxname == nullptr) {
        throw intarkdb::Exception(ExceptionType::BINDER, "Index name is null");
    }
    if (strlen(pg_stmt.idxname) >= GS_MAX_NAME_LEN) {
        throw intarkdb::Exception(ExceptionType::BINDER,
                                  fmt::format("Index name is too long, max length:{}", GS_MAX_NAME_LEN));
    }

    // check index type
    if (pg_stmt.accessMethod != nullptr && strcmp(pg_stmt.accessMethod, "art") != 0) {
        throw intarkdb::Exception(ExceptionType::NOT_IMPLEMENTED,
                                  fmt::format("Index type {} not supported yet!", pg_stmt.accessMethod));
    }
}

auto Binder::BindCreateIndex(PGIndexStmt* pg_stmt) -> std::unique_ptr<CreateIndexStatement> {
    // CheckSysPrivilege
    if (catalog_.CheckSysPrivilege(CREATE_TABLE) != GS_TRUE) {
        throw intarkdb::Exception(ExceptionType::BINDER, fmt::format("user {} create index permission denied!", user_));
    }

    CheckParamValid(*pg_stmt);
    BindName(pg_stmt->idxname);

    auto lower_column_names = std::vector<std::string>{};
    for (auto cell = pg_stmt->indexParams->head; cell != nullptr; cell = cell->next) {
        auto index_element = NullCheckPtrCast<duckdb_libpgquery::PGIndexElem>(cell->data.ptr_value);
        CheckUnsupportedFeature(*index_element);
        lower_column_names.emplace_back(intarkdb::StringUtil::Lower(index_element->name));
    }

    if (lower_column_names.size() > GS_MAX_INDEX_COLUMNS) {
        throw intarkdb::Exception(ExceptionType::BINDER,
                                  fmt::format("Too many index columns, max count:{}\n", GS_MAX_INDEX_COLUMNS));
    }

    const auto& schema_name = pg_stmt->relation->schemaname ? std::string(pg_stmt->relation->schemaname) : user_;
    const auto& table_name = std::string(pg_stmt->relation->relname);
    auto table = BindBaseTableRef(schema_name, table_name, std::nullopt);
    const auto& metaInfo = table->GetTableInfo().GetTableMetaInfo();
    if (metaInfo.space_id != SQL_SPACE_TYPE_USERS) {
        throw intarkdb::Exception(ExceptionType::CATALOG,
                                  "Cannot create index on system table : " + std::string(metaInfo.name));
    }

    if (metaInfo.is_timescale && (pg_stmt->unique || pg_stmt->primary)) {
        throw intarkdb::Exception(ExceptionType::BINDER,
                                  "Primary key or unique key is not supported for time scale table");
    }

    auto index_stmt = std::make_unique<CreateIndexStatement>(table_name, pg_stmt->idxname, pg_stmt->unique,
                                                             pg_stmt->primary, std::move(lower_column_names));
    if (metaInfo.is_timescale) {
        index_stmt->GetIndexRefMutable().GetIndexMutable().parted = GS_TRUE;
    }
#ifdef ENABLE_PG_QUERY
    index_stmt->SetConflictOpt(pg_stmt->onconflict);
#endif
    return index_stmt;
}
