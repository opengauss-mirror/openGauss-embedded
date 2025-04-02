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
* bind_synonym.cpp
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/binder/bind_synonym.cpp
*
* -------------------------------------------------------------------------
*/
#include "binder/binder.h"
#include "common/exception.h"
#include "common/null_check_ptr.h"
#include "common/string_util.h"
#include "common/util.h"

auto Binder::BindCreateSynonym(duckdb_libpgquery::PGSynonymStmt *stmt) -> std::unique_ptr<SynonymStatement> {
    auto result = std::make_unique<SynonymStatement>();

    if (!stmt->synonym) {
        throw intarkdb::Exception(ExceptionType::BINDER, "Synonym name is null");
    }
    if (!stmt->table) {
        throw intarkdb::Exception(ExceptionType::BINDER, "Table name is null");
    }

    auto synonym_var = NullCheckPtrCast<duckdb_libpgquery::PGRangeVar>(stmt->synonym);
    result->synonym_schema = synonym_var->schemaname ? synonym_var->schemaname : user_;
    result->synonym_name = synonym_var->relname;

    auto table_var = NullCheckPtrCast<duckdb_libpgquery::PGRangeVar>(stmt->table);
    result->table_schema = table_var->schemaname ? table_var->schemaname : user_;
    result->table_name = table_var->relname;

    return result;
}
