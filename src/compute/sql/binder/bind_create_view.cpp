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
* bind_create_view.cpp
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/binder/bind_create_view.cpp
*
* -------------------------------------------------------------------------
*/

#include "binder/binder.h"
#include "planner/planner.h"
#include "common/string_util.h"

void CheckUnSpportedWithCreateViewOptions(duckdb_libpgquery::PGViewStmt *stmt) {
    if (stmt->aliases) {
        // 没有实现aliases，以后实现
        throw intarkdb::Exception(ExceptionType::SYNTAX, "unsupported create view with aliases");
    }

    if (stmt->options && stmt->options->length > 0) {
        throw intarkdb::Exception(ExceptionType::SYNTAX, "unsupported create view with options");
    }
    if (stmt->withCheckOption != duckdb_libpgquery::PGViewCheckOption::PG_NO_CHECK_OPTION) {
        throw intarkdb::Exception(ExceptionType::SYNTAX, "unsupported create view with check option");
    }

    if (stmt->onconflict == duckdb_libpgquery::PG_REPLACE_ON_CONFLICT) {
        throw intarkdb::Exception(ExceptionType::SYNTAX, "unsupported create view with on conflict");
    }

    if (stmt->view->relpersistence == duckdb_libpgquery::PG_RELPERSISTENCE_TEMP) {
        throw intarkdb::Exception(ExceptionType::SYNTAX, "unsupported create view with temp");
    }
}

auto Binder::BindCreateView(duckdb_libpgquery::PGViewStmt *stmt) -> std::unique_ptr<CreateViewStatement> {
    // CheckSysPrivilege
    if (catalog_.CheckSysPrivilege(CREATE_VIEW) != GS_TRUE) {
        throw intarkdb::Exception(ExceptionType::BINDER,
                                fmt::format("user {} create view permission denied!", user_));
    }

    auto viewName = intarkdb::StringUtil::Lower(stmt->view->relname);
    auto queryStmt = BindStatement(stmt->query);

    if(ParamCount() > 0 ) {
        throw intarkdb::Exception(ExceptionType::BINDER, "unsupported create view with params");
    }

    CheckUnSpportedWithCreateViewOptions(stmt);

    auto viewStmt = std::make_unique<CreateViewStatement>(std::move(viewName), std::move(queryStmt));

    if (stmt->onconflict == duckdb_libpgquery::PG_IGNORE_ON_CONFLICT) {
        viewStmt->ignore_conflict = true;
    }

    return viewStmt;
}
