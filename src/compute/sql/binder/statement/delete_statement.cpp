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
 * delete_statement.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/binder/statement/delete_statement.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "binder/statement/delete_statement.h"

DeleteStatement::DeleteStatement(std::unique_ptr<BoundBaseTable> table, std::unique_ptr<BoundExpression> expr)
    : BoundStatement(StatementType::DELETE_STATEMENT), target_table(std::move(table)), condition(std::move(expr)) {}

DeleteStatement::DeleteStatement() : BoundStatement(StatementType::DELETE_STATEMENT) {}
