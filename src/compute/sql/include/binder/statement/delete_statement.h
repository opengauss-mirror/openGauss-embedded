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
 * delete_statement.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/binder/statement/delete_statement.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <memory>
#include <string>

#include "binder/bound_expression.h"
#include "binder/bound_statement.h"
#include "binder/table_ref/bound_base_table.h"

class DeleteStatement : public BoundStatement {
   public:
    explicit DeleteStatement(std::unique_ptr<BoundBaseTable> table, std::unique_ptr<BoundExpression> expr);
    DeleteStatement();
    ~DeleteStatement() = default;

    std::unique_ptr<BoundBaseTable> target_table;
    std::unique_ptr<BoundExpression> condition;
};
