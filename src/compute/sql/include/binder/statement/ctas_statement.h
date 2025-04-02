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
 * ctas_statement.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/binder/statement/ctas_statement.h
 *
 * -------------------------------------------------------------------------
 */
// ctas: create table as select
#pragma once

#include "binder/bound_statement.h"
#include "binder/statement/create_statement.h"
#include "binder/statement/select_statement.h"
#include "common/exception.h"

class CtasStatement : public BoundStatement {
   public:
    explicit CtasStatement() : BoundStatement(StatementType::CTAS_STATEMENT) {}
    explicit CtasStatement(std::string table) : BoundStatement(StatementType::CTAS_STATEMENT), tablename_(table) {}
    explicit CtasStatement(std::string table, std::unique_ptr<SelectStatement> select_statement,
                           std::unique_ptr<CreateStatement> create_statement, bool temporary = false)
        : BoundStatement(StatementType::CTAS_STATEMENT),
          tablename_(table),
          as_select_(std::move(select_statement)),
          create_stmt_(std::move(create_statement)) {}
    ~CtasStatement() {}

    const std::string TableName() const { return tablename_; }

   public:
    std::string tablename_;
    std::unique_ptr<SelectStatement> as_select_;
    std::unique_ptr<CreateStatement> create_stmt_;
};
