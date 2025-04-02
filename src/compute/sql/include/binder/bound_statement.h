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
 * bound_statement.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/binder/bound_statement.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <fmt/format.h>

#include <string>
#include <unordered_map>
#include <vector>

#include "binder/statement_type.h"
#include "type/value.h"

// wrapper pgquery statement
namespace duckdb_libpgquery {
struct PGNode;
};
struct ParsedStatement {
    duckdb_libpgquery::PGNode* stmt;
};

// base class of SQL statement
class BoundStatement {
   public:
    explicit BoundStatement(StatementType type) : type_(type) {}
    virtual ~BoundStatement() = default;

    virtual std::string ToString() const { return "BoundStatement"; }

    StatementType Type() const { return type_; }

   private:
    StatementType type_;

   public:
    //! The number of prepared statement parameters (if any)
    uint32_t n_param = 0;

    //! sql statement
    std::string query;

    StatementProperty props;
};
