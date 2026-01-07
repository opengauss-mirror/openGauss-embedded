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
* insert_statement.h
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/include/binder/statement/insert_statement.h
*
* -------------------------------------------------------------------------
*/
#pragma once

#include <memory>
#include <vector>

#include "binder/bound_statement.h"
#include "binder/bound_query_source.h"
#include "binder/statement/select_statement.h"
#include "catalog/column.h"

typedef enum InsertOnConflictActionAlias {
	ONCONFLICT_ALIAS_NONE,    /* No "OR [IGNORE|REPLACE]" clause */
	ONCONFLICT_ALIAS_REPLACE, /* INSERT OR REPLACE */
	ONCONFLICT_ALIAS_IGNORE   /* INSERT OR IGNORE */
} InsertOnConflictActionAlias;

class InsertStatement : public BoundStatement {
   public:
    explicit InsertStatement(std::unique_ptr<BoundQuerySource> table, std::unique_ptr<SelectStatement> select)
        : BoundStatement(StatementType::INSERT_STATEMENT), table_(std::move(table)), select_(std::move(select)) {}

    std::unique_ptr<BoundQuerySource> table_;

    //! The select statement to insert from
    std::unique_ptr<SelectStatement> select_;

    //! Bind columns
    std::vector<Column> bound_columns_;
    std::vector<Column> unbound_defaults_;
    std::vector<Column> bound_defaults_;

    InsertOnConflictActionAlias opt_or_action_;

    auto ToString() const -> std::string override;
};
