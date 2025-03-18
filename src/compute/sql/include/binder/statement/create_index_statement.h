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
* create_index_statement.h
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/include/binder/statement/create_index_statement.h
*
* -------------------------------------------------------------------------
*/
#pragma once

#include "binder/bound_statement.h"
#include "binder/statement/constraint.h"
#include "catalog/column.h"
#include "catalog/index.h"
#ifdef ENABLE_PG_QUERY
#include "nodes/primnodes.hpp"
#endif

class CreateIndexStatement : public BoundStatement {
   public:
    explicit CreateIndexStatement(const std::string& table_name, char* index_name, bool is_unique, bool is_primary,
                                  std::vector<std::string>&& columns);
    ~CreateIndexStatement() {
        if (index_.GetIndexMutable().cols != NULL) {
            delete[] index_.GetIndexMutable().cols;
        }
    }

    const Index& GetIndexRef() const { return index_; }

    Index& GetIndexRefMutable() { return index_; }

    const std::string& GetTableName() const { return index_.TableName(); }
    const std::string& GetIndexName() const { return index_.Name(); }

#ifdef ENABLE_PG_QUERY
    const duckdb_libpgquery::PGOnCreateConflict GetConflictOpt() const { return on_conflict_; }
    void SetConflictOpt(duckdb_libpgquery::PGOnCreateConflict opt) { on_conflict_ = opt; }
#endif

   private:
    std::vector<std::string> column_names_;
    Index index_;
    //! What to do on create conflict
#ifdef ENABLE_PG_QUERY
    duckdb_libpgquery::PGOnCreateConflict on_conflict_{duckdb_libpgquery::PGOnCreateConflict::PG_ERROR_ON_CONFLICT};
#endif
};
