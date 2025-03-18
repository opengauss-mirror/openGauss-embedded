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
 * create_statement.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/binder/statement/create_statement.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <unordered_map>

#include "binder/bound_expression.h"
#include "binder/bound_statement.h"
#include "binder/expressions/bound_constant.h"
#include "binder/statement/constraint.h"
#include "binder/statement/select_statement.h"
#include "catalog/column.h"
#include "catalog/index.h"
#include "common/exception.h"
#ifdef ENABLE_PG_QUERY
#include "nodes/primnodes.hpp"
#endif

class Column;
class Constraint;

class CreateStatement : public BoundStatement {
   public:
    explicit CreateStatement(std::string table, std::vector<Column> columns);
    explicit CreateStatement(std::string table);
    explicit CreateStatement(std::string_view table);
    ~CreateStatement() {}

    const std::string& GetTableName() const { return tablename_; }

    const std::vector<Column>& GetColumns() const { return columns_; }
    void SetColumns(std::vector<Column>&& columns) { columns_ = std::move(columns); }

    const std::vector<Constraint>& GetConstraints() const { return constraints_; }
    auto SetConstraints(std::vector<Constraint>&& constraints) { constraints_ = std::move(constraints); }

    std::vector<exp_constraint_def_t> GetConstraintDefs() const {
        std::vector<exp_constraint_def_t> contras;
        for (const auto& cons : constraints_) {
            contras.push_back(cons.GetConsDef());
        }
        return contras;
    }

    const std::vector<Index>& GetIndexs() const { return indexs_; }
    void SetIndexs(std::vector<Index>&& indexs) { indexs_ = std::move(indexs); }
    std::vector<Index>& GetIndexsMutable() { return indexs_; }

    std::vector<exp_index_def_t> GetIndexDefs() const {
        std::vector<exp_index_def_t> indexs;
        for (const auto& index : indexs_) {
            indexs.push_back(index.GetIndex());
        }
        return indexs;
    }

    void SetTimescale(bool timescale_flag) { is_timescale_ = timescale_flag; }
    bool Timescale() const { return is_timescale_; }

    void SetRetention(char* ps_retention) { retention_ = ps_retention; }
    const std::string& Retention() const { return retention_; }

    void SetPartition(bool parted) { parted_ = parted; }
    bool Partition() const { return parted_; }

    const exp_part_obj_def_t& GetPartObjDef() const { return part_def_; }
    exp_part_obj_def_t& GetPartObjDefMutable() { return part_def_; }

#ifdef ENABLE_PG_QUERY
    const duckdb_libpgquery::PGOnCreateConflict GetConflictOpt() const { return on_conflict_; }
    void SetConflictOpt(duckdb_libpgquery::PGOnCreateConflict opt) { on_conflict_ = opt; }
#endif

    // TABLE COMMENT
    void SetComment(char* comment) {
        if (comment) {
            comment_ = comment;
        }
    }

    const std::string& Comment() const { return comment_; }

    // AUTOINCREMENT
    void SetAutoIncrement(bool autoincrement) { has_autoincrement_ = autoincrement; }
    bool HasAutoIncrement() const { return has_autoincrement_; }

   private:
    std::string tablename_;
    std::vector<Column> columns_;
    std::vector<Index> indexs_;
    std::vector<Constraint> constraints_;
    bool is_timescale_{GS_FALSE};
    std::string retention_;
    bool parted_{GS_FALSE};
    exp_part_obj_def_t part_def_;
    std::string comment_;
    bool has_autoincrement_{GS_FALSE};
#ifdef ENABLE_PG_QUERY
    duckdb_libpgquery::PGOnCreateConflict on_conflict_{duckdb_libpgquery::PGOnCreateConflict::PG_ERROR_ON_CONFLICT};
#endif
};
