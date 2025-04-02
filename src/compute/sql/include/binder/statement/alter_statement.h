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
 * alter_statement.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/binder/statement/alter_statement.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <map>
#include "binder/alter_table_info.h"
#include "binder/bound_statement.h"
#include "binder/statement/constraint.h"
#include "catalog/column.h"
#include "catalog/index.h"

class Column;

class AlterStatement : public BoundStatement {
   public:
    explicit AlterStatement(const std::string schema_name, const std::string table_name)
        : BoundStatement(StatementType::ALTER_STATEMENT),
        schema_name_(schema_name),
        table_name_(table_name){};

    ~AlterStatement();

    const std::string& GetTableName() const { return table_name_; }

    const GsAlterTableType AlterType() const { return info->alter_table_type; }

    const exp_altable_def_t& GetAlterTableInfoDefs() const { return info->GetAltableDefs(); }

    exp_altable_def_t& GetAlterTableInfoDefMutable() { return info->GetAltableDefsMutable(); }



   public:
    std::string schema_name_;
    std::string table_name_;
    std::unique_ptr<AlterTableInfo> info;
    std::vector<Column> al_cols;

   public:
    std::vector<Constraint> constraints;
    std::string hpartbound_;  // 微秒时间戳字符串，如1692670992000000
   protected:
    AlterStatement(const AlterStatement& other);
};