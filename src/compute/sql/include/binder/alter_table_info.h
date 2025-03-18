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
 * alter_table_info.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/binder/alter_table_info.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <cstdint>
#include <memory>
#include <vector>
#include "binder/bound_expression.h"
#include "catalog/column.h"
#include "storage/gstor/zekernel/common/cm_defs.h"
#include "type/type_system.h"
//===--------------------------------------------------------------------===//
// Alter Table
//===--------------------------------------------------------------------===//
// alter table，复用gstor定义
using GsAlterTableType = altable_action_t;

class AlterTableInfo {
   public:
    AlterTableInfo(GsAlterTableType type, char* table_name, const std::vector<col_text_t>& old_names,
                   const std::vector<col_text_t>& new_names, const std::vector<Column>& al_cols);
    AlterTableInfo(GsAlterTableType type, char* table_name, const std::vector<exp_al_column_def_t>& al_cols);
    AlterTableInfo(GsAlterTableType type, char* table_name, const exp_alt_table_prop_t& alt_table);
    AlterTableInfo(GsAlterTableType type, char* table_name, const exp_alt_table_part_t& part_opt);
    AlterTableInfo(GsAlterTableType type, char* table_name, char* table_comment);
    AlterTableInfo(GsAlterTableType type, char* table_name, char* constraint_name, constraint_type_t cons_type,
                   std::vector<std::string>&& col_names);
    virtual ~AlterTableInfo();

    const exp_altable_def_t& GetAltableDefs() const { return alter_table_; }

    exp_altable_def_t& GetAltableDefsMutable() { return alter_table_; }

   public:
    GsAlterTableType alter_table_type;
    exp_altable_def_t alter_table_;
    std::vector<std::string> col_names_;
};