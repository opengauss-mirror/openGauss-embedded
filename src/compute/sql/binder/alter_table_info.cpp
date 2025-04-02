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
 * alter_table_info.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/binder/alter_table_info.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "binder/alter_table_info.h"

static void alter_column_copy(const exp_column_def_t* in_col, exp_column_def_t* out_col) {
    out_col->name.str = in_col->name.str;
    out_col->name.len = in_col->name.len;
    out_col->col_type = in_col->col_type;
    out_col->col_slot = in_col->col_slot;
    out_col->size = in_col->size;
    out_col->nullable = in_col->nullable;
    out_col->is_primary = in_col->is_primary;
    out_col->is_default = in_col->is_default;
    if (out_col->is_default) {
        out_col->default_val.str = in_col->default_val.str;
        out_col->default_val.len = in_col->default_val.len;
    }
    if (in_col->comment.len > 0) {
        out_col->comment.str = in_col->comment.str;
        out_col->comment.len = in_col->comment.len;
    }
    out_col->precision = in_col->precision;
    out_col->scale = in_col->scale;
    out_col->is_unique = in_col->is_unique;
    out_col->is_comment = in_col->is_comment;
    out_col->is_check = in_col->is_check;
}
//===--------------------------------------------------------------------===//
// AlterTableInfo
//===--------------------------------------------------------------------===//
AlterTableInfo::AlterTableInfo(GsAlterTableType type, char* table_name, const std::vector<col_text_t>& old_names,
                               const std::vector<col_text_t>& new_names, const std::vector<Column>& al_cols)
    : alter_table_type(type) {
    alter_table_.action = alter_table_type;
    alter_table_.name.str = table_name;
    alter_table_.name.len = (uint32)strlen(table_name);
    alter_table_.name.assign = ASSIGN_TYPE_EQUAL;
    if (alter_table_type == GsAlterTableType::ALTABLE_RENAME_COLUMN) {
        alter_table_.col_count = new_names.size();
    } else {
        alter_table_.col_count = al_cols.size();
    }
    alter_table_.cols = new exp_al_column_def_t[sizeof(exp_al_column_def_t) * alter_table_.col_count]();
    for (uint32 idx = 0; idx < alter_table_.col_count; idx++) {
        exp_al_column_def_t* def = &alter_table_.cols[idx];
        auto& col = al_cols[idx].GetRaw();
        if (alter_table_type == GsAlterTableType::ALTABLE_RENAME_COLUMN) {
            def->name.str = old_names[idx].str;
            def->name.len = old_names[idx].len;
            def->name.assign = old_names[idx].assign;
            def->new_name.str = new_names[idx].str;
            def->new_name.len = new_names[idx].len;
            def->new_name.assign = new_names[idx].assign;
        } else {
            def->name.str = col.name.str;
            def->name.len = col.name.len;
            def->name.assign = col.name.assign;
        }
        if (alter_table_type == GsAlterTableType::ALTABLE_ADD_COLUMN ||
            alter_table_type == GsAlterTableType::ALTABLE_MODIFY_COLUMN) {
            alter_column_copy(&col, &def->col_def);
        }
        def->constr_def = NULL;
    }
    alter_table_.constraint = NULL;
}

AlterTableInfo::AlterTableInfo(GsAlterTableType type, char* table_name, const std::vector<exp_al_column_def_t>& al_cols)
    : alter_table_type(type) {
    alter_table_.action = alter_table_type;
    alter_table_.name.str = table_name;
    alter_table_.name.len = (uint32)strlen(table_name);
    alter_table_.name.assign = ASSIGN_TYPE_EQUAL;
    alter_table_.col_count = al_cols.size();
    alter_table_.cols = new exp_al_column_def_t[sizeof(exp_al_column_def_t) * alter_table_.col_count]();
    for (uint32 idx = 0; idx < alter_table_.col_count; idx++) {
        exp_al_column_def_t* def = &alter_table_.cols[idx];
        def->name.str = al_cols[idx].name.str;
        def->name.len = al_cols[idx].name.len;
        def->name.assign = al_cols[idx].name.assign;
        if (alter_table_type == GsAlterTableType::ALTABLE_RENAME_COLUMN) {
            def->new_name.str = al_cols[idx].new_name.str;
            def->new_name.len = al_cols[idx].new_name.len;
            def->new_name.assign = al_cols[idx].new_name.assign;
        }
        if (alter_table_type == GsAlterTableType::ALTABLE_ADD_COLUMN ||
            alter_table_type == GsAlterTableType::ALTABLE_MODIFY_COLUMN) {
            def->col_def = al_cols[idx].col_def;
        }
        def->constr_def = NULL;
    }
    alter_table_.constraint = NULL;
}

AlterTableInfo::AlterTableInfo(GsAlterTableType type, char* table_name, const exp_alt_table_prop_t& alt_table)
    : alter_table_type(type) {
    alter_table_.action = alter_table_type;
    alter_table_.name.str = table_name;
    alter_table_.name.len = static_cast<uint32>(strlen(table_name));
    alter_table_.name.assign = ASSIGN_TYPE_EQUAL;

    alter_table_.altable.new_name.str = alt_table.new_name.str;
    alter_table_.altable.new_name.len = alt_table.new_name.len;

    alter_table_.col_count = 0;
    alter_table_.cols = NULL;
    alter_table_.constraint = NULL;
}

AlterTableInfo::AlterTableInfo(GsAlterTableType type, char* table_name, const exp_alt_table_part_t& part_opt)
    : alter_table_type(type) {
    alter_table_.action = alter_table_type;
    alter_table_.name.str = table_name;
    alter_table_.name.len = static_cast<uint32>(strlen(table_name));
    alter_table_.name.assign = ASSIGN_TYPE_EQUAL;

    alter_table_.part_opt.part_name.str = part_opt.part_name.str;
    alter_table_.part_opt.part_name.len = part_opt.part_name.len;
    alter_table_.part_opt.part_name.assign = part_opt.part_name.assign;

    alter_table_.part_opt.part_type = part_opt.part_type;

    alter_table_.col_count = 0;
    alter_table_.cols = NULL;
    alter_table_.constraint = NULL;
}

AlterTableInfo::AlterTableInfo(GsAlterTableType type, char* table_name, char* table_comment) : alter_table_type(type) {
    alter_table_.action = alter_table_type;
    alter_table_.name.str = table_name;
    alter_table_.name.len = static_cast<uint32>(strlen(table_name));
    alter_table_.name.assign = ASSIGN_TYPE_EQUAL;
    alter_table_.comment = table_comment;

    alter_table_.col_count = 0;
    alter_table_.cols = NULL;
    alter_table_.constraint = NULL;
}

AlterTableInfo::AlterTableInfo(GsAlterTableType type, char* table_name, char* constraint_name,
                               constraint_type_t cons_type, std::vector<std::string>&& col_names)
    : alter_table_type(type), col_names_(std::move(col_names)) {
    memset(&alter_table_, 0, sizeof(exp_altable_def_t));  // 初始化
    alter_table_.action = alter_table_type;
    alter_table_.name.str = table_name;
    alter_table_.name.len = static_cast<uint32>(strlen(table_name));
    alter_table_.name.assign = ASSIGN_TYPE_EQUAL;
    alter_table_.cols = NULL;
    alter_table_.col_count = 0;
    alter_table_.constraint = new exp_constraint_def_t{};
    alter_table_.constraint->type = cons_type;
    alter_table_.constraint->name.str = constraint_name;
    alter_table_.constraint->name.len = static_cast<uint32>(strlen(constraint_name));
    alter_table_.constraint->col_count = col_names_.size();
    alter_table_.constraint->cols = new col_text_t[sizeof(col_text_t) * col_names_.size()]();
    for (uint32 idx = 0; idx < col_names_.size(); idx++) {
        alter_table_.constraint->cols[idx].str = (char*)col_names_[idx].c_str();
        alter_table_.constraint->cols[idx].len = static_cast<uint32>(col_names_[idx].size());
        alter_table_.constraint->cols[idx].assign = ASSIGN_TYPE_EQUAL;
    }
}

AlterTableInfo::~AlterTableInfo() {
    for (uint32 idx = 0; idx < alter_table_.col_count; idx++) {
        // note: constr_def 好像没用到
        if (alter_table_.cols[idx].constr_def != NULL) {
            delete[] alter_table_.cols[idx].constr_def;
        }
    }

    if (alter_table_.cols != NULL) {
        delete[] alter_table_.cols;
    }

    if (alter_table_.constraint != NULL) {
        if (alter_table_.constraint->cols != NULL) {
            delete[] alter_table_.constraint->cols;
        }
        delete alter_table_.constraint;
    }
}
