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
 * constraint.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/binder/statement/constraint.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "binder/statement/constraint.h"

#include <stdexcept>

Constraint::Constraint() {}

Constraint::Constraint(constraint_type_t type, const std::vector<std::string>& cons_ref_columns, char* cons_name)
    : cons_ref_columns_(cons_ref_columns) {
    def_ = {};
    def_.type = type;
    if (cons_name == NULL) {
        def_.name.str = NULL;
        def_.name.len = 0;
    } else {
        cons_name_ = cons_name;
        def_.name.str = (char*)cons_name_.c_str();
        def_.name.len = cons_name_.length();
    }
    def_.name.assign = ASSIGN_TYPE_EQUAL;
    def_.col_count = cons_ref_columns_.size();
    cons_ref_column_info_ = std::make_unique<col_text_t[]>(sizeof(col_text_t) * def_.col_count);
    def_.cols = cons_ref_column_info_.get();

    for (size_t i = 0; i < cons_ref_columns_.size(); i++) {
        col_text_t* cons_col = def_.cols + i;
        cons_col->str = (char*)cons_ref_columns_[i].c_str();
        cons_col->len = cons_ref_columns_[i].length();
        cons_col->assign = ASSIGN_TYPE_EQUAL;
    }
}

Constraint::Constraint(constraint_type_t type,
                        const std::vector<std::string>& columns, 
                        const std::vector<std::string>& fk_ref_columns,
                        const std::string & fk_ref_user,
                        const std::string & fk_ref_table,
                        char* cons_name)
                    : cons_ref_columns_(columns),
                    fk_ref_columns_(fk_ref_columns),
                    fk_ref_user_(fk_ref_user),
                    fk_ref_table_(fk_ref_table)
{
    def_ = {};
    def_.type = type;
    if (cons_name == NULL) {
        def_.name.str = NULL;
        def_.name.len = 0;
    } else {
        cons_name_ = cons_name;
        def_.name.str = (char*)cons_name_.c_str();
        def_.name.len = cons_name_.length();
    }
    def_.name.assign = ASSIGN_TYPE_EQUAL;

    def_.col_count = cons_ref_columns_.size();
    cons_ref_column_info_ = std::make_unique<col_text_t[]>(sizeof(col_text_t) * def_.col_count);
    def_.cols = cons_ref_column_info_.get();
    for (size_t i = 0; i < cons_ref_columns_.size(); i++) {
        col_text_t* cons_col = def_.cols + i;
        cons_col->str = (char*)cons_ref_columns_[i].c_str();
        cons_col->len = cons_ref_columns_[i].length();
        cons_col->assign = ASSIGN_TYPE_EQUAL;
    }

    def_.fk_ref_col_count = fk_ref_columns_.size();
    fk_ref_column_info_ = std::make_unique<text_t[]>(sizeof(text_t) * def_.fk_ref_col_count);
    def_.fk_ref_cols = fk_ref_column_info_.get();
    for (size_t i = 0; i < fk_ref_columns_.size(); i++) {
        text_t* fk_cons_col = def_.fk_ref_cols + i;
        fk_cons_col->str = (char*)fk_ref_columns_[i].c_str();
        fk_cons_col->len = fk_ref_columns_[i].length();
    }
    def_.fk_ref_user.str = (char*)fk_ref_user_.c_str();
    def_.fk_ref_user.len = fk_ref_user_.length();
    def_.fk_ref_table.str = (char*)fk_ref_table_.c_str();
    def_.fk_ref_table.len = fk_ref_table_.length();
}

Constraint::Constraint(const Constraint& other) {
    cons_ref_columns_ = other.cons_ref_columns_;
    cons_name_ = other.cons_name_;
    cons_ref_column_info_ = std::make_unique<col_text_t[]>(sizeof(col_text_t) * def_.col_count);
    def_ = other.def_;

    if (cons_name_.empty()) {
        def_.name = {};
    } else {
        def_.name.str = (char*)cons_name_.c_str();
        def_.name.len = cons_name_.length();
    }
    def_.cols = cons_ref_column_info_.get();
    for (size_t i = 0; i < cons_ref_columns_.size(); i++) {
        col_text_t* cons_col = def_.cols + i;
        cons_col->str = (char*)cons_ref_columns_[i].c_str();
        cons_col->len = cons_ref_columns_[i].length();
        cons_col->assign = ASSIGN_TYPE_EQUAL;
    }

    // fk
    fk_ref_user_ = other.fk_ref_user_;
    fk_ref_table_ = other.fk_ref_table_;
    fk_ref_columns_ = other.fk_ref_columns_;
    fk_ref_column_info_ = std::make_unique<text_t[]>(other.def_.fk_ref_col_count);
    def_.fk_ref_cols = fk_ref_column_info_.get();
    for (size_t i = 0; i < fk_ref_columns_.size(); i++) {
        text_t * fk_ref_col = def_.fk_ref_cols + i;
        fk_ref_col->str = (char*)fk_ref_columns_[i].c_str();
        fk_ref_col->len = fk_ref_columns_[i].length();
    }
    def_.fk_ref_user.str = (char*)fk_ref_user_.c_str();
    def_.fk_ref_user.len = fk_ref_user_.length();
    def_.fk_ref_table.str = (char*)fk_ref_table_.c_str();
    def_.fk_ref_table.len = fk_ref_table_.length();
}

Constraint& Constraint::operator=(const Constraint& other) {
    if (this != &other) {
        cons_ref_columns_ = other.cons_ref_columns_;
        cons_name_ = other.cons_name_;
        cons_ref_column_info_ = std::make_unique<col_text_t[]>(sizeof(col_text_t) * def_.col_count);
        def_ = other.def_;

        if (cons_name_.empty()) {
            def_.name = {};
        } else {
            def_.name.str = (char*)cons_name_.c_str();
            def_.name.len = cons_name_.length();
        }
        def_.cols = cons_ref_column_info_.get();
        for (size_t i = 0; i < cons_ref_columns_.size(); i++) {
            col_text_t* cons_col = def_.cols + i;
            cons_col->str = (char*)cons_ref_columns_[i].c_str();
            cons_col->len = cons_ref_columns_[i].length();
            cons_col->assign = ASSIGN_TYPE_EQUAL;
        }

        // fk
        fk_ref_user_ = other.fk_ref_user_;
        fk_ref_table_ = other.fk_ref_table_;
        fk_ref_columns_ = other.fk_ref_columns_;
        fk_ref_column_info_ = std::make_unique<text_t[]>(other.def_.fk_ref_col_count);
        def_.fk_ref_cols = fk_ref_column_info_.get();
        for (size_t i = 0; i < fk_ref_columns_.size(); i++) {
            text_t * fk_ref_col = def_.fk_ref_cols + i;
            fk_ref_col->str = (char*)fk_ref_columns_[i].c_str();
            fk_ref_col->len = fk_ref_columns_[i].length();
        }
        def_.fk_ref_user.str = (char*)fk_ref_user_.c_str();
        def_.fk_ref_user.len = fk_ref_user_.length();
        def_.fk_ref_table.str = (char*)fk_ref_table_.c_str();
        def_.fk_ref_table.len = fk_ref_table_.length();
    }
    return *this;
}

Constraint& Constraint::operator=(Constraint&& other) {
    if (this != &other) {
        cons_ref_columns_ = std::move(other.cons_ref_columns_);
        cons_name_ = std::move(other.cons_name_);
        cons_ref_column_info_ = std::move(other.cons_ref_column_info_);
        def_ = other.def_;

        if (cons_name_.empty()) {
            def_.name = {};
        } else {
            def_.name.str = (char*)cons_name_.c_str();
            def_.name.len = cons_name_.length();
        }
        def_.cols = cons_ref_column_info_.get();
        for (size_t i = 0; i < cons_ref_columns_.size(); i++) {
            col_text_t* cons_col = def_.cols + i;
            cons_col->str = (char*)cons_ref_columns_[i].c_str();
            cons_col->len = cons_ref_columns_[i].length();
            cons_col->assign = ASSIGN_TYPE_EQUAL;
        }

        // fk
        fk_ref_user_ = other.fk_ref_user_;
        fk_ref_table_ = other.fk_ref_table_;
        fk_ref_columns_ = other.fk_ref_columns_;
        fk_ref_column_info_ = std::make_unique<text_t[]>(other.def_.fk_ref_col_count);
        def_.fk_ref_cols = fk_ref_column_info_.get();
        for (size_t i = 0; i < fk_ref_columns_.size(); i++) {
            text_t * fk_ref_col = def_.fk_ref_cols + i;
            fk_ref_col->str = (char*)fk_ref_columns_[i].c_str();
            fk_ref_col->len = fk_ref_columns_[i].length();
        }
        def_.fk_ref_user.str = (char*)fk_ref_user_.c_str();
        def_.fk_ref_user.len = fk_ref_user_.length();
        def_.fk_ref_table.str = (char*)fk_ref_table_.c_str();
        def_.fk_ref_table.len = fk_ref_table_.length();

        other.def_ = {};
    }
    return *this;
}

Constraint::Constraint(Constraint&& other) noexcept { *this = std::move(other); }

Constraint::~Constraint() {}
