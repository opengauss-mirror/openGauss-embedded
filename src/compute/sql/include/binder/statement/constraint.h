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
 * constraint.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/binder/statement/constraint.h
 *
 * -------------------------------------------------------------------------
 */

#pragma once

#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "catalog/column.h"
#include "catalog/index.h"
#include "storage/gstor/gstor_executor.h"

class Constraint {
   public:
    Constraint();
    Constraint(const Constraint& other);
    Constraint(Constraint&& other) noexcept;
    Constraint& operator=(const Constraint& other);
    Constraint& operator=(Constraint&& other);
    explicit Constraint(constraint_type_t type, const std::vector<std::string>& columns, char* name = NULL);
    explicit Constraint(constraint_type_t type,
                        const std::vector<std::string>& columns, 
                        const std::vector<std::string>& fk_ref_columns,
                        const std::string & fk_ref_user,
                        const std::string & fk_ref_table,
                        char* name = NULL);
    ~Constraint();

   public:
    constraint_type_t Type() const { return def_.type; }

   public:
    uint16 GetConsColCount() { return cons_ref_columns_.size(); }
    const std::string& GetConsColumnName(uint32 idx) const { return cons_ref_columns_[idx]; }
    const exp_constraint_def_t& GetConsDef() const { return def_; }

   public:
    std::vector<std::string> cons_ref_columns_;  // constraint column name list 索引涉及的列名
    std::string cons_name_;                      // constraint name 约束名
    exp_constraint_def_t def_;
    std::unique_ptr<col_text_t[]> cons_ref_column_info_;

    std::vector<std::string> fk_ref_columns_;
    std::string fk_ref_user_;
    std::string fk_ref_table_;
    std::unique_ptr<text_t[]> fk_ref_column_info_;

    char fk_matchtype_;
    knl_refactor_t fk_upd_action_;
    knl_refactor_t fk_del_action_;
};
