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
 * copy_statement.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/binder/statement/copy_statement.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <memory>

#include "binder/bound_statement.h"
#include "binder/statement/select_statement.h"
#include "binder/table_ref/bound_base_table.h"
#include "type/value.h"

struct CopyInfo {
    std::unique_ptr<BoundBaseTable> table_ref;
    std::vector<std::unique_ptr<BoundExpression>> select_list;
    bool is_from;
    std::string format;
    std::string file_path;
    std::unordered_map<std::string, std::vector<Value>> options;
};

class CopyStatement : public BoundStatement {
   public:
    CopyStatement();
    std::string ToString() const override;

    std::unique_ptr<CopyInfo> info;
    std::unique_ptr<SelectStatement> select_statement;
};
