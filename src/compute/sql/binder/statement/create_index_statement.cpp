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
* create_index_statement.cpp
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/binder/statement/create_index_statement.cpp
*
* -------------------------------------------------------------------------
*/
#include "binder/statement/create_index_statement.h"
#ifdef _MSC_VER
CreateIndexStatement::CreateIndexStatement(const std::string& table_name, char* index_name, bool is_unique,
                                           bool is_primary, std::vector<std::string>&& columns)
    : BoundStatement(StatementType::INDEX_STATEMENT),
      column_names_(std::move(columns)),
      index_(std::string(index_name), table_name, exp_index_def_t{}) {
    // 创建 exp_index_def_t 实例
    exp_index_def_t index_def;
    index_def.name.str = index_name;
    index_def.name.len = static_cast<uint32_t>(strlen(index_name));
    index_def.name.assign = ASSIGN_TYPE_EQUAL;
    index_def.cols = nullptr;  // 使用 nullptr
    index_def.col_count = static_cast<uint32_t>(column_names_.size());
    index_def.is_unique = is_unique;
    index_def.is_primary = is_primary;

    // 先构造 index_ 然后再赋值
    index_ = Index(std::string(index_name), table_name, index_def);

    // 处理列
    index_.GetIndexMutable().cols = new col_text_t[index_.GetRefColCount()]();
    auto cur_col = index_.GetIndexMutable().cols;
    for (uint32_t idx = 0; idx < index_.GetRefColCount(); idx++) {
        cur_col->str = const_cast<char*>(column_names_[idx].c_str());
        cur_col->len = column_names_[idx].size();
        cur_col->assign = ASSIGN_TYPE_EQUAL;
        ++cur_col;
    }
}
#else
CreateIndexStatement::CreateIndexStatement(const std::string& table_name, char* index_name, bool is_unique, bool is_primary,
                                           std::vector<std::string>&& columns)
    : BoundStatement(StatementType::INDEX_STATEMENT),column_names_(std::move(columns)),
      index_(std::string(index_name), table_name,
             exp_index_def_t{.name = {.str = index_name,
                                      .len = static_cast<uint32_t>(strlen(index_name)),
                                      .assign = ASSIGN_TYPE_EQUAL},
                             .cols = NULL,
                             .col_count = static_cast<uint32_t>(column_names_.size()),
                             .is_unique = is_unique,
                             .is_primary = is_primary}) {
    index_.GetIndexMutable().cols = new col_text_t[index_.GetRefColCount()]();
    auto cur_col = index_.GetIndexMutable().cols;
    for (uint32 idx=0; idx < index_.GetRefColCount(); idx++){
        cur_col->str = const_cast<char*>(column_names_[idx].c_str());
        cur_col->len = column_names_[idx].size();
        cur_col->assign = ASSIGN_TYPE_EQUAL;
        ++cur_col;
    }
}
#endif