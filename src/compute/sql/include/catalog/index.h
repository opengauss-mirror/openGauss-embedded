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
* index.h
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/include/catalog/index.h
*
* -------------------------------------------------------------------------
*/
#pragma once

#include <string>

#include "catalog/schema.h"
#include "storage/gstor/gstor_executor.h"

class Index {
   public:
    explicit Index(const exp_index_def_t& index) : index_def_(index) { name_ = std::string(index_def_.name.str); }
    Index(const std::string& name, const std::string& table_name, const exp_index_def_t& col)
        : name_(name), table_name_(table_name), index_def_(col) {}

    // 索引涉及的列数
    uint32 GetRefColCount() const { return index_def_.col_count; }

    const std::string& Name() const { return name_; }

    const std::string& TableName() const { return table_name_; }

    const exp_index_def_t& GetIndex() const { return index_def_; }
    const exp_index_def_t& GetIndexDef() const { return GetIndex(); }

    exp_index_def_t& GetIndexMutable() { return index_def_; }

   private:
    std::string name_;
    std::string table_name_;
    exp_index_def_t index_def_;
};
