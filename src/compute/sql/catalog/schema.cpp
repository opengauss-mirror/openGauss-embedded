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
 * schema.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/catalog/schema.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "catalog/schema.h"

#include "common/string_util.h"

Schema::Schema(const std::string& table_name, const std::vector<Column>& columns) {
    for (const auto& col : columns) {
        columns_.push_back(ColumnToSchemaColumnInfo(table_name, col));
    }
}

auto Schema::GetColumns() const -> std::vector<Column> {
    std::vector<Column> cols;
    for (const auto& col : columns_) {
        cols.emplace_back(SchemaColumnInfoToColumn(col));
    }
    return cols;
}

auto Schema::GetColumn(const uint32_t idx) const -> Column {
    const auto& col = columns_[idx];
    return SchemaColumnInfoToColumn(col);
}

auto Schema::GetColumnInfoByIdx(size_t idx) const -> const SchemaColumnInfo& {
    if (idx < columns_.size()) {
        return columns_[idx];
    }
    throw std::runtime_error("get column info outofbounds idx=" + std::to_string(idx));
}

Schema::Schema(std::vector<SchemaColumnInfo>&& columns) : columns_(std::move(columns)) {}

auto Schema::cmp_col_alias(const std::vector<std::string>& name, const std::string& alias) -> bool {
    if (name.size() == 1) {
        return name[0] == alias;
    }
    return false;
}

auto Schema::cmp_col_name(const std::vector<std::string>& name, const std::vector<std::string>& col_name) -> bool {
    if (name.size() == 1) {
        return name[0] == col_name[0];
    }
    return name[0] == col_name[0] && name[1] == col_name[1];
}
// rename column name if has same name
auto Schema::RenameSchemaColumnIfExistSameNameColumns(const Schema& schema) -> Schema {
    intarkdb::CaseInsensitiveMap<int> column_names;
    std::vector<SchemaColumnInfo> column_defs;
    const auto& columns = schema.GetColumnInfos();
    size_t col_num = columns.size();
    column_defs.reserve(col_num);
    for (size_t i = 0; i < col_num; ++i) {
        const auto& col = columns[i];
        SchemaColumnInfo new_col = col;
        std::string name = "";
        if (!new_col.alias.empty()) {
            name = new_col.alias;
            new_col.alias.clear();
        } else {
            name = new_col.col_name.back();
        }
        auto iter = column_names.find(name);
        if (iter != column_names.end()) {
            name = fmt::format("{}_{}", name, iter->second);
            iter->second++;
        } else {
            column_names.insert({name, 1});
        }
        new_col.col_name.back() = name;
        new_col.slot = i;  // 重新设置 slot
        column_defs.emplace_back(std::move(new_col));
    }
    return Schema(std::move(column_defs));
}
