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
 * column_info.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/catalog/column_info.h
 *
 * -------------------------------------------------------------------------
 */

#pragma once

#include <string>
#include <vector>

#include "catalog/column.h"
#include "type/type_id.h"

struct SchemaColumnInfo {
    std::vector<std::string> col_name;  // [table_name, column_name]
    std::string alias;
    LogicalType col_type;
    uint32_t slot;  // 实体表中的列ID

    SchemaColumnInfo() = default;
    SchemaColumnInfo(const std::vector<std::string>& col_name, const std::string& alias, const LogicalType& col_type,
                     uint32_t slot)
        : col_name(col_name), alias(alias), col_type(col_type), slot(slot) {}

    const std::string& GetColNameWithoutTableName() const {
        if (!alias.empty()) {
            return alias;
        }
        return col_name.back();
    }
};

auto SchemaColumnInfoToColumn(const SchemaColumnInfo& col) -> Column;
auto ColumnToSchemaColumnInfo(const std::string& table_name, const Column& col) -> SchemaColumnInfo;
