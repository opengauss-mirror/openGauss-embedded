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
* column_info.cpp
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/catalog/column_info.cpp
*
* -------------------------------------------------------------------------
*/
#include "catalog/column_info.h"


auto SchemaColumnInfoToColumn(const SchemaColumnInfo& col) -> Column {
    auto def = intarkdb::NewColumnDef(col.col_type);
    def.nullable = true;
    def.col_slot = col.slot;
    if (!col.alias.empty()) {
        return Column(col.alias, def);
    } else {
        std::string name = fmt::format("{}", fmt::join(col.col_name, "."));
        return Column(name, def);
    }
}

auto ColumnToSchemaColumnInfo(const std::string& table_name, const Column& col) -> SchemaColumnInfo {
    SchemaColumnInfo info;
    info.col_name = std::vector<std::string>{table_name, col.NameWithoutPrefix()};
    info.alias = "";
    info.col_type = col.GetLogicalType();
    info.slot = col.Slot();
    return info;
}
