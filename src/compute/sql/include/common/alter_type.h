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
* alter_type.h
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/include/common/alter_type.h
*
* -------------------------------------------------------------------------
*/
#pragma once

#include <string_view>
#include <fmt/format.h>
#include "nodes/parsenodes.hpp"

template <>
struct fmt::formatter<duckdb_libpgquery::PGAlterTableType> : formatter<std::string_view> {
    template <typename FormatContext>
    auto format(duckdb_libpgquery::PGAlterTableType c, FormatContext &ctx) const {
        std::string_view name;
        switch (c) {
            case duckdb_libpgquery::PG_AT_AddColumn:
                name = "PG_AT_AddColumn";
                break;
            case duckdb_libpgquery::PG_AT_ColumnDefault:
                name = "PG_AT_ColumnDefault";
                break;
            case duckdb_libpgquery::PG_AT_DropColumn:
                name = "PG_AT_DropColumn";
                break;
            case duckdb_libpgquery::PG_AT_AlterColumnType:
                name = "PG_AT_AlterColumnType";
                break;
            case duckdb_libpgquery::PG_AT_AddConstraint:
                name = "PG_AT_AddConstraint";
                break;
            default:
                name = "PG_AT_UnSupported";
                break;
        }
        return formatter<std::string_view>::format(name, ctx);
    }
};