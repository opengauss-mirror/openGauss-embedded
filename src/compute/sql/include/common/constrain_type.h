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
* constrain_type.h
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/include/common/constrain_type.h
*
* -------------------------------------------------------------------------
*/
#pragma once

#include <string_view>
#include <fmt/format.h>
#include "nodes/parsenodes.hpp"

template <>
struct fmt::formatter<duckdb_libpgquery::PGConstrType> : formatter<std::string_view> {
    template <typename FormatContext>
    auto format(duckdb_libpgquery::PGConstrType c, FormatContext &ctx) const {
        std::string_view name;
        switch (c) {
            case duckdb_libpgquery::PG_CONSTR_NULL:
                name = "PG_CONSTR_NULL";
                break;
            case duckdb_libpgquery::PG_CONSTR_NOTNULL:
                name = "PG_CONSTR_NOTNULL";
                break;
            case duckdb_libpgquery::PG_CONSTR_DEFAULT:
                name = "PG_CONSTR_DEFAULT";
                break;
            case duckdb_libpgquery::PG_CONSTR_IDENTITY:
                name = "PG_CONSTR_IDENTITY";
                break;
            case duckdb_libpgquery::PG_CONSTR_CHECK:
                name = "PG_CONSTR_CHECK";
                break;
            case duckdb_libpgquery::PG_CONSTR_PRIMARY:
                name = "PG_CONSTR_PRIMARY";
                break;
            case duckdb_libpgquery::PG_CONSTR_UNIQUE:
                name = "PG_CONSTR_UNIQUE";
                break;
            case duckdb_libpgquery::PG_CONSTR_EXCLUSION:
                name = "PG_CONSTR_EXCLUSION";
                break;
            case duckdb_libpgquery::PG_CONSTR_FOREIGN:
                name = "PG_CONSTR_FOREIGN";
                break;
            case duckdb_libpgquery::PG_CONSTR_ATTR_DEFERRABLE:
                name = "PG_CONSTR_ATTR_DEFERRABLE";
                break;
            case duckdb_libpgquery::PG_CONSTR_ATTR_NOT_DEFERRABLE:
                name = "PG_CONSTR_ATTR_NOT_DEFERRABLE";
                break;
            case duckdb_libpgquery::PG_CONSTR_ATTR_DEFERRED:
                name = "PG_CONSTR_ATTR_DEFERRED";
                break;
            case duckdb_libpgquery::PG_CONSTR_ATTR_IMMEDIATE:
                name = "PG_CONSTR_ATTR_IMMEDIATE";
                break;
            case duckdb_libpgquery::PG_CONSTR_COMPRESSION:
                name = "PG_CONSTR_COMPRESSION";
                break;
            case duckdb_libpgquery::PG_CONSTR_GENERATED_VIRTUAL:
                name = "PG_CONSTR_GENERATED_VIRTUAL";
                break;
            case duckdb_libpgquery::PG_CONSTR_GENERATED_STORED:
                name = "PG_CONSTR_GENERATED_STORED";
                break;
            case duckdb_libpgquery::PG_CONSTR_EXTENSION_COMMENT:
                name = "PG_CONSTR_EXTENSION_COMMENT";
                break;
            case duckdb_libpgquery::PG_CONSTR_EXTENSION_AUTOINCREMENT:
                name = "PG_CONSTR_EXTENSION_AUTOINCREMENT";
                break;
        }
        return formatter<std::string_view>::format(name, ctx);
    }
};