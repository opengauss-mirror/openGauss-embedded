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
 * aexpr_type.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/common/aexpr_type.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <fmt/format.h>

#include <string_view>

#include "nodes/parsenodes.hpp"

using AExprType = duckdb_libpgquery::PGAExpr_Kind;

template <>
struct fmt::formatter<AExprType> : formatter<std::string_view> {
    template <typename FormatContext>
    auto format(AExprType c, FormatContext &ctx) const {
        std::string_view name;
        switch (c) {
            case duckdb_libpgquery::PG_AEXPR_OP:
                name = "AEXPR_OP";
                break;
            case duckdb_libpgquery::PG_AEXPR_OP_ANY:
                name = "AEXPR_OP_ANY";
                break;
            case duckdb_libpgquery::PG_AEXPR_OP_ALL:
                name = "AEXPR_OP_ALL";
                break;
            case duckdb_libpgquery::PG_AEXPR_DISTINCT:
                name = "AEXPR_DISTINCT";
                break;
            case duckdb_libpgquery::PG_AEXPR_NOT_DISTINCT:
                name = "AEXPR_NOT_DISTINCT";
                break;
            case duckdb_libpgquery::PG_AEXPR_NULLIF:
                name = "AEXPR_NULLIF";
                break;
            case duckdb_libpgquery::PG_AEXPR_OF:
                name = "AEXPR_OF";
                break;
            case duckdb_libpgquery::PG_AEXPR_IN:
                name = "AEXPR_IN";
                break;
            case duckdb_libpgquery::PG_AEXPR_LIKE:
                name = "AEXPR_LIKE";
                break;
            case duckdb_libpgquery::PG_AEXPR_ILIKE:
                name = "AEXPR_ILIKE";
                break;
            case duckdb_libpgquery::PG_AEXPR_GLOB:
                name = "AEXPR_GLOB";
                break;
            case duckdb_libpgquery::PG_AEXPR_SIMILAR:
                name = "AEXPR_SIMILAR";
                break;
            case duckdb_libpgquery::PG_AEXPR_BETWEEN:
                name = "AEXPR_BETWEEN";
                break;
            case duckdb_libpgquery::PG_AEXPR_NOT_BETWEEN:
                name = "AEXPR_NOT_BETWEEN";
                break;
            case duckdb_libpgquery::PG_AEXPR_BETWEEN_SYM:
                name = "AEXPR_BETWEEN_SYM";
                break;
            case duckdb_libpgquery::PG_AEXPR_NOT_BETWEEN_SYM:
                name = "AEXPR_NOT_BETWEEN_SYM";
                break;
            case duckdb_libpgquery::AEXPR_PAREN:
                name = "AEXPR_PAREN";
                break;
            default:
                name = "UNKNOWN";
                break;
        }
        return formatter<std::string_view>::format(name, ctx);
    }
};
