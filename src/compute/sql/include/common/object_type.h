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
* object_type.h
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/include/common/object_type.h
*
* -------------------------------------------------------------------------
*/
#pragma once
#include <fmt/format.h>

#include "nodes/parsenodes.hpp"

template <>
struct fmt::formatter<duckdb_libpgquery::PGObjectType> : formatter<std::string_view> {
    template <typename FormatContext>
    auto format(duckdb_libpgquery::PGObjectType c, FormatContext& ctx) const {
        std::string_view name;
        switch (c) {
            case duckdb_libpgquery::PG_OBJECT_COLUMN:
                name = "PG_OBJECT_COLUMN";
                break;
            case duckdb_libpgquery::PG_OBJECT_TABLE:
                name = "PG_OBJECT_TABLE";
                break;
            case duckdb_libpgquery::PG_OBJECT_VIEW:
                name = "PG_OBJECT_VIEW";
                break;
            case duckdb_libpgquery::PG_OBJECT_DATABASE:
                name = "PG_OBJECT_DATABASE";
                break;
            case duckdb_libpgquery::PG_OBJECT_INDEX:
                name = "PG_OBJECT_INDEX";
                break;
            case duckdb_libpgquery::PG_OBJECT_SYNONYM:
                name = "PG_OBJECT_SYNONYM";
                break;
            default:
                name = "PG_OBJECT_UNKNOWN";
                break;
        }
        return formatter<std::string_view>::format(name, ctx);
    }
};
