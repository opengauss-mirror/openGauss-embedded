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
* grant_type.h
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/include/common/grant_type.h
*
* -------------------------------------------------------------------------
*/
#pragma once

#include <fmt/format.h>

#include <string_view>

#include "nodes/parsenodes.hpp"

template <>
struct fmt::formatter<duckdb_libpgquery::PGGrantTargetType> : formatter<std::string_view> {
    template <typename FormatContext>
    auto format(duckdb_libpgquery::PGGrantTargetType c, FormatContext &ctx) const {
        std::string_view name;
        switch (c) {
            case duckdb_libpgquery::ACL_TARGET_OBJECT:
                name = "ACL_TARGET_OBJECT";
                break;
            case duckdb_libpgquery::ACL_TARGET_ALL_IN_SCHEMA:
                name = "ACL_TARGET_ALL_IN_SCHEMA";
                break;
            case duckdb_libpgquery::ACL_TARGET_DEFAULTS:
                name = "ACL_TARGET_DEFAULTS";
                break;
            default:
                name = "ACL_TARGET_DEFAULTS";
                break;
        }
        return formatter<std::string_view>::format(name, ctx);
    }
};
