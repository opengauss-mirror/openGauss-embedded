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
 * type_str.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/type/type_str.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <fmt/format.h>

#include <string>
#include <string_view>
#include <unordered_map>

#include "type_id.h"

constexpr const char* UNKNOWN_TYPE_NAME = "UNKNOWN";

template <>
struct fmt::formatter<GStorDataType> : formatter<std::string_view> {
    template <typename FormatContext>
    auto format(GStorDataType c, FormatContext& ctx) const {
        std::string_view name;
        switch (c) {
            case GStorDataType::GS_TYPE_TINYINT:
                name = "TINYINT";
                break;
            case GStorDataType::GS_TYPE_UTINYINT:
                name = "UTINYINT";
                break;
            case GStorDataType::GS_TYPE_SMALLINT:
                name = "SMALLINT";
                break;
            case GStorDataType::GS_TYPE_USMALLINT:
                name = "USMALLINT";
                break;
            case GStorDataType::GS_TYPE_INTEGER:
                name = "INTEGER";
                break;
            case GStorDataType::GS_TYPE_UINT32:
                name = "UINT32";
                break;
            case GStorDataType::GS_TYPE_BIGINT:
                name = "BIGINT";
                break;
            case GStorDataType::GS_TYPE_UINT64:
                name = "UINT64";
                break;
            case GStorDataType::GS_TYPE_HUGEINT:
                name = "HUGEINT";
                break;
            case GStorDataType::GS_TYPE_STRING:
                name = "STRING";
                break;
            case GStorDataType::GS_TYPE_VARCHAR:
                name = "VARCHAR";
                break;
            case GStorDataType::GS_TYPE_BOOLEAN:
                name = "BOOLEAN";
                break;
            case GStorDataType::GS_TYPE_NUMBER:
                name = "NUMBER";
                break;
            case GStorDataType::GS_TYPE_DECIMAL:
                name = "DECIMAL";
                break;
            case GStorDataType::GS_TYPE_REAL:
                name = "DOUBLE";
                break;
            case GStorDataType::GS_TYPE_CHAR:
                name = "CHAR";
                break;
            case GStorDataType::GS_TYPE_DATE:
                name = "DATE";
                break;
            case GStorDataType::GS_TYPE_TIMESTAMP:
                name = "TIMESTAMP";
                break;
            case GStorDataType::GS_TYPE_BINARY:
                name = "BINARY";
                break;
            case GStorDataType::GS_TYPE_VARBINARY:
                name = "VARBINARY";
                break;
            case GStorDataType::GS_TYPE_CLOB:
                name = "CLOB";
                break;
            case GStorDataType::GS_TYPE_BLOB:
                name = "BLOB";
                break;
            case GStorDataType::GS_TYPE_INTERVAL_YM:
                name = "INTERVAL_YM";
                break;
            case GStorDataType::GS_TYPE_INTERVAL:
                name = "INTERVAL";
                break;
            case GStorDataType::GS_TYPE_INTERVAL_DS:
                name = "INTERVAL_DS";
                break;
            case GStorDataType::GS_TYPE_RAW:
                name = "RAW";
                break;
            case GStorDataType::GS_TYPE_FLOAT:
                name = "FLOAT";
                break;
            case GStorDataType::GS_TYPE_TIMESTAMP_TZ:
                name = "TIMESTAMP_TZ";
                break;
            default:
                name = UNKNOWN_TYPE_NAME;
        }
        return formatter<std::string_view>::format(name, ctx);
    }
};

template <>
struct fmt::formatter<LogicalType> : formatter<std::string_view> {
    template <typename FormatContext>
    auto format(LogicalType c, FormatContext& ctx) const {
        std::string name = "";
        switch (c.type) {
            case GS_TYPE_VARCHAR: {
                name = fmt::format("{}({})", "VARCHAR", c.length);
                break;
            }
            case GS_TYPE_DECIMAL:
            case GS_TYPE_NUMBER: {
                name = fmt::format("{}({},{})", "DECIMAL", c.precision, c.scale);
                break;
            }
            default: {
                name = fmt::format("{}", c.type);
            }
        }
        return formatter<std::string_view>::format(name, ctx);
    }
};
