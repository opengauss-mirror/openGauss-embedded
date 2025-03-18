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
 * join_type.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/common/join_type.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <fmt/format.h>

#include <cstdint>

#include "nodes/nodes.hpp"
/**
 * Join types.
 */
enum class JoinType : uint8_t {
    Invalid = 0,
    LeftJoin = 1,
    RightJoin = 2,
    CrossJoin = 3,
    OuterJoin = 4,
    SemiJoin = 5,
    AntiJoin = 6,
    MarkJoin = 7,
    SingleJoin = 8,
    FullJoin = 9,
};

template <>
struct fmt::formatter<duckdb_libpgquery::PGJoinType> : formatter<std::string_view> {
    template <typename FormatContext>
    auto format(duckdb_libpgquery::PGJoinType c, FormatContext &ctx) const {
        std::string_view name;
        switch (c) {
            case duckdb_libpgquery::PG_JOIN_INNER:
                name = "INNER JOIN";
                break;
            case duckdb_libpgquery::PG_JOIN_LEFT:
                name = "LEFT JOIN";
                break;
            case duckdb_libpgquery::PG_JOIN_FULL:
                name = "FULL JOIN";
                break;
            case duckdb_libpgquery::PG_JOIN_RIGHT:
                name = "RIGHT JOIN";
                break;
            case duckdb_libpgquery::PG_JOIN_SEMI:
                name = "SEMI JOIN";
                break;
            case duckdb_libpgquery::PG_JOIN_ANTI:
                name = "ANTI JOIN";
                break;
            case duckdb_libpgquery::PG_JOIN_UNIQUE_OUTER:
                name = "UNIQUE OUTER JOIN";
                break;
            case duckdb_libpgquery::PG_JOIN_UNIQUE_INNER:
                name = "UNIQUE INNER JOIN";
                break;

            case duckdb_libpgquery::PG_JOIN_POSITION:
                name = "POSITION JOIN";
                break;
            default:
                name = "unknown";
        }
        return formatter<std::string_view>::format(name, ctx);
    }
};

template <>
struct fmt::formatter<JoinType> : formatter<std::string_view> {
    template <typename FormatContext>
    auto format(JoinType c, FormatContext &ctx) const {
        std::string_view name;
        switch (c) {
            case JoinType::Invalid: {
                name = "INVALID";
                break;
            }
            case JoinType::LeftJoin: {
                name = "LEFT_JOIN";
                break;
            }
            case JoinType::RightJoin: {
                name = "RIGHT_JOIN";
                break;
            }
            case JoinType::CrossJoin: {
                name = "INNER_JOIN";
                break;
            }
            case JoinType::OuterJoin: {
                name = "OUTER_JOIN";
                break;
            }
            case JoinType::SemiJoin: {
                name = "SEMI_JOIN";
                break;
            }
            case JoinType::AntiJoin: {
                name = "ANTI_JOIN";
                break;
            }
            case JoinType::MarkJoin: {
                name = "MARK_JOIN";
                break;
            }
            case JoinType::SingleJoin: {
                name = "SINGLE_JOIN";
                break;
            }
            case JoinType::FullJoin: {
                name = "FULL_JOIN";
                break;
            }
            default:
                name = "unknown";
        }
        return formatter<std::string_view>::format(name, ctx);
    }
};
