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
 * troolean.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/type/troolean.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <fmt/format.h>

#include <cstdint>

// 三值逻辑
enum class Trivalent : uint32_t {
    TRI_FALSE = 0,
    TRI_TRUE = 1,
    UNKNOWN = 2,
};

template <>
struct fmt::formatter<Trivalent> : formatter<std::string_view> {
    template <typename FormatContext>
    auto format(Trivalent c, FormatContext& ctx) const {
        std::string_view name;
        switch (c) {
            case Trivalent::TRI_FALSE:
                name = "0";
                break;
            case Trivalent::TRI_TRUE:
                name = "1";
                break;
            case Trivalent::UNKNOWN:
                name = "";
                break;
        }
        return formatter<std::string_view>::format(name, ctx);
    }
};

struct TrivalentOper {
    static Trivalent Not(Trivalent v) {
        switch (v) {
            case Trivalent::TRI_FALSE:
                return Trivalent::TRI_TRUE;
            case Trivalent::TRI_TRUE:
                return Trivalent::TRI_FALSE;
            default:
                return Trivalent::UNKNOWN;
        }
    }
    static Trivalent And(Trivalent left, Trivalent right) {
        if (left == Trivalent::TRI_TRUE && right == Trivalent::TRI_TRUE) {
            return Trivalent::TRI_TRUE;
        } else if (left == Trivalent::TRI_FALSE || right == Trivalent::TRI_FALSE) {
            return Trivalent::TRI_FALSE;
        } else {
            return Trivalent::UNKNOWN;
        }
    }

    static Trivalent Or(Trivalent left, Trivalent right) {
        if (left == Trivalent::TRI_TRUE || right == Trivalent::TRI_TRUE) {
            return Trivalent::TRI_TRUE;
        } else if (left == Trivalent::UNKNOWN || right == Trivalent::UNKNOWN) {
            return Trivalent::UNKNOWN;
        } else {
            return Trivalent::TRI_FALSE;
        }
    }
};
