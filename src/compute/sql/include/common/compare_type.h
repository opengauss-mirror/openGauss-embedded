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
 * compare_type.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/common/compare_type.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <fmt/core.h>
#include <fmt/format.h>
#include <stdint.h>

#include <string>

namespace intarkdb {
enum class ComparisonType : uint8_t {
    InValid,
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual
};

// 操作字符串转换为枚举类型
ComparisonType ToComparisonType(const std::string& name);

// 取反操作操作类型
ComparisonType RevertComparisonType(ComparisonType);

std::string RevertComparisonOpName(const std::string& name);

// 反转操作数
ComparisonType FilpComparisonType(ComparisonType type);

// 比较操作符是否合法
bool IsCompareOp(const std::string& name);

std::string CompareTypeToString(ComparisonType type);
};  // namespace intarkdb

template <>
struct fmt::formatter<intarkdb::ComparisonType> : formatter<std::string_view> {
    template <typename FormatContext>
    auto format(intarkdb::ComparisonType c, FormatContext& ctx) const {
        std::string_view name;
        switch (c) {
            case intarkdb::ComparisonType::InValid: {
                name = "invalid";
                break;
            }
            case intarkdb::ComparisonType::Equal: {
                name = "=";
                break;
            }
            case intarkdb::ComparisonType::NotEqual: {
                name = "!=";
                break;
            }
            case intarkdb::ComparisonType::LessThan: {
                name = "<";
                break;
            }
            case intarkdb::ComparisonType::LessThanOrEqual: {
                name = "<=";
                break;
            }
            case intarkdb::ComparisonType::GreaterThan: {
                name = ">";
                break;
            }
            case intarkdb::ComparisonType::GreaterThanOrEqual: {
                name = ">=";
                break;
            }
        }
        return formatter<std::string_view>::format(name, ctx);
    }
};
