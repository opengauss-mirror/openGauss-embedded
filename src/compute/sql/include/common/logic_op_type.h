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
 * logic_op_type.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/common/logic_op_type.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <fmt/core.h>
#include <fmt/format.h>

#include <cstdint>

namespace intarkdb {

enum class LogicOpType : uint8_t { InValid, Not, And, Or };

bool IsBinaryLogicalOp(const std::string& op_name);

LogicOpType ToLogicOpType(const std::string& op_name);

}  // namespace intarkdb

template <>
struct fmt::formatter<intarkdb::LogicOpType> : formatter<std::string_view> {
    template <typename FormatContext>
    auto format(intarkdb::LogicOpType c, FormatContext& ctx) const {
        std::string_view name;
        switch (c) {
            case intarkdb::LogicOpType::Not: {
                name = "!";
                break;
            }
            case intarkdb::LogicOpType::And: {
                name = "and";
                break;
            }
            case intarkdb::LogicOpType::Or: {
                name = "or";
                break;
            }
            default: {
                name = "invalid";
                break;
            }
        }
        return formatter<std::string_view>::format(name, ctx);
    }
};
