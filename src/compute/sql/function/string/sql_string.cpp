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
 * sql_string.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/function/string/sql_string.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "function/string/sql_string.h"

#include "type/type_id.h"
#include "type/value.h"

namespace intarkdb {

auto StringSet::Register(FunctionContext& context) -> void {
    context.RegisterFunction(
        "||",
        {{GS_TYPE_VARCHAR, GS_TYPE_VARCHAR},
         GS_TYPE_VARCHAR,
         [](const std::vector<LogicalType>& types) {
             if (types[0].TypeId() == GS_TYPE_VARCHAR && types[1].TypeId() == GS_TYPE_VARCHAR) {
                 int64_t total_len = types[0].Length() + types[1].Length();
                 return total_len > LogicalType::MAX_VARCHAR_LENGTH
                            ? LogicalType::Varchar(LogicalType::MAX_VARCHAR_LENGTH)
                            : LogicalType::Varchar(total_len);
             }
             return LogicalType::Varchar(LogicalType::MAX_VARCHAR_LENGTH);
         }},
        [](const std::vector<Value>& args) {
            return ValueFactory::ValueVarchar(args[0].GetCastAs<std::string>() + args[1].GetCastAs<std::string>());
        });
    // 特殊支持 boolean 与 varchar 的拼接
    context.RegisterFunction(
        "||",
        {{GS_TYPE_BOOLEAN, GS_TYPE_VARCHAR},
         GS_TYPE_VARCHAR,
         [](const std::vector<LogicalType>& types) { return LogicalType::Varchar(LogicalType::MAX_VARCHAR_LENGTH); }},
        [](const std::vector<Value>& args) {
            return ValueFactory::ValueVarchar(args[0].GetCastAs<std::string>() + args[1].GetCastAs<std::string>());
        });
    context.RegisterFunction(
        "||",
        {{GS_TYPE_VARCHAR, GS_TYPE_BOOLEAN},
         GS_TYPE_VARCHAR,
         [](const std::vector<LogicalType>& types) { return LogicalType::Varchar(LogicalType::MAX_VARCHAR_LENGTH); }},
        [](const std::vector<Value>& args) {
            return ValueFactory::ValueVarchar(args[0].GetCastAs<std::string>() + args[1].GetCastAs<std::string>());
        });
}
}  // namespace intarkdb
