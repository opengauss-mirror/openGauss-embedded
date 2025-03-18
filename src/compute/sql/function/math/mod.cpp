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
* mod.cpp
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/function/math/mod.cpp
*
* -------------------------------------------------------------------------
*/
#include "function/math/mod.h"

#include <stdint.h>

#include "function/math/divided.h"
#include "type/operator/decimal_cast.h"
#include "type/type_id.h"
#include "type/value.h"

namespace intarkdb {

template <typename T>
bool IsZero(T& value) {
    return value == 0;
}

template <>
bool IsZero(double& value) {
    return std::abs(value) < std::numeric_limits<double>::epsilon();
}

template <>
bool IsZero(dec4_t& value) {
    return DECIMAL_IS_ZERO(&value);
}

template <typename T>
bool Mod(T left, T right, T& result) {
    if (IsZero(right)) {
        return false;
    }
    result = left % right;
    return true;
}

template <>
bool Mod<double>(double left, double right, double& result) {
    if (IsZero(right)) {
        return false;
    }
    result = std::fmod(left, right);
    return true;
}

auto ModSet::Register(FunctionContext& context) -> void {
    context.RegisterFunction("%", {{GS_TYPE_TINYINT, GS_TYPE_TINYINT}, GS_TYPE_TINYINT},
                             [](const std::vector<Value>& args) {
                                 int8_t result;
                                 auto r = Mod(args[0].GetCastAs<int8_t>(), args[1].GetCastAs<int8_t>(), result);
                                 return r ? ValueFactory::ValueTinyInt(result) : Value(GS_TYPE_TINYINT);
                             });
    context.RegisterFunction("%", {{GS_TYPE_SMALLINT, GS_TYPE_SMALLINT}, GS_TYPE_SMALLINT},
                             [](const std::vector<Value>& args) {
                                 int16_t result;
                                 auto r = Mod(args[0].GetCastAs<int16_t>(), args[1].GetCastAs<int16_t>(), result);
                                 return r ? ValueFactory::ValueSmallInt(result) : Value(GS_TYPE_SMALLINT);
                             });
    context.RegisterFunction("%", {{GS_TYPE_INTEGER, GS_TYPE_INTEGER}, GS_TYPE_INTEGER},
                             [](const std::vector<Value>& args) {
                                 int32_t result;
                                 auto r = Mod(args[0].GetCastAs<int32_t>(), args[1].GetCastAs<int32_t>(), result);
                                 return r ? ValueFactory::ValueInt(result) : Value(GS_TYPE_INTEGER);
                             });
    context.RegisterFunction("%", {{GS_TYPE_BIGINT, GS_TYPE_BIGINT}, GS_TYPE_BIGINT},
                             [](const std::vector<Value>& args) {
                                 int64_t result;
                                 auto r = Mod(args[0].GetCastAs<int64_t>(), args[1].GetCastAs<int64_t>(), result);
                                 return r ? ValueFactory::ValueBigInt(result) : Value(GS_TYPE_BIGINT);
                             });
    context.RegisterFunction("%", {{GS_TYPE_UTINYINT, GS_TYPE_UTINYINT}, GS_TYPE_UTINYINT},
                             [](const std::vector<Value>& args) {
                                 uint8_t result;
                                 auto r = Mod(args[0].GetCastAs<uint8_t>(), args[1].GetCastAs<uint8_t>(), result);
                                 return r ? ValueFactory::ValueUnsignTinyInt(result) : Value(GS_TYPE_UTINYINT);
                             });
    context.RegisterFunction("%", {{GS_TYPE_USMALLINT, GS_TYPE_USMALLINT}, GS_TYPE_USMALLINT},
                             [](const std::vector<Value>& args) {
                                 uint16_t result;
                                 auto r = Mod(args[0].GetCastAs<uint16_t>(), args[1].GetCastAs<uint16_t>(), result);
                                 return r ? ValueFactory::ValueUnsignSmallInt(result) : Value(GS_TYPE_USMALLINT);
                             });
    context.RegisterFunction("%", {{GS_TYPE_UINT32, GS_TYPE_UINT32}, GS_TYPE_UINT32},
                             [](const std::vector<Value>& args) {
                                 uint32_t result;
                                 auto r = Mod(args[0].GetCastAs<uint32_t>(), args[1].GetCastAs<uint32_t>(), result);
                                 return r ? ValueFactory::ValueUnsignInt(result) : Value(GS_TYPE_UINT32);
                             });
    context.RegisterFunction("%", {{GS_TYPE_UINT64, GS_TYPE_UINT64}, GS_TYPE_UINT64},
                             [](const std::vector<Value>& args) {
                                 uint64_t result;
                                 auto r = Mod(args[0].GetCastAs<uint64_t>(), args[1].GetCastAs<uint64_t>(), result);
                                 return r ? ValueFactory::ValueUnsignBigInt(result) : Value(GS_TYPE_UINT64);
                             });
    context.RegisterFunction("%", {{GS_TYPE_FLOAT, GS_TYPE_FLOAT}, GS_TYPE_REAL}, [](const std::vector<Value>& args) {
        double result;
        auto r = Mod(args[0].GetCastAs<double>(), args[1].GetCastAs<double>(), result);
        return r ? ValueFactory::ValueDouble(result) : Value(GS_TYPE_REAL);
    });

    context.RegisterFunction("%", {{GS_TYPE_REAL, GS_TYPE_REAL}, GS_TYPE_REAL}, [](const std::vector<Value>& args) {
        double result;
        auto r = Mod(args[0].GetCastAs<double>(), args[1].GetCastAs<double>(), result);
        return r ? ValueFactory::ValueDouble(result) : Value(GS_TYPE_REAL);
    });
}

}  // namespace intarkdb
