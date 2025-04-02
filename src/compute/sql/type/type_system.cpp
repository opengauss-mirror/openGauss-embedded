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
 * type_system.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/type/type_system.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "type/type_system.h"

#include <assert.h>
#include <fmt/chrono.h>

#include <cmath>
#include <cstdint>
#include <ctime>
#include <locale>
#include <sstream>
#include <stdexcept>

#include "cm_dec4.h"
#include "cm_dec8.h"
#include "cm_decimal.h"
#include "common/string_util.h"
#include "storage/gstor/zekernel/common/cm_date.h"
#include "storage/gstor/zekernel/common/cm_nls.h"
#include "type/operator/add_operator.h"
#include "type/operator/cast_operators.h"
#include "type/type_str.h"
#include "type/valid.h"

constexpr double DOUBLE_EPSILON = 1e-15;

#define DOUBLE_IS_ZERO(var) (((var) >= -DOUBLE_EPSILON) && ((var) <= DOUBLE_EPSILON))

template <typename T>
CMP_RESULT compare_type(T x, T y) {
    if (x == y) {
        return CMP_RESULT::EQUAL;
    }
    return (x < y) ? CMP_RESULT::LESS : CMP_RESULT::GREATER;
}

template <>
CMP_RESULT compare_type<double>(double x, double y) {
    bool x_is_nan = IsNan(x);
    bool y_is_nan = IsNan(y);
    if (x_is_nan || y_is_nan) {
        if (x_is_nan && y_is_nan) {
            return CMP_RESULT::EQUAL;
        }
        return x_is_nan ? CMP_RESULT::GREATER : CMP_RESULT::LESS;
    }
    if (!IsFinite(x) || !IsFinite(y)) {
        if (!IsFinite(x) && !IsFinite(y)) {
            // x is inf or -inf
            if (std::signbit(x) == std::signbit(y)) {
                return CMP_RESULT::EQUAL;
            }
            return std::signbit(x) ? CMP_RESULT::LESS : CMP_RESULT::GREATER;
        }
        if (!IsFinite(x)) {  // x is inf  or -inf
            return std::signbit(x) ? CMP_RESULT::LESS : CMP_RESULT::GREATER;
        } else {  // y is inf or -inf
            return !std::signbit(y) ? CMP_RESULT::LESS : CMP_RESULT::GREATER;
        }
    }
    double diff = x - y;
    x = std::fabs(x);
    y = std::fabs(y);
    if (std::fabs(diff) <= DOUBLE_EPSILON * std::max(x, y)) {
        return CMP_RESULT::EQUAL;
    }
    return (diff > 0) ? CMP_RESULT::GREATER : CMP_RESULT::LESS;
}

template <>
CMP_RESULT compare_type<dec4_t>(dec4_t x, dec4_t y) {
    int32 result = cm_dec4_cmp(&x, &y);
    if (result == 0) {
        return CMP_RESULT::EQUAL;
    }
    return result > 0 ? CMP_RESULT::GREATER : CMP_RESULT::LESS;
}

template <>
Value DivValue<double>(const Value& base, const Value& val) {
    auto v = val.GetCastAs<double>();
    if (v == 0 || v == 0.0) {
        return Value(GStorDataType::GS_TYPE_REAL);  // null , type is real
    }
    return ValueFactory::ValueDouble(base.GetCastAs<double>() / v);
}

template <>
Value ModValue<st_dec4>(const Value& base, const Value& val) {
    auto mod = val.GetCastAs<double>();
    if (mod == 0 || mod == 0.0) {
        return Value(GStorDataType::GS_TYPE_DECIMAL);
    }
    auto v = ValueFactory::ValueDouble(std::fmod(base.GetCastAs<double>(), mod));
    return DataType::GetTypeInstance(GS_TYPE_DECIMAL)->CastValue(v);
}

template <>
Value ModValue<double>(const Value& base, const Value& val) {
    auto v = val.GetCastAs<double>();
    if (v == 0 || v == 0.0) {
        return Value(GStorDataType::GS_TYPE_REAL);  // null , type is real
    }
    return Value(GStorDataType::GS_TYPE_REAL, std::fmod(base.GetCastAs<double>(), v));
}

template <>
Value NegatvieValue<st_dec4>(const Value& val) {
    double v = val.GetCastAs<double>();
    return DataType::GetTypeInstance(GS_TYPE_DECIMAL)->CastValue(ValueFactory::ValueDouble(-1 * v));
}

auto DataType::Equal(const Value& left, const Value& right) const -> Trivalent {
    if (left.IsNull() || right.IsNull()) {
        return Trivalent::UNKNOWN;
    }
    return Compare(left, right) == CMP_RESULT::EQUAL ? Trivalent::TRI_TRUE : Trivalent::TRI_FALSE;
}

auto DataType::LessThan(const Value& left, const Value& right) const -> Trivalent {
    if (left.IsNull() || right.IsNull()) {
        return Trivalent::UNKNOWN;
    }
    return Compare(left, right) == CMP_RESULT::LESS ? Trivalent::TRI_TRUE : Trivalent::TRI_FALSE;
}

auto DataType::LessThanOrEqual(const Value& left, const Value& right) const -> Trivalent {
    if (left.IsNull() || right.IsNull()) {
        return Trivalent::UNKNOWN;
    }
    auto r = Compare(left, right);
    return (r == CMP_RESULT::EQUAL || r == CMP_RESULT::LESS) ? Trivalent::TRI_TRUE : Trivalent::TRI_FALSE;
}

CMP_RESULT UnSignedIntType::Compare(const Value& left, const Value& right) const {
    switch (right.GetType()) {
        case GStorDataType::GS_TYPE_TINYINT:
        case GStorDataType::GS_TYPE_SMALLINT:
        case GStorDataType::GS_TYPE_INTEGER:
            return compare_type(left.GetCastAs<BigIntType::StorType>(), right.GetCastAs<BigIntType::StorType>());
        case GStorDataType::GS_TYPE_USMALLINT:
        case GStorDataType::GS_TYPE_UTINYINT:
        case GStorDataType::GS_TYPE_UINT32:
            return compare_type(left.GetCastAs<StorType>(), right.GetCastAs<StorType>());
        case GStorDataType::GS_TYPE_BIGINT:
            return compare_type(left.GetCastAs<BigIntType::StorType>(), right.GetCastAs<BigIntType::StorType>());
        case GStorDataType::GS_TYPE_UINT64:
            return compare_type(left.GetCastAs<UnSignedBigIntType::StorType>(),
                                right.GetCastAs<UnSignedBigIntType::StorType>());
        case GStorDataType::GS_TYPE_HUGEINT:
            return compare_type(left.GetCastAs<HugeIntType::StorType>(), right.GetCastAs<HugeIntType::StorType>());
        case GStorDataType::GS_TYPE_BOOLEAN:
            return compare_type(left.GetCastAs<BooleanType::StorType>(), right.GetCastAs<BooleanType::StorType>());
        case GStorDataType::GS_TYPE_REAL:
        case GStorDataType::GS_TYPE_FLOAT:
            return compare_type(left.GetCastAs<RealType::StorType>(), right.GetCastAs<RealType::StorType>());
        case GStorDataType::GS_TYPE_NUMBER:
        case GStorDataType::GS_TYPE_DECIMAL:
            return compare_type(left.GetCastAs<DecimalType::StorType>(), right.GetCastAs<DecimalType::StorType>());
        case GStorDataType::GS_TYPE_CHAR:
        case GStorDataType::GS_TYPE_CLOB:
        case GStorDataType::GS_TYPE_VARCHAR: {
            try {
                return compare_type(left.GetCastAs<StorType>(), right.GetCastAs<StorType>());
            } catch (const std::exception& ex) {
                return CMP_RESULT::LESS;
            }
        }
        case GStorDataType::GS_TYPE_DATE:
        case GStorDataType::GS_TYPE_TIMESTAMP:
            return compare_type(left.GetCastAs<TimestampType::StorType>(), right.GetCastAs<TimestampType::StorType>());
        default:
            break;
    }
    throw std::runtime_error(fmt::format("can't not compare {} with {}", left.GetType(), right.GetType()));
}

CMP_RESULT UnSignedBigIntType::Compare(const Value& left, const Value& right) const {
    switch (right.GetType()) {
        case GStorDataType::GS_TYPE_TINYINT:
        case GStorDataType::GS_TYPE_SMALLINT:
        case GStorDataType::GS_TYPE_INTEGER:
        case GStorDataType::GS_TYPE_BIGINT:
            return compare_type(left.GetCastAs<HugeIntType::StorType>(), right.GetCastAs<HugeIntType::StorType>());
        case GStorDataType::GS_TYPE_USMALLINT:
        case GStorDataType::GS_TYPE_UTINYINT:
        case GStorDataType::GS_TYPE_UINT32:
        case GStorDataType::GS_TYPE_UINT64:
            return compare_type(left.GetCastAs<StorType>(), right.GetCastAs<StorType>());
        case GStorDataType::GS_TYPE_HUGEINT:
            return compare_type(left.GetCastAs<HugeIntType::StorType>(), right.GetCastAs<HugeIntType::StorType>());
        case GStorDataType::GS_TYPE_BOOLEAN:
            return compare_type(left.GetCastAs<StorType>(), right.GetCastAs<StorType>());
        case GStorDataType::GS_TYPE_REAL:
        case GStorDataType::GS_TYPE_FLOAT:
            return compare_type(left.GetCastAs<RealType::StorType>(), right.GetCastAs<RealType::StorType>());
        case GStorDataType::GS_TYPE_NUMBER:
        case GStorDataType::GS_TYPE_DECIMAL:
            return compare_type(left.GetCastAs<DecimalType::StorType>(), right.GetCastAs<DecimalType::StorType>());
        case GStorDataType::GS_TYPE_CHAR:
        case GStorDataType::GS_TYPE_CLOB:
        case GStorDataType::GS_TYPE_VARCHAR: {
            try {
                return compare_type(left.GetCastAs<StorType>(), right.GetCastAs<StorType>());
            } catch (const std::exception& ex) {
                return CMP_RESULT::LESS;
            }
        }
        case GStorDataType::GS_TYPE_DATE:
        case GStorDataType::GS_TYPE_TIMESTAMP:
            return compare_type(left.GetCastAs<HugeIntType::StorType>(), right.GetCastAs<HugeIntType::StorType>());
        default:
            break;
    }
    throw std::runtime_error(fmt::format("can't not compare {} with {}", left.GetType(), right.GetType()));
}

CMP_RESULT IntegerType::Compare(const Value& left, const Value& right) const {
    switch (right.GetType()) {
        case GStorDataType::GS_TYPE_TINYINT:
        case GStorDataType::GS_TYPE_SMALLINT:
        case GStorDataType::GS_TYPE_INTEGER:
            return compare_type(left.GetCastAs<StorType>(), right.GetCastAs<IntegerType::StorType>());
        case GStorDataType::GS_TYPE_USMALLINT:
        case GStorDataType::GS_TYPE_UTINYINT:
        case GStorDataType::GS_TYPE_UINT32:
            return compare_type(left.GetCastAs<UnSignedIntType::StorType>(),
                                right.GetCastAs<UnSignedIntType::StorType>());
        case GStorDataType::GS_TYPE_BIGINT:
            return compare_type(left.GetCastAs<BigIntType::StorType>(), right.GetCastAs<BigIntType::StorType>());
        case GStorDataType::GS_TYPE_UINT64:
            return compare_type(left.GetCastAs<UnSignedBigIntType::StorType>(),
                                right.GetCastAs<UnSignedBigIntType::StorType>());
        case GStorDataType::GS_TYPE_HUGEINT:
            return compare_type(left.GetCastAs<HugeIntType::StorType>(), right.GetCastAs<HugeIntType::StorType>());
        case GStorDataType::GS_TYPE_BOOLEAN:
            return compare_type(left.GetCastAs<BooleanType::StorType>(), right.GetCastAs<BooleanType::StorType>());
        case GStorDataType::GS_TYPE_REAL:
        case GStorDataType::GS_TYPE_FLOAT:
            return compare_type(left.GetCastAs<RealType::StorType>(), right.GetCastAs<RealType::StorType>());
        case GStorDataType::GS_TYPE_NUMBER:
        case GStorDataType::GS_TYPE_DECIMAL:
            return compare_type(left.GetCastAs<DecimalType::StorType>(), right.GetCastAs<DecimalType::StorType>());
        case GStorDataType::GS_TYPE_CHAR:
        case GStorDataType::GS_TYPE_CLOB:
        case GStorDataType::GS_TYPE_VARCHAR: {
            try {
                return compare_type(left.GetCastAs<StorType>(), right.GetCastAs<StorType>());
            } catch (const std::exception& ex) {
                return CMP_RESULT::LESS;
            }
        }
        case GStorDataType::GS_TYPE_DATE:
        case GStorDataType::GS_TYPE_TIMESTAMP:
            return compare_type(left.GetCastAs<TimestampType::StorType>(), right.GetCastAs<TimestampType::StorType>());
        default:
            break;
    }
    throw std::runtime_error(fmt::format("can't not compare {} with {}", left.GetType(), right.GetType()));
}

CMP_RESULT BigIntType::Compare(const Value& left, const Value& right) const {
    switch (right.GetType()) {
        case GStorDataType::GS_TYPE_TINYINT:
        case GStorDataType::GS_TYPE_SMALLINT:
        case GStorDataType::GS_TYPE_INTEGER:
        case GStorDataType::GS_TYPE_USMALLINT:
        case GStorDataType::GS_TYPE_UTINYINT:
        case GStorDataType::GS_TYPE_UINT32:
        case GStorDataType::GS_TYPE_BIGINT:
        case GStorDataType::GS_TYPE_BOOLEAN:
            return compare_type(left.GetCastAs<StorType>(), right.GetCastAs<StorType>());
        case GStorDataType::GS_TYPE_UINT64:
            return compare_type(left.GetCastAs<UnSignedBigIntType::StorType>(),
                                right.GetCastAs<UnSignedBigIntType::StorType>());
        case GStorDataType::GS_TYPE_HUGEINT:
            return compare_type(left.GetCastAs<HugeIntType::StorType>(), right.GetCastAs<HugeIntType::StorType>());
        case GStorDataType::GS_TYPE_REAL:
        case GStorDataType::GS_TYPE_FLOAT:
            return compare_type(left.GetCastAs<RealType::StorType>(), right.GetCastAs<RealType::StorType>());
        case GStorDataType::GS_TYPE_NUMBER:
        case GStorDataType::GS_TYPE_DECIMAL:
            return compare_type(left.GetCastAs<DecimalType::StorType>(), right.GetCastAs<DecimalType::StorType>());
        case GStorDataType::GS_TYPE_CHAR:
        case GStorDataType::GS_TYPE_CLOB:
        case GStorDataType::GS_TYPE_VARCHAR: {
            try {
                return compare_type(left.GetCastAs<StorType>(), right.GetCastAs<StorType>());
            } catch (const std::exception& ex) {
                return CMP_RESULT::LESS;
            }
        }
        case GStorDataType::GS_TYPE_DATE:
        case GStorDataType::GS_TYPE_TIMESTAMP:
            return compare_type(left.GetCastAs<TimestampType::StorType>(), right.GetCastAs<TimestampType::StorType>());
        default:
            break;
    }
    throw std::runtime_error(fmt::format("can't not compare {} with {}", left.GetType(), right.GetType()));
}

CMP_RESULT HugeIntType::Compare(const Value& left, const Value& right) const {
    switch (right.GetType()) {
        case GStorDataType::GS_TYPE_TINYINT:
        case GStorDataType::GS_TYPE_SMALLINT:
        case GStorDataType::GS_TYPE_INTEGER:
        case GStorDataType::GS_TYPE_USMALLINT:
        case GStorDataType::GS_TYPE_UTINYINT:
        case GStorDataType::GS_TYPE_UINT32:
        case GStorDataType::GS_TYPE_BIGINT:
        case GStorDataType::GS_TYPE_BOOLEAN:
        case GStorDataType::GS_TYPE_UINT64:
        case GStorDataType::GS_TYPE_HUGEINT:
            return compare_type(left.GetCastAs<StorType>(), right.GetCastAs<StorType>());
        case GStorDataType::GS_TYPE_REAL:
        case GStorDataType::GS_TYPE_FLOAT:
            return compare_type(left.GetCastAs<RealType::StorType>(), right.GetCastAs<RealType::StorType>());
        case GStorDataType::GS_TYPE_NUMBER:
        case GStorDataType::GS_TYPE_DECIMAL:
            return compare_type(left.GetCastAs<DecimalType::StorType>(), right.GetCastAs<DecimalType::StorType>());
        case GStorDataType::GS_TYPE_CHAR:
        case GStorDataType::GS_TYPE_CLOB:
        case GStorDataType::GS_TYPE_VARCHAR: {
            try {
                return compare_type(left.GetCastAs<StorType>(), right.GetCastAs<StorType>());
            } catch (const std::exception& ex) {
                return CMP_RESULT::LESS;
            }
        }
        case GStorDataType::GS_TYPE_DATE:
        case GStorDataType::GS_TYPE_TIMESTAMP:
            return compare_type(left.GetCastAs<StorType>(), right.GetCastAs<StorType>());
        default:
            break;
    }
    throw std::runtime_error(fmt::format("can't not compare {} with {}", left.GetType(), right.GetType()));
}

CMP_RESULT RealType::Compare(const Value& left, const Value& right) const {
    switch (right.GetType()) {
        case GStorDataType::GS_TYPE_TINYINT:
        case GStorDataType::GS_TYPE_SMALLINT:
        case GStorDataType::GS_TYPE_INTEGER:
        case GStorDataType::GS_TYPE_USMALLINT:
        case GStorDataType::GS_TYPE_UTINYINT:
        case GStorDataType::GS_TYPE_UINT32:
        case GStorDataType::GS_TYPE_BIGINT:
        case GStorDataType::GS_TYPE_BOOLEAN:
        case GStorDataType::GS_TYPE_UINT64:
        case GStorDataType::GS_TYPE_HUGEINT:
        case GStorDataType::GS_TYPE_REAL:
        case GStorDataType::GS_TYPE_FLOAT:
        case GStorDataType::GS_TYPE_NUMBER:
        case GStorDataType::GS_TYPE_DECIMAL:
            return compare_type(left.GetCastAs<StorType>(), right.GetCastAs<StorType>());
        case GStorDataType::GS_TYPE_CHAR:
        case GStorDataType::GS_TYPE_CLOB:
        case GStorDataType::GS_TYPE_VARCHAR: {
            try {
                return compare_type(left.GetCastAs<StorType>(), right.GetCastAs<StorType>());
            } catch (const std::exception& ex) {
                return CMP_RESULT::LESS;
            }
        }
        case GStorDataType::GS_TYPE_DATE:
        case GStorDataType::GS_TYPE_TIMESTAMP:
            return compare_type(left.GetCastAs<StorType>(), right.GetCastAs<StorType>());
        default:
            break;
    }
    throw std::runtime_error(fmt::format("can't not compare {} with {}", left.GetType(), right.GetType()));
}

size_t VarcharType::UTF8Length(const Value& v) const {
    const std::string& str = v.Get<std::string>();
    return intarkdb::UTF8Util::Length(str);
}

// bool VarcharType::IsUTF8(const Value& v) {
//     const std::string& str = v.Get<std::string>();
//     return is_utf8_string(str);
// }

Value VarcharType::UTF8Substr(const Value& v, size_t start, size_t len) const {
    const std::string& str = v.Get<std::string>();
    return ValueFactory::ValueVarchar(intarkdb::UTF8Util::Substr(str, start, len));
}

CMP_RESULT VarcharType::Compare(const Value& left, const Value& right) const {
    try {
        switch (right.GetType()) {
            case GStorDataType::GS_TYPE_TINYINT:
            case GStorDataType::GS_TYPE_SMALLINT:
            case GStorDataType::GS_TYPE_INTEGER:
                return compare_type(left.GetCastAs<IntegerType::StorType>(), right.GetCastAs<IntegerType::StorType>());
            case GStorDataType::GS_TYPE_USMALLINT:
            case GStorDataType::GS_TYPE_UTINYINT:
            case GStorDataType::GS_TYPE_UINT32:
                return compare_type(left.GetCastAs<UnSignedIntType::StorType>(),
                                    right.GetCastAs<UnSignedIntType::StorType>());
            case GStorDataType::GS_TYPE_BIGINT:
                return compare_type(left.GetCastAs<BigIntType::StorType>(), right.GetCastAs<BigIntType::StorType>());
            case GStorDataType::GS_TYPE_BOOLEAN: {
                bool str_to_boolean = false;
                auto r = TryCast::Operation(left.GetCastAs<VarcharType::StorType>(), str_to_boolean);
                if (r) {
                    return compare_type((uint32_t)str_to_boolean, right.GetCastAs<BooleanType::StorType>());
                }
                break;
            }
            case GStorDataType::GS_TYPE_UINT64:
                return compare_type(left.GetCastAs<UnSignedBigIntType::StorType>(),
                                    right.GetCastAs<UnSignedBigIntType::StorType>());
            case GStorDataType::GS_TYPE_HUGEINT:
                return compare_type(left.GetCastAs<HugeIntType::StorType>(), right.GetCastAs<HugeIntType::StorType>());
            case GStorDataType::GS_TYPE_REAL:
            case GStorDataType::GS_TYPE_FLOAT:
                return compare_type(left.GetCastAs<RealType::StorType>(), right.GetCastAs<RealType::StorType>());
            case GStorDataType::GS_TYPE_NUMBER:
            case GStorDataType::GS_TYPE_DECIMAL:
                return compare_type(left.GetCastAs<DecimalType::StorType>(), right.GetCastAs<DecimalType::StorType>());
            case GStorDataType::GS_TYPE_CHAR:
            case GStorDataType::GS_TYPE_CLOB:
            case GStorDataType::GS_TYPE_VARCHAR: {
                return compare_type(left.GetCastAs<StorType>(), right.GetCastAs<StorType>());
            }
            case GStorDataType::GS_TYPE_DATE:
                return compare_type(left.GetCastAs<DateType::StorType>(), right.GetCastAs<DateType::StorType>());
            case GStorDataType::GS_TYPE_TIMESTAMP: {
                return compare_type(left.GetCastAs<TimestampType::StorType>(),
                                    right.GetCastAs<TimestampType::StorType>());
                break;
            }
            default:
                break;
        }
    } catch (const std::exception& ex) {
        return CMP_RESULT::GREATER;
    }
    throw std::runtime_error(fmt::format("can't not compare {} with {}", left.GetType(), right.GetType()));
}

CMP_RESULT BooleanType::Compare(const Value& left, const Value& right) const {
    switch (right.GetType()) {
        case GStorDataType::GS_TYPE_TINYINT:
        case GStorDataType::GS_TYPE_SMALLINT:
        case GStorDataType::GS_TYPE_INTEGER:
            return compare_type(left.GetCastAs<IntegerType::StorType>(), right.GetCastAs<IntegerType::StorType>());
        case GStorDataType::GS_TYPE_USMALLINT:
        case GStorDataType::GS_TYPE_UTINYINT:
        case GStorDataType::GS_TYPE_UINT32:
            return compare_type(left.GetCastAs<UnSignedIntType::StorType>(),
                                right.GetCastAs<UnSignedIntType::StorType>());
        case GStorDataType::GS_TYPE_BIGINT:
            return compare_type(left.GetCastAs<BigIntType::StorType>(), right.GetCastAs<BigIntType::StorType>());
        case GStorDataType::GS_TYPE_BOOLEAN:
            // FIXME: 确认大小比较规则
            return compare_type(left.GetCastAs<BooleanType::StorType>(), right.GetCastAs<BooleanType::StorType>());
        case GStorDataType::GS_TYPE_UINT64:
            return compare_type(left.GetCastAs<UnSignedBigIntType::StorType>(),
                                right.GetCastAs<UnSignedBigIntType::StorType>());
        case GStorDataType::GS_TYPE_HUGEINT:
            return compare_type(left.GetCastAs<HugeIntType::StorType>(), right.GetCastAs<HugeIntType::StorType>());
        case GStorDataType::GS_TYPE_REAL:
        case GStorDataType::GS_TYPE_FLOAT:
            return compare_type(left.GetCastAs<RealType::StorType>(), right.GetCastAs<RealType::StorType>());
        case GStorDataType::GS_TYPE_NUMBER:
        case GStorDataType::GS_TYPE_DECIMAL:
            return compare_type(left.GetCastAs<DecimalType::StorType>(), right.GetCastAs<DecimalType::StorType>());
        case GStorDataType::GS_TYPE_CHAR:
        case GStorDataType::GS_TYPE_CLOB:
        case GStorDataType::GS_TYPE_VARCHAR: {
            bool str_to_boolean = false;
            auto r = TryCast::Operation(right.GetCastAs<VarcharType::StorType>(), str_to_boolean);
            if (r) {
                return compare_type(left.GetCastAs<BooleanType::StorType>(), (uint32_t)str_to_boolean);
            }
            break;
        }
        case GStorDataType::GS_TYPE_DATE:
            return compare_type(left.GetCastAs<DateType::StorType>(), right.GetCastAs<DateType::StorType>());
        case GStorDataType::GS_TYPE_TIMESTAMP:
            return compare_type(left.GetCastAs<TimestampType::StorType>().ts,
                                right.GetCastAs<TimestampType::StorType>().ts);
        default:
            break;
    }
    throw std::runtime_error(fmt::format("can't not compare {} with {}", left.GetType(), right.GetType()));
}

auto TimestampType::ToString(const Value& value) const -> std::string { return value.GetCastAs<std::string>(); }

CMP_RESULT TimestampType::Compare(const Value& left, const Value& right) const {
    switch (right.GetType()) {
        case GStorDataType::GS_TYPE_TINYINT:
        case GStorDataType::GS_TYPE_SMALLINT:
        case GStorDataType::GS_TYPE_INTEGER:
        case GStorDataType::GS_TYPE_BIGINT:
        case GStorDataType::GS_TYPE_USMALLINT:
        case GStorDataType::GS_TYPE_UTINYINT:
        case GStorDataType::GS_TYPE_UINT32:
        case GStorDataType::GS_TYPE_UINT64:
        case GStorDataType::GS_TYPE_HUGEINT:
            return compare_type(left.GetCastAs<TimestampType::StorType>(), right.GetCastAs<TimestampType::StorType>());
        case GStorDataType::GS_TYPE_REAL:
        case GStorDataType::GS_TYPE_FLOAT:
            return compare_type(left.GetCastAs<RealType::StorType>(), right.GetCastAs<RealType::StorType>());
        case GStorDataType::GS_TYPE_BOOLEAN:
        case GStorDataType::GS_TYPE_NUMBER:
        case GStorDataType::GS_TYPE_DECIMAL:
            // ignore compare
            break;
        case GStorDataType::GS_TYPE_CHAR:
        case GStorDataType::GS_TYPE_CLOB:
        case GStorDataType::GS_TYPE_VARCHAR: {
            return compare_type(left.GetCastAs<TimestampType::StorType>(), right.GetCastAs<TimestampType::StorType>());
        }
        case GStorDataType::GS_TYPE_DATE:
        case GStorDataType::GS_TYPE_TIMESTAMP:
            return compare_type(left.GetCastAs<TimestampType::StorType>(), right.GetCastAs<TimestampType::StorType>());
        default:
            break;
    }
    throw std::runtime_error(fmt::format("can't not compare {} with {}", left.GetType(), right.GetType()));
}

auto DateType::ToString(const Value& value) const -> std::string { return value.GetCastAs<std::string>(); }

CMP_RESULT DateType::Compare(const Value& left, const Value& right) const {
    switch (right.GetType()) {
        case GStorDataType::GS_TYPE_TINYINT:
        case GStorDataType::GS_TYPE_SMALLINT:
        case GStorDataType::GS_TYPE_INTEGER:
        case GStorDataType::GS_TYPE_BIGINT:
            return compare_type(left.GetCastAs<BigIntType::StorType>(), right.GetCastAs<BigIntType::StorType>());
        case GStorDataType::GS_TYPE_USMALLINT:
        case GStorDataType::GS_TYPE_UTINYINT:
        case GStorDataType::GS_TYPE_UINT32:
            return compare_type(left.GetCastAs<UnSignedIntType::StorType>(),
                                right.GetCastAs<UnSignedIntType::StorType>());
        case GStorDataType::GS_TYPE_BOOLEAN:
            return compare_type(left.GetCastAs<uint64_t>(), right.GetCastAs<uint64_t>());
        case GStorDataType::GS_TYPE_UINT64:
            return compare_type(left.GetCastAs<UnSignedBigIntType::StorType>(),
                                right.GetCastAs<UnSignedBigIntType::StorType>());
        case GStorDataType::GS_TYPE_HUGEINT:
            return compare_type(left.GetCastAs<HugeIntType::StorType>(), right.GetCastAs<HugeIntType::StorType>());
        case GStorDataType::GS_TYPE_REAL:
        case GStorDataType::GS_TYPE_FLOAT:
            return compare_type(left.GetCastAs<RealType::StorType>(), right.GetCastAs<RealType::StorType>());
        case GStorDataType::GS_TYPE_NUMBER:
        case GStorDataType::GS_TYPE_DECIMAL:
            // ignore compare
            break;
        case GStorDataType::GS_TYPE_CHAR:
        case GStorDataType::GS_TYPE_CLOB:
        case GStorDataType::GS_TYPE_VARCHAR: {
            // 字符串转换为日期( 格式为 YYYY-MM-DD HH:MM:SS) YYY-MM-DD HH24:MI:SS
            auto str = right.GetCastAs<VarcharType::StorType>();
            return compare_type(left.GetCastAs<TimestampType::StorType>().ts,
                                right.GetCastAs<TimestampType::StorType>().ts);
        }
        case GStorDataType::GS_TYPE_DATE:
            return compare_type(left.Get<StorType>(), right.Get<StorType>());
        case GStorDataType::GS_TYPE_TIMESTAMP:
            return compare_type(left.GetCastAs<TimestampType::StorType>(), right.GetCastAs<TimestampType::StorType>());
        default:
            break;
    }
    throw std::runtime_error(fmt::format("can't not compare {} with {}", left.GetType(), right.GetType()));
}

Value DecimalType::Add(const Value& base, const Value& val) const {
    dec4_t dec = base.GetCastAs<StorType>();
    auto val_dec = val.GetCastAs<StorType>();
    dec8_t dec8;
    dec8_t val_dec8;

    if (cm_dec_4_to_8(&dec8, &dec, cm_dec4_stor_sz(&dec)) != GS_SUCCESS) {
        throw std::runtime_error("cm_dec4_to_8 error");
    }
    if (cm_dec_4_to_8(&val_dec8, &val_dec, cm_dec4_stor_sz(&val_dec)) != GS_SUCCESS) {
        throw std::runtime_error("cm_dec4_to_8 error");
    }
    dec8_t result;
    if (cm_dec8_add(&dec8, &val_dec8, &result) != GS_SUCCESS) {
        throw std::runtime_error("decimal add operator error");
    }
    if (cm_dec_8_to_4(&dec, &result) != GS_SUCCESS) {
        throw std::runtime_error("cm_dec8_to_4 error");
    }
    return Value(GStorDataType::GS_TYPE_DECIMAL, dec);
}

Value DecimalType::Dec(const Value& base, const Value& val) const {
    dec4_t dec = base.GetCastAs<StorType>();
    auto val_dec = val.GetCastAs<StorType>();
    dec8_t dec8;
    dec8_t val_dec8;

    if (cm_dec_4_to_8(&dec8, &dec, cm_dec4_stor_sz(&dec)) != GS_SUCCESS) {
        throw std::runtime_error("cm_dec4_to_8 error");
    }
    if (cm_dec_4_to_8(&val_dec8, &val_dec, cm_dec4_stor_sz(&val_dec)) != GS_SUCCESS) {
        throw std::runtime_error("cm_dec4_to_8 error");
    }
    dec8_t result;
    if (cm_dec8_subtract(&dec8, &val_dec8, &result) != GS_SUCCESS) {
        throw std::runtime_error("decimal minus operator error");
    }
    if (cm_dec_8_to_4(&dec, &result) != GS_SUCCESS) {
        throw std::runtime_error("cm_dec8_to_4 error");
    }
    return Value(GStorDataType::GS_TYPE_DECIMAL, dec);
}

Value DecimalType::Mul(const Value& base, const Value& val) const {
    dec4_t dec = base.GetCastAs<StorType>();
    auto val_dec = val.GetCastAs<StorType>();
    dec8_t dec8;
    dec8_t val_dec8;

    if (cm_dec_4_to_8(&dec8, &dec, cm_dec4_stor_sz(&dec)) != GS_SUCCESS) {
        throw std::runtime_error("cm_dec4_to_8 error");
    }
    if (cm_dec_4_to_8(&val_dec8, &val_dec, cm_dec4_stor_sz(&val_dec)) != GS_SUCCESS) {
        throw std::runtime_error("cm_dec4_to_8 error");
    }
    dec8_t result;
    if (cm_dec8_multiply(&dec8, &val_dec8, &result) != GS_SUCCESS) {
        throw std::runtime_error("decimal multiply operator error");
    }
    if (cm_dec_8_to_4(&dec, &result) != GS_SUCCESS) {
        throw std::runtime_error("cm_dec8_to_4 error");
    }
    return Value(GStorDataType::GS_TYPE_DECIMAL, dec);
}

Value DecimalType::Div(const Value& base, const Value& val) const {
    dec4_t dec = base.GetCastAs<StorType>();
    auto val_dec = val.GetCastAs<StorType>();
    // check val_dec is zero or not
    if (DECIMAL_IS_ZERO(&val_dec) == GS_TRUE) {
        return Value(GStorDataType::GS_TYPE_DECIMAL);
    }
    dec8_t dec8;
    dec8_t val_dec8;

    if (cm_dec_4_to_8(&dec8, &dec, cm_dec4_stor_sz(&dec)) != GS_SUCCESS) {
        throw std::runtime_error("cm_dec4_to_8 error");
    }
    if (cm_dec_4_to_8(&val_dec8, &val_dec, cm_dec4_stor_sz(&val_dec)) != GS_SUCCESS) {
        throw std::runtime_error("cm_dec4_to_8 error");
    }
    dec8_t result;
    if (cm_dec8_divide(&dec8, &val_dec8, &result) != GS_SUCCESS) {
        throw std::runtime_error("decimal divide operator error");
    }
    if (cm_dec_8_to_4(&dec, &result) != GS_SUCCESS) {
        throw std::runtime_error("cm_dec8_to_4 error");
    }
    return ValueFactory::ValueDouble(cm_dec4_to_real(&dec));
}

CMP_RESULT DecimalType::Compare(const Value& left, const Value& right) const {
    switch (right.GetType()) {
        case GStorDataType::GS_TYPE_TINYINT:
        case GStorDataType::GS_TYPE_SMALLINT:
        case GStorDataType::GS_TYPE_INTEGER:
        case GStorDataType::GS_TYPE_USMALLINT:
        case GStorDataType::GS_TYPE_UTINYINT:
        case GStorDataType::GS_TYPE_UINT32:
        case GStorDataType::GS_TYPE_BIGINT:
        case GStorDataType::GS_TYPE_UINT64:
        case GStorDataType::GS_TYPE_HUGEINT:
        case GStorDataType::GS_TYPE_FLOAT:
        case GStorDataType::GS_TYPE_REAL:
        case GStorDataType::GS_TYPE_BOOLEAN:
            return compare_type(left.GetCastAs<RealType::StorType>(), right.GetCastAs<RealType::StorType>());
        case GStorDataType::GS_TYPE_NUMBER:
        case GStorDataType::GS_TYPE_DECIMAL:
            return compare_type(left.GetCastAs<DecimalType::StorType>(), right.GetCastAs<DecimalType::StorType>());
        case GStorDataType::GS_TYPE_CHAR:
        case GStorDataType::GS_TYPE_CLOB:
        case GStorDataType::GS_TYPE_VARCHAR: {
            return compare_type(left.GetCastAs<RealType::StorType>(), right.GetCastAs<RealType::StorType>());
        }
        // ignore
        case GStorDataType::GS_TYPE_DATE:
        case GStorDataType::GS_TYPE_TIMESTAMP:
        default:
            break;
    }
    throw std::runtime_error(fmt::format("can't not compare {} with {}", left.GetType(), right.GetType()));
}

CMP_RESULT RawType::Compare(const Value& left, const Value& right) const {
    try {
        switch (right.GetType()) {
            case GStorDataType::GS_TYPE_TINYINT:
            case GStorDataType::GS_TYPE_SMALLINT:
            case GStorDataType::GS_TYPE_INTEGER:
                return compare_type(left.GetCastAs<IntegerType::StorType>(), right.GetCastAs<IntegerType::StorType>());
            case GStorDataType::GS_TYPE_USMALLINT:
            case GStorDataType::GS_TYPE_UTINYINT:
            case GStorDataType::GS_TYPE_UINT32:
                return compare_type(left.GetCastAs<UnSignedIntType::StorType>(),
                                    right.GetCastAs<UnSignedIntType::StorType>());
            case GStorDataType::GS_TYPE_BIGINT:
                return compare_type(left.GetCastAs<BigIntType::StorType>(), right.GetCastAs<BigIntType::StorType>());
            case GStorDataType::GS_TYPE_BOOLEAN: {
                auto left_value = left.GetCastAs<VarcharType::StorType>();
                if (left_value == "TRUE" || left_value == "true") {
                    return compare_type((uint32_t)1, right.GetCastAs<BooleanType::StorType>());
                }
                if (left_value == "FALSE" || left_value == "false") {
                    return compare_type((uint32_t)0, right.GetCastAs<BooleanType::StorType>());
                }
                break;
            }
            case GStorDataType::GS_TYPE_UINT64:
                return compare_type(left.GetCastAs<UnSignedBigIntType::StorType>(),
                                    right.GetCastAs<UnSignedBigIntType::StorType>());
            case GStorDataType::GS_TYPE_HUGEINT:
                return compare_type(left.GetCastAs<HugeIntType::StorType>(), right.GetCastAs<HugeIntType::StorType>());
            case GStorDataType::GS_TYPE_REAL:
            case GStorDataType::GS_TYPE_FLOAT:
                return compare_type(left.GetCastAs<RealType::StorType>(), right.GetCastAs<RealType::StorType>());
            case GStorDataType::GS_TYPE_NUMBER:
            case GStorDataType::GS_TYPE_DECIMAL:
                return compare_type(left.GetCastAs<DecimalType::StorType>(), right.GetCastAs<DecimalType::StorType>());
            case GStorDataType::GS_TYPE_CHAR:
            case GStorDataType::GS_TYPE_CLOB:
            case GStorDataType::GS_TYPE_VARCHAR:
            case GStorDataType::GS_TYPE_RAW:
            case GStorDataType::GS_TYPE_BLOB:
            case GStorDataType::GS_TYPE_BINARY: {
                return compare_type(left.GetCastAs<StorType>(), right.GetCastAs<StorType>());
            }
            case GStorDataType::GS_TYPE_DATE:
            case GStorDataType::GS_TYPE_TIMESTAMP: {
                // 字符串转换为日期( 格式为 YYYY-MM-DD HH:MM:SS) YYY-MM-DD HH24:MI:SS
                auto str = left.GetCastAs<VarcharType::StorType>();
                time_t timestamp_type;
                auto r = cm_str2time(const_cast<char*>(str.data()), nullptr, &timestamp_type);
                if (r == GS_SUCCESS) {
                    return compare_type(static_cast<int64_t>(timestamp_type),
                                        right.GetCastAs<TimestampType::StorType>().ts);
                }
                break;
            }
            default:
                break;
        }
    } catch (const std::exception& ex) {
        return CMP_RESULT::GREATER;
    }
    throw std::runtime_error(fmt::format("can't not compare {} with {}", left.GetType(), right.GetType()));
}

CMP_RESULT BlobType::Compare(const Value& left, const Value& right) const {
    try {
        switch (right.GetType()) {
            case GStorDataType::GS_TYPE_TINYINT:
            case GStorDataType::GS_TYPE_SMALLINT:
            case GStorDataType::GS_TYPE_INTEGER:
                return compare_type(left.GetCastAs<IntegerType::StorType>(), right.GetCastAs<IntegerType::StorType>());
            case GStorDataType::GS_TYPE_USMALLINT:
            case GStorDataType::GS_TYPE_UTINYINT:
            case GStorDataType::GS_TYPE_UINT32:
                return compare_type(left.GetCastAs<UnSignedIntType::StorType>(),
                                    right.GetCastAs<UnSignedIntType::StorType>());
            case GStorDataType::GS_TYPE_BIGINT:
                return compare_type(left.GetCastAs<BigIntType::StorType>(), right.GetCastAs<BigIntType::StorType>());
            case GStorDataType::GS_TYPE_BOOLEAN: {
                auto left_value = left.GetCastAs<VarcharType::StorType>();
                if (left_value == "TRUE" || left_value == "true") {
                    return compare_type((uint32_t)1, right.GetCastAs<BooleanType::StorType>());
                }
                if (left_value == "FALSE" || left_value == "false") {
                    return compare_type((uint32_t)0, right.GetCastAs<BooleanType::StorType>());
                }
                break;
            }
            case GStorDataType::GS_TYPE_UINT64:
                return compare_type(left.GetCastAs<UnSignedBigIntType::StorType>(),
                                    right.GetCastAs<UnSignedBigIntType::StorType>());
            case GStorDataType::GS_TYPE_HUGEINT:
                return compare_type(left.GetCastAs<HugeIntType::StorType>(), right.GetCastAs<HugeIntType::StorType>());
            case GStorDataType::GS_TYPE_REAL:
            case GStorDataType::GS_TYPE_FLOAT:
                return compare_type(left.GetCastAs<RealType::StorType>(), right.GetCastAs<RealType::StorType>());
            case GStorDataType::GS_TYPE_NUMBER:
            case GStorDataType::GS_TYPE_DECIMAL:
                return compare_type(left.GetCastAs<DecimalType::StorType>(), right.GetCastAs<DecimalType::StorType>());
            case GStorDataType::GS_TYPE_CHAR:
            case GStorDataType::GS_TYPE_CLOB:
            case GStorDataType::GS_TYPE_VARCHAR:
            case GStorDataType::GS_TYPE_RAW:
            case GStorDataType::GS_TYPE_BLOB:
            case GStorDataType::GS_TYPE_BINARY: {
                return compare_type(left.GetCastAs<StorType>(), right.GetCastAs<StorType>());
            }
            case GStorDataType::GS_TYPE_DATE:
            case GStorDataType::GS_TYPE_TIMESTAMP: {
                // 字符串转换为日期( 格式为 YYYY-MM-DD HH:MM:SS) YYY-MM-DD HH24:MI:SS
                auto str = left.GetCastAs<VarcharType::StorType>();
                time_t timestamp_type;
                auto r = cm_str2time(const_cast<char*>(str.data()), nullptr, &timestamp_type);
                if (r == GS_SUCCESS) {
                    return compare_type(static_cast<int64_t>(timestamp_type),
                                        right.GetCastAs<TimestampType::StorType>().ts);
                }
                break;
            }
            default:
                break;
        }
    } catch (const std::exception& ex) {
        return CMP_RESULT::GREATER;
    }
    throw std::runtime_error(fmt::format("can't not compare {} with {}", left.GetType(), right.GetType()));
}

CMP_RESULT BinaryType::Compare(const Value& left, const Value& right) const {
    try {
        switch (right.GetType()) {
            case GStorDataType::GS_TYPE_TINYINT:
            case GStorDataType::GS_TYPE_SMALLINT:
            case GStorDataType::GS_TYPE_INTEGER:
                return compare_type(left.GetCastAs<IntegerType::StorType>(), right.GetCastAs<IntegerType::StorType>());
            case GStorDataType::GS_TYPE_USMALLINT:
            case GStorDataType::GS_TYPE_UTINYINT:
            case GStorDataType::GS_TYPE_UINT32:
                return compare_type(left.GetCastAs<UnSignedIntType::StorType>(),
                                    right.GetCastAs<UnSignedIntType::StorType>());
            case GStorDataType::GS_TYPE_BIGINT:
                return compare_type(left.GetCastAs<BigIntType::StorType>(), right.GetCastAs<BigIntType::StorType>());
            case GStorDataType::GS_TYPE_BOOLEAN: {
                auto left_value = left.GetCastAs<VarcharType::StorType>();
                if (left_value == "TRUE" || left_value == "true") {
                    return compare_type((uint32_t)1, right.GetCastAs<BooleanType::StorType>());
                }
                if (left_value == "FALSE" || left_value == "false") {
                    return compare_type((uint32_t)0, right.GetCastAs<BooleanType::StorType>());
                }
                break;
            }
            case GStorDataType::GS_TYPE_UINT64:
                return compare_type(left.GetCastAs<UnSignedBigIntType::StorType>(),
                                    right.GetCastAs<UnSignedBigIntType::StorType>());
            case GStorDataType::GS_TYPE_HUGEINT:
                return compare_type(left.GetCastAs<HugeIntType::StorType>(), right.GetCastAs<HugeIntType::StorType>());
            case GStorDataType::GS_TYPE_REAL:
            case GStorDataType::GS_TYPE_FLOAT:
                return compare_type(left.GetCastAs<RealType::StorType>(), right.GetCastAs<RealType::StorType>());
            case GStorDataType::GS_TYPE_NUMBER:
            case GStorDataType::GS_TYPE_DECIMAL:
                return compare_type(left.GetCastAs<DecimalType::StorType>(), right.GetCastAs<DecimalType::StorType>());
            case GStorDataType::GS_TYPE_CHAR:
            case GStorDataType::GS_TYPE_CLOB:
            case GStorDataType::GS_TYPE_VARCHAR:
            case GStorDataType::GS_TYPE_RAW:
            case GStorDataType::GS_TYPE_BLOB:
            case GStorDataType::GS_TYPE_BINARY: {
                return compare_type(left.GetCastAs<StorType>(), right.GetCastAs<StorType>());
            }
            case GStorDataType::GS_TYPE_DATE:
            case GStorDataType::GS_TYPE_TIMESTAMP: {
                // 字符串转换为日期( 格式为 YYYY-MM-DD HH:MM:SS) YYY-MM-DD HH24:MI:SS
                auto str = left.GetCastAs<VarcharType::StorType>();
                time_t timestamp_type;
                auto r = cm_str2time(const_cast<char*>(str.data()), nullptr, &timestamp_type);
                if (r == GS_SUCCESS) {
                    return compare_type(static_cast<int64_t>(timestamp_type),
                                        right.GetCastAs<TimestampType::StorType>().ts);
                }
                break;
            }
            default:
                break;
        }
    } catch (const std::exception& ex) {
        return CMP_RESULT::GREATER;
    }
    throw std::runtime_error(fmt::format("can't not compare {} with {}", left.GetType(), right.GetType()));
}

Value IntegerType::CastValue(const Value& v) const {
    auto src = v.GetCastAs<StorType>();
    switch (Type()) {
        case GStorDataType::GS_TYPE_TINYINT:
            return Value(GS_TYPE_TINYINT, static_cast<int32_t>(Cast::Operation<StorType, int8_t>(src)));
        case GStorDataType::GS_TYPE_SMALLINT:
            return Value(GS_TYPE_SMALLINT, static_cast<int32_t>(Cast::Operation<StorType, int16_t>(src)));
        default:
            return Value(GS_TYPE_INTEGER, static_cast<int32_t>(Cast::Operation<StorType, int32_t>(src)));
    }
}

Value BigIntType::CastValue(const Value& v) const { return Value(Type(), v.GetCastAs<StorType>()); }
Value UnSignedIntType::CastValue(const Value& v) const {
    auto src = v.GetCastAs<StorType>();
    switch (Type()) {
        case GStorDataType::GS_TYPE_UTINYINT:
            return Value(GS_TYPE_UTINYINT, static_cast<uint32_t>(Cast::Operation<StorType, uint8_t>(src)));
        case GStorDataType::GS_TYPE_USMALLINT:
            return Value(GS_TYPE_USMALLINT, static_cast<uint32_t>(Cast::Operation<StorType, uint16_t>(src)));
        default:
            return Value(GS_TYPE_UINT32, static_cast<uint32_t>(Cast::Operation<StorType, uint32_t>(src)));
    }
}

Value UnSignedBigIntType::CastValue(const Value& v) const { return Value(Type(), v.GetCastAs<StorType>()); }

Value HugeIntType::CastValue(const Value& v) const { return Value(Type(), v.GetCastAs<StorType>()); }
Value BooleanType::CastValue(const Value& v) const { return Value(Type(), v.GetCastAs<Trivalent>()); }
Value RealType::CastValue(const Value& v) const { return Value(Type(), v.GetCastAs<StorType>()); }
Value VarcharType::CastValue(const Value& v) const { return Value(Type(), v.GetCastAs<StorType>()); }
Value DecimalType::CastValue(const Value& v) const { return Value(Type(), v.GetCastAs<StorType>()); }
Value TimestampType::CastValue(const Value& v) const { return Value(Type(), v.GetCastAs<StorType>()); }
Value DateType::CastValue(const Value& v) const { return ValueFactory::ValueDate(v.GetCastAs<StorType>()); }

Value RawType::CastValue(const Value& v) const { return Value(Type(), v.GetCastAs<StorType>()); }
Value BlobType::CastValue(const Value& v) const { return Value(Type(), v.GetCastAs<StorType>()); }
Value BinaryType::CastValue(const Value& v) const { return Value(Type(), v.GetCastAs<StorType>()); }

auto DecimalType::IsDecimalFormat(const std::string& numberStr, int M, int D) -> DecimalCheckType {
    // FIXME: 有可能出现科学计数法吗
    std::string_view numberSv = numberStr;
    if (numberSv[0] == '-' || numberSv[0] == '+') {
        numberSv = numberSv.substr(1);
    }

    // 检查小数点位置
    size_t decimalIndex = numberSv.find('.');
    size_t decimalDigits = 0;
    if (decimalIndex != std::string::npos) {
        // 计算小数位数
        decimalDigits = numberSv.length() - decimalIndex - 1;
    }

    // 计算有效数字个数
    int significantDigits = 0;
    for (char ch : numberSv) {
        if (::isdigit(ch)) {
            significantDigits++;
        }
    }

    // 比较有效数字个数和小数位数与要求的M和D
    if (significantDigits > M) {
        return DecimalCheckType::SCALE_OVERFLOW;
    }
    if ((int)decimalDigits > D) {
        return DecimalCheckType::PRECISION_OVERFLOW;
    }
    return DecimalCheckType::SUCCESS;
}

auto DecimalType::TrimDecimal(const std::string& numberStr, int precision) -> std::string {
    std::string_view numberSv = numberStr;
    size_t decimalIndex = numberSv.find('.');
    if (decimalIndex != std::string::npos) {
        size_t decimalDigits = numberSv.length() - decimalIndex - 1;
        if (precision == 0) {
            numberSv = numberSv.substr(0, decimalIndex);
        } else if ((int)decimalDigits > precision) {
            numberSv = numberSv.substr(0, decimalIndex + precision + 1);
        }
    }
    return std::string(numberSv);
}

// TODO 使用数组应该可以更快,等类型比较稳定再修改
std::unordered_map<GStorDataType, const DataType*> DataType::typeMap = {
    {GStorDataType::GS_TYPE_INTEGER, new IntegerType(GStorDataType::GS_TYPE_INTEGER)},
    {GStorDataType::GS_TYPE_SMALLINT, new IntegerType(GStorDataType::GS_TYPE_SMALLINT)},
    {GStorDataType::GS_TYPE_TINYINT, new IntegerType(GStorDataType::GS_TYPE_TINYINT)},
    {GStorDataType::GS_TYPE_BIGINT, new BigIntType()},
    {GStorDataType::GS_TYPE_VARCHAR, new VarcharType()},
    {GStorDataType::GS_TYPE_STRING, new VarcharType()},
    {GStorDataType::GS_TYPE_REAL, new RealType()},
    {GStorDataType::GS_TYPE_BOOLEAN, new BooleanType()},
    {GStorDataType::GS_TYPE_TIMESTAMP, new TimestampType()},
    {GStorDataType::GS_TYPE_FLOAT, new RealType(true)},
    {GStorDataType::GS_TYPE_DECIMAL, new DecimalType()},
    {GStorDataType::GS_TYPE_DATE, new DateType()},
    {GStorDataType::GS_TYPE_NUMBER, new DecimalType()},
    {GStorDataType::GS_TYPE_CLOB, new VarcharType()},
    {GStorDataType::GS_TYPE_HUGEINT, new HugeIntType()},
    {GStorDataType::GS_TYPE_CHAR, new VarcharType()},
    {GStorDataType::GS_TYPE_UTINYINT, new UnSignedIntType(GStorDataType::GS_TYPE_UTINYINT)},
    {GStorDataType::GS_TYPE_USMALLINT, new UnSignedIntType(GStorDataType::GS_TYPE_USMALLINT)},
    {GStorDataType::GS_TYPE_UINT32, new UnSignedIntType(GStorDataType::GS_TYPE_UINT32)},
    {GStorDataType::GS_TYPE_UINT64, new UnSignedBigIntType()},
    {GStorDataType::GS_TYPE_RAW, new RawType()},
    {GStorDataType::GS_TYPE_BLOB, new BlobType()},
    {GStorDataType::GS_TYPE_BINARY, new BinaryType()}};

const DataType* DataType::GetTypeInstance(GStorDataType t) {
    auto iter = typeMap.find(t);
    if (iter == typeMap.end()) {
        // FIXME: not use fmt::format("{}",t); 会导致循环调用
        throw std::invalid_argument(fmt::format("not support typeId={}", static_cast<int>(t)));
    }
    return iter->second;
}

bool DataType::IsSupportType(GStorDataType t) { return typeMap.find(t) != typeMap.end(); }
