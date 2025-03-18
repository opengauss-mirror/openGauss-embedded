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
 * value.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/type/value.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "type/value.h"

#include <fmt/core.h>
#include <fmt/format.h>

#include <cstdint>

#include "cm_date.h"
#include "cm_dec4.h"
#include "cm_defs.h"
#include "common/exception.h"
#include "common/string_util.h"
#include "type/operator/cast_operators.h"
#include "type/type_system.h"

Value::Value(const exp_column_def_t& col_def) {
    data_type_ = col_def.col_type;
    value_ = intarkdb::GetStoreValue(col_def.col_type, col_def.crud_value);
    if (value_.index() == 0) {
        is_null_ = true;
    }
    if (!is_null_ && IsDecimal()) {
        SetScaleAndPrecision(col_def.scale, col_def.precision);
    }
}

Value::Value(GStorDataType data_type, const col_text_t& v, int scale, int precision) {
    data_type_ = data_type;
    value_ = intarkdb::GetStoreValue(data_type, v);
    if (value_.index() == 0) {
        is_null_ = true;
    }
    if (!is_null_ && IsDecimal()) {
        SetScaleAndPrecision(scale, precision);
    }
}

Value::Value(LogicalType data_type, const col_text_t& v) {
    data_type_ = data_type.TypeId();
    value_ = intarkdb::GetStoreValue(data_type.TypeId(), v);
    if (value_.index() == 0) {
        is_null_ = true;
    }
    if (!is_null_ && IsDecimal()) {
        SetScaleAndPrecision(data_type.Scale(), data_type.Precision());
    }
}

std::string Value::ToString() const { return IsNull() ? "null" : GetCastAs<std::string>(); }

std::string Value::ToSQLString() const {
    if (IsNull()) {
        return "null";
    }
    if (IsInteger() || IsFloat() || data_type_ == GS_TYPE_BOOLEAN) {
        return ToString();
    }
    return "'" + ToString() + "'";
}

bool Value::IsNull() const {
    if (is_null_) {
        return is_null_;
    }
    // bool unkown 的特殊处理
    if (data_type_ == GS_TYPE_BOOLEAN && Trivalent::UNKNOWN == static_cast<Trivalent>(std::get<uint32_t>(value_))) {
        return true;
    }
    return false;
}

Trivalent Value::Equal(const Value& v) const {
    if (IsNull() || v.IsNull()) {
        return Trivalent::UNKNOWN;
    }
    return DataType::GetTypeInstance(GetType())->Equal(*this, v);
}

Trivalent Value::LessThan(const Value& v) const {
    if (IsNull() || v.IsNull()) {
        return Trivalent::UNKNOWN;
    }
    return DataType::GetTypeInstance(GetType())->LessThan(*this, v);
}

Trivalent Value::LessThanOrEqual(const Value& v) const {
    if (IsNull() || v.IsNull()) {
        return Trivalent::UNKNOWN;
    }
    return DataType::GetTypeInstance(GetType())->LessThanOrEqual(*this, v);
}

const char* Value::GetRawBuff() const {
    if (IsNull()) {
        return nullptr;
    }
    if (intarkdb::IsString(data_type_)) {
        return std::get<std::string>(value_).data();
    }

    return reinterpret_cast<const char*>(&value_);
}

int32_t Value::Size() const {
    if (IsNull()) {
        return 0;
    }
    if (intarkdb::IsString(data_type_)) {
        return std::get<std::string>(value_).length();
    }
    if (intarkdb::IsDecimal(data_type_)) {
        // FIXME: 尽量不要有特殊逻辑
        const dec4_t& dec = std::get<dec4_t>(value_);
        return (1 + dec.ncells) * sizeof(c4typ_t);
    }
    return intarkdb::GetTypeSize(data_type_);
}

bool Value::IsInteger() const { return intarkdb::IsInteger(data_type_); }

bool Value::IsUnSigned() const {
    if (!IsInteger()) {
        return false;
    }
    return intarkdb::IsUnSigned(data_type_);
}

bool Value::IsDecimal() const { return intarkdb::IsDecimal(data_type_); }
bool Value::IsFloat() const { return intarkdb::IsFloat(data_type_); }
bool Value::IsString() const { return intarkdb::IsString(data_type_); }
bool Value::IsNumeric() const { return intarkdb::IsNumeric(data_type_); }

LogicalType Value::GetLogicalType() const {
    LogicalType logical_type = GS_TYPE_NULL;
    if (data_type_ == GS_TYPE_DECIMAL) {
        logical_type = LogicalType::Decimal(precision, scale);
    } else if (data_type_ == GS_TYPE_VARCHAR) {
        auto size = Size();
        logical_type = LogicalType{GS_TYPE_VARCHAR, size, 0, 0, size};
    } else {
        logical_type = LogicalType{data_type_, 0, 0, 0, static_cast<int32_t>(intarkdb::GetTypeSize(data_type_))};
    }
    return logical_type;
}

bool Value::TryCast(Value& val, GStorDataType ToType) {
    try {
        // TODO: 需要优化 DataType，不要throw exception,又忽略
        val = DataType::GetTypeInstance(ToType)->CastValue(val);
    } catch (...) {
        return false;
    }
    return true;
}

template <>
std::string Value::GetCastAs<std::string>() const {
    if (is_null_) {
        throw intarkdb::Exception(ExceptionType::EXECUTOR, "null can't use CastAs");
    }
    switch (GetType()) {
        case GS_TYPE_BOOLEAN: {
            return Cast::Operation<Trivalent, std::string>(static_cast<Trivalent>(std::get<uint32_t>(value_)));
        }
        case GS_TYPE_UTINYINT:
        case GS_TYPE_USMALLINT:
        case GS_TYPE_UINT32:
            return Cast::Operation<uint32_t, std::string>(std::get<uint32_t>(value_));
        case GS_TYPE_TINYINT:
        case GS_TYPE_SMALLINT:
        case GS_TYPE_INTEGER:
            return Cast::Operation<int32_t, std::string>(std::get<int32_t>(value_));
        case GS_TYPE_BIGINT:
            return Cast::Operation<int64_t, std::string>(std::get<int64_t>(value_));
        case GS_TYPE_UINT64:
            return Cast::Operation<uint64_t, std::string>(std::get<uint64_t>(value_));
        case GS_TYPE_HUGEINT:
            return Cast::Operation<hugeint_t, std::string>(std::get<hugeint_t>(value_));
        case GS_TYPE_CHAR:
        case GS_TYPE_CLOB:
        case GS_TYPE_STRING:
        case GS_TYPE_VARCHAR:
            return std::get<std::string>(value_);
        case GS_TYPE_RAW:
        case GS_TYPE_BLOB:
        case GS_TYPE_BINARY: {
            return std::get<std::string>(value_);
        }
        case GS_TYPE_FLOAT:
        case GS_TYPE_REAL:
            return Cast::Operation<double, std::string>(std::get<double>(value_));
        case GS_TYPE_NUMBER:
        case GS_TYPE_DECIMAL: {
            return DecimalCast::Operation<std::string>(std::get<dec4_t>(value_), scale, precision);
        }
        case GS_TYPE_DATE: {
            return Cast::Operation<date_stor_t, std::string>(std::get<date_stor_t>(value_));
        }
        case GS_TYPE_TIMESTAMP: {
            return Cast::Operation<timestamp_stor_t, std::string>(std::get<timestamp_stor_t>(value_));
        }
        default:
            break;
    }
    throw std::invalid_argument("unspported CastAs Type");
}

// 检查值是否符合列定义
bool CheckValueWithDef(Value& val, const exp_column_def_t& def) {
    if (val.IsNull()) {
        if (!def.nullable) {
            throw std::runtime_error(fmt::format("column ( {} ) not nullable!", def.name.str));
        }
        return true;
    }

    // switch (val.GetType()) {
    switch (def.col_type) {
        case GS_TYPE_SMALLINT: {
            int32 data = *((int32*)val.GetRawBuff());
            if (data > SHRT_MAX || data < SHRT_MIN) {
                throw intarkdb::Exception(
                    ExceptionType::OUT_OF_RANGE,
                    fmt::format("column ( {} ), value ( {} ) out of range!", def.name.str, val.ToString()));
            }
            break;
        }
        case GS_TYPE_TINYINT: {
            int32 data = *((int32*)val.GetRawBuff());
            if (data > SCHAR_MAX || data < SCHAR_MIN) {
                throw intarkdb::Exception(
                    ExceptionType::OUT_OF_RANGE,
                    fmt::format("column ( {} ), value ( {} ) out of range!", def.name.str, val.ToString()));
            }
            break;
        }
        case GStorDataType::GS_TYPE_CHAR: {
            if (val.Size() > 1) {
                throw intarkdb::Exception(
                    ExceptionType::OUT_OF_RANGE,
                    fmt::format("column ( {} ), value ( {} ) out of range!", def.name.str, val.ToString()));
            }
            break;
        }
        case GStorDataType::GS_TYPE_VARCHAR:
        case GStorDataType::GS_TYPE_STRING: {
            const std::string& str = val.Get<std::string>();
            intarkdb::UTF8StringView str_view(str);
            if (!str_view.IsUTF8()) {
                throw intarkdb::Exception(ExceptionType::EXECUTOR, "not supported non-utf-8 string");
            }
            auto len = str_view.Length();
            if (len > def.size) {
                throw intarkdb::Exception(
                    ExceptionType::OUT_OF_RANGE,
                    fmt::format("len ( {} ) > size ( {} ), column ( {} ), value ( {} ) out of range!", len, def.size,
                                def.name.str, val.ToString()));
            }
            break;
        }
        case GStorDataType::GS_TYPE_NUMBER:
        case GStorDataType::GS_TYPE_DECIMAL: {
            if (!val.SetScaleAndPrecision(def.scale, def.precision)) {
                throw intarkdb::Exception(
                    ExceptionType::OUT_OF_RANGE,
                    fmt::format("column ( {} ), value ( {} ) out of range!", def.name.str, val.ToString()));
            }
            break;
        }

        default:
            break;
    }
    return true;
}

Value ValueFactory::ValueDate(timestamp_stor_t v) { return ValueDate(DateUtils::ToDate(v)); }
Value ValueFactory::ValueDate(date_stor_t v) { return Value(GS_TYPE_DATE, v); }

Value ValueFactory::ValueDate(const char* v) {
    auto date = Cast::Operation<std::string, date_stor_t>(std::string(v));
    return ValueFactory::ValueDate(date);
}

Value ValueFactory::ValueTimeStamp(timestamp_stor_t v) { return Value(GS_TYPE_TIMESTAMP, v); }

Value ValueFactory::ValueTimeStamp(const char* v) {
    auto tm = Cast::Operation<std::string, timestamp_stor_t>(std::string(v));
    return ValueFactory::ValueTimeStamp(tm);
}

auto ValueCast::CastValue(Value value, LogicalType target_type) -> Value {
    if (!value.IsNull()) {
        // FIXME: trans decimal to decimal , 使用特殊逻辑，请优化处理逻辑
        if (value.IsDecimal() && intarkdb::IsDecimal(target_type.TypeId())) {
            dec4_t dec;
            std::string msg;
            if (!DecimalTryCast::Operation(value.Get<dec4_t>(), dec, target_type.Scale(), target_type.Precision(),
                                           msg)) {
                throw intarkdb::Exception(ExceptionType::DECIMAL, "cast fail : " + msg);
            }
            value = ValueFactory::ValueDecimal(dec, target_type.Precision(), target_type.Scale());
        } else {
            value = DataType::GetTypeInstance(target_type.TypeId())->CastValue(value);
            if (value.IsDecimal() && !value.SetScaleAndPrecision(target_type.scale, target_type.precision)) {
                throw intarkdb::Exception(ExceptionType::DECIMAL,
                                          fmt::format("column ( {} ) out of range!", value.ToString()));
            }
        }
    } else {
        value = ValueFactory::ValueNull(target_type);
    }
    return value;
}
#ifdef _MSC_VER
bool Value::SetScaleAndPrecision(int32_t in_scale, int32_t in_precision) {
    if (IsDecimal()) {
        scale = in_scale;
        precision = in_precision;
        auto p = std::get_if<dec4_t>(&value_);  // 兼容null的情况
        if (p) {
            dec8_t dec8;
            if (cm_dec_4_to_8(&dec8, p, cm_dec4_stor_sz(p)) != GS_SUCCESS) {
                return false;
            }
            if (cm_adjust_dec8(&dec8, in_precision, in_scale) != GS_SUCCESS) {
                return false;
            }
            if (cm_dec_8_to_4(p, &dec8) != GS_SUCCESS) {
                return false;
            }
        }
    }
    return true;
}
#endif