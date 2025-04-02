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
 * value.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/type/value.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <fmt/core.h>
#include <fmt/format.h>
#include <stdint.h>

#include <any>
#include <stdexcept>
#include <string>
#include <string_view>
#include <variant>

#include "common/exception.h"
#include "storage/gstor/zekernel/common/cm_dec4.h"
#include "storage/gstor/zekernel/common/cm_dec8.h"
#include "storage/gstor/zekernel/common/cm_decimal.h"
#include "type/date.h"
#include "type/hugeint.h"
#include "type/operator/cast_operators.h"
#include "type/operator/decimal_cast.h"
#include "type/timestamp_t.h"
#include "type/troolean.h"
#include "type/type_id.h"

class Value {
   public:
    EXPORT_API Value(GStorDataType data_type, int32_t v) : data_type_(data_type), value_(v) {}
    EXPORT_API Value(GStorDataType data_type, uint32_t v) : data_type_(data_type), value_(v) {}
    EXPORT_API Value(GStorDataType data_type, int64_t v) : data_type_(data_type), value_(v) {}
    EXPORT_API Value(GStorDataType data_type, uint64_t v) : data_type_(data_type), value_(v) {}
    EXPORT_API Value(GStorDataType data_type, const char* v) : data_type_(data_type), value_(std::string(v)) {}
    EXPORT_API Value(GStorDataType data_type, std::string_view v) : data_type_(data_type), value_(std::string(v)) {}
    EXPORT_API Value(GStorDataType data_type, std::string v) : data_type_(data_type), value_(std::move(v)) {}
    EXPORT_API Value(GStorDataType data_type, double v) : data_type_(data_type), value_(v) {}
    EXPORT_API Value(GStorDataType data_type, bool b) : data_type_(data_type), value_(uint32_t(b)) {}
    EXPORT_API Value(GStorDataType data_type, Trivalent v) : data_type_(data_type), value_(uint32_t(v)) {}
    EXPORT_API Value(GStorDataType data_type, dec4_t v) : data_type_(data_type), value_(v) {}
    EXPORT_API Value(GStorDataType data_type, hugeint_t v) : data_type_(data_type), value_(v) {}
    EXPORT_API Value(GStorDataType data_type, timestamp_stor_t v) : data_type_(data_type), value_(v) {}
    EXPORT_API Value(GStorDataType data_type, date_stor_t v) : data_type_(data_type), value_(v) {}
    EXPORT_API Value(const exp_column_def_t& col_def);
    EXPORT_API Value(GStorDataType data_type, const col_text_t&, int scale, int precision);
    EXPORT_API Value(LogicalType data_type, const col_text_t&);
    EXPORT_API explicit Value(GStorDataType data_type = GS_TYPE_NULL)
        : data_type_(data_type), is_null_(true) {}  // for Null

    EXPORT_API Value(const Value& v)
        : scale(v.scale), precision(v.precision), data_type_(v.data_type_), value_(v.value_), is_null_(v.is_null_) {}
    EXPORT_API Value(Value&& v) noexcept
        : scale(v.scale),
          precision(v.precision),
          data_type_(v.data_type_),
          value_(std::move(v.value_)),
          is_null_(v.is_null_) {}

    EXPORT_API Value& operator=(Value&& v) noexcept {
        data_type_ = v.data_type_;
        value_ = std::move(v.value_);
        is_null_ = v.is_null_;
        scale = v.scale;
        precision = v.precision;
        return *this;
    }

    EXPORT_API Value& operator=(const Value& v) {
        data_type_ = v.data_type_;
        value_ = v.value_;
        is_null_ = v.is_null_;
        scale = v.scale;
        precision = v.precision;
        return *this;
    }

    EXPORT_API GStorDataType GetType() const { return data_type_; }
    EXPORT_API LogicalType GetLogicalType() const;
    EXPORT_API std::string ToString() const;
    EXPORT_API std::string ToSQLString() const;

    EXPORT_API bool IsNull() const;

    template <typename T>
    EXPORT_API T Get() const {
        return std::get<T>(value_);
    }

    static bool TryCast(Value& val, GStorDataType ToType);

    template <typename T>
    EXPORT_API T GetCastAs() const {
        if (is_null_) {  // IsNull() 会触发BOOLEAN的特殊情况
            throw intarkdb::Exception(ExceptionType::EXECUTOR, "null can't use CastAs");
        }
        switch (GetType()) {
            case GS_TYPE_BOOLEAN: {
                auto troolean = static_cast<Trivalent>(std::get<uint32_t>(value_));
                return Cast::Operation<Trivalent, T>(troolean);
            }
            case GS_TYPE_UTINYINT:
            case GS_TYPE_USMALLINT:
            case GS_TYPE_UINT32:
                // FIXME: UTINYINT USMALLINT 考虑使用适当的宽度类型，保证可以检查出溢出问题
                return Cast::Operation<uint32_t, T>(std::get<uint32_t>(value_));
            case GS_TYPE_TINYINT:
            case GS_TYPE_SMALLINT:
            case GS_TYPE_INTEGER:
                return Cast::Operation<int32_t, T>(std::get<int32_t>(value_));
            case GS_TYPE_BIGINT:
                return Cast::Operation<int64_t, T>(std::get<int64_t>(value_));
            case GS_TYPE_UINT64:
                return Cast::Operation<uint64_t, T>(std::get<uint64_t>(value_));
            case GS_TYPE_HUGEINT:
                return Cast::Operation<hugeint_t, T>(std::get<hugeint_t>(value_));
            case GS_TYPE_CHAR:
            case GS_TYPE_CLOB:
            case GS_TYPE_STRING:
            case GS_TYPE_VARCHAR:
            case GS_TYPE_RAW:
            case GS_TYPE_BLOB:
            case GS_TYPE_BINARY:
                return Cast::Operation<std::string, T>(std::get<std::string>(value_));
            case GS_TYPE_FLOAT:
            case GS_TYPE_REAL:
                return Cast::Operation<double, T>(std::get<double>(value_));
            case GS_TYPE_NUMBER:
            case GS_TYPE_DECIMAL:
                return DecimalCast::Operation<T>(std::get<dec4_t>(value_), scale, precision);
            case GS_TYPE_DATE:
                return Cast::Operation<date_stor_t, T>(std::get<date_stor_t>(value_));
            case GS_TYPE_TIMESTAMP:
                return Cast::Operation<timestamp_stor_t, T>(std::get<timestamp_stor_t>(value_));
            default:
                break;
        }
        throw std::invalid_argument("unspported CastAs Type");
    }

    EXPORT_API Trivalent Equal(const Value& v) const;
    EXPORT_API Trivalent LessThan(const Value& v) const;
    EXPORT_API Trivalent LessThanOrEqual(const Value& v) const;

    EXPORT_API int32_t Size() const;

    EXPORT_API const char* GetRawBuff() const;

    EXPORT_API bool IsInteger() const;
    EXPORT_API bool IsDecimal() const;
    EXPORT_API bool IsUnSigned() const;
    EXPORT_API bool IsFloat() const;
    EXPORT_API bool IsString() const;
    EXPORT_API bool IsNumeric() const;
    #ifdef _MSC_VER
    EXPORT_API bool SetScaleAndPrecision(int32_t in_scale, int32_t in_precision);
    #else
    EXPORT_API bool SetScaleAndPrecision(int32_t in_scale, int32_t in_precision) {
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
    // default decimal attr
    int32_t scale{DEFALUT_DECIMAL_SCALE};
    int32_t precision{DEFALUT_DECIMAL_PRECISION};

   private:
    GStorDataType data_type_;
    intarkdb::InternalValue value_;
    bool is_null_{false};
};

template <>
std::string Value::GetCastAs<std::string>() const;

bool CheckValueWithDef(Value& val, const exp_column_def_t& def);

struct ValueFactory {
    EXPORT_API static Value ValueNull() { return Value(GS_TYPE_INTEGER); }  // 默认null的类型为int32
    EXPORT_API static Value ValueNull(LogicalType data_type) {
        Value v(data_type.TypeId());
        v.SetScaleAndPrecision(data_type.Scale(), data_type.Precision());
        return v;
    }
    EXPORT_API static Value ValueTinyInt(int8_t v) { return Value(GS_TYPE_TINYINT, static_cast<int32_t>(v)); }
    EXPORT_API static Value ValueUnsignTinyInt(uint8_t v) { return Value(GS_TYPE_UTINYINT, static_cast<uint32_t>(v)); }
    EXPORT_API static Value ValueSmallInt(int16_t v) { return Value(GS_TYPE_SMALLINT, static_cast<int32_t>(v)); }
    EXPORT_API static Value ValueUnsignSmallInt(uint16_t v) {
        return Value(GS_TYPE_USMALLINT, static_cast<uint32_t>(v));
    }
    EXPORT_API static Value ValueInt(int32_t v) { return Value(GS_TYPE_INTEGER, static_cast<int32_t>(v)); }
    EXPORT_API static Value ValueUnsignInt(uint32_t v) { return Value(GS_TYPE_UINT32, static_cast<uint32_t>(v)); }
    EXPORT_API static Value ValueBigInt(int64_t v) { return Value(GS_TYPE_BIGINT, static_cast<int64_t>(v)); }
    EXPORT_API static Value ValueUnsignBigInt(uint64_t v) { return Value(GS_TYPE_UINT64, static_cast<uint64_t>(v)); }
    EXPORT_API static Value ValueHugeInt(hugeint_t v) { return Value(GS_TYPE_HUGEINT, v); }
    EXPORT_API static Value ValueBool(bool v) { return Value(GS_TYPE_BOOLEAN, static_cast<uint32_t>(v)); }
    EXPORT_API static Value ValueBool(Trivalent v) { return Value(GS_TYPE_BOOLEAN, static_cast<uint32_t>(v)); }
    EXPORT_API static Value ValueDouble(double v) { return Value(GS_TYPE_REAL, static_cast<double>(v)); }
    EXPORT_API static Value ValueDecimal(st_dec4 v) { return Value(GS_TYPE_DECIMAL, static_cast<st_dec4>(v)); }
    EXPORT_API static Value ValueDecimal(st_dec4 v, uint16_t precision, uint16_t scale) {
        auto val = Value(GS_TYPE_DECIMAL, static_cast<st_dec4>(v));
        // FIXME: check scale and precision
        val.SetScaleAndPrecision(scale, precision);
        return val;
    }
    EXPORT_API static Value ValueVarchar(const std::string& v) { return Value(GS_TYPE_VARCHAR, v); }
    EXPORT_API static Value ValueVarchar(std::string&& v) { return Value(GS_TYPE_VARCHAR, std::move(v)); }
    EXPORT_API static Value ValueVarchar(std::string_view v) { return Value(GS_TYPE_VARCHAR, std::string(v)); }
    EXPORT_API static Value ValueVarchar(const char* v) { return Value(GS_TYPE_VARCHAR, std::string(v)); }
    EXPORT_API static Value ValueDate(timestamp_stor_t v);
    EXPORT_API static Value ValueDate(date_stor_t v);
    EXPORT_API static Value ValueDate(const char* v);
    EXPORT_API static Value ValueTimeStamp(timestamp_stor_t v);
    EXPORT_API static Value ValueTimeStamp(const char* v);
    EXPORT_API static Value ValueBlob(const std::string& v) { return Value(GS_TYPE_BLOB, v); }
    EXPORT_API static Value ValueBlob(const uint8_t* v, uint32_t len) {
        return Value(GS_TYPE_BLOB, std::string((const char*)v, len));
    }
};

struct ValueCast {
    EXPORT_API static auto CastValue(Value v, LogicalType data_type) -> Value;
};
