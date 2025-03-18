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
 * type_system.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/type/type_system.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <memory>
#include <unordered_map>

#include "common/exception.h"
#include "common/gstor_exception.h"
#include "common/util.h"
#include "storage/gstor/zekernel/common/cm_dec4.h"
#include "type/hugeint.h"
#include "type/operator/decimal_cast.h"
#include "type/timestamp_t.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/type_str.h"

enum class CMP_RESULT : int8_t {
    LESS = -1,
    EQUAL = 0,
    GREATER = 1,
};

constexpr int64_t DAY_TO_MIRCO_SEC = 86400000000;

template <typename T>
Value AddValue(const Value& base, const Value& val) {
    return Value(base.GetType(), base.GetCastAs<T>() + val.GetCastAs<T>());
}

template <typename T>
Value DecValue(const Value& base, const Value& val) {
    return Value(base.GetType(), base.GetCastAs<T>() - val.GetCastAs<T>());
}

template <typename T>
Value MulValue(const Value& base, const Value& val) {
    return Value(base.GetType(), base.GetCastAs<T>() * val.GetCastAs<T>());
}

template <typename T>
Value DivValue(const Value& base, const Value& val) {
    auto v = val.GetCastAs<T>();
    if (v == 0 || v == 0.0) {
        return Value(base.GetType());  // return null
    }
    return Value(base.GetType(), base.GetCastAs<T>() / v);
}

template <>
Value DivValue<double>(const Value& base, const Value& val);

template <typename T>
Value ModValue(const Value& base, const Value& val) {
    auto mod = val.GetCastAs<T>();
    if (mod == 0 || mod == 0.0) {
        return Value(base.GetType());
    }
    return Value(base.GetType(), base.GetCastAs<T>() % mod);
}

template <typename T>
Value NegatvieValue(const Value& val) {
    return Value(val.GetType(), -val.GetCastAs<T>());
}

template <>
Value ModValue<st_dec4>(const Value& base, const Value& val);

template <>
Value ModValue<double>(const Value& base, const Value& val);

template <>
Value NegatvieValue<st_dec4>(const Value& val);

class DataType {
   public:
    DataType(GStorDataType t) : type(t) {}

    virtual auto Type() const -> GStorDataType { return type; }
    virtual auto Name() const -> const char* = 0;

    EXPORT_API static const DataType* GetTypeInstance(GStorDataType);
    EXPORT_API static bool IsSupportType(GStorDataType);

    static std::unordered_map<GStorDataType, const DataType*> typeMap;

    virtual auto ToString(const Value& value) const -> std::string = 0;

    virtual auto Equal(const Value& left, const Value& right) const -> Trivalent;
    virtual auto LessThan(const Value& left, const Value& right) const -> Trivalent;
    virtual auto LessThanOrEqual(const Value& left, const Value& right) const -> Trivalent;

    // TODO: 可以删除
    virtual CMP_RESULT Compare(const Value& left, const Value& right) const = 0;

    virtual Value CastValue(const Value& v) const = 0;

   private:
    GStorDataType type{GStorDataType::GS_TYPE_BASE};  // 数据类型
};

class NumberType : public DataType {
   public:
    NumberType(GStorDataType t) : DataType(t) {}
    // 数字操作
    virtual Value Add(const Value& base, const Value& val) const = 0;
    virtual Value Dec(const Value& base, const Value& val) const = 0;
    virtual Value Mul(const Value& base, const Value& val) const = 0;
    virtual Value Div(const Value& base, const Value& val) const = 0;
    virtual Value Mod(const Value& base, const Value& val) const = 0;
    virtual Value Negatvie(const Value& val) const = 0;
};

class IntegerBaseType : public NumberType {
   public:
    IntegerBaseType(GStorDataType t) : NumberType(t) {}
    virtual auto IsSigned() const -> bool = 0;
};

class IntegerType : public IntegerBaseType {
   public:
    using StorType = int32_t;
    // IntegerType 需要支持几个类型，所有需要单独设置type
    IntegerType(GStorDataType t) : IntegerBaseType(t) {}
    virtual auto Name() const -> const char* override { return "Integer"; }
    virtual auto IsSigned() const -> bool override { return true; }

    virtual auto ToString(const Value& value) const -> std::string override { return value.GetCastAs<std::string>(); }

    virtual CMP_RESULT Compare(const Value& left, const Value& right) const override;
    virtual Value CastValue(const Value& v) const override;

    // 不保证base 和 val 都是 INTEGER类型,所以先castvalue
    virtual Value Add(const Value& base, const Value& val) const override {
        auto v = CastValue(base);
        return AddValue<StorType>(v, val);
    }
    virtual Value Dec(const Value& base, const Value& val) const override {
        auto v = CastValue(base);
        return DecValue<StorType>(v, val);
    }
    virtual Value Mul(const Value& base, const Value& val) const override {
        auto v = CastValue(base);
        return MulValue<StorType>(v, val);
    }
    virtual Value Div(const Value& base, const Value& val) const override {
        auto v = CastValue(base);
        return DivValue<double>(v, val);
    }
    virtual Value Mod(const Value& base, const Value& val) const override {
        auto v = CastValue(base);
        return ModValue<StorType>(v, val);
    }
    virtual Value Negatvie(const Value& val) const override { return NegatvieValue<StorType>(CastValue(val)); }
};

class UnSignedIntType : public IntegerBaseType {
   public:
    using StorType = uint32_t;
    UnSignedIntType(GStorDataType t) : IntegerBaseType(t) {}
    virtual auto Name() const -> const char* override { return "Unsigned Integer"; }
    virtual auto IsSigned() const -> bool override { return false; }

    virtual auto ToString(const Value& value) const -> std::string override { return value.GetCastAs<std::string>(); }

    virtual CMP_RESULT Compare(const Value& left, const Value& right) const override;
    virtual Value CastValue(const Value& v) const override;

    virtual Value Add(const Value& base, const Value& val) const override {
        auto v = CastValue(base);
        return AddValue<StorType>(v, val);
    }
    virtual Value Dec(const Value& base, const Value& val) const override {
        auto v = CastValue(base);
        return DecValue<StorType>(v, val);
    }
    virtual Value Mul(const Value& base, const Value& val) const override {
        auto v = CastValue(base);
        return MulValue<StorType>(v, val);
    }
    virtual Value Div(const Value& base, const Value& val) const override {
        auto v = CastValue(base);
        return DivValue<double>(v, val);
    }
    virtual Value Mod(const Value& base, const Value& val) const override {
        auto v = CastValue(base);
        return ModValue<StorType>(v, val);
    }
    virtual Value Negatvie(const Value& val) const override { return NegatvieValue<StorType>(CastValue(val)); }
};

class UnSignedBigIntType : public IntegerBaseType {
   public:
    using StorType = uint64_t;
    UnSignedBigIntType() : IntegerBaseType(GStorDataType::GS_TYPE_UINT64) {}
    virtual auto Name() const -> const char* override { return "Unsigned Big Int"; }
    virtual auto IsSigned() const -> bool override { return false; }

    virtual auto ToString(const Value& value) const -> std::string override { return value.GetCastAs<std::string>(); }

    virtual CMP_RESULT Compare(const Value& left, const Value& right) const override;
    virtual Value CastValue(const Value& v) const override;
    virtual Value Add(const Value& base, const Value& val) const override {
        auto v = CastValue(base);
        return AddValue<StorType>(v, val);
    }
    virtual Value Dec(const Value& base, const Value& val) const override {
        auto v = CastValue(base);
        return DecValue<StorType>(v, val);
    }
    virtual Value Mul(const Value& base, const Value& val) const override {
        auto v = CastValue(base);
        return MulValue<StorType>(v, val);
    }
    virtual Value Div(const Value& base, const Value& val) const override {
        auto v = CastValue(base);
        return DivValue<double>(v, val);
    }
    virtual Value Mod(const Value& base, const Value& val) const override {
        auto v = CastValue(base);
        return ModValue<StorType>(v, val);
    }
    virtual Value Negatvie(const Value& val) const override { return NegatvieValue<StorType>(CastValue(val)); }
};

class BigIntType : public IntegerBaseType {
   public:
    using StorType = int64_t;
    BigIntType() : IntegerBaseType(GStorDataType::GS_TYPE_BIGINT) {}
    virtual auto Name() const -> const char* override { return "BigInt"; }
    virtual auto IsSigned() const -> bool override { return true; }
    virtual auto ToString(const Value& value) const -> std::string override { return value.GetCastAs<std::string>(); }

    virtual CMP_RESULT Compare(const Value& left, const Value& right) const override;
    virtual Value CastValue(const Value& v) const override;
    virtual Value Add(const Value& base, const Value& val) const override {
        auto v = CastValue(base);
        return AddValue<StorType>(v, val);
    }
    virtual Value Dec(const Value& base, const Value& val) const override {
        auto v = CastValue(base);
        return DecValue<StorType>(v, val);
    }
    virtual Value Mul(const Value& base, const Value& val) const override {
        auto v = CastValue(base);
        return MulValue<StorType>(v, val);
    }
    virtual Value Div(const Value& base, const Value& val) const override {
        auto v = CastValue(base);
        return DivValue<double>(v, val);
    }
    virtual Value Mod(const Value& base, const Value& val) const override {
        auto v = CastValue(base);
        return ModValue<StorType>(v, val);
    }
    virtual Value Negatvie(const Value& val) const override { return NegatvieValue<StorType>(CastValue(val)); }
};

class HugeIntType : public IntegerBaseType {
   public:
    using StorType = hugeint_t;
    HugeIntType() : IntegerBaseType(GStorDataType::GS_TYPE_HUGEINT) {}
    virtual auto Name() const -> const char* override { return "HugeInt"; }
    virtual auto IsSigned() const -> bool override { return true; }
    virtual auto ToString(const Value& value) const -> std::string override { return value.GetCastAs<std::string>(); }

    virtual CMP_RESULT Compare(const Value& left, const Value& right) const override;
    virtual Value CastValue(const Value& v) const override;
    virtual Value Add(const Value& base, const Value& val) const override {
        auto v = CastValue(base);
        return AddValue<StorType>(v, val);
    }
    virtual Value Dec(const Value& base, const Value& val) const override {
        auto v = CastValue(base);
        return DecValue<StorType>(v, val);
    }
    virtual Value Mul(const Value& base, const Value& val) const override {
        auto v = CastValue(base);
        return MulValue<StorType>(v, val);
    }
    virtual Value Div(const Value& base, const Value& val) const override {
        auto v = CastValue(base);
        return DivValue<double>(v, val);
    }
    virtual Value Mod(const Value& base, const Value& val) const override {
        auto v = CastValue(base);
        return ModValue<StorType>(v, val);
    }
    virtual Value Negatvie(const Value& val) const override { return NegatvieValue<StorType>(CastValue(val)); }
};

class RealType : public NumberType {
   public:
    using StorType = double;
    RealType() : NumberType(GStorDataType::GS_TYPE_REAL) {}
    RealType(bool float_type) : NumberType(GStorDataType::GS_TYPE_FLOAT) { is_float = true; }
    virtual auto Name() const -> const char* override {
        if (!is_float) {
            return "Float";
        }
        return "Real";
    }

    virtual auto ToString(const Value& value) const -> std::string override { return value.GetCastAs<std::string>(); }

    virtual CMP_RESULT Compare(const Value& left, const Value& right) const override;
    virtual Value CastValue(const Value& v) const override;
    virtual Value Add(const Value& base, const Value& val) const override {
        auto v = CastValue(base);
        return AddValue<StorType>(v, val);
    }
    virtual Value Dec(const Value& base, const Value& val) const override {
        auto v = CastValue(base);
        return DecValue<StorType>(v, val);
    }
    virtual Value Mul(const Value& base, const Value& val) const override {
        auto v = CastValue(base);
        return MulValue<StorType>(v, val);
    }
    virtual Value Div(const Value& base, const Value& val) const override {
        auto v = CastValue(base);
        return DivValue<StorType>(v, val);
    }
    virtual Value Mod(const Value& base, const Value& val) const override {
        auto v = CastValue(base);
        return ModValue<StorType>(v, val);
    }
    virtual Value Negatvie(const Value& val) const override { return NegatvieValue<StorType>(CastValue(val)); }

    bool is_float = false;
};

class VarcharType : public DataType {
   public:
    using StorType = std::string;
    VarcharType() : DataType(GStorDataType::GS_TYPE_VARCHAR) {}

    virtual auto Name() const -> const char* override { return "Varchar"; }

    virtual auto ToString(const Value& value) const -> std::string override { return value.Get<std::string>(); }


    virtual CMP_RESULT Compare(const Value& left, const Value& right) const override;
    virtual Value CastValue(const Value& v) const override;

    size_t UTF8Length(const Value& v) const;
    // bool IsUTF8(const Value& v);
    Value UTF8Substr(const Value& v, size_t start, size_t len) const;
};

class BooleanType : public DataType {
   public:
    using StorType = uint32_t;
    BooleanType() : DataType(GStorDataType::GS_TYPE_BOOLEAN) {}
    virtual auto Name() const -> const char* override { return "Boolean"; }

    virtual auto ToString(const Value& value) const -> std::string override { return value.GetCastAs<std::string>(); }

    auto Not(const Value& value) const -> Value {
        return ValueFactory::ValueBool(TrivalentOper::Not((Trivalent)value.GetCastAs<StorType>()));
    }

    auto And(const Value& left, const Value& right) const -> Value {
        auto left_value = (Trivalent)left.GetCastAs<StorType>();
        auto right_value = (Trivalent)right.GetCastAs<StorType>();
        if (left_value == Trivalent::TRI_TRUE && right_value == Trivalent::TRI_TRUE) {
            return ValueFactory::ValueBool(Trivalent::TRI_TRUE);
        } else if (left_value == Trivalent::TRI_FALSE || right_value == Trivalent::TRI_FALSE) {
            return ValueFactory::ValueBool(Trivalent::TRI_FALSE);
        } else {
            return ValueFactory::ValueBool(Trivalent::UNKNOWN);
        }
    }

    auto Or(const Value& left, const Value& right) const -> Value {
        auto left_value = (Trivalent)left.GetCastAs<StorType>();
        auto right_value = (Trivalent)right.GetCastAs<StorType>();
        if (left_value == Trivalent::TRI_TRUE || right_value == Trivalent::TRI_TRUE) {
            return ValueFactory::ValueBool(Trivalent::TRI_TRUE);
        } else if (left_value == Trivalent::UNKNOWN || right_value == Trivalent::UNKNOWN) {
            return ValueFactory::ValueBool(Trivalent::UNKNOWN);
        } else {
            return ValueFactory::ValueBool(Trivalent::TRI_FALSE);
        }
    }

    virtual CMP_RESULT Compare(const Value& left, const Value& right) const override;
    virtual Value CastValue(const Value& v) const override;
};

// TIMESTAMP类型
class TimestampType : public DataType {
   public:
    using StorType = timestamp_stor_t;
    TimestampType() : DataType(GStorDataType::GS_TYPE_TIMESTAMP) {}
    virtual auto Name() const -> const char* override { return "Timestamp"; }
    virtual auto ToString(const Value& value) const -> std::string override;

    virtual CMP_RESULT Compare(const Value& left, const Value& right) const override;
    virtual Value CastValue(const Value& v) const override;
};

class DateType : public DataType {
   public:
    using StorType = date_stor_t;
    DateType() : DataType(GStorDataType::GS_TYPE_DATE) {}
    virtual auto Name() const -> const char* override { return "Date"; }
    virtual auto ToString(const Value& value) const -> std::string override;

    virtual CMP_RESULT Compare(const Value& left, const Value& right) const override;
    virtual Value CastValue(const Value& v) const override;
};

class DecimalType : public NumberType {
   public:
    using StorType = dec4_t;
    DecimalType() : NumberType(GStorDataType::GS_TYPE_DECIMAL) {}
    virtual auto Name() const -> const char* override { return "DECIMAL"; }

    enum class DecimalCheckType : uint8_t { SUCCESS, SCALE_OVERFLOW, PRECISION_OVERFLOW, DECIMAL_OVERFLOW };
    static auto IsDecimalFormat(const std::string& numberStr, int M, int D) -> DecimalCheckType;
    static auto TrimDecimal(const std::string& numberStr, int precision) -> std::string;

    virtual auto ToString(const Value& value) const -> std::string override {
        return DecimalCast::Operation<std::string>(value.Get<dec4_t>(), value.scale, value.precision);
    }

    virtual CMP_RESULT Compare(const Value& left, const Value& right) const override;
    virtual Value CastValue(const Value& v) const override;

    virtual Value Add(const Value& base, const Value& val) const override;
    virtual Value Dec(const Value& base, const Value& val) const override;
    virtual Value Mul(const Value& base, const Value& val) const override;
    virtual Value Div(const Value& base, const Value& val) const override;
    virtual Value Mod(const Value& base, const Value& val) const override {
        auto v = CastValue(base);
        return ModValue<StorType>(base, val);
    }
    virtual Value Negatvie(const Value& val) const override { return NegatvieValue<StorType>(CastValue(val)); }
};

class RawType : public DataType {
   public:
    using StorType = std::string;
    RawType() : DataType(GStorDataType::GS_TYPE_RAW) {}

    virtual auto Name() const -> const char* override { return "Raw"; }

    virtual auto ToString(const Value& value) const -> std::string override { return value.GetCastAs<StorType>(); }

    virtual CMP_RESULT Compare(const Value& left, const Value& right) const override;
    virtual Value CastValue(const Value& v) const override;
};

class BlobType : public DataType {
   public:
    using StorType = std::string;
    BlobType() : DataType(GStorDataType::GS_TYPE_BLOB) {}

    virtual auto Name() const -> const char* override { return "Blob"; }

    // Returns the hexadecimal ascii code
    virtual auto ToString(const Value& value) const -> std::string override { return value.GetCastAs<StorType>(); }

    virtual CMP_RESULT Compare(const Value& left, const Value& right) const override;
    virtual Value CastValue(const Value& v) const override;
};

class BinaryType : public DataType {
   public:
    using StorType = std::string;
    BinaryType() : DataType(GStorDataType::GS_TYPE_BINARY) {}

    virtual auto Name() const -> const char* override { return "Binary"; }

    virtual auto ToString(const Value& value) const -> std::string override { return value.GetCastAs<StorType>(); }

    virtual CMP_RESULT Compare(const Value& left, const Value& right) const override;
    virtual Value CastValue(const Value& v) const override;
};

enum class ObjectType : uint8_t {
    INVALID = 0,
    TABLE_ENTRY = 1,
    SCHEMA_ENTRY = 2,
    VIEW_ENTRY = 3,
    INDEX_ENTRY = 4,
    SYNONYM_ENTRY = 5,
    SEQUENCE_ENTRY = 6,
};

typedef enum PGObjectType {
    PG_OBJECT_ACCESS_METHOD,
    PG_OBJECT_AGGREGATE,
    PG_OBJECT_AMOP,
    PG_OBJECT_AMPROC,
    PG_OBJECT_ATTRIBUTE, /* type's attribute, when distinct from column */
    PG_OBJECT_CAST,
    PG_OBJECT_COLUMN,
    PG_OBJECT_COLLATION,
    PG_OBJECT_CONVERSION,
    PG_OBJECT_DATABASE,
    PG_OBJECT_DEFAULT,
    PG_OBJECT_DEFACL,
    PG_OBJECT_DOMAIN,
    PG_OBJECT_DOMCONSTRAINT,
    PG_OBJECT_EVENT_TRIGGER,
    PG_OBJECT_EXTENSION,
    PG_OBJECT_FDW,
    PG_OBJECT_FOREIGN_SERVER,
    PG_OBJECT_FOREIGN_TABLE,
    PG_OBJECT_FUNCTION,
    PG_OBJECT_TABLE_MACRO,
    PG_OBJECT_INDEX,
    PG_OBJECT_LANGUAGE,
    PG_OBJECT_LARGEOBJECT,
    PG_OBJECT_MATVIEW,
    PG_OBJECT_OPCLASS,
    PG_OBJECT_OPERATOR,
    PG_OBJECT_OPFAMILY,
    PG_OBJECT_POLICY,
    PG_OBJECT_PUBLICATION,
    PG_OBJECT_PUBLICATION_REL,
    PG_OBJECT_ROLE,
    PG_OBJECT_RULE,
    PG_OBJECT_SCHEMA,
    PG_OBJECT_SEQUENCE,
    PG_OBJECT_SUBSCRIPTION,
    PG_OBJECT_STATISTIC_EXT,
    PG_OBJECT_TABCONSTRAINT,
    PG_OBJECT_TABLE,
    PG_OBJECT_TABLESPACE,
    PG_OBJECT_TRANSFORM,
    PG_OBJECT_TRIGGER,
    PG_OBJECT_TSCONFIGURATION,
    PG_OBJECT_TSDICTIONARY,
    PG_OBJECT_TSPARSER,
    PG_OBJECT_TSTEMPLATE,
    PG_OBJECT_TYPE,
    PG_OBJECT_USER_MAPPING,
    PG_OBJECT_VIEW
} PGObjectType;
