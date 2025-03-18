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
 * type_id.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/type/type_id.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "type/type_id.h"

#include <stdexcept>

#include "binder/transform_typename.h"
#include "function/cast_rules.h"
#include "type/type_str.h"
#include "type/type_system.h"

uint8_t LogicalType::Scale() const { return intarkdb::IsDecimal(type) ? scale : 0; }

uint8_t LogicalType::Precision() const { return intarkdb::IsDecimal(type) ? precision : 0; }

uint32_t LogicalType::Length() const { return intarkdb::IsString(type) ? length : width; }

bool LogicalType::GetDecimalProperties(uint8_t& width, uint8_t& scale) const {
    switch (type) {
        case GS_TYPE_NULL:
            width = 0;
            scale = 0;
            break;
        case GS_TYPE_BOOLEAN:
            width = 1;
            scale = 0;
            break;
        case GS_TYPE_TINYINT:
            // tinyint: [-127, 127] = DECIMAL(3,0)
            width = 3;
            scale = 0;
            break;
        case GS_TYPE_SMALLINT:
            // smallint: [-32767, 32767] = DECIMAL(5,0)
            width = 5;
            scale = 0;
            break;
        case GS_TYPE_INTEGER:
            // integer: [-2147483647, 2147483647] = DECIMAL(10,0)
            width = 10;
            scale = 0;
            break;
        case GS_TYPE_BIGINT:
            // bigint: [-9223372036854775807, 9223372036854775807] = DECIMAL(19,0)
            width = 19;
            scale = 0;
            break;
        case GS_TYPE_UTINYINT:
            // UInt8 — [0 : 255]
            width = 3;
            scale = 0;
            break;
        case GS_TYPE_USMALLINT:
            // UInt16 — [0 : 65535]
            width = 5;
            scale = 0;
            break;
        case GS_TYPE_UINT32:
            // UInt32 — [0 : 4294967295]
            width = 10;
            scale = 0;
            break;
        case GS_TYPE_UINT64:
            // UInt64 — [0 : 18446744073709551615]
            width = 20;
            scale = 0;
            break;
        case GS_TYPE_HUGEINT:
            // hugeint: max size decimal (38, 0)
            // note that a hugeint is not guaranteed to fit in this
            width = 38;
            scale = 0;
            break;
        case GS_TYPE_NUMBER:
        case GS_TYPE_DECIMAL:
            width = Precision();
            scale = Scale();
            break;
        default:
            // Nonsense values to ensure initialization
            return false;
    }
    return true;
}

LogicalType LogicalType::ToDeicmalType() const {
    if (intarkdb::IsDecimal(type)) {
        return *this;
    }
    uint8_t width = 0;
    uint8_t scale = 0;
    if (!GetDecimalProperties(width, scale)) {
        throw intarkdb::Exception(ExceptionType::EXECUTOR, "cant convert to decimal type");
    }
    return LogicalType(GS_TYPE_DECIMAL, 0, width, scale, 8);
}

bool LogicalType::operator==(const LogicalType& other) const {
    if (type != other.type) {
        return false;
    }
    if (intarkdb::IsString(type)) {
        return length == other.length;
    }
    if (intarkdb::IsDecimal(type)) {
        return precision == other.precision && scale == other.scale;
    }
    return true;
}

namespace intarkdb {

// 获取类型的优先级
int GetPriority(GStorDataType t) {
    switch (t) {
        case GS_TYPE_NULL:
            return 0;
        case GS_TYPE_PARAM:
            return 2;
        case GS_TYPE_BOOLEAN:
            return 10;
        case GS_TYPE_TINYINT:
            return 11;
        case GS_TYPE_SMALLINT:
            return 12;
        case GS_TYPE_INTEGER:
            return 13;
        case GS_TYPE_BIGINT:
            return 14;
        case GS_TYPE_DATE:
            return 15;
        case GS_TYPE_TIMESTAMP:
            return 19;
        case GS_TYPE_DECIMAL:
        case GS_TYPE_NUMBER:
            return 21;
        case GS_TYPE_FLOAT:
        case GS_TYPE_REAL:
            return 23;
        case GS_TYPE_CHAR:
        case GS_TYPE_VARCHAR:
        case GS_TYPE_STRING:
            return 25;
        case GS_TYPE_BLOB:
            return 26;
        case GS_TYPE_INTERVAL:
            return 27;
        case GS_TYPE_UTINYINT:
            return 28;
        case GS_TYPE_USMALLINT:
            return 29;
        case GS_TYPE_UINT32:
            return 30;
        case GS_TYPE_UINT64:
            return 21;
        case GS_TYPE_HUGEINT:
            return 50;
        default:
            break;
    }
    throw intarkdb::Exception(ExceptionType::FATAL, fmt::format("unkonw type {} for prioprity", t));
}

GStorDataType GetNegativeType(GStorDataType a) {
    if (a == GS_TYPE_UTINYINT) {
        return GS_TYPE_SMALLINT;
    } else if (a == GS_TYPE_USMALLINT) {
        return GS_TYPE_INTEGER;
    } else if (a == GS_TYPE_UINT32) {
        return GS_TYPE_BIGINT;
    } else if (a == GS_TYPE_UINT64) {
        return GS_TYPE_REAL;
    }
    return a;
}

bool IsNumeric(GStorDataType type) {
    switch (type) {
        case GS_TYPE_TINYINT:
        case GS_TYPE_UTINYINT:
        case GS_TYPE_SMALLINT:
        case GS_TYPE_USMALLINT:
        case GS_TYPE_INTEGER:
        case GS_TYPE_UINT32:
        case GS_TYPE_UINT64:
        case GS_TYPE_BIGINT:
        case GS_TYPE_HUGEINT:
        case GS_TYPE_REAL:
        case GS_TYPE_FLOAT:
        case GS_TYPE_DECIMAL:
        case GS_TYPE_NUMBER:
            return true;
        default:
            return false;
    }
}

bool IsUnSigned(GStorDataType type) {
    switch (type) {
        case GS_TYPE_UTINYINT:
        case GS_TYPE_USMALLINT:
        case GS_TYPE_UINT32:
        case GS_TYPE_UINT64:
            return true;
        default:
            return false;
    }
}

bool IsInteger(GStorDataType type) {
    switch (type) {
        case GS_TYPE_TINYINT:
        case GS_TYPE_UTINYINT:
        case GS_TYPE_SMALLINT:
        case GS_TYPE_USMALLINT:
        case GS_TYPE_INTEGER:
        case GS_TYPE_UINT32:
        case GS_TYPE_UINT64:
        case GS_TYPE_BIGINT:
        case GS_TYPE_HUGEINT:
            return true;
        default:
            return false;
    }
}

bool IsDecimal(GStorDataType type) { return type == GS_TYPE_DECIMAL || type == GS_TYPE_NUMBER; }

bool IsFloat(GStorDataType type) { return IsDecimal(type) || type == GS_TYPE_REAL || type == GS_TYPE_FLOAT; }

bool IsString(GStorDataType type) {
    return type == GStorDataType::GS_TYPE_VARCHAR || type == GStorDataType::GS_TYPE_CLOB ||
           type == GStorDataType::GS_TYPE_CHAR || type == GStorDataType::GS_TYPE_STRING ||
           type == GStorDataType::GS_TYPE_RAW || type == GStorDataType::GS_TYPE_BLOB ||
           type == GStorDataType::GS_TYPE_BINARY;
}

// 检查是否可以转换为 decimal
static LogicalType DecimalSizeCheck(const LogicalType& left, const LogicalType& right) {
    // make sure left.TypeId() != right.TypeId()
    if (left.TypeId() == GS_TYPE_DECIMAL) {
        // make sure right is decimal
        return DecimalSizeCheck(right, left);
    }
    auto scale = right.Scale();
    auto precision = right.Precision();

    uint8_t other_precision = 0;
    uint8_t ohter_scale = 0;
    if (!left.GetDecimalProperties(other_precision, ohter_scale)) {  // 把整数类型 -> Decimal(precision,0)
        throw intarkdb::Exception(ExceptionType::CONVERSION,
                                  fmt::format("{} is not a compatible numeric type", left));  // 无法转换成功
    }
    const auto effective_width = precision - scale;
    if (other_precision > effective_width) {
        auto new_precision = other_precision + scale;
        if (new_precision > DecimalPrecision::max) {
            new_precision = DecimalPrecision::max;
        }
        return LogicalType::Decimal(new_precision, scale);  // 转换为新 Decimal(new_precision , scale)
    }
    return right;  // right 的范围 > left 的范围，直接返回 right
}

static LogicalType CombineNumericTypes(const LogicalType& left, const LogicalType& right) {
    if (GetPriority(left.TypeId()) > GetPriority(right.TypeId())) {
        return CombineNumericTypes(right, left);
    }
    if (CastRules::ImplicitCastable(left, right) >= 0) {
        if (right.TypeId() == GS_TYPE_DECIMAL) {
            return DecimalSizeCheck(left, right);
        }
        return right;
    }
    if (CastRules::ImplicitCastable(right, left) >= 0) {
        if (left.TypeId() == GS_TYPE_DECIMAL) {
            return DecimalSizeCheck(right, left);
        }
        return left;
    }
    // 默认 unsigned 的 id > signed 的 id
    if (left.TypeId() == GS_TYPE_BIGINT || right.TypeId() == GS_TYPE_UINT64) {
        return GS_TYPE_HUGEINT;
    }
    if (left.TypeId() == GS_TYPE_INTEGER || right.TypeId() == GS_TYPE_UINT32) {
        return GS_TYPE_BIGINT;
    }
    if (left.TypeId() == GS_TYPE_SMALLINT || right.TypeId() == GS_TYPE_USMALLINT) {
        return GS_TYPE_INTEGER;
    }
    if (left.TypeId() == GS_TYPE_TINYINT || right.TypeId() == GS_TYPE_UTINYINT) {
        return GS_TYPE_SMALLINT;
    }
    throw intarkdb::Exception(ExceptionType::CONVERSION,
                              fmt::format("not supported compatible type {} and {}", left, right));
}

constexpr int DEFAULT_VARCHAR_LENGTH = 1024;

// 获取兼容两者的类型
LogicalType GetCompatibleType(const LogicalType& left, const LogicalType& right) {
    if (left.TypeId() != right.TypeId() && IsNumeric(left.TypeId()) && IsNumeric(right.TypeId())) {
        auto t = CombineNumericTypes(left, right);
        if (t.TypeId() == GS_TYPE_HUGEINT) {
            // FIXME: 目前插入不支持 hugeint
            return LogicalType::Double();
        }
        return t;
    }
    if (left.TypeId() == GS_TYPE_PARAM) {  // for prepare parameter
        return right;
    }
    if (right.TypeId() == GS_TYPE_PARAM) {
        return left;
    }

    auto left_priority = GetPriority(left.TypeId());
    auto right_priority = GetPriority(right.TypeId());
    if (left_priority < right_priority) {
        if (right.TypeId() == GS_TYPE_VARCHAR) {
            // 为了可以使其他列能够转换成功，设置一个比较长的长度
            return LogicalType::Varchar(right.Length() > DEFAULT_VARCHAR_LENGTH ? right.Length()
                                                                                : DEFAULT_VARCHAR_LENGTH);
        }
        return right;
    }
    if (right_priority < left_priority) {
        if (left.TypeId() == GS_TYPE_VARCHAR) {
            return LogicalType::Varchar(left.Length() > DEFAULT_VARCHAR_LENGTH ? left.Length()
                                                                               : DEFAULT_VARCHAR_LENGTH);
        }
        return left;
    }
    // left == right
    // FIXME: varchar , 处理多字符集问题
    if (left.TypeId() == GS_TYPE_VARCHAR) {
        return left.Length() > right.Length() ? left : right;
    }
    if (left.TypeId() == GS_TYPE_DECIMAL) {  // 生成兼容的宽度和精度
        auto extra_width_left = left.Precision() - left.Scale();
        auto extra_width_right = right.Precision() - right.Scale();
        int8_t extra_width = std::max(extra_width_left, extra_width_right);
        uint8_t scale = std::max(left.Scale(), right.Scale());
        uint8_t precision = extra_width + scale;
        if (precision > DecimalPrecision::max) {
            precision = DecimalPrecision::max;
            scale = precision - extra_width;
        }
        return LogicalType::Decimal(precision, scale);
    }
    return left;
}

LogicalType NewLogicalType(const exp_column_def_t& def) {
    LogicalType type = GS_TYPE_NULL;
    type.type = def.col_type;
    if (def.col_type == GS_TYPE_VARCHAR) {
        type.length = def.size;
        type.width = def.size;
    } else if (def.col_type == GS_TYPE_DECIMAL) {
        type.precision = def.precision;
        type.scale = def.scale;
        // TODO width 不确定
        type.width = sizeof(dec4_t);  // dec4_t 最大长度
    } else {
        type.width = def.size;
    }
    return type;
}

LogicalType GetAddDecimalType(const LogicalType& left, const LogicalType& right) {
    int16_t max_precision = std::max(left.Precision(), right.Precision());
    int16_t max_scale = std::max(left.Scale(), right.Scale());
    int16_t max_precision_over_scale = std::max(left.Precision() - left.Scale(), right.Precision() - right.Scale());
    int16_t target_precision = std::max<int16_t>(max_precision, max_scale + max_precision_over_scale) + 1;
    if (target_precision > DecimalPrecision::upper && max_precision <= DecimalPrecision::upper) {
        target_precision = DecimalPrecision::upper;
    }
    return LogicalType::Decimal(target_precision, max_scale);
}

LogicalType GetMulDecimalType(const LogicalType& left, const LogicalType& right) {
    auto precision = left.Precision() + right.Precision();
    auto max_precision = std::max(left.Precision(), right.Precision());
    auto scale = left.Scale() + right.Scale();
    if (scale > DecimalPrecision::max) {
        throw intarkdb::Exception(ExceptionType::CONVERSION, "decimal scale overflow");
    }
    if (precision > DecimalPrecision::upper && max_precision <= DecimalPrecision::upper &&
        scale < DecimalPrecision::upper) {
        precision = DecimalPrecision::upper;
    }
    if (precision > DecimalPrecision::max) {
        precision = DecimalPrecision::max;
    }
    return LogicalType::Decimal(precision, scale);
}

exp_column_def_t NewColumnDef(const LogicalType& type) {
    exp_column_def_t def = {};
    def.col_type = type.TypeId();
    if (type.TypeId() == GS_TYPE_VARCHAR) {
        def.size = type.Length();
    } else if (type.TypeId() == GS_TYPE_DECIMAL) {
        def.precision = type.Precision();
        def.scale = type.Scale();
        def.size = type.Length();
    } else {
        def.size = type.Length();
    }
    return def;
}

LogicalType NewLogicalType(GStorDataType type) { return LogicalType{type, 0, 0, 0, 0}; }

auto IsNull(const col_text_t& crud_value) -> bool {
    if (crud_value.len == INT16_MAX || crud_value.str == nullptr) {
        return true;
    }
    return false;
}

class ColTextConverter {
   public:
    explicit ColTextConverter(const col_text_t& value) : crud_value(value) {}

    template <typename T>
    operator T() && {
        throw std::runtime_error("not supported");
    }

   private:
    const col_text_t& crud_value;
};

template <>
ColTextConverter::operator int32_t() && {
    return *(int32_t*)(crud_value.str);
}

template <>
ColTextConverter::operator int64_t() && {
    return *(int64_t*)(crud_value.str);
}

template <>
ColTextConverter::operator uint32_t() && {
    return *(uint32_t*)(crud_value.str);
}

template <>
ColTextConverter::operator uint64_t() && {
    return *(uint64_t*)(crud_value.str);
}

template <>
ColTextConverter::operator std::string() && {
    return std::string(crud_value.str, crud_value.len);
}

template <>
ColTextConverter::operator hugeint_t() && {
    return *(hugeint_t*)(crud_value.str);
}

template <>
ColTextConverter::operator double() && {
    return *(double*)(crud_value.str);
}

template <>
ColTextConverter::operator dec4_t() && {
    // dec4_t 为不定长类型
    dec4_t* ptr = (dec4_t*)(crud_value.str);
    dec4_t val;
    val.head = ptr->head;
    for(int i = 0; i < val.ncells; i++) {
        val.cells[i] = ptr->cells[i];
    }
    return val;
}

template <>
ColTextConverter::operator date_stor_t() && {
    return *(date_stor_t*)(crud_value.str);
}

template <>
ColTextConverter::operator timestamp_stor_t() && {
    return *(timestamp_stor_t*)(crud_value.str);
}

InternalValue GetStoreValue(GStorDataType col_type, const col_text_t& crud_value) {
    InternalValue variant_val;
    if (IsNull(crud_value)) {
        return variant_val;
    }
    switch (col_type) {
        case GS_TYPE_TINYINT:
        case GS_TYPE_SMALLINT:
        case GS_TYPE_INTEGER: {
            int32_t val = ColTextConverter(crud_value);
            variant_val = val;
            break;
        }
        case GS_TYPE_BIGINT: {
            int64_t val = ColTextConverter(crud_value);
            variant_val = val;
            break;
        }
        case GS_TYPE_BOOLEAN:
        case GS_TYPE_UTINYINT:
        case GS_TYPE_USMALLINT:
        case GS_TYPE_UINT32: {
            uint32_t val = ColTextConverter(crud_value);
            variant_val = val;
            break;
        }
        case GS_TYPE_UINT64: {
            uint64_t val = ColTextConverter(crud_value);
            variant_val = val;
            break;
        }
        case GS_TYPE_HUGEINT: {
            hugeint_t val = ColTextConverter(crud_value);
            variant_val = val;
            break;
        }
        case GS_TYPE_FLOAT:
        case GS_TYPE_REAL: {
            double val = ColTextConverter(crud_value);
            variant_val = val;
            break;
        }
        case GS_TYPE_DECIMAL:
        case GS_TYPE_NUMBER: {
            dec4_t val = ColTextConverter(crud_value);
            variant_val = val;
            break;
        }
        case GS_TYPE_DATE: {
            date_stor_t val = ColTextConverter(crud_value);
            variant_val = val;
            break;
        }
        case GS_TYPE_TIMESTAMP: {
            timestamp_stor_t val = ColTextConverter(crud_value);
            variant_val = val;
            break;
        }
        case GS_TYPE_CHAR:
        case GS_TYPE_CLOB:
        case GS_TYPE_STRING:
        case GS_TYPE_BLOB:
        case GS_TYPE_RAW:
        case GS_TYPE_BINARY:
        case GS_TYPE_VARCHAR: {
            std::string val = ColTextConverter(crud_value);
            variant_val = val;
            break;
        }
        default:
            throw intarkdb::Exception(ExceptionType::INVALID_TYPE, fmt::format("not supported type {}", col_type));
    }
    return variant_val;
}

size_t GetTypeSize(GStorDataType type) {
    size_t size = 0;
    switch (type) {
        case GS_TYPE_NULL:
            size = 0;
            break;
        case GS_TYPE_TINYINT:
        case GS_TYPE_SMALLINT:
        case GS_TYPE_INTEGER:
            size = sizeof(int32_t);
            break;
        case GS_TYPE_BIGINT:
            size = sizeof(int64_t);
            break;
        case GS_TYPE_BOOLEAN:
        case GS_TYPE_UTINYINT:
        case GS_TYPE_USMALLINT:
        case GS_TYPE_UINT32:
            size = sizeof(uint32_t);
            break;
        case GS_TYPE_UINT64:
            size = sizeof(uint64_t);
            break;
        case GS_TYPE_HUGEINT:
            size = sizeof(hugeint_t);
            break;
        case GS_TYPE_FLOAT:
        case GS_TYPE_REAL:
            size = sizeof(double);
            break;
        case GS_TYPE_DECIMAL:
        case GS_TYPE_NUMBER:
            size = sizeof(dec4_t);
            break;
        case GS_TYPE_DATE:
            size = sizeof(date_stor_t);
            break;
        case GS_TYPE_TIMESTAMP:
            size = sizeof(timestamp_stor_t);
            break;
        case GS_TYPE_CHAR:
        case GS_TYPE_CLOB:
        case GS_TYPE_STRING:
        case GS_TYPE_BLOB:
        case GS_TYPE_RAW:
        case GS_TYPE_BINARY:
        case GS_TYPE_VARCHAR:
            // 变长类型无法确定大小
            size = COLUMN_VARCHAR_SIZE_DEFAULT;
            break;
        default:
            throw intarkdb::Exception(ExceptionType::INVALID_TYPE, fmt::format("not supported type {}", type));
    }
    return size;
}

};  // namespace intarkdb
