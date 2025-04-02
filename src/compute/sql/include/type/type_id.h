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
 * type_id.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/type/type_id.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <stdint.h>

#include <string>
#include <unordered_map>
#include <variant>

#include "storage/gstor/gstor_executor.h"
#include "storage/gstor/zekernel/common/cm_dec4.h"
#include "storage/gstor/zekernel/common/cm_defs.h"
#include "type/date.h"
#include "type/hugeint.h"
#include "type/timestamp_t.h"

// 类型系统，复用gstor定义
using GStorDataType = gs_type_t;

struct DecimalPrecision {
    static constexpr uint8_t max = 38;
    static constexpr uint8_t upper = 18;
};

struct LogicalType {
    static constexpr int32_t MAX_VARCHAR_LENGTH = 8192;
    // SQL 类型
    GStorDataType type;
    // for varchar
    int32_t length;
    // for decimal
    int32_t precision;
    int32_t scale;
    // for other types
    int32_t width;  // 数据宽度

    // 构造函数
    LogicalType() : type(GS_TYPE_NULL), length(0), precision(0), scale(0), width(0) {}
    LogicalType(GStorDataType type) : type(type), length(0), precision(0), scale(0), width(0) {}
    LogicalType(GStorDataType type, int32_t length, int32_t precision, int32_t scale, int32_t width)
        : type(type), length(length), precision(precision), scale(scale), width(width) {}

    GStorDataType TypeId() const { return type; }

    EXPORT_API uint8_t Scale() const;
    EXPORT_API uint8_t Precision() const;
    EXPORT_API uint32_t Length() const;

    bool operator==(const LogicalType& other) const;

    bool GetDecimalProperties(uint8_t& width, uint8_t& scale) const;

    LogicalType ToDeicmalType() const;

    static LogicalType Decimal(uint8_t precision, uint8_t scale) {
        // TODO: 通过precision 和 scale 计算decimal的最大大小
        return LogicalType(GS_TYPE_DECIMAL, 0, precision, scale, 8);
    }
    static LogicalType Integer() { return LogicalType(GS_TYPE_INTEGER, 0, 0, 0, sizeof(int32_t)); }
    static LogicalType Bigint() { return LogicalType(GS_TYPE_BIGINT, 0, 0, 0, sizeof(int64_t)); }
    static LogicalType Double() { return LogicalType(GS_TYPE_REAL, 0, 0, 0, sizeof(double)); }
    static LogicalType Varchar(int32_t length) { return LogicalType(GS_TYPE_VARCHAR, length, 0, 0, length); }
    static LogicalType Boolean() { return LogicalType(GS_TYPE_BOOLEAN, 0, 0, 0, sizeof(uint32_t)); }
    static LogicalType UINT64() { return LogicalType(GS_TYPE_UINT64, 0, 0, 0, sizeof(uint64_t)); }
};

constexpr int DEFALUT_DECIMAL_SCALE = 3;
constexpr int DEFALUT_DECIMAL_PRECISION = 18;

namespace intarkdb {

using InternalValue = std::variant<std::monostate, uint32_t, uint64_t, int32_t, int64_t, double, float, std::string,
                                   dec4_t, hugeint_t, timestamp_stor_t, date_stor_t>;

auto IsNull(const col_text_t&) -> bool;
InternalValue GetStoreValue(GStorDataType type, const col_text_t& text);

size_t GetTypeSize(GStorDataType type);

GStorDataType GetNegativeType(GStorDataType a);

// 预定列定义
constexpr exp_column_def_t INT64_DEF(bool is_nullable = true) {
    exp_column_def_t def = {};
    def.col_type = GS_TYPE_BIGINT;
    def.size = sizeof(int64_t);
    def.nullable = is_nullable;
    return def;
}
constexpr exp_column_def_t INT32_DEF(bool is_nullable = true) {
    exp_column_def_t def = {};
    def.col_type = GS_TYPE_INTEGER;
    def.size = sizeof(int32_t);
    def.nullable = is_nullable;
    return def;
}

constexpr exp_column_def_t VARCHAR_DEF(int32_t length, bool is_nullable = true) {
    exp_column_def_t def = {};
    def.col_type = GS_TYPE_VARCHAR;
    def.size = length;
    def.nullable = is_nullable;
    return def;
}

// 获取类型的优先级
int GetPriority(GStorDataType t);

bool IsNumeric(GStorDataType type);
bool IsFloat(GStorDataType type);
bool IsInteger(GStorDataType type);
bool IsUnSigned(GStorDataType type);
bool IsDecimal(GStorDataType type);
bool IsString(GStorDataType type);  // 是否字符串或者二进制数据(底层使用string存储数据)

// 获取兼容两者的类型
LogicalType GetCompatibleType(const LogicalType& left, const LogicalType& right);
#ifdef _MSC_VER
EXPORT_API LogicalType NewLogicalType(const exp_column_def_t& def);
EXPORT_API LogicalType NewLogicalType(GStorDataType type);
#else
LogicalType NewLogicalType(const exp_column_def_t& def);
LogicalType NewLogicalType(GStorDataType type);
#endif
// 获取decimal 加法 与 减法后的类型
LogicalType GetAddDecimalType(const LogicalType& left, const LogicalType& right);
// 获取decimal 乘法后的类型
LogicalType GetMulDecimalType(const LogicalType& left, const LogicalType& right);

exp_column_def_t NewColumnDef(const LogicalType& type);

};  // namespace intarkdb
