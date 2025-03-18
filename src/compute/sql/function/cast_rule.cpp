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
 * cast_rule.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/function/cast_rule.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "function/cast_rules.h"

namespace intarkdb {

// 转换成目标类型的代价
static int64_t CastCost(const LogicalType& t) {
    switch (t.TypeId()) {
        case GS_TYPE_INTEGER:
            return 103;
        case GS_TYPE_BIGINT:
            return 101;
        case GS_TYPE_REAL:
        case GS_TYPE_FLOAT:
            return 102;
        case GS_TYPE_HUGEINT:
            return 120;
        case GS_TYPE_TIMESTAMP:
            return 120;
        case GS_TYPE_VARCHAR:
            return 149;
        case GS_TYPE_DECIMAL:
        case GS_TYPE_NUMBER:
            return 104;
        default:
            return 110;
    }
}

static int64_t ImplicitForTinyInt(const LogicalType& to) {
    switch (to.TypeId()) {
        case GS_TYPE_SMALLINT:
        case GS_TYPE_INTEGER:
        case GS_TYPE_BIGINT:
        case GS_TYPE_HUGEINT:
        case GS_TYPE_REAL:
        case GS_TYPE_FLOAT:
        case GS_TYPE_DECIMAL:
        case GS_TYPE_NUMBER:
        case GS_TYPE_VARCHAR:
        case GS_TYPE_BLOB:
        case GS_TYPE_CLOB:
        case GS_TYPE_BINARY:
        case GS_TYPE_CHAR:
            return CastCost(to);
        default:
            return -1;
    }
}

static int64_t ImplicitForSmallInt(const LogicalType& to) {
    switch (to.TypeId()) {
        case GS_TYPE_INTEGER:
        case GS_TYPE_BIGINT:
        case GS_TYPE_HUGEINT:
        case GS_TYPE_REAL:
        case GS_TYPE_FLOAT:
        case GS_TYPE_DECIMAL:
        case GS_TYPE_VARCHAR:
        case GS_TYPE_BLOB:
        case GS_TYPE_CLOB:
        case GS_TYPE_BINARY:
        case GS_TYPE_CHAR:
            return CastCost(to);
        default:
            return -1;
    }
}

static int64_t ImplicitForInteger(const LogicalType& to) {
    switch (to.TypeId()) {
        case GS_TYPE_BIGINT:
        case GS_TYPE_HUGEINT:
        case GS_TYPE_REAL:
        case GS_TYPE_FLOAT:
        case GS_TYPE_DECIMAL:
        case GS_TYPE_VARCHAR:
        case GS_TYPE_BLOB:
        case GS_TYPE_CLOB:
        case GS_TYPE_BINARY:
        case GS_TYPE_CHAR:
            return CastCost(to);
        default:
            return -1;
    }
}

static int64_t ImplicitForBigInt(const LogicalType& to) {
    switch (to.TypeId()) {
        case GS_TYPE_HUGEINT:
        case GS_TYPE_REAL:
        case GS_TYPE_FLOAT:
        case GS_TYPE_DECIMAL:
        case GS_TYPE_VARCHAR:
        case GS_TYPE_BLOB:
        case GS_TYPE_CLOB:
        case GS_TYPE_BINARY:
        case GS_TYPE_CHAR:
            return CastCost(to);
        default:
            return -1;
    }
}

static int64_t ImplicitForUTinyInt(const LogicalType& to) {
    switch (to.TypeId()) {
        case GS_TYPE_USMALLINT:
        case GS_TYPE_UINT32:
        case GS_TYPE_UINT64:
        case GS_TYPE_SMALLINT:
        case GS_TYPE_INTEGER:
        case GS_TYPE_BIGINT:
        case GS_TYPE_HUGEINT:
        case GS_TYPE_REAL:
        case GS_TYPE_FLOAT:
        case GS_TYPE_DECIMAL:
        case GS_TYPE_VARCHAR:
        case GS_TYPE_BLOB:
        case GS_TYPE_CLOB:
        case GS_TYPE_BINARY:
        case GS_TYPE_CHAR:
            return CastCost(to);
        default:
            return -1;
    }
}

static int64_t ImplicitForUSmallInt(const LogicalType& to) {
    switch (to.TypeId()) {
        case GS_TYPE_UINT32:
        case GS_TYPE_UINT64:
        case GS_TYPE_INTEGER:
        case GS_TYPE_BIGINT:
        case GS_TYPE_HUGEINT:
        case GS_TYPE_REAL:
        case GS_TYPE_FLOAT:
        case GS_TYPE_DECIMAL:
        case GS_TYPE_VARCHAR:
        case GS_TYPE_BLOB:
        case GS_TYPE_CLOB:
        case GS_TYPE_BINARY:
        case GS_TYPE_CHAR:
            return CastCost(to);
        default:
            return -1;
    }
}

static int64_t ImplicitForUInt32(const LogicalType& to) {
    switch (to.TypeId()) {
        case GS_TYPE_UINT64:
        case GS_TYPE_BIGINT:
        case GS_TYPE_HUGEINT:
        case GS_TYPE_REAL:
        case GS_TYPE_FLOAT:
        case GS_TYPE_DECIMAL:
        case GS_TYPE_VARCHAR:
        case GS_TYPE_BLOB:
        case GS_TYPE_CLOB:
        case GS_TYPE_BINARY:
        case GS_TYPE_CHAR:
            return CastCost(to);
        default:
            return -1;
    }
}

static int64_t ImplicitForUInt64(const LogicalType& to) {
    switch (to.TypeId()) {
        case GS_TYPE_HUGEINT:
        case GS_TYPE_REAL:
        case GS_TYPE_FLOAT:
        case GS_TYPE_DECIMAL:
        case GS_TYPE_VARCHAR:
        case GS_TYPE_BLOB:
        case GS_TYPE_CLOB:
        case GS_TYPE_BINARY:
        case GS_TYPE_CHAR:
            return CastCost(to);
        default:
            return -1;
    }
}

static int64_t ImplicitForHugeInt(const LogicalType& to) {
    switch (to.TypeId()) {
        case GS_TYPE_REAL:
        case GS_TYPE_FLOAT:
        case GS_TYPE_DECIMAL:
        case GS_TYPE_VARCHAR:
        case GS_TYPE_BLOB:
        case GS_TYPE_CLOB:
        case GS_TYPE_BINARY:
        case GS_TYPE_CHAR:
            return CastCost(to);
        default:
            return -1;
    }
}

static int64_t ImplicitForDouble(const LogicalType& to) {
    switch (to.TypeId()) {
        case GS_TYPE_REAL:
        case GS_TYPE_FLOAT:
            return 0;  // 为了 REAL -> FLOAT , FLOAT -> REAL 可以0代价转换
        case GS_TYPE_VARCHAR:
        case GS_TYPE_BLOB:
        case GS_TYPE_CLOB:
        case GS_TYPE_BINARY:
        case GS_TYPE_CHAR:
            return CastCost(to);
        default:
            return -1;
    }
}

static int64_t ImplicitForDate(const LogicalType& to) {
    switch (to.TypeId()) {
        case GS_TYPE_TIMESTAMP:
        case GS_TYPE_VARCHAR:
            return CastCost(to);
        default:
            return -1;
    }
}

static int64_t ImplicitForTimeStamp(const LogicalType& to) {
    switch (to.TypeId()) {
        case GS_TYPE_DATE:
        case GS_TYPE_VARCHAR:
            return CastCost(to);
        default:
            return -1;
    }
}

static int64_t ImplicitForDecimal(const LogicalType& to) {
    switch (to.TypeId()) {
        case GS_TYPE_REAL:
        case GS_TYPE_FLOAT:
        case GS_TYPE_VARCHAR:
        case GS_TYPE_BLOB:
        case GS_TYPE_CLOB:
        case GS_TYPE_BINARY:
        case GS_TYPE_CHAR:
            return CastCost(to);
        default:
            return -1;
    }
}

static int64_t ImplicitForVarchar(const LogicalType& to) {
    switch (to.TypeId()) {
        case GS_TYPE_VARCHAR:
        case GS_TYPE_CHAR:
        case GS_TYPE_BLOB:
        case GS_TYPE_CLOB:
        case GS_TYPE_BINARY:
            return 0;
        default:
            return -1;
    }
}

static int64_t ImplicitForBoolean(const LogicalType& to) {
    switch (to.TypeId()) {
        case GS_TYPE_TINYINT:
        case GS_TYPE_SMALLINT:
        case GS_TYPE_INTEGER:
        case GS_TYPE_BIGINT:
        case GS_TYPE_UTINYINT:
        case GS_TYPE_USMALLINT:
        case GS_TYPE_UINT32:
        case GS_TYPE_UINT64:
        case GS_TYPE_HUGEINT:
        case GS_TYPE_REAL:
        case GS_TYPE_FLOAT:
        case GS_TYPE_DECIMAL:
        case GS_TYPE_NUMBER:
        case GS_TYPE_VARCHAR:
            return CastCost(to);
        default:
            return -1;
    }
}

// 判断是否可以隐式转换，返回代价 ，不能转换返回 < 0
int64_t CastRules::ImplicitCastable(const LogicalType& from, const LogicalType& to) {
    if (from.TypeId() == GS_TYPE_NULL) {
        // null can cast to everything
        return CastCost(to);
    }
    if (from.TypeId() == GS_TYPE_PARAM) {
        // cost nothing
        return 0;
    }
    if (from.TypeId() == to.TypeId()) {
        // same type
        return 0;
    }

    switch (from.TypeId()) {
        case GS_TYPE_TINYINT:
            return ImplicitForTinyInt(to);
        case GS_TYPE_SMALLINT:
            return ImplicitForSmallInt(to);
        case GS_TYPE_INTEGER:
            return ImplicitForInteger(to);
        case GS_TYPE_BIGINT:
            return ImplicitForBigInt(to);
        case GS_TYPE_UTINYINT:
            return ImplicitForUTinyInt(to);
        case GS_TYPE_USMALLINT:
            return ImplicitForUSmallInt(to);
        case GS_TYPE_UINT32:
            return ImplicitForUInt32(to);
        case GS_TYPE_UINT64:
            return ImplicitForUInt64(to);
        case GS_TYPE_HUGEINT:
            return ImplicitForHugeInt(to);
        case GS_TYPE_REAL:
        case GS_TYPE_FLOAT:
            return ImplicitForDouble(to);
        case GS_TYPE_DATE:
            return ImplicitForDate(to);
        case GS_TYPE_TIMESTAMP:
            return ImplicitForTimeStamp(to);
        case GS_TYPE_NUMBER:
        case GS_TYPE_DECIMAL:
            return ImplicitForDecimal(to);
        case GS_TYPE_VARCHAR:
        case GS_TYPE_CHAR:
        case GS_TYPE_BLOB:
        case GS_TYPE_CLOB:
        case GS_TYPE_BINARY:
            return ImplicitForVarchar(to);
        case GS_TYPE_BOOLEAN:
            return ImplicitForBoolean(to);
        default:
            return -1;
    }
}

}  // namespace intarkdb
