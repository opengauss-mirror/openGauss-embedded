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
 * minus.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/function/math/minus.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "function/math/minus.h"

#include "type/operator/decimal_cast.h"
#include "type/operator/minus_operator.h"
#include "type/type_id.h"
#include "type/value.h"

namespace intarkdb {

auto MinusSet::Register(FunctionContext& context) -> void {
    context.RegisterFunction("-", {{GS_TYPE_TINYINT}, GS_TYPE_TINYINT}, [](const std::vector<Value>& args) {
        return ValueFactory::ValueTinyInt(NegativeOp::Operation<int8_t, int8_t>(args[0].GetCastAs<int8_t>()));
    });
    context.RegisterFunction("-", {{GS_TYPE_SMALLINT}, GS_TYPE_SMALLINT}, [](const std::vector<Value>& args) {
        return ValueFactory::ValueSmallInt(NegativeOp::Operation<int16_t, int16_t>(args[0].GetCastAs<int16_t>()));
    });
    context.RegisterFunction("-", {{GS_TYPE_INTEGER}, GS_TYPE_INTEGER}, [](const std::vector<Value>& args) {
        return ValueFactory::ValueInt(NegativeOp::Operation<int32_t, int32_t>(args[0].GetCastAs<int32_t>()));
    });
    context.RegisterFunction("-", {{GS_TYPE_BIGINT}, GS_TYPE_BIGINT}, [](const std::vector<Value>& args) {
        return ValueFactory::ValueBigInt(NegativeOp::Operation<int64_t, int64_t>(args[0].GetCastAs<int64_t>()));
    });
    context.RegisterFunction("-", {{GS_TYPE_UTINYINT}, GS_TYPE_UTINYINT}, [](const std::vector<Value>& args) {
        return ValueFactory::ValueUnsignTinyInt(NegativeOp::Operation<uint8_t, uint8_t>(args[0].GetCastAs<uint8_t>()));
    });
    context.RegisterFunction("-", {{GS_TYPE_USMALLINT}, GS_TYPE_USMALLINT}, [](const std::vector<Value>& args) {
        return ValueFactory::ValueUnsignSmallInt(
            NegativeOp::Operation<uint16_t, uint16_t>(args[0].GetCastAs<uint16_t>()));
    });
    context.RegisterFunction("-", {{GS_TYPE_UINT32}, GS_TYPE_UINT32}, [](const std::vector<Value>& args) {
        return ValueFactory::ValueUnsignInt(NegativeOp::Operation<uint32_t, uint32_t>(args[0].GetCastAs<uint32_t>()));
    });
    context.RegisterFunction("-", {{GS_TYPE_UINT64}, GS_TYPE_UINT64}, [](const std::vector<Value>& args) {
        return ValueFactory::ValueUnsignBigInt(
            NegativeOp::Operation<uint64_t, uint64_t>(args[0].GetCastAs<uint64_t>()));
    });
    context.RegisterFunction("-", {{GS_TYPE_HUGEINT}, GS_TYPE_HUGEINT}, [](const std::vector<Value>& args) {
        return ValueFactory::ValueHugeInt(NegativeOp::Operation<hugeint_t, hugeint_t>(args[0].GetCastAs<hugeint_t>()));
    });
    context.RegisterFunction("-", {{GS_TYPE_REAL}, GS_TYPE_REAL}, [](const std::vector<Value>& args) {
        return ValueFactory::ValueDouble(NegativeOp::Operation<double, double>(args[0].GetCastAs<double>()));
    });
    context.RegisterFunction("-", {{GS_TYPE_FLOAT}, GS_TYPE_FLOAT}, [](const std::vector<Value>& args) {
        return ValueFactory::ValueDouble(NegativeOp::Operation<double, double>(args[0].GetCastAs<double>()));
    });
    context.RegisterFunction(
        "-", {{GS_TYPE_DECIMAL}, GS_TYPE_DECIMAL, [](const std::vector<LogicalType>& args) { return args[0]; }},
        [](const std::vector<Value>& args) {
            auto target_type = args[0].GetLogicalType();
            auto val = NegativeOp::Operation<dec4_t, dec4_t>(args[0].GetCastAs<dec4_t>());
            return ValueFactory::ValueDecimal(
                DecimalCast::Operation<dec4_t>(val, target_type.Scale(), target_type.Precision()));
        });
    context.RegisterFunction("-", {{GS_TYPE_HUGEINT}, GS_TYPE_HUGEINT}, [](const std::vector<Value>& args) {
        return ValueFactory::ValueHugeInt(NegativeOp::Operation<hugeint_t, hugeint_t>(args[0].GetCastAs<hugeint_t>()));
    });
    context.RegisterFunction(
        "-", {{GS_TYPE_TINYINT, GS_TYPE_TINYINT}, GS_TYPE_TINYINT}, [](const std::vector<Value>& args) {
            return ValueFactory::ValueTinyInt(
                MinusOp::Operation<int8_t, int8_t, int8_t>(args[0].GetCastAs<int8_t>(), args[1].GetCastAs<int8_t>()));
        });
    context.RegisterFunction("-", {{GS_TYPE_SMALLINT, GS_TYPE_SMALLINT}, GS_TYPE_SMALLINT},
                             [](const std::vector<Value>& args) {
                                 return ValueFactory::ValueSmallInt(MinusOp::Operation<int16_t, int16_t, int16_t>(
                                     args[0].GetCastAs<int16_t>(), args[1].GetCastAs<int16_t>()));
                             });
    context.RegisterFunction("-", {{GS_TYPE_INTEGER, GS_TYPE_INTEGER}, GS_TYPE_INTEGER},
                             [](const std::vector<Value>& args) {
                                 return ValueFactory::ValueInt(MinusOp::Operation<int32_t, int32_t, int32_t>(
                                     args[0].GetCastAs<int32_t>(), args[1].GetCastAs<int32_t>()));
                             });
    context.RegisterFunction("-", {{GS_TYPE_BIGINT, GS_TYPE_BIGINT}, GS_TYPE_BIGINT},
                             [](const std::vector<Value>& args) {
                                 return ValueFactory::ValueBigInt(MinusOp::Operation<int64_t, int64_t, int64_t>(
                                     args[0].GetCastAs<int64_t>(), args[1].GetCastAs<int64_t>()));
                             });
    context.RegisterFunction("-", {{GS_TYPE_UTINYINT, GS_TYPE_UTINYINT}, GS_TYPE_UTINYINT},
                             [](const std::vector<Value>& args) {
                                 return ValueFactory::ValueUnsignTinyInt(MinusOp::Operation<uint8_t, uint8_t, uint8_t>(
                                     args[0].GetCastAs<uint8_t>(), args[1].GetCastAs<uint8_t>()));
                             });
    context.RegisterFunction(
        "-", {{GS_TYPE_USMALLINT, GS_TYPE_USMALLINT}, GS_TYPE_USMALLINT}, [](const std::vector<Value>& args) {
            return ValueFactory::ValueUnsignSmallInt(MinusOp::Operation<uint16_t, uint16_t, uint16_t>(
                args[0].GetCastAs<uint16_t>(), args[1].GetCastAs<uint16_t>()));
        });
    context.RegisterFunction("-", {{GS_TYPE_UINT32, GS_TYPE_UINT32}, GS_TYPE_UINT32},
                             [](const std::vector<Value>& args) {
                                 return ValueFactory::ValueUnsignInt(MinusOp::Operation<uint32_t, uint32_t, uint32_t>(
                                     args[0].GetCastAs<uint32_t>(), args[1].GetCastAs<uint32_t>()));
                             });
    context.RegisterFunction(
        "-", {{GS_TYPE_UINT64, GS_TYPE_UINT64}, GS_TYPE_UINT64}, [](const std::vector<Value>& args) {
            return ValueFactory::ValueUnsignBigInt(MinusOp::Operation<uint64_t, uint64_t, uint64_t>(
                args[0].GetCastAs<uint64_t>(), args[1].GetCastAs<uint64_t>()));
        });
    context.RegisterFunction("-",{{GS_TYPE_HUGEINT, GS_TYPE_HUGEINT}, GS_TYPE_HUGEINT}, [](const std::vector<Value>& args) {
        return ValueFactory::ValueHugeInt(MinusOp::Operation<hugeint_t, hugeint_t, hugeint_t>(args[0].GetCastAs<hugeint_t>(), args[1].GetCastAs<hugeint_t>()));
    });
    context.RegisterFunction("-", {{GS_TYPE_REAL, GS_TYPE_REAL}, GS_TYPE_REAL}, [](const std::vector<Value>& args) {
        return ValueFactory::ValueDouble(
            MinusOp::Operation<double, double, double>(args[0].GetCastAs<double>(), args[1].GetCastAs<double>()));
    });
    context.RegisterFunction(
        "-",
        {{GS_TYPE_DECIMAL, GS_TYPE_DECIMAL},
         GS_TYPE_DECIMAL,
         [](const std::vector<LogicalType>& types) {
             if (types[0].TypeId() == GS_TYPE_PARAM || types[1].TypeId() == GS_TYPE_PARAM) {
                 return types[0].TypeId() == GS_TYPE_PARAM ? types[1] : types[0];
             }
             auto left = types[0].ToDeicmalType();
             auto right = types[1].ToDeicmalType();
             return GetAddDecimalType(left, right);
         }},
        [](const std::vector<Value>& args) {
            LogicalType left = args[0].GetLogicalType();
            LogicalType right = args[1].GetLogicalType();
            if (!intarkdb::IsDecimal(left.TypeId())) {
                if (!intarkdb::IsFloat(left.TypeId())) {  // real or float
                    left = left.ToDeicmalType();
                } else {
                    left = GS_TYPE_UNKNOWN;
                }
            }
            if (!intarkdb::IsDecimal(right.TypeId())) {
                if (!intarkdb::IsFloat(right.TypeId())) {
                    right = right.ToDeicmalType();
                } else {
                    right = GS_TYPE_UNKNOWN;
                }
            }

            LogicalType result_type;
            if (left.TypeId() == GS_TYPE_UNKNOWN || right.TypeId() == GS_TYPE_UNKNOWN) {
                // one of the type is decimal
                result_type = left.TypeId() == GS_TYPE_UNKNOWN ? right : left;
            } else {
                result_type = GetAddDecimalType(left, right);
            }
            auto result =
                MinusOp::Operation<dec4_t, dec4_t, dec4_t>(args[0].GetCastAs<dec4_t>(), args[1].GetCastAs<dec4_t>());
            result = DecimalCast::Operation<dec4_t>(result, result_type.Scale(), result_type.Precision());
            return ValueFactory::ValueDecimal(result, result_type.Precision(), result_type.Scale());
        });
    context.RegisterFunction("-", {{GS_TYPE_DATE, GS_TYPE_BIGINT}, GS_TYPE_DATE}, [](const std::vector<Value>& args) {
        return ValueFactory::ValueDate(MinusOp::Operation<date_stor_t, int64_t, date_stor_t>(
            args[0].GetCastAs<date_stor_t>(), args[1].GetCastAs<int64_t>()));
    });
}

}  // namespace intarkdb
