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
 * compare.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/function/compare/compare.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "function/compare/compare.h"

#include <cmath>

#include "cm_dec4.h"
#include "type/type_system.h"
#include "type/valid.h"

namespace intarkdb {

constexpr double DOUBLE_EPSILON = 1e-15;

#define DOUBLE_IS_ZERO(var) (((var) >= -DOUBLE_EPSILON) && ((var) <= DOUBLE_EPSILON))

static auto compare_double(double x, double y) -> CMP_RESULT {
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

auto EqualSet::Register(FunctionContext& context) -> void {
    context.RegisterFunction(
        "=", {{GS_TYPE_TINYINT, GS_TYPE_TINYINT}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            return ValueFactory::ValueBool(args[0].GetCastAs<int8_t>() == args[1].GetCastAs<int8_t>());
        });
    context.RegisterFunction(
        "=", {{GS_TYPE_SMALLINT, GS_TYPE_SMALLINT}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            return ValueFactory::ValueBool(args[0].GetCastAs<int16_t>() == args[1].GetCastAs<int16_t>());
        });
    context.RegisterFunction(
        "=", {{GS_TYPE_INTEGER, GS_TYPE_INTEGER}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            return ValueFactory::ValueBool(args[0].GetCastAs<int32_t>() == args[1].GetCastAs<int32_t>());
        });
    context.RegisterFunction(
        "=", {{GS_TYPE_BIGINT, GS_TYPE_BIGINT}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            return ValueFactory::ValueBool(args[0].GetCastAs<int64_t>() == args[1].GetCastAs<int64_t>());
        });
    context.RegisterFunction(
        "=", {{GS_TYPE_UTINYINT, GS_TYPE_UTINYINT}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            return ValueFactory::ValueBool(args[0].GetCastAs<uint8_t>() == args[1].GetCastAs<uint8_t>());
        });
    context.RegisterFunction(
        "=", {{GS_TYPE_USMALLINT, GS_TYPE_USMALLINT}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            return ValueFactory::ValueBool(args[0].GetCastAs<uint16_t>() == args[1].GetCastAs<uint16_t>());
        });
    context.RegisterFunction(
        "=", {{GS_TYPE_UINT32, GS_TYPE_UINT32}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            return ValueFactory::ValueBool(args[0].GetCastAs<uint32_t>() == args[1].GetCastAs<uint32_t>());
        });
    context.RegisterFunction(
        "=", {{GS_TYPE_UINT64, GS_TYPE_UINT64}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            return ValueFactory::ValueBool(args[0].GetCastAs<uint64_t>() == args[1].GetCastAs<uint64_t>());
        });
    // 放前面，避免 出现参数 ? = 'abc' 时，会先匹配到int和varchar的比较
    context.RegisterFunction(
        "=", {{GS_TYPE_VARCHAR, GS_TYPE_VARCHAR}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            return ValueFactory::ValueBool(args[0].GetCastAs<std::string>() == args[1].GetCastAs<std::string>());
        });
    // int with varchar
    context.RegisterFunction(
        "=", {{GS_TYPE_BIGINT, GS_TYPE_VARCHAR}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            try {
                return ValueFactory::ValueBool(args[0].GetCastAs<int64_t>() == args[1].GetCastAs<int64_t>());
            } catch (const std::exception& ex) {
                return ValueFactory::ValueBool(false);
            }
        });
    context.RegisterFunction(
        "=", {{GS_TYPE_VARCHAR, GS_TYPE_BIGINT}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            try {
                return ValueFactory::ValueBool(args[0].GetCastAs<int64_t>() == args[1].GetCastAs<int64_t>());
            } catch (const std::exception& ex) {
                return ValueFactory::ValueBool(false);
            }
        });
    context.RegisterFunction(
        "=", {{GS_TYPE_UINT64, GS_TYPE_VARCHAR}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            try {
                return ValueFactory::ValueBool(args[0].GetCastAs<uint64_t>() == args[1].GetCastAs<uint64_t>());
            } catch (const std::exception& ex) {
                return ValueFactory::ValueBool(false);
            }
        });
    context.RegisterFunction(
        "=", {{GS_TYPE_VARCHAR, GS_TYPE_UINT64}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            try {
                return ValueFactory::ValueBool(args[0].GetCastAs<uint64_t>() == args[1].GetCastAs<uint64_t>());
            } catch (const std::exception& ex) {
                return ValueFactory::ValueBool(false);
            }
        });
    context.RegisterFunction("=", {{GS_TYPE_BOOLEAN, GS_TYPE_BOOLEAN}, GS_TYPE_BOOLEAN},
                             [](const std::vector<Value>& args) {
                                 return ValueFactory::ValueBool(args[0].GetCastAs<bool>() == args[1].GetCastAs<bool>());
                             });
    context.RegisterFunction(
        "=", {{GS_TYPE_BOOLEAN, GS_TYPE_BIGINT}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            return ValueFactory::ValueBool(args[0].GetCastAs<int64_t>() == args[1].GetCastAs<int64_t>());
        });
    context.RegisterFunction(
        "=", {{GS_TYPE_BIGINT, GS_TYPE_BOOLEAN}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            return ValueFactory::ValueBool(args[0].GetCastAs<int64_t>() == args[1].GetCastAs<int64_t>());
        });
    context.RegisterFunction(
        "=", {{GS_TYPE_FLOAT, GS_TYPE_FLOAT}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            return ValueFactory::ValueBool(compare_double(args[0].GetCastAs<double>(), args[1].GetCastAs<double>()) ==
                                           CMP_RESULT::EQUAL);
        });
    context.RegisterFunction("=", {{GS_TYPE_REAL, GS_TYPE_REAL}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
        return ValueFactory::ValueBool(compare_double(args[0].GetCastAs<double>(), args[1].GetCastAs<double>()) ==
                                       CMP_RESULT::EQUAL);
    });

    // float with varchar
    context.RegisterFunction(
        "=", {{GS_TYPE_FLOAT, GS_TYPE_VARCHAR}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            return ValueFactory::ValueBool(compare_double(args[0].GetCastAs<double>(), args[1].GetCastAs<double>()) ==
                                           CMP_RESULT::EQUAL);
        });
    context.RegisterFunction(
        "=", {{GS_TYPE_VARCHAR, GS_TYPE_FLOAT}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            return ValueFactory::ValueBool(compare_double(args[0].GetCastAs<double>(), args[1].GetCastAs<double>()) ==
                                           CMP_RESULT::EQUAL);
        });
    context.RegisterFunction(
        "=", {{GS_TYPE_REAL, GS_TYPE_VARCHAR}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            return ValueFactory::ValueBool(compare_double(args[0].GetCastAs<double>(), args[1].GetCastAs<double>()) ==
                                           CMP_RESULT::EQUAL);
        });
    context.RegisterFunction(
        "=", {{GS_TYPE_VARCHAR, GS_TYPE_REAL}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            return ValueFactory::ValueBool(compare_double(args[0].GetCastAs<double>(), args[1].GetCastAs<double>()) ==
                                           CMP_RESULT::EQUAL);
        });

    context.RegisterFunction("=", {{GS_TYPE_DECIMAL, GS_TYPE_DECIMAL}, GS_TYPE_BOOLEAN},
                             [](const std::vector<Value>& args) {
                                 auto x1 = args[0].GetCastAs<dec4_t>();
                                 auto x2 = args[1].GetCastAs<dec4_t>();
                                 auto ret = cm_dec4_cmp(&x1, &x2);
                                 return ValueFactory::ValueBool(ret == 0);
                             });
    context.RegisterFunction("=", {{GS_TYPE_DATE, GS_TYPE_DATE}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
        return ValueFactory::ValueBool(args[0].GetCastAs<date_stor_t>() == args[1].GetCastAs<date_stor_t>());
    });

    context.RegisterFunction("=", {{GS_TYPE_TIMESTAMP, GS_TYPE_TIMESTAMP}, GS_TYPE_BOOLEAN},
                             [](const std::vector<Value>& args) {
                                 return ValueFactory::ValueBool(args[0].GetCastAs<timestamp_stor_t>() ==
                                                                args[1].GetCastAs<timestamp_stor_t>());
                             });
    context.RegisterFunction(
        "=", {{GS_TYPE_TIMESTAMP, GS_TYPE_BIGINT}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            return ValueFactory::ValueBool(args[0].GetCastAs<int64_t>() == args[1].GetCastAs<int64_t>());
        });
    context.RegisterFunction(
        "=", {{GS_TYPE_BIGINT, GS_TYPE_TIMESTAMP}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            return ValueFactory::ValueBool(args[0].GetCastAs<int64_t>() == args[1].GetCastAs<int64_t>());
        });

    context.RegisterFunction("=", {{GS_TYPE_VARCHAR, GS_TYPE_TIMESTAMP}, GS_TYPE_BOOLEAN},
                             [](const std::vector<Value>& args) {
                                 return ValueFactory::ValueBool(args[0].GetCastAs<timestamp_stor_t>() ==
                                                                args[1].GetCastAs<timestamp_stor_t>());
                             });
    context.RegisterFunction("=", {{GS_TYPE_BLOB, GS_TYPE_BLOB}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
        return ValueFactory::ValueBool(args[0].GetCastAs<std::string>() == args[1].GetCastAs<std::string>());
    });
    context.RegisterFunction("=", {{GS_TYPE_TIMESTAMP, GS_TYPE_VARCHAR}, GS_TYPE_BOOLEAN},
                             [](const std::vector<Value>& args) {
                                 return ValueFactory::ValueBool(args[0].GetCastAs<timestamp_stor_t>() ==
                                                                args[1].GetCastAs<timestamp_stor_t>());
                             });
    context.RegisterFunction(
        "=", {{GS_TYPE_VARCHAR, GS_TYPE_DATE}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            return ValueFactory::ValueBool(args[0].GetCastAs<date_stor_t>() == args[1].GetCastAs<date_stor_t>());
        });
    context.RegisterFunction(
        "=", {{GS_TYPE_DATE, GS_TYPE_VARCHAR}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            return ValueFactory::ValueBool(args[0].GetCastAs<date_stor_t>() == args[1].GetCastAs<date_stor_t>());
        });
    context.RegisterFunction(
        "=", {{GS_TYPE_CLOB, GS_TYPE_VARCHAR}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            return ValueFactory::ValueBool(args[0].GetCastAs<std::string>() == args[1].GetCastAs<std::string>());
        });
    context.RegisterFunction(
        "=", {{GS_TYPE_VARCHAR, GS_TYPE_CLOB}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            return ValueFactory::ValueBool(args[0].GetCastAs<std::string>() == args[1].GetCastAs<std::string>());
        });
    context.RegisterFunction("=", {{GS_TYPE_CLOB, GS_TYPE_CLOB}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
        return ValueFactory::ValueBool(args[0].GetCastAs<std::string>() == args[1].GetCastAs<std::string>());
    });
    context.RegisterFunction("=", {{GS_TYPE_VARCHAR, GS_TYPE_BOOLEAN}, GS_TYPE_BOOLEAN},
                             [](const std::vector<Value>& args) {
                                 return ValueFactory::ValueBool(args[0].GetCastAs<bool>() == args[1].GetCastAs<bool>());
                             });
    context.RegisterFunction("=", {{GS_TYPE_BOOLEAN, GS_TYPE_VARCHAR}, GS_TYPE_BOOLEAN},
                             [](const std::vector<Value>& args) {
                                 return ValueFactory::ValueBool(args[0].GetCastAs<bool>() == args[1].GetCastAs<bool>());
                             });
}

auto LessThanSet::Register(FunctionContext& context) -> void {
    context.RegisterFunction("<", {{GS_TYPE_BOOLEAN, GS_TYPE_BOOLEAN}, GS_TYPE_BOOLEAN},
                             [](const std::vector<Value>& args) {
                                 return ValueFactory::ValueBool(args[0].GetCastAs<bool>() < args[1].GetCastAs<bool>());
                             });
    context.RegisterFunction(
        "<", {{GS_TYPE_TINYINT, GS_TYPE_TINYINT}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            return ValueFactory::ValueBool(args[0].GetCastAs<int8_t>() < args[1].GetCastAs<int8_t>());
        });

    context.RegisterFunction(
        "<", {{GS_TYPE_SMALLINT, GS_TYPE_SMALLINT}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            return ValueFactory::ValueBool(args[0].GetCastAs<int16_t>() < args[1].GetCastAs<int16_t>());
        });

    context.RegisterFunction(
        "<", {{GS_TYPE_INTEGER, GS_TYPE_INTEGER}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            return ValueFactory::ValueBool(args[0].GetCastAs<int32_t>() < args[1].GetCastAs<int32_t>());
        });

    context.RegisterFunction(
        "<", {{GS_TYPE_BIGINT, GS_TYPE_BIGINT}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            return ValueFactory::ValueBool(args[0].GetCastAs<int64_t>() < args[1].GetCastAs<int64_t>());
        });
    context.RegisterFunction(
        "<", {{GS_TYPE_UTINYINT, GS_TYPE_UTINYINT}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            return ValueFactory::ValueBool(args[0].GetCastAs<uint8_t>() < args[1].GetCastAs<uint8_t>());
        });
    context.RegisterFunction(
        "<", {{GS_TYPE_USMALLINT, GS_TYPE_USMALLINT}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            return ValueFactory::ValueBool(args[0].GetCastAs<uint16_t>() < args[1].GetCastAs<uint16_t>());
        });
    context.RegisterFunction(
        "<", {{GS_TYPE_UINT32, GS_TYPE_UINT32}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            return ValueFactory::ValueBool(args[0].GetCastAs<uint32_t>() < args[1].GetCastAs<uint32_t>());
        });
    context.RegisterFunction(
        "<", {{GS_TYPE_UINT64, GS_TYPE_UINT64}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            return ValueFactory::ValueBool(args[0].GetCastAs<uint64_t>() < args[1].GetCastAs<uint64_t>());
        });
    context.RegisterFunction(
        "<", {{GS_TYPE_VARCHAR, GS_TYPE_VARCHAR}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            return ValueFactory::ValueBool(args[0].GetCastAs<std::string>() < args[1].GetCastAs<std::string>());
        });
    // int with varchar
    context.RegisterFunction(
        "<", {{GS_TYPE_BIGINT, GS_TYPE_VARCHAR}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            try {
                return ValueFactory::ValueBool(args[0].GetCastAs<int64_t>() < args[1].GetCastAs<int64_t>());
            } catch (const std::exception& ex) {
                return ValueFactory::ValueBool(true);
            }
        });
    context.RegisterFunction(
        "<", {{GS_TYPE_VARCHAR, GS_TYPE_BIGINT}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            try {
                return ValueFactory::ValueBool(args[0].GetCastAs<int64_t>() < args[1].GetCastAs<int64_t>());
            } catch (const std::exception& ex) {
                return ValueFactory::ValueBool(false);
            }
        });
    context.RegisterFunction(
        "<", {{GS_TYPE_UINT64, GS_TYPE_VARCHAR}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            try {
                return ValueFactory::ValueBool(args[0].GetCastAs<uint64_t>() < args[1].GetCastAs<uint64_t>());
            } catch (const std::exception& ex) {
                return ValueFactory::ValueBool(false);
            }
        });
    context.RegisterFunction(
        "<", {{GS_TYPE_VARCHAR, GS_TYPE_UINT64}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            try {
                return ValueFactory::ValueBool(args[0].GetCastAs<uint64_t>() < args[1].GetCastAs<uint64_t>());
            } catch (const std::exception& ex) {
                return ValueFactory::ValueBool(false);
            }
        });
    context.RegisterFunction(
        "<", {{GS_TYPE_FLOAT, GS_TYPE_FLOAT}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            return ValueFactory::ValueBool(compare_double(args[0].GetCastAs<double>(), args[1].GetCastAs<double>()) ==
                                           CMP_RESULT::LESS);
        });
    context.RegisterFunction("<", {{GS_TYPE_REAL, GS_TYPE_REAL}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
        return ValueFactory::ValueBool(compare_double(args[0].GetCastAs<double>(), args[1].GetCastAs<double>()) ==
                                       CMP_RESULT::LESS);
    });
    // float with varchar
    context.RegisterFunction(
        "<", {{GS_TYPE_FLOAT, GS_TYPE_VARCHAR}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            return ValueFactory::ValueBool(compare_double(args[0].GetCastAs<double>(), args[1].GetCastAs<double>()) ==
                                           CMP_RESULT::LESS);
        });
    context.RegisterFunction(
        "<", {{GS_TYPE_VARCHAR, GS_TYPE_FLOAT}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            return ValueFactory::ValueBool(compare_double(args[0].GetCastAs<double>(), args[1].GetCastAs<double>()) ==
                                           CMP_RESULT::LESS);
        });
    context.RegisterFunction(
        "<", {{GS_TYPE_REAL, GS_TYPE_VARCHAR}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            return ValueFactory::ValueBool(compare_double(args[0].GetCastAs<double>(), args[1].GetCastAs<double>()) ==
                                           CMP_RESULT::LESS);
        });
    context.RegisterFunction(
        "<", {{GS_TYPE_VARCHAR, GS_TYPE_REAL}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            return ValueFactory::ValueBool(compare_double(args[0].GetCastAs<double>(), args[1].GetCastAs<double>()) ==
                                           CMP_RESULT::LESS);
        });
    context.RegisterFunction("<", {{GS_TYPE_DECIMAL, GS_TYPE_DECIMAL}, GS_TYPE_BOOLEAN},
                             [](const std::vector<Value>& args) {
                                 auto x1 = args[0].GetCastAs<dec4_t>();
                                 auto x2 = args[1].GetCastAs<dec4_t>();
                                 auto ret = cm_dec4_cmp(&x1, &x2);
                                 return ValueFactory::ValueBool(ret == -1);
                             });
    context.RegisterFunction("<", {{GS_TYPE_DATE, GS_TYPE_DATE}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
        return ValueFactory::ValueBool(args[0].GetCastAs<date_stor_t>() < args[1].GetCastAs<date_stor_t>());
    });
    context.RegisterFunction("<", {{GS_TYPE_TIMESTAMP, GS_TYPE_TIMESTAMP}, GS_TYPE_BOOLEAN},
                             [](const std::vector<Value>& args) {
                                 return ValueFactory::ValueBool(args[0].GetCastAs<timestamp_stor_t>() <
                                                                args[1].GetCastAs<timestamp_stor_t>());
                             });
    context.RegisterFunction(
        "<", {{GS_TYPE_TIMESTAMP, GS_TYPE_BIGINT}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            return ValueFactory::ValueBool(args[0].GetCastAs<int64_t>() < args[1].GetCastAs<int64_t>());
        });
    context.RegisterFunction(
        "<", {{GS_TYPE_BIGINT, GS_TYPE_TIMESTAMP}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            return ValueFactory::ValueBool(args[0].GetCastAs<int64_t>() < args[1].GetCastAs<int64_t>());
        });
    context.RegisterFunction("<", {{GS_TYPE_VARCHAR, GS_TYPE_TIMESTAMP}, GS_TYPE_BOOLEAN},
                             [](const std::vector<Value>& args) {
                                 return ValueFactory::ValueBool(args[0].GetCastAs<timestamp_stor_t>() <
                                                                args[1].GetCastAs<timestamp_stor_t>());
                             });
    context.RegisterFunction("<", {{GS_TYPE_TIMESTAMP, GS_TYPE_VARCHAR}, GS_TYPE_BOOLEAN},
                             [](const std::vector<Value>& args) {
                                 return ValueFactory::ValueBool(args[0].GetCastAs<timestamp_stor_t>() <
                                                                args[1].GetCastAs<timestamp_stor_t>());
                             });
    context.RegisterFunction(
        "<", {{GS_TYPE_VARCHAR, GS_TYPE_DATE}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            return ValueFactory::ValueBool(args[0].GetCastAs<date_stor_t>() < args[1].GetCastAs<date_stor_t>());
        });
    context.RegisterFunction(
        "<", {{GS_TYPE_DATE, GS_TYPE_VARCHAR}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            return ValueFactory::ValueBool(args[0].GetCastAs<date_stor_t>() < args[1].GetCastAs<date_stor_t>());
        });

    context.RegisterFunction("<", {{GS_TYPE_BLOB, GS_TYPE_BLOB}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
        return ValueFactory::ValueBool(args[0].GetCastAs<std::string>() < args[1].GetCastAs<std::string>());
    });
    context.RegisterFunction(
        "<", {{GS_TYPE_CLOB, GS_TYPE_VARCHAR}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            return ValueFactory::ValueBool(args[0].GetCastAs<std::string>() < args[1].GetCastAs<std::string>());
        });
    context.RegisterFunction(
        "<", {{GS_TYPE_VARCHAR, GS_TYPE_CLOB}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            return ValueFactory::ValueBool(args[0].GetCastAs<std::string>() < args[1].GetCastAs<std::string>());
        });
    context.RegisterFunction("<", {{GS_TYPE_CLOB, GS_TYPE_CLOB}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
        return ValueFactory::ValueBool(args[0].GetCastAs<std::string>() < args[1].GetCastAs<std::string>());
    });
}

auto LessThanOrEqualSet::Register(FunctionContext& context) -> void {
    context.RegisterFunction("<=", {{GS_TYPE_BOOLEAN, GS_TYPE_BOOLEAN}, GS_TYPE_BOOLEAN},
                             [](const std::vector<Value>& args) {
                                 return ValueFactory::ValueBool(args[0].GetCastAs<bool>() <= args[1].GetCastAs<bool>());
                             });
    context.RegisterFunction(
        "<=", {{GS_TYPE_TINYINT, GS_TYPE_TINYINT}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            return ValueFactory::ValueBool(args[0].GetCastAs<int8_t>() <= args[1].GetCastAs<int8_t>());
        });
    context.RegisterFunction(
        "<=", {{GS_TYPE_SMALLINT, GS_TYPE_SMALLINT}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            return ValueFactory::ValueBool(args[0].GetCastAs<int16_t>() <= args[1].GetCastAs<int16_t>());
        });
    context.RegisterFunction(
        "<=", {{GS_TYPE_INTEGER, GS_TYPE_INTEGER}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            return ValueFactory::ValueBool(args[0].GetCastAs<int32_t>() <= args[1].GetCastAs<int32_t>());
        });
    context.RegisterFunction(
        "<=", {{GS_TYPE_BIGINT, GS_TYPE_BIGINT}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            return ValueFactory::ValueBool(args[0].GetCastAs<int64_t>() <= args[1].GetCastAs<int64_t>());
        });
    context.RegisterFunction(
        "<=", {{GS_TYPE_UTINYINT, GS_TYPE_UTINYINT}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            return ValueFactory::ValueBool(args[0].GetCastAs<uint8_t>() <= args[1].GetCastAs<uint8_t>());
        });
    context.RegisterFunction(
        "<=", {{GS_TYPE_USMALLINT, GS_TYPE_USMALLINT}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            return ValueFactory::ValueBool(args[0].GetCastAs<uint16_t>() <= args[1].GetCastAs<uint16_t>());
        });
    context.RegisterFunction(
        "<=", {{GS_TYPE_UINT32, GS_TYPE_UINT32}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            return ValueFactory::ValueBool(args[0].GetCastAs<uint32_t>() <= args[1].GetCastAs<uint32_t>());
        });
    context.RegisterFunction(
        "<=", {{GS_TYPE_UINT64, GS_TYPE_UINT64}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            return ValueFactory::ValueBool(args[0].GetCastAs<uint64_t>() <= args[1].GetCastAs<uint64_t>());
        });
    context.RegisterFunction(
        "<=", {{GS_TYPE_VARCHAR, GS_TYPE_VARCHAR}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            return ValueFactory::ValueBool(args[0].GetCastAs<std::string>() <= args[1].GetCastAs<std::string>());
        });
    // int with varchar
    context.RegisterFunction(
        "<=", {{GS_TYPE_BIGINT, GS_TYPE_VARCHAR}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            try {
                return ValueFactory::ValueBool(args[0].GetCastAs<int64_t>() <= args[1].GetCastAs<int64_t>());
            } catch (const std::exception& ex) {
                return ValueFactory::ValueBool(true);
            }
        });
    context.RegisterFunction(
        "<=", {{GS_TYPE_VARCHAR, GS_TYPE_BIGINT}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            try {
                return ValueFactory::ValueBool(args[0].GetCastAs<int64_t>() <= args[1].GetCastAs<int64_t>());
            } catch (const std::exception& ex) {
                return ValueFactory::ValueBool(false);
            }
        });
    context.RegisterFunction(
        "<=", {{GS_TYPE_UINT64, GS_TYPE_VARCHAR}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            try {
                return ValueFactory::ValueBool(args[0].GetCastAs<uint64_t>() <= args[1].GetCastAs<uint64_t>());
            } catch (const std::exception& ex) {
                return ValueFactory::ValueBool(true);
            }
        });
    context.RegisterFunction(
        "<=", {{GS_TYPE_VARCHAR, GS_TYPE_UINT64}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            try {
                return ValueFactory::ValueBool(args[0].GetCastAs<uint64_t>() <= args[1].GetCastAs<uint64_t>());
            } catch (const std::exception& ex) {
                return ValueFactory::ValueBool(false);
            }
        });
    context.RegisterFunction("<=", {{GS_TYPE_FLOAT, GS_TYPE_FLOAT}, GS_TYPE_BOOLEAN},
                             [](const std::vector<Value>& args) {
                                 auto ret = compare_double(args[0].GetCastAs<double>(), args[1].GetCastAs<double>());
                                 return ValueFactory::ValueBool(ret == CMP_RESULT::LESS || ret == CMP_RESULT::EQUAL);
                             });
    context.RegisterFunction("<=", {{GS_TYPE_REAL, GS_TYPE_REAL}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
        auto ret = compare_double(args[0].GetCastAs<double>(), args[1].GetCastAs<double>());
        return ValueFactory::ValueBool(ret == CMP_RESULT::LESS || ret == CMP_RESULT::EQUAL);
    });
    // float with varchar
    context.RegisterFunction("<=", {{GS_TYPE_FLOAT, GS_TYPE_VARCHAR}, GS_TYPE_BOOLEAN},
                             [](const std::vector<Value>& args) {
                                 auto ret = compare_double(args[0].GetCastAs<double>(), args[1].GetCastAs<double>());
                                 return ValueFactory::ValueBool(ret == CMP_RESULT::LESS || ret == CMP_RESULT::EQUAL);
                             });
    context.RegisterFunction("<=", {{GS_TYPE_VARCHAR, GS_TYPE_FLOAT}, GS_TYPE_BOOLEAN},
                             [](const std::vector<Value>& args) {
                                 auto ret = compare_double(args[0].GetCastAs<double>(), args[1].GetCastAs<double>());
                                 return ValueFactory::ValueBool(ret == CMP_RESULT::LESS || ret == CMP_RESULT::EQUAL);
                             });
    context.RegisterFunction("<=", {{GS_TYPE_REAL, GS_TYPE_VARCHAR}, GS_TYPE_BOOLEAN},
                             [](const std::vector<Value>& args) {
                                 auto ret = compare_double(args[0].GetCastAs<double>(), args[1].GetCastAs<double>());
                                 return ValueFactory::ValueBool(ret == CMP_RESULT::LESS || ret == CMP_RESULT::EQUAL);
                             });
    context.RegisterFunction("<=", {{GS_TYPE_VARCHAR, GS_TYPE_REAL}, GS_TYPE_BOOLEAN},
                             [](const std::vector<Value>& args) {
                                 auto ret = compare_double(args[0].GetCastAs<double>(), args[1].GetCastAs<double>());
                                 return ValueFactory::ValueBool(ret == CMP_RESULT::LESS || ret == CMP_RESULT::EQUAL);
                             });
    context.RegisterFunction("<=", {{GS_TYPE_DECIMAL, GS_TYPE_DECIMAL}, GS_TYPE_BOOLEAN},
                             [](const std::vector<Value>& args) {
                                 auto x1 = args[0].GetCastAs<dec4_t>();
                                 auto x2 = args[1].GetCastAs<dec4_t>();
                                 auto ret = cm_dec4_cmp(&x1, &x2);
                                 return ValueFactory::ValueBool(ret <= 0);
                             });
    context.RegisterFunction("<=", {{GS_TYPE_DATE, GS_TYPE_DATE}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
        return ValueFactory::ValueBool(args[0].GetCastAs<date_stor_t>() <= args[1].GetCastAs<date_stor_t>());
    });
    context.RegisterFunction("<=", {{GS_TYPE_TIMESTAMP, GS_TYPE_TIMESTAMP}, GS_TYPE_BOOLEAN},
                             [](const std::vector<Value>& args) {
                                 return ValueFactory::ValueBool(args[0].GetCastAs<timestamp_stor_t>() <=
                                                                args[1].GetCastAs<timestamp_stor_t>());
                             });
    context.RegisterFunction(
        "<=", {{GS_TYPE_TIMESTAMP, GS_TYPE_BIGINT}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            return ValueFactory::ValueBool(args[0].GetCastAs<int64_t>() <= args[1].GetCastAs<int64_t>());
        });
    context.RegisterFunction(
        "<=", {{GS_TYPE_BIGINT, GS_TYPE_TIMESTAMP}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            return ValueFactory::ValueBool(args[0].GetCastAs<int64_t>() <= args[1].GetCastAs<int64_t>());
        });
    context.RegisterFunction("<=", {{GS_TYPE_VARCHAR, GS_TYPE_TIMESTAMP}, GS_TYPE_BOOLEAN},
                             [](const std::vector<Value>& args) {
                                 return ValueFactory::ValueBool(args[0].GetCastAs<timestamp_stor_t>() <=
                                                                args[1].GetCastAs<timestamp_stor_t>());
                             });
    context.RegisterFunction("<=", {{GS_TYPE_TIMESTAMP, GS_TYPE_VARCHAR}, GS_TYPE_BOOLEAN},
                             [](const std::vector<Value>& args) {
                                 return ValueFactory::ValueBool(args[0].GetCastAs<timestamp_stor_t>() <=
                                                                args[1].GetCastAs<timestamp_stor_t>());
                             });
    context.RegisterFunction(
        "<=", {{GS_TYPE_VARCHAR, GS_TYPE_DATE}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            return ValueFactory::ValueBool(args[0].GetCastAs<date_stor_t>() <= args[1].GetCastAs<date_stor_t>());
        });
    context.RegisterFunction(
        "<=", {{GS_TYPE_DATE, GS_TYPE_VARCHAR}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            return ValueFactory::ValueBool(args[0].GetCastAs<date_stor_t>() <= args[1].GetCastAs<date_stor_t>());
        });

    context.RegisterFunction("<=", {{GS_TYPE_BLOB, GS_TYPE_BLOB}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
        return ValueFactory::ValueBool(args[0].GetCastAs<std::string>() <= args[1].GetCastAs<std::string>());
    });

    context.RegisterFunction(
        "<=", {{GS_TYPE_CLOB, GS_TYPE_VARCHAR}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            return ValueFactory::ValueBool(args[0].GetCastAs<std::string>() <= args[1].GetCastAs<std::string>());
        });
    context.RegisterFunction(
        "<=", {{GS_TYPE_VARCHAR, GS_TYPE_CLOB}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
            return ValueFactory::ValueBool(args[0].GetCastAs<std::string>() <= args[1].GetCastAs<std::string>());
        });
    context.RegisterFunction("<=", {{GS_TYPE_CLOB, GS_TYPE_CLOB}, GS_TYPE_BOOLEAN}, [](const std::vector<Value>& args) {
        return ValueFactory::ValueBool(args[0].GetCastAs<std::string>() <= args[1].GetCastAs<std::string>());
    });
}
}  // namespace intarkdb
