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
 * mul_operator.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/type/operator/mul_operator.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include "common/exception.h"
#include "storage/gstor/zekernel/common/cm_dec4.h"
#include "type/hugeint.h"

namespace intarkdb {

struct TryMultiplyOpWithOverflowCheck {
    template <typename LEFT, typename RIGHT, typename RESULT>
    static inline bool Operation(LEFT a, RIGHT b, RESULT& c) {
        if ((a == 0) || (b == 0)) {
            c = 0;
            return true;
        }
#ifdef _MSC_VER
        if ((a > 0) && (b > 0)) {
            if (a > (LLONG_MAX / b)) {
                return false;  // 溢出
            }
        } else if ((a < 0) && (b < 0)) {
            if (a < (LLONG_MAX / b)) {
                return false;  // 溢出
            }
        } else {
            if (((a > 0) && (b < 0)) && (b < (LLONG_MIN / a))) {
                return false;  // 溢出
            } else if (((a < 0) && (b > 0)) && (a < (LLONG_MIN / b))) {
                return false;  // 溢出
            }
        }
#else
        if ((a > 0) && (b > 0)) {
            if (a > (std::numeric_limits<RESULT>::max() / b)) {
                return false;  // 溢出
            }
        } else if ((a < 0) && (b < 0)) {
            if (a < (std::numeric_limits<RESULT>::max() / b)) {
                return false;  // 溢出
            }
        } else {
            if (((a > 0) && (b < 0)) && (b < (std::numeric_limits<RESULT>::min() / a))) {
                return false;  // 溢出
            } else if (((a < 0) && (b > 0)) && (a < (std::numeric_limits<RESULT>::min() / b))) {
                return false;  // 溢出
            }
        }
#endif
        c = a * b;
        return true;
    }
};

template <>
bool TryMultiplyOpWithOverflowCheck::Operation(dec4_t a, dec4_t b, dec4_t& c);

template <>
bool TryMultiplyOpWithOverflowCheck::Operation(double a, double b, double& c);

template <>
bool TryMultiplyOpWithOverflowCheck::Operation(hugeint_t a, hugeint_t b, hugeint_t& c);

struct MultiplyOp {
    template <typename T, typename U, typename R>
    static inline R Operation(T left, U right) {
        R result;
        if (!TryMultiplyOpWithOverflowCheck::Operation(left, right, result)) {
            // 出现溢出 抛出异常 , 提示信息 overflow in multiply operator of {} + {} , 打印出具体的值
            // TODO: 需要处理 dec4_t 的 format 问题
            throw intarkdb::Exception(ExceptionType::OUT_OF_RANGE, "overflow in multiply operator");
        }
        return result;
    }
};

}  // namespace intarkdb
