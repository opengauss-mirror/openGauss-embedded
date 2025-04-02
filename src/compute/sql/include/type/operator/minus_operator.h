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
 * minus_operator.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/type/operator/minus_operator.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include "common/exception.h"
#include "storage/gstor/zekernel/common/cm_dec4.h"
#include "type/hugeint.h"
#include "type/date.h"

namespace intarkdb {
struct TryMinusOpWithOverflowCheck {
    template <typename LEFT, typename RIGHT, typename RESULT>
    static inline bool Operation(LEFT left, RIGHT right, RESULT& result) {
        throw intarkdb::Exception(ExceptionType::EXECUTOR, "unsupported minus this type");
    }

    // 两个数都是相同类型
    template <typename T>
    static inline bool Operation(T a, T b, T& c) {
        if (std::is_signed<T>::value) {
            if (b > 0 && a < std::numeric_limits<T>::min() + b) {
                return false;  // 会下溢
            }
            if (b < 0 && a > std::numeric_limits<T>::max() + b) {
                return false;  // 会上溢
            }
        } else {
            if (a < b) {
                return false;
            }
        }
        c = a - b;
        return true;
    }

};

template<>
bool TryMinusOpWithOverflowCheck::Operation(dec4_t a, dec4_t b, dec4_t& c);

template<>
bool TryMinusOpWithOverflowCheck::Operation(double a, double b, double& c);

template<>
bool TryMinusOpWithOverflowCheck::Operation(hugeint_t a, hugeint_t b, hugeint_t& c);

template <>
bool TryMinusOpWithOverflowCheck::Operation(date_stor_t a, int64_t b, date_stor_t& c);

struct TryNegativeOpWithOverflowCheck {
    template <typename T, typename R>
    static inline bool Operation(T a, R& result) {
        // 检查负数是否超出范围
#ifdef _MSC_VER
        if (a == INT_MIN) {
            return false;
        }
#else
        if (a == std::numeric_limits<T>::min()) {
            return false;
        }
#endif
        result = -a;
        return true;
    }
};

template <>
bool TryNegativeOpWithOverflowCheck::Operation(dec4_t a, dec4_t& c);

template <>
bool TryNegativeOpWithOverflowCheck::Operation(double a, double& c);

template <>
bool TryNegativeOpWithOverflowCheck::Operation(hugeint_t a, hugeint_t& c);


struct MinusOp {
    template <typename T, typename U, typename R>
    static inline R Operation(T left, U right) {
        R result;
        if (!TryMinusOpWithOverflowCheck::Operation(left, right, result)) {
            // 出现溢出 抛出异常 , 提示信息 overflow in add operator of {} - {} , 打印出具体的值
            // TODO: 需要处理 dec4_t 的 format 问题
            throw intarkdb::Exception(ExceptionType::OUT_OF_RANGE, "overflow in minus operator");
        }
        return result;
    }
};

struct NegativeOp {
    template <typename T, typename R>
    static inline R Operation(T a) {
        R result;
        if (!TryNegativeOpWithOverflowCheck::Operation(a, result)) {
            throw intarkdb::Exception(ExceptionType::OUT_OF_RANGE, "overflow in negative operator");
        }
        return result;
    }
};

}  // namespace intarkdb
