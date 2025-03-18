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
 * add_operator.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/type/operator/add_operator.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include "common/exception.h"
#include "storage/gstor/zekernel/common/cm_dec4.h"
#include "type/hugeint.h"
#include "type/date.h"

namespace intarkdb {
struct TryAddOpWithOverflowCheck {
    template <typename LEFT, typename RIGHT, typename RESULT>
    static inline bool Operation(LEFT left, RIGHT right, RESULT& result) {
        throw intarkdb::Exception(ExceptionType::EXECUTOR, "unsupported add this type");
    }
};

template <>
bool TryAddOpWithOverflowCheck::Operation(int32_t a, int32_t b, int32_t& c);

template <>
bool TryAddOpWithOverflowCheck::Operation(int8_t a, int8_t b, int8_t& c);

template <>
bool TryAddOpWithOverflowCheck::Operation(int16_t a, int16_t b, int16_t& c);

template <>
bool TryAddOpWithOverflowCheck::Operation(int64_t a, int64_t b, int64_t& c);

template <>
bool TryAddOpWithOverflowCheck::Operation(uint8_t a, uint8_t b, uint8_t& c);

template <>
bool TryAddOpWithOverflowCheck::Operation(uint16_t a, uint16_t b, uint16_t& c);

template <>
bool TryAddOpWithOverflowCheck::Operation(uint32_t a, uint32_t b, uint32_t& c);

template <>
bool TryAddOpWithOverflowCheck::Operation(uint64_t a, uint64_t b, uint64_t& c);

template <>
bool TryAddOpWithOverflowCheck::Operation(hugeint_t a, hugeint_t b, hugeint_t& c);

template <>
bool TryAddOpWithOverflowCheck::Operation(dec4_t a, dec4_t b, dec4_t& c);

template <>
bool TryAddOpWithOverflowCheck::Operation(double a, double b, double& c);

template <>
bool TryAddOpWithOverflowCheck::Operation(date_stor_t a, int64_t b, date_stor_t& c);

struct AddOp {
    template <typename T, typename U, typename R>
    static inline R Operation(T left, U right) {
        R result;
        if (!TryAddOpWithOverflowCheck::Operation(left, right, result)) {
            // 出现溢出 抛出异常 , 提示信息 overflow in add operator of {} + {} , 打印出具体的值
            // TODO: 需要处理 dec4_t 的 format 问题
            throw intarkdb::Exception(ExceptionType::OUT_OF_RANGE, "overflow in add operator");
        }
        return result;
    }
};

}  // namespace intarkdb
