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
 * add_operator.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/type/operator/add_operator.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "type/operator/add_operator.h"

#include "storage/gstor/zekernel/common/cm_dec8.h"
#include "storage/gstor/zekernel/common/cm_decimal.h"

namespace intarkdb {

template <>
bool TryAddOpWithOverflowCheck::Operation(int32_t a, int32_t b, int32_t& c) {
    if (a > 0 && b > 0 && a > INT32_MAX - b) {
        return false;
    }
    if (a < 0 && b < 0 && a < INT32_MIN - b) {
        return false;
    }
    c = a + b;
    return true;
}

template <>
bool TryAddOpWithOverflowCheck::Operation(int8_t a, int8_t b, int8_t& c) {
    if (a > 0 && b > 0 && a > INT8_MAX - b) {
        return false;
    }
    if (a < 0 && b < 0 && a < INT8_MIN - b) {
        return false;
    }
    c = a + b;
    return true;
}

template <>
bool TryAddOpWithOverflowCheck::Operation(int16_t a, int16_t b, int16_t& c) {
    if (a > 0 && b > 0 && a > INT16_MAX - b) {
        return false;
    }
    if (a < 0 && b < 0 && a < INT16_MIN - b) {
        return false;
    }
    c = a + b;
    return true;
}

template <>
bool TryAddOpWithOverflowCheck::Operation(int64_t a, int64_t b, int64_t& c) {
    if (a > 0 && b > 0 && a > INT64_MAX - b) {
        return false;
    }
    if (a < 0 && b < 0 && a < INT64_MIN - b) {
        return false;
    }
    c = a + b;
    return true;
}

template <>
bool TryAddOpWithOverflowCheck::Operation(uint8_t a, uint8_t b, uint8_t& c) {
    if (a > 0 && b > 0 && a > UINT8_MAX - b) {
        return false;
    }
    c = a + b;
    return true;
}

template <>
bool TryAddOpWithOverflowCheck::Operation(uint16_t a, uint16_t b, uint16_t& c) {
    if (a > 0 && b > 0 && a > UINT16_MAX - b) {
        return false;
    }
    c = a + b;
    return true;
}

template <>
bool TryAddOpWithOverflowCheck::Operation(uint32_t a, uint32_t b, uint32_t& c) {
    if (a > 0 && b > 0 && a > UINT32_MAX - b) {
        return false;
    }
    c = a + b;
    return true;
}

template <>
bool TryAddOpWithOverflowCheck::Operation(uint64_t a, uint64_t b, uint64_t& c) {
    if (a > 0 && b > 0 && a > UINT64_MAX - b) {
        return false;
    }
    c = a + b;
    return true;
}

template <>
bool TryAddOpWithOverflowCheck::Operation(dec4_t a, dec4_t b, dec4_t& c) {
    dec8_t d1;
    dec8_t d2;

    if (cm_dec_4_to_8(&d1, &a, cm_dec4_stor_sz(&a)) != GS_SUCCESS) {
        GS_LOG_RUN_ERR("add operation cm_dec4_to_8 error");
        return false;
    }
    if (cm_dec_4_to_8(&d2, &b, cm_dec4_stor_sz(&b)) != GS_SUCCESS) {
        GS_LOG_RUN_ERR("add operation cm_dec4_to_8 error");
        return false;
    }
    dec8_t result;
    if (cm_dec8_add(&d1, &d2, &result) != GS_SUCCESS) {
        GS_LOG_RUN_ERR("add operation cm_dec8_add error");
        return false;
    }
    if (cm_dec_8_to_4(&c, &result) != GS_SUCCESS) {
        GS_LOG_RUN_ERR("add operation cm_dec8_to_4 error");
        return false;
    }
    return true;
}

template <>
bool TryAddOpWithOverflowCheck::Operation(hugeint_t a, hugeint_t b, hugeint_t& c) {
    c = Hugeint::Add(a, b);
    return true;
}

template <>
bool TryAddOpWithOverflowCheck::Operation(double a, double b, double& c) {
    c = a + b;
    return true;
}

template <>
bool TryAddOpWithOverflowCheck::Operation(date_stor_t a, int64_t b, date_stor_t& c) {
    int64_t date_int = a.dates.ts;
    int64_t day = b * DateUtils::US_PER_DAY;
    if (day > 0 && date_int > INT64_MAX - day) {
        return false;
    }
    c.dates.ts = date_int + day;
    return true;
}

}  // namespace intarkdb
