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
 * mul_operator.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/type/operator/mul_operator.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "type/operator/mul_operator.h"

#include "storage/gstor/zekernel/common/cm_dec8.h"
#include "storage/gstor/zekernel/common/cm_decimal.h"

namespace intarkdb {

template <>
bool TryMultiplyOpWithOverflowCheck::Operation(dec4_t a, dec4_t b, dec4_t& c) {
    dec8_t d1;
    dec8_t d2;

    if (cm_dec_4_to_8(&d1, &a, cm_dec4_stor_sz(&a)) != GS_SUCCESS) {
        GS_LOG_RUN_ERR("multiply operation cm_dec4_to_8 error");
        return false;
    }
    if (cm_dec_4_to_8(&d2, &b, cm_dec4_stor_sz(&b)) != GS_SUCCESS) {
        GS_LOG_RUN_ERR("multiply operation cm_dec4_to_8 error");
        return false;
    }
    dec8_t result;
    if (cm_dec8_multiply(&d1, &d2, &result) != GS_SUCCESS) {
        GS_LOG_RUN_ERR("decimal multiply operation error");
        return false;
    }
    if (cm_dec_8_to_4(&c, &result) != GS_SUCCESS) {
        GS_LOG_RUN_ERR("multiply operation cm_dec8_to_4 error");
        return false;
    }
    return true;
}

template <>
bool TryMultiplyOpWithOverflowCheck::Operation(double a, double b, double& c) {
    c = a * b;
    return true;
}

template <>
bool TryMultiplyOpWithOverflowCheck::Operation(hugeint_t a, hugeint_t b, hugeint_t& c) {
    c = Hugeint::Multiply(a, b);
    return true;
}

}  // namespace intarkdb
