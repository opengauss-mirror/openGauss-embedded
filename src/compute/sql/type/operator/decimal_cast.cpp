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
 * decimal_cast.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/type/operator/decimal_cast.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "type/operator/decimal_cast.h"

#include "storage/gstor/zekernel/common/cm_dec8.h"
#include "storage/gstor/zekernel/common/cm_decimal.h"
#include "type/type_id.h"

static void FormatZero(int scale, int precision, std::string& result) {
    // 补充前导0
    if ((result.length() > 0 && result[0] == '.') || (result.length() > 1 && result[0] == '-' && result[1] == '.')) {
        if (result[0] == '-') {
            result.insert(1, "0");
        } else {
            result.insert(0, "0");
        }
    }
    // 补充尾部0
    if (scale > 0) {
        auto dot_pos = result.find('.');
        if (dot_pos != std::string::npos) {
            int zero_count = (int)scale - ((int)result.length() - (int)dot_pos - 1);
            if (zero_count > 0) {
                result.append(zero_count, '0');
            }
        } else {
            result.append(".");
            result.append(scale, '0');
        }
    }
}

template <>
bool DecimalTryCast::Operation(const dec4_t& input, std::string& result, int scale, int precision, std::string& msg) {
    char buf[GS_MAX_DEC_OUTPUT_ALL_PREC];
    if (cm_dec4_to_str(&input, GS_MAX_DEC_OUTPUT_ALL_PREC, buf) != GS_SUCCESS) {
        msg = "decimal to string failed";
        return false;
    }
    result = std::string(buf);
    FormatZero(scale, precision, result);
    return true;
}

template <>
bool DecimalTryCast::Operation(const dec4_t& input, dec4_t& result, int scale, int precision, std::string& msg) {
    result = input;
    dec8_t dec8;
    const dec4_t* p = &input;
    if (precision > DecimalPrecision::max || precision <= 0) {
        msg = "Precision must be between 1 and 38!";
        return false;
    }
    if (cm_dec_4_to_8(&dec8, p, cm_dec4_stor_sz(p)) != GS_SUCCESS) {
        msg = "dec4 to 8 fail";
        return false;
    }
    if (cm_adjust_dec8_with_mode(&dec8, precision, scale, ROUND_TRUNC) != GS_SUCCESS) {
        msg = "convert out of range";
        return false;
    }
    if (cm_dec_8_to_4(&result, &dec8) != GS_SUCCESS) {
        msg = "dec8 to 4 fail";
        return false;
    }
    return true;
}

template <>
bool DecimalTryCast::Operation(const dec4_t& input, int32_t& result, int scale, int precision, std::string& msg) {
    return cm_dec4_to_int32(&input, &result, ROUND_HALF_UP) == GS_SUCCESS;
}

template <>
bool DecimalTryCast::Operation(const dec4_t& input, int64_t& result, int scale, int precision, std::string& msg) {
    return cm_dec4_to_int64(&input, (long long*)&result, ROUND_HALF_UP) == GS_SUCCESS;
}

template <>
bool DecimalTryCast::Operation(const dec4_t& input, uint32_t& result, int scale, int precision, std::string& msg) {
    return cm_dec4_to_uint32(&input, &result, ROUND_HALF_UP) == GS_SUCCESS;
}

template <>
bool DecimalTryCast::Operation(const dec4_t& input, uint64_t& result, int scale, int precision, std::string& msg) {
    return cm_dec4_to_uint64(&input, (uint64*)&result, ROUND_HALF_UP) == GS_SUCCESS;
}

template <>
bool DecimalTryCast::Operation(const dec4_t& input, hugeint_t& result, int scale, int precision, std::string& msg) {
    auto tmp = cm_dec4_to_real(&input);
    return Hugeint::TryConvert(tmp, result);
}

template <>
bool DecimalTryCast::Operation(const dec4_t& input, double& result, int scale, int precision, std::string& msg) {
    result = cm_dec4_to_real(&input);
    return true;
}
