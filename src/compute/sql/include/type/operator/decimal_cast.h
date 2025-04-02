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
* decimal_cast.h
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/include/type/operator/decimal_cast.h
*
* -------------------------------------------------------------------------
*/
#pragma once

#include <stdexcept>

#include "common/exception.h"
#include "storage/gstor/zekernel/common/cm_dec4.h"
#include "type/hugeint.h"

struct DecimalTryCast {
    template <class DST>
    EXPORT_API static inline bool Operation(const dec4_t& input, DST& result, int scale, int precision, std::string& msg) {
        throw std::runtime_error("can't cast decimal to other type");
    }
};

struct DecimalCast {
    template <class DST>
    EXPORT_API static inline DST Operation(const dec4_t& input, int scale, int precision) {
        DST result;
        std::string msg;
        if (!DecimalTryCast::Operation(input, result, scale, precision, msg)) {
            throw intarkdb::Exception(ExceptionType::DECIMAL, msg);
        }
        return result;
    }
};

#ifdef _MSC_VER
template <>
EXPORT_API bool DecimalTryCast::Operation(const dec4_t& input, std::string& result, int scale, int precision,
                                          std::string& msg);

template <>
EXPORT_API bool DecimalTryCast::Operation(const dec4_t& input, dec4_t& result, int scale, int precision,
                                          std::string& msg);

template <>
EXPORT_API bool DecimalTryCast::Operation(const dec4_t& input, int32_t& result, int scale, int precision,
                                          std::string& msg);

template <>
EXPORT_API bool DecimalTryCast::Operation(const dec4_t& input, int64_t& result, int scale, int precision,
                                          std::string& msg);

template <>
EXPORT_API bool DecimalTryCast::Operation(const dec4_t& input, uint32_t& result, int scale, int precision,
                                          std::string& msg);

template <>
EXPORT_API bool DecimalTryCast::Operation(const dec4_t& input, uint64_t& result, int scale, int precision,
                                          std::string& msg);

template <>
EXPORT_API bool DecimalTryCast::Operation(const dec4_t& input, hugeint_t& result, int scale, int precision,
                                          std::string& msg);

template <>
EXPORT_API bool DecimalTryCast::Operation(const dec4_t& input, double& result, int scale, int precision,
                                          std::string& msg);
#else
template <>
bool DecimalTryCast::Operation(const dec4_t& input, std::string& result, int scale, int precision, std::string& msg);

template <>
bool DecimalTryCast::Operation(const dec4_t& input, dec4_t& result, int scale, int precision, std::string& msg);

template <>
bool DecimalTryCast::Operation(const dec4_t& input, int32_t& result, int scale, int precision, std::string& msg);

template <>
bool DecimalTryCast::Operation(const dec4_t& input, int64_t& result, int scale, int precision, std::string& msg);

template <>
bool DecimalTryCast::Operation(const dec4_t& input, uint32_t& result, int scale, int precision, std::string& msg);

template <>
bool DecimalTryCast::Operation(const dec4_t& input, uint64_t& result, int scale, int precision, std::string& msg);

template <>
bool DecimalTryCast::Operation(const dec4_t& input, hugeint_t& result, int scale, int precision, std::string& msg);

template <>
bool DecimalTryCast::Operation(const dec4_t& input, double& result, int scale, int precision, std::string& msg);
#endif