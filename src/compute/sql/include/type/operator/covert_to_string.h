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
* covert_to_string.h
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/include/type/operator/covert_to_string.h
*
* -------------------------------------------------------------------------
*/
#pragma once

#include <stdint.h>

#include <string>

#include "type/hugeint.h"

struct ConvertToString {
    template <class SRC>
    static inline std::string Operation(SRC input) {
        // throw InternalException("Unrecognized type for ConvertToString %s", GetTypeId<SRC>());
        throw std::runtime_error("Unrecognized type for ConvertToString");
    }
};

template <>
std::string ConvertToString::Operation(bool input);
template <>
std::string ConvertToString::Operation(int8_t input);
template <>
std::string ConvertToString::Operation(int16_t input);
template <>
std::string ConvertToString::Operation(int32_t input);
template <>
std::string ConvertToString::Operation(int64_t input);
template <>
std::string ConvertToString::Operation(uint8_t input);
template <>
std::string ConvertToString::Operation(uint16_t input);
template <>
std::string ConvertToString::Operation(uint32_t input);
template <>
std::string ConvertToString::Operation(uint64_t input);
template <>
std::string ConvertToString::Operation(hugeint_t input);
template <>
std::string ConvertToString::Operation(float input);
template <>
std::string ConvertToString::Operation(double input);
