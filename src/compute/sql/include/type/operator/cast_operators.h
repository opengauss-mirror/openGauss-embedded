//===----------------------------------------------------------------------===//
// Copyright 2018-2022 Stichting DuckDB Foundation
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice (including the next paragraph)
// shall be included in all copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.
//===----------------------------------------------------------------------===//
#pragma once

#include <stdexcept>
#include <string_view>

#include "common/exception.h"
#include "common/winapi.h"
#include "storage/gstor/zekernel/common/cm_dec4.h"
#include "type/date.h"
#include "type/hugeint.h"
#include "type/timestamp_t.h"
#include "type/troolean.h"

struct EXPORT_API TryCast {
    template <class SRC, class DST>
    EXPORT_API static inline bool Operation(SRC input, DST &result, bool strict = false) {
        // 补充详细信息
        throw std::runtime_error("Unimplemented type for cast ");
    }
};

struct EXPORT_API Cast {
    template <typename SRC, typename DST>
    EXPORT_API static inline DST Operation(SRC input, bool strict = false) {
        DST result;
        if (!TryCast::Operation(input, result, strict)) {
            // TODO 补充详细信息
            throw intarkdb::Exception(ExceptionType::OUT_OF_RANGE,
                    "invalid type cast, maybe out of type range or unsupported type cast");
        }
        return result;
    }
};

//===--------------------------------------------------------------------===//
// Cast bool -> Numeric
//===--------------------------------------------------------------------===//
template <>
EXPORT_API bool TryCast::Operation(bool input, bool &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(bool input, int8_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(bool input, int16_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(bool input, int32_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(bool input, int64_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(bool input, hugeint_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(bool input, uint8_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(bool input, uint16_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(bool input, uint32_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(bool input, uint64_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(bool input, float &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(bool input, double &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(bool input, dec4_t &result, bool strict);

template <>
EXPORT_API bool TryCast::Operation(Trivalent input, bool &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(Trivalent input, int32_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(Trivalent input, uint32_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(Trivalent input, Trivalent &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(Trivalent input, int64_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(Trivalent input, uint64_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(Trivalent input, hugeint_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(Trivalent input, double &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(Trivalent input, dec4_t &result, bool strict);  // 如何指定格式?
template <>
EXPORT_API bool TryCast::Operation(Trivalent input, std::string &result, bool strict);

//===--------------------------------------------------------------------===//
// Cast int8_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
EXPORT_API bool TryCast::Operation(int8_t input, bool &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(int8_t input, int8_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(int8_t input, int16_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(int8_t input, int32_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(int8_t input, int64_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(int8_t input, hugeint_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(int8_t input, uint8_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(int8_t input, uint16_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(int8_t input, uint32_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(int8_t input, uint64_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(int8_t input, float &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(int8_t input, double &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(int8_t input, dec4_t &result, bool strict);

//===--------------------------------------------------------------------===//
// Cast int16_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
EXPORT_API bool TryCast::Operation(int16_t input, bool &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(int16_t input, int8_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(int16_t input, int16_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(int16_t input, int32_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(int16_t input, int64_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(int16_t input, hugeint_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(int16_t input, uint8_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(int16_t input, uint16_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(int16_t input, uint32_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(int16_t input, uint64_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(int16_t input, float &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(int16_t input, double &result, bool strict);

//===--------------------------------------------------------------------===//
// Cast int32_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
EXPORT_API bool TryCast::Operation(int32_t input, bool &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(int32_t input, int8_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(int32_t input, int16_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(int32_t input, int32_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(int32_t input, int64_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(int32_t input, hugeint_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(int32_t input, uint8_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(int32_t input, uint16_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(int32_t input, uint32_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(int32_t input, uint64_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(int32_t input, float &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(int32_t input, double &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(int32_t input, dec4_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(int32_t input, Trivalent &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(int32_t input, std::string &result, bool strict);

//===--------------------------------------------------------------------===//
// Cast int64_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
EXPORT_API bool TryCast::Operation(int64_t input, bool &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(int64_t input, int8_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(int64_t input, int16_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(int64_t input, int32_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(int64_t input, int64_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(int64_t input, hugeint_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(int64_t input, uint8_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(int64_t input, uint16_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(int64_t input, uint32_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(int64_t input, uint64_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(int64_t input, float &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(int64_t input, double &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(int64_t input, dec4_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(int64_t input, timestamp_stor_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(int64_t input, date_stor_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(int64_t input, std::string &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(int64_t input, Trivalent &result, bool strict);
//===--------------------------------------------------------------------===//
// Cast hugeint_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
EXPORT_API bool TryCast::Operation(hugeint_t input, bool &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(hugeint_t input, int8_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(hugeint_t input, int16_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(hugeint_t input, int32_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(hugeint_t input, int64_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(hugeint_t input, hugeint_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(hugeint_t input, uint8_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(hugeint_t input, uint16_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(hugeint_t input, uint32_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(hugeint_t input, uint64_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(hugeint_t input, float &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(hugeint_t input, double &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(hugeint_t input, dec4_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(hugeint_t input, timestamp_stor_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(hugeint_t input, std::string &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(hugeint_t input, Trivalent &result, bool strict);

//===--------------------------------------------------------------------===//
// Cast uint8_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
EXPORT_API bool TryCast::Operation(uint8_t input, bool &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(uint8_t input, int8_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(uint8_t input, int16_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(uint8_t input, int32_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(uint8_t input, int64_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(uint8_t input, hugeint_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(uint8_t input, uint8_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(uint8_t input, uint16_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(uint8_t input, uint32_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(uint8_t input, uint64_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(uint8_t input, float &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(uint8_t input, double &result, bool strict);

//===--------------------------------------------------------------------===//
// Cast uint16_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
EXPORT_API bool TryCast::Operation(uint16_t input, bool &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(uint16_t input, int8_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(uint16_t input, int16_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(uint16_t input, int32_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(uint16_t input, int64_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(uint16_t input, hugeint_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(uint16_t input, uint8_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(uint16_t input, uint16_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(uint16_t input, uint32_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(uint16_t input, uint64_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(uint16_t input, float &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(uint16_t input, double &result, bool strict);

//===--------------------------------------------------------------------===//
// Cast uint32_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
EXPORT_API bool TryCast::Operation(uint32_t input, bool &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(uint32_t input, int8_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(uint32_t input, int16_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(uint32_t input, int32_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(uint32_t input, int64_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(uint32_t input, hugeint_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(uint32_t input, uint8_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(uint32_t input, uint16_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(uint32_t input, uint32_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(uint32_t input, uint64_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(uint32_t input, float &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(uint32_t input, double &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(uint32_t input, dec4_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(uint32_t input, timestamp_stor_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(uint32_t input, std::string &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(uint32_t input, Trivalent &result, bool strict);

//===--------------------------------------------------------------------===//
// Cast uint64_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
EXPORT_API bool TryCast::Operation(uint64_t input, bool &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(uint64_t input, int8_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(uint64_t input, int16_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(uint64_t input, int32_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(uint64_t input, int64_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(uint64_t input, hugeint_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(uint64_t input, uint8_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(uint64_t input, uint16_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(uint64_t input, uint32_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(uint64_t input, uint64_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(uint64_t input, float &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(uint64_t input, double &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(uint64_t input, timestamp_stor_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(uint64_t input, std::string &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(uint64_t input, dec4_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(uint64_t input, Trivalent &result, bool strict);

//===--------------------------------------------------------------------===//
// Cast float -> Numeric
//===--------------------------------------------------------------------===//
template <>
EXPORT_API bool TryCast::Operation(float input, bool &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(float input, int8_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(float input, int16_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(float input, int32_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(float input, int64_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(float input, hugeint_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(float input, uint8_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(float input, uint16_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(float input, uint32_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(float input, uint64_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(float input, float &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(float input, double &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(float input, dec4_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(float input, Trivalent &result, bool strict);

//===--------------------------------------------------------------------===//
// Cast double -> Numeric
//===--------------------------------------------------------------------===//
template <>
EXPORT_API bool TryCast::Operation(double input, bool &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(double input, int8_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(double input, int16_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(double input, int32_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(double input, int64_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(double input, hugeint_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(double input, uint8_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(double input, uint16_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(double input, uint32_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(double input, uint64_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(double input, float &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(double input, double &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(double input, dec4_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(double input, timestamp_stor_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(double input, std::string &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(double input, Trivalent &result, bool strict);

//===--------------------------------------------------------------------===//
// String -> Numeric Casts
//===--------------------------------------------------------------------===//
template <>
EXPORT_API bool TryCast::Operation(std::string input, bool &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(std::string input, Trivalent &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(std::string input, int8_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(std::string input, int16_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(std::string input, int32_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(std::string input, int64_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(std::string input, uint8_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(std::string input, uint16_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(std::string input, uint32_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(std::string input, uint64_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(std::string input, hugeint_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(std::string input, float &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(std::string input, double &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(std::string input, std::string &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(std::string input, dec4_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(std::string input, timestamp_stor_t &result, bool strict);

template <>
EXPORT_API bool TryCast::Operation(timestamp_stor_t input, timestamp_stor_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(timestamp_stor_t input, int32_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(timestamp_stor_t input, int64_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(timestamp_stor_t input, uint32_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(timestamp_stor_t input, uint64_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(timestamp_stor_t input, hugeint_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(timestamp_stor_t input, std::string &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(timestamp_stor_t input, date_stor_t &result, bool strict);

template <>
EXPORT_API bool TryCast::Operation(std::string input, date_stor_t &result, bool strict);

// date cast
//
template <>
EXPORT_API bool TryCast::Operation(date_stor_t input, std::string &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(date_stor_t input, timestamp_stor_t &result, bool strict);
template <>
EXPORT_API bool TryCast::Operation(date_stor_t input, date_stor_t &result, bool strict);
