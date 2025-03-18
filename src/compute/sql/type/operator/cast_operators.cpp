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
#include "type/operator/cast_operators.h"

#include <fmt/chrono.h>
#include <fmt/format.h>
#include <string.h>

#include "cm_date.h"
#include "cm_dec4.h"
#include "type/hugeint.h"
#include "type/operator/fast_float.h"
#include "type/operator/numeric_cast.h"
#include "type/timestamp_t.h"
//===--------------------------------------------------------------------===//
// Cast bool -> Numeric
//===--------------------------------------------------------------------===//

template <>
bool TryCast::Operation(bool input, bool &result, bool strict) {
    return NumericTryCast::Operation<bool, bool>(input, result, strict);
}

template <>
bool TryCast::Operation(bool input, int8_t &result, bool strict) {
    return NumericTryCast::Operation<bool, int8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(bool input, int16_t &result, bool strict) {
    return NumericTryCast::Operation<bool, int16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(bool input, int32_t &result, bool strict) {
    return NumericTryCast::Operation<bool, int32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(bool input, int64_t &result, bool strict) {
    return NumericTryCast::Operation<bool, int64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(bool input, hugeint_t &result, bool strict) {
    return NumericTryCast::Operation<bool, hugeint_t>(input, result, strict);
}

template <>
bool TryCast::Operation(bool input, uint8_t &result, bool strict) {
    return NumericTryCast::Operation<bool, uint8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(bool input, uint16_t &result, bool strict) {
    return NumericTryCast::Operation<bool, uint16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(bool input, uint32_t &result, bool strict) {
    return NumericTryCast::Operation<bool, uint32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(bool input, uint64_t &result, bool strict) {
    return NumericTryCast::Operation<bool, uint64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(bool input, float &result, bool strict) {
    return NumericTryCast::Operation<bool, float>(input, result, strict);
}

template <>
bool TryCast::Operation(bool input, double &result, bool strict) {
    return NumericTryCast::Operation<bool, double>(input, result, strict);
}
template <>
bool TryCast::Operation(bool input, dec4_t &result, bool strict) {
    uint32_t r = input;
    cm_uint32_to_dec4(r, &result);
    return true;
}

template <>
bool TryCast::Operation(bool input, timestamp_stor_t &result, bool strict) {
    return NumericTryCast::Operation<bool, int64_t>(input, result.ts, strict);
}

template <>
bool TryCast::Operation(Trivalent input, bool &result, bool strict) {
    result = static_cast<bool>(input);
    return true;
}

template <>
bool TryCast::Operation(Trivalent input, int32_t &result, bool strict) {
    result = static_cast<int32_t>(input);
    return true;
}

template <>
bool TryCast::Operation(Trivalent input, uint32_t &result, bool strict) {
    result = static_cast<uint32_t>(input);
    return true;
}

template <>
bool TryCast::Operation(Trivalent input, Trivalent &result, bool strict) {
    result = input;
    return true;
}

template <>
bool TryCast::Operation(Trivalent input, int64_t &result, bool strict) {
    result = static_cast<int64_t>(input);
    return true;
}

template <>
bool TryCast::Operation(Trivalent input, uint64_t &result, bool strict) {
    result = static_cast<uint64_t>(input);
    return true;
}
template <>
bool TryCast::Operation(Trivalent input, hugeint_t &result, bool strict) {
    int32_t t = static_cast<int32_t>(input);
    return Hugeint::TryConvert(t, result);
}

template <>
bool TryCast::Operation(Trivalent input, double &result, bool strict) {
    result = static_cast<double>(input);
    return true;
}

template <>
bool TryCast::Operation(Trivalent input, dec4_t &result, bool strict) {
    uint32_t r = static_cast<uint32_t>(input);
    cm_uint32_to_dec4(r, &result);
    return true;
}

template <>
bool TryCast::Operation(Trivalent input, std::string &result, bool strict) {
    result = fmt::format("{}", input);
    return true;
}

//===--------------------------------------------------------------------===//
// Cast int8_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(int8_t input, bool &result, bool strict) {
    return NumericTryCast::Operation<int8_t, bool>(input, result, strict);
}

template <>
bool TryCast::Operation(int8_t input, int8_t &result, bool strict) {
    return NumericTryCast::Operation<int8_t, int8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int8_t input, int16_t &result, bool strict) {
    return NumericTryCast::Operation<int8_t, int16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int8_t input, int32_t &result, bool strict) {
    return NumericTryCast::Operation<int8_t, int32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int8_t input, int64_t &result, bool strict) {
    return NumericTryCast::Operation<int8_t, int64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int8_t input, hugeint_t &result, bool strict) {
    return NumericTryCast::Operation<int8_t, hugeint_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int8_t input, uint8_t &result, bool strict) {
    return NumericTryCast::Operation<int8_t, uint8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int8_t input, uint16_t &result, bool strict) {
    return NumericTryCast::Operation<int8_t, uint16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int8_t input, uint32_t &result, bool strict) {
    return NumericTryCast::Operation<int8_t, uint32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int8_t input, uint64_t &result, bool strict) {
    return NumericTryCast::Operation<int8_t, uint64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int8_t input, float &result, bool strict) {
    return NumericTryCast::Operation<int8_t, float>(input, result, strict);
}

template <>
bool TryCast::Operation(int8_t input, double &result, bool strict) {
    return NumericTryCast::Operation<int8_t, double>(input, result, strict);
}
template <>
bool TryCast::Operation(int8_t input, dec4_t &result, bool strict) {
    int32_t r = input;
    cm_int32_to_dec4(r, &result);
    return true;
}
//===--------------------------------------------------------------------===//
// Cast int16_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(int16_t input, bool &result, bool strict) {
    return NumericTryCast::Operation<int16_t, bool>(input, result, strict);
}

template <>
bool TryCast::Operation(int16_t input, int8_t &result, bool strict) {
    return NumericTryCast::Operation<int16_t, int8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int16_t input, int16_t &result, bool strict) {
    return NumericTryCast::Operation<int16_t, int16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int16_t input, int32_t &result, bool strict) {
    return NumericTryCast::Operation<int16_t, int32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int16_t input, int64_t &result, bool strict) {
    return NumericTryCast::Operation<int16_t, int64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int16_t input, hugeint_t &result, bool strict) {
    return NumericTryCast::Operation<int16_t, hugeint_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int16_t input, uint8_t &result, bool strict) {
    return NumericTryCast::Operation<int16_t, uint8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int16_t input, uint16_t &result, bool strict) {
    return NumericTryCast::Operation<int16_t, uint16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int16_t input, uint32_t &result, bool strict) {
    return NumericTryCast::Operation<int16_t, uint32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int16_t input, uint64_t &result, bool strict) {
    return NumericTryCast::Operation<int16_t, uint64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int16_t input, float &result, bool strict) {
    return NumericTryCast::Operation<int16_t, float>(input, result, strict);
}

template <>
bool TryCast::Operation(int16_t input, double &result, bool strict) {
    return NumericTryCast::Operation<int16_t, double>(input, result, strict);
}

//===--------------------------------------------------------------------===//
// Cast int32_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(int32_t input, bool &result, bool strict) {
    return NumericTryCast::Operation<int32_t, bool>(input, result, strict);
}

template <>
bool TryCast::Operation(int32_t input, int8_t &result, bool strict) {
    return NumericTryCast::Operation<int32_t, int8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int32_t input, int16_t &result, bool strict) {
    return NumericTryCast::Operation<int32_t, int16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int32_t input, int32_t &result, bool strict) {
    return NumericTryCast::Operation<int32_t, int32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int32_t input, int64_t &result, bool strict) {
    return NumericTryCast::Operation<int32_t, int64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int32_t input, hugeint_t &result, bool strict) {
    return NumericTryCast::Operation<int32_t, hugeint_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int32_t input, uint8_t &result, bool strict) {
    return NumericTryCast::Operation<int32_t, uint8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int32_t input, uint16_t &result, bool strict) {
    return NumericTryCast::Operation<int32_t, uint16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int32_t input, uint32_t &result, bool strict) {
    return NumericTryCast::Operation<int32_t, uint32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int32_t input, uint64_t &result, bool strict) {
    return NumericTryCast::Operation<int32_t, uint64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int32_t input, float &result, bool strict) {
    return NumericTryCast::Operation<int32_t, float>(input, result, strict);
}

template <>
bool TryCast::Operation(int32_t input, double &result, bool strict) {
    return NumericTryCast::Operation<int32_t, double>(input, result, strict);
}

template <>
bool TryCast::Operation(int32_t input, dec4_t &result, bool strict) {
    cm_int32_to_dec4(input, &result);
    return true;
}

template <>
bool TryCast::Operation(int32_t input, Trivalent &result, bool strict) {
    result = input == 0 ? Trivalent::TRI_FALSE : Trivalent::TRI_TRUE;
    return true;
}

template <>
bool TryCast::Operation(int32_t input, std::string &result, bool strict) {
    result = fmt::format("{}", input);
    return true;
}

//===--------------------------------------------------------------------===//
// Cast int64_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(int64_t input, bool &result, bool strict) {
    return NumericTryCast::Operation<int64_t, bool>(input, result, strict);
}

template <>
bool TryCast::Operation(int64_t input, int8_t &result, bool strict) {
    return NumericTryCast::Operation<int64_t, int8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int64_t input, int16_t &result, bool strict) {
    return NumericTryCast::Operation<int64_t, int16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int64_t input, int32_t &result, bool strict) {
    return NumericTryCast::Operation<int64_t, int32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int64_t input, int64_t &result, bool strict) {
    return NumericTryCast::Operation<int64_t, int64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int64_t input, hugeint_t &result, bool strict) {
    return NumericTryCast::Operation<int64_t, hugeint_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int64_t input, uint8_t &result, bool strict) {
    return NumericTryCast::Operation<int64_t, uint8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int64_t input, uint16_t &result, bool strict) {
    return NumericTryCast::Operation<int64_t, uint16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int64_t input, uint32_t &result, bool strict) {
    return NumericTryCast::Operation<int64_t, uint32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int64_t input, uint64_t &result, bool strict) {
    return NumericTryCast::Operation<int64_t, uint64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(int64_t input, float &result, bool strict) {
    return NumericTryCast::Operation<int64_t, float>(input, result, strict);
}

template <>
bool TryCast::Operation(int64_t input, double &result, bool strict) {
    return NumericTryCast::Operation<int64_t, double>(input, result, strict);
}

template <>
bool TryCast::Operation(int64_t input, dec4_t &result, bool strict) {
    cm_int64_to_dec4(input, &result);
    return true;
}

template <>
bool TryCast::Operation(int64_t input, timestamp_stor_t &result, bool strict) {
    // 默认输入的整数是 unix 时间戳，单位为微秒
    result.ts = input;
    return true;
}

template <>
bool TryCast::Operation(int64_t input, date_stor_t &result, bool strict) {
    bool r = TryCast::Operation<int64_t, timestamp_stor_t>(input, result.dates, strict);
    return r ? TryCast::Operation<timestamp_stor_t, date_stor_t>(result.dates, result, strict) : false;
}

template <>
bool TryCast::Operation(int64_t input, std::string &result, bool strict) {
    result = fmt::format("{}", input);
    return true;
}

template <>
bool TryCast::Operation(int64_t input, Trivalent &result, bool strict) {
    result = input == 0 ? Trivalent::TRI_FALSE : Trivalent::TRI_TRUE;
    return true;
}

//===--------------------------------------------------------------------===//
// Cast hugeint_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(hugeint_t input, bool &result, bool strict) {
    return NumericTryCast::Operation<hugeint_t, bool>(input, result, strict);
}

template <>
bool TryCast::Operation(hugeint_t input, int8_t &result, bool strict) {
    return NumericTryCast::Operation<hugeint_t, int8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(hugeint_t input, int16_t &result, bool strict) {
    return NumericTryCast::Operation<hugeint_t, int16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(hugeint_t input, int32_t &result, bool strict) {
    return NumericTryCast::Operation<hugeint_t, int32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(hugeint_t input, int64_t &result, bool strict) {
    return NumericTryCast::Operation<hugeint_t, int64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(hugeint_t input, hugeint_t &result, bool strict) {
    return NumericTryCast::Operation<hugeint_t, hugeint_t>(input, result, strict);
}

template <>
bool TryCast::Operation(hugeint_t input, uint8_t &result, bool strict) {
    return NumericTryCast::Operation<hugeint_t, uint8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(hugeint_t input, uint16_t &result, bool strict) {
    return NumericTryCast::Operation<hugeint_t, uint16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(hugeint_t input, uint32_t &result, bool strict) {
    return NumericTryCast::Operation<hugeint_t, uint32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(hugeint_t input, uint64_t &result, bool strict) {
    return NumericTryCast::Operation<hugeint_t, uint64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(hugeint_t input, float &result, bool strict) {
    return NumericTryCast::Operation<hugeint_t, float>(input, result, strict);
}

template <>
bool TryCast::Operation(hugeint_t input, double &result, bool strict) {
    return NumericTryCast::Operation<hugeint_t, double>(input, result, strict);
}

template <>
bool TryCast::Operation(hugeint_t input, dec4_t &result, bool strict) {
    std::string num_str = Hugeint::ToString(input);
    if (cm_str_to_dec4(num_str.c_str(), &result) != GS_SUCCESS) {
        return false;
    }
    return true;
}

template <>
bool TryCast::Operation(hugeint_t input, timestamp_stor_t &result, bool strict) {
    return NumericTryCast::Operation<hugeint_t, int64_t>(input, result.ts, strict);
}

template <>
bool TryCast::Operation(hugeint_t input, std::string &result, bool strict) {
    result = Hugeint::ToString(input);
    return true;
}

template <>
bool TryCast::Operation(hugeint_t input, Trivalent &result, bool strict) {
    result = Hugeint::Equals(input, 0) ? Trivalent::TRI_FALSE : Trivalent::TRI_TRUE;
    return true;
}

//===--------------------------------------------------------------------===//
// Cast uint8_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(uint8_t input, bool &result, bool strict) {
    return NumericTryCast::Operation<uint8_t, bool>(input, result, strict);
}

template <>
bool TryCast::Operation(uint8_t input, int8_t &result, bool strict) {
    return NumericTryCast::Operation<uint8_t, int8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint8_t input, int16_t &result, bool strict) {
    return NumericTryCast::Operation<uint8_t, int16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint8_t input, int32_t &result, bool strict) {
    return NumericTryCast::Operation<uint8_t, int32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint8_t input, int64_t &result, bool strict) {
    return NumericTryCast::Operation<uint8_t, int64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint8_t input, hugeint_t &result, bool strict) {
    return NumericTryCast::Operation<uint8_t, hugeint_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint8_t input, uint8_t &result, bool strict) {
    return NumericTryCast::Operation<uint8_t, uint8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint8_t input, uint16_t &result, bool strict) {
    return NumericTryCast::Operation<uint8_t, uint16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint8_t input, uint32_t &result, bool strict) {
    return NumericTryCast::Operation<uint8_t, uint32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint8_t input, uint64_t &result, bool strict) {
    return NumericTryCast::Operation<uint8_t, uint64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint8_t input, float &result, bool strict) {
    return NumericTryCast::Operation<uint8_t, float>(input, result, strict);
}

template <>
bool TryCast::Operation(uint8_t input, double &result, bool strict) {
    return NumericTryCast::Operation<uint8_t, double>(input, result, strict);
}

//===--------------------------------------------------------------------===//
// Cast uint16_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(uint16_t input, bool &result, bool strict) {
    return NumericTryCast::Operation<uint16_t, bool>(input, result, strict);
}

template <>
bool TryCast::Operation(uint16_t input, int8_t &result, bool strict) {
    return NumericTryCast::Operation<uint16_t, int8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint16_t input, int16_t &result, bool strict) {
    return NumericTryCast::Operation<uint16_t, int16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint16_t input, int32_t &result, bool strict) {
    return NumericTryCast::Operation<uint16_t, int32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint16_t input, int64_t &result, bool strict) {
    return NumericTryCast::Operation<uint16_t, int64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint16_t input, hugeint_t &result, bool strict) {
    return NumericTryCast::Operation<uint16_t, hugeint_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint16_t input, uint8_t &result, bool strict) {
    return NumericTryCast::Operation<uint16_t, uint8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint16_t input, uint16_t &result, bool strict) {
    return NumericTryCast::Operation<uint16_t, uint16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint16_t input, uint32_t &result, bool strict) {
    return NumericTryCast::Operation<uint16_t, uint32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint16_t input, uint64_t &result, bool strict) {
    return NumericTryCast::Operation<uint16_t, uint64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint16_t input, float &result, bool strict) {
    return NumericTryCast::Operation<uint16_t, float>(input, result, strict);
}

template <>
bool TryCast::Operation(uint16_t input, double &result, bool strict) {
    return NumericTryCast::Operation<uint16_t, double>(input, result, strict);
}

//===--------------------------------------------------------------------===//
// Cast uint32_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(uint32_t input, bool &result, bool strict) {
    return NumericTryCast::Operation<uint32_t, bool>(input, result, strict);
}

template <>
bool TryCast::Operation(uint32_t input, int8_t &result, bool strict) {
    return NumericTryCast::Operation<uint32_t, int8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint32_t input, int16_t &result, bool strict) {
    return NumericTryCast::Operation<uint32_t, int16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint32_t input, int32_t &result, bool strict) {
    return NumericTryCast::Operation<uint32_t, int32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint32_t input, int64_t &result, bool strict) {
    return NumericTryCast::Operation<uint32_t, int64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint32_t input, hugeint_t &result, bool strict) {
    return NumericTryCast::Operation<uint32_t, hugeint_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint32_t input, uint8_t &result, bool strict) {
    return NumericTryCast::Operation<uint32_t, uint8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint32_t input, uint16_t &result, bool strict) {
    return NumericTryCast::Operation<uint32_t, uint16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint32_t input, uint32_t &result, bool strict) {
    return NumericTryCast::Operation<uint32_t, uint32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint32_t input, uint64_t &result, bool strict) {
    return NumericTryCast::Operation<uint32_t, uint64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint32_t input, float &result, bool strict) {
    return NumericTryCast::Operation<uint32_t, float>(input, result, strict);
}

template <>
bool TryCast::Operation(uint32_t input, double &result, bool strict) {
    return NumericTryCast::Operation<uint32_t, double>(input, result, strict);
}

template <>
bool TryCast::Operation(uint32_t input, dec4_t &result, bool strict) {
    cm_uint32_to_dec4(input, &result);
    return true;
}
template <>
bool TryCast::Operation(uint32_t input, timestamp_stor_t &result, bool strict) {
    return NumericTryCast::Operation<uint32_t, int64_t>(input, result.ts, strict);
}

template <>
bool TryCast::Operation(uint32_t input, std::string &result, bool strict) {
    result = fmt::format("{}", input);
    return true;
}

template <>
bool TryCast::Operation(uint32_t input, Trivalent &result, bool strict) {
    result = input == 0 ? Trivalent::TRI_FALSE : Trivalent::TRI_TRUE;
    return true;
}

//===--------------------------------------------------------------------===//
// Cast uint64_t -> Numeric
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(uint64_t input, bool &result, bool strict) {
    return NumericTryCast::Operation<uint64_t, bool>(input, result, strict);
}

template <>
bool TryCast::Operation(uint64_t input, int8_t &result, bool strict) {
    return NumericTryCast::Operation<uint64_t, int8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint64_t input, int16_t &result, bool strict) {
    return NumericTryCast::Operation<uint64_t, int16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint64_t input, int32_t &result, bool strict) {
    return NumericTryCast::Operation<uint64_t, int32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint64_t input, int64_t &result, bool strict) {
    return NumericTryCast::Operation<uint64_t, int64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint64_t input, hugeint_t &result, bool strict) {
    return NumericTryCast::Operation<uint64_t, hugeint_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint64_t input, uint8_t &result, bool strict) {
    return NumericTryCast::Operation<uint64_t, uint8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint64_t input, uint16_t &result, bool strict) {
    return NumericTryCast::Operation<uint64_t, uint16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint64_t input, uint32_t &result, bool strict) {
    return NumericTryCast::Operation<uint64_t, uint32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint64_t input, uint64_t &result, bool strict) {
    return NumericTryCast::Operation<uint64_t, uint64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(uint64_t input, float &result, bool strict) {
    return NumericTryCast::Operation<uint64_t, float>(input, result, strict);
}

template <>
bool TryCast::Operation(uint64_t input, double &result, bool strict) {
    return NumericTryCast::Operation<uint64_t, double>(input, result, strict);
}

template <>
bool TryCast::Operation(uint64_t input, timestamp_stor_t &result, bool strict) {
    return NumericTryCast::Operation<uint64_t, int64_t>(input, result.ts, strict);
}

template <>
bool TryCast::Operation(uint64_t input, std::string &result, bool strict) {
    result = fmt::format("{}", input);
    return true;
}

template <>
bool TryCast::Operation(uint64_t input, dec4_t &result, bool strict) {
    std::string num_str = std::to_string(input);
    if (cm_str_to_dec4(num_str.c_str(), &result) != GS_SUCCESS) {
        return false;
    }
    return true;
}

template <>
bool TryCast::Operation(uint64_t input, Trivalent &result, bool strict) {
    result = input == 0 ? Trivalent::TRI_FALSE : Trivalent::TRI_TRUE;
    return true;
}

//===--------------------------------------------------------------------===//
// Cast float -> Numeric
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(float input, bool &result, bool strict) {
    return NumericTryCast::Operation<float, bool>(input, result, strict);
}

template <>
bool TryCast::Operation(float input, int8_t &result, bool strict) {
    return NumericTryCast::Operation<float, int8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(float input, int16_t &result, bool strict) {
    return NumericTryCast::Operation<float, int16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(float input, int32_t &result, bool strict) {
    return NumericTryCast::Operation<float, int32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(float input, int64_t &result, bool strict) {
    return NumericTryCast::Operation<float, int64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(float input, hugeint_t &result, bool strict) {
    return NumericTryCast::Operation<float, hugeint_t>(input, result, strict);
}

template <>
bool TryCast::Operation(float input, uint8_t &result, bool strict) {
    return NumericTryCast::Operation<float, uint8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(float input, uint16_t &result, bool strict) {
    return NumericTryCast::Operation<float, uint16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(float input, uint32_t &result, bool strict) {
    return NumericTryCast::Operation<float, uint32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(float input, uint64_t &result, bool strict) {
    return NumericTryCast::Operation<float, uint64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(float input, float &result, bool strict) {
    return NumericTryCast::Operation<float, float>(input, result, strict);
}

template <>
bool TryCast::Operation(float input, double &result, bool strict) {
    return NumericTryCast::Operation<float, double>(input, result, strict);
}

template <>
bool TryCast::Operation(float input, dec4_t &result, bool strict) {
    auto s = cm_real_to_dec4(input, &result);
    if (s != GS_SUCCESS) {
        return false;
    }
    return true;
}

template <>
bool TryCast::Operation(float input, Trivalent &result, bool strict) {
    result = input == 0 ? Trivalent::TRI_FALSE : Trivalent::TRI_TRUE;
    return true;
}

//===--------------------------------------------------------------------===//
// Cast double -> Numeric
//===--------------------------------------------------------------------===//
template <>
bool TryCast::Operation(double input, bool &result, bool strict) {
    return NumericTryCast::Operation<double, bool>(input, result, strict);
}

template <>
bool TryCast::Operation(double input, int8_t &result, bool strict) {
    return NumericTryCast::Operation<double, int8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(double input, int16_t &result, bool strict) {
    return NumericTryCast::Operation<double, int16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(double input, int32_t &result, bool strict) {
    return NumericTryCast::Operation<double, int32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(double input, int64_t &result, bool strict) {
    return NumericTryCast::Operation<double, int64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(double input, hugeint_t &result, bool strict) {
    return NumericTryCast::Operation<double, hugeint_t>(input, result, strict);
}

template <>
bool TryCast::Operation(double input, uint8_t &result, bool strict) {
    return NumericTryCast::Operation<double, uint8_t>(input, result, strict);
}

template <>
bool TryCast::Operation(double input, uint16_t &result, bool strict) {
    return NumericTryCast::Operation<double, uint16_t>(input, result, strict);
}

template <>
bool TryCast::Operation(double input, uint32_t &result, bool strict) {
    return NumericTryCast::Operation<double, uint32_t>(input, result, strict);
}

template <>
bool TryCast::Operation(double input, uint64_t &result, bool strict) {
    return NumericTryCast::Operation<double, uint64_t>(input, result, strict);
}

template <>
bool TryCast::Operation(double input, float &result, bool strict) {
    return NumericTryCast::Operation<double, float>(input, result, strict);
}

template <>
bool TryCast::Operation(double input, double &result, bool strict) {
    return NumericTryCast::Operation<double, double>(input, result, strict);
}

template <>
bool TryCast::Operation(double input, dec4_t &result, bool strict) {
    auto s = cm_real_to_dec4(input, &result);
    if (s != GS_SUCCESS) {
        return false;
    }
    return true;
}

template <>
bool TryCast::Operation(double input, timestamp_stor_t &result, bool strict) {
    return NumericTryCast::Operation<double, int64_t>(input, result.ts, strict);
}

template <>
bool TryCast::Operation(double input, std::string &result, bool strict) {
    result = fmt::format("{}", input);
    if (result == "inf" || result == "-inf") {
        result += "inity";
        if (result[0] == '-') {
            result[1] = 'I';
        } else {
            result[0] = 'I';
        }
        return true;
    }
    if ((input > 0 && input < 0.0001) || (input < 0 && input > -0.0001)) {
        // 绝对值小于0.000000000000001的实数，按默认处理，会按科学计数法显示；  否则按实际显示
        if (!((input > 0 && input < 0.000000000000001) || (input < 0 && input > -0.000000000000001))) {
            result = fmt::format("{:.15f}", input);
            auto len = result.length();
            while (result[len - 1] == '0') {
                len--;
            }
            result = result.substr(0, len);
        }
    }
    return true;
}

template <>
bool TryCast::Operation(double input, Trivalent &result, bool strict) {
    result = input == 0 ? Trivalent::TRI_FALSE : Trivalent::TRI_TRUE;
    return true;
}

//===--------------------------------------------------------------------===//
// Cast String -> Numeric
//===--------------------------------------------------------------------===//
template <typename T>
struct IntegerCastData {
    using Result = T;
    Result result;
    bool seen_decimal;
};

struct IntegerCastOperation {
    template <class T, bool NEGATIVE>
    static bool HandleDigit(T &state, uint8_t digit) {
        using result_t = typename T::Result;
        if (NEGATIVE) {
            if (state.result < (NumericLimits<result_t>::Minimum() + digit) / 10) {
                return false;
            }
            state.result = state.result * 10 - digit;
        } else {
            if (state.result > (NumericLimits<result_t>::Maximum() - digit) / 10) {
                return false;
            }
            state.result = state.result * 10 + digit;
        }
        return true;
    }

    template <class T, bool NEGATIVE>
    static bool HandleHexDigit(T &state, uint8_t digit) {
        using result_t = typename T::Result;
        if (state.result > (NumericLimits<result_t>::Maximum() - digit) / 16) {
            return false;
        }
        state.result = state.result * 16 + digit;
        return true;
    }

    template <class T, bool NEGATIVE>
    static bool HandleBinaryDigit(T &state, uint8_t digit) {
        using result_t = typename T::Result;
        if (state.result > (NumericLimits<result_t>::Maximum() - digit) / 2) {
            return false;
        }
        state.result = state.result * 2 + digit;
        return true;
    }

    template <class T, bool NEGATIVE>
    static bool HandleExponent(T &state, int32_t exponent) {
        using result_t = typename T::Result;
        double dbl_res = state.result * std::pow(10.0L, exponent);
        if (dbl_res < (double)NumericLimits<result_t>::Minimum() ||
            dbl_res > (double)NumericLimits<result_t>::Maximum()) {
            return false;
        }
        state.result = (result_t)std::nearbyint(dbl_res);
        return true;
    }

    template <class T, bool NEGATIVE, bool ALLOW_EXPONENT>
    static bool HandleDecimal(T &state, uint8_t digit) {
        if (state.seen_decimal) {
            return true;
        }
        state.seen_decimal = true;
        // round the integer based on what is after the decimal point
        // if digit >= 5, then we round up (or down in case of negative numbers)
        auto increment = digit >= 5;
        if (!increment) {
            return true;
        }
        if (NEGATIVE) {
            if (state.result == NumericLimits<typename T::Result>::Minimum()) {
                return false;
            }
            state.result--;
        } else {
            if (state.result == NumericLimits<typename T::Result>::Maximum()) {
                return false;
            }
            state.result++;
        }
        return true;
    }

    template <class T, bool NEGATIVE>
    static bool Finalize(T &state) {
        return true;
    }
};

template <class T, bool NEGATIVE, bool ALLOW_EXPONENT, class OP = IntegerCastOperation, char decimal_separator = '.'>
static bool IntegerCastLoop(const char *buf, size_t len, T &result, bool strict) {
    size_t start_pos;
    if (NEGATIVE) {
        start_pos = 1;
    } else {
        if (*buf == '+') {
            if (strict) {
                // leading plus is not allowed in strict mode
                return false;
            }
            start_pos = 1;
        } else {
            start_pos = 0;
        }
    }
    size_t pos = start_pos;
    while (pos < len) {
        if (!std::isdigit(buf[pos])) {
            // not a digit!
            if (buf[pos] == decimal_separator) {
                if (strict) {
                    return false;
                }
                bool number_before_period = pos > start_pos;
                // decimal point: we accept decimal values for integers as well
                // we just truncate them
                // make sure everything after the period is a number
                pos++;
                size_t start_digit = pos;
                while (pos < len) {
                    if (!std::isdigit(buf[pos])) {
                        break;
                    }
                    if (!OP::template HandleDecimal<T, NEGATIVE, ALLOW_EXPONENT>(result, buf[pos] - '0')) {
                        return false;
                    }
                    pos++;
                }
                // make sure there is either (1) one number after the period, or (2) one number before the period
                // i.e. we accept "1." and ".1" as valid numbers, but not "."
                if (!(number_before_period || pos > start_digit)) {
                    return false;
                }
                if (pos >= len) {
                    break;
                }
            }
            if (std::isspace(buf[pos])) {
                // skip any trailing spaces
                while (++pos < len) {
                    if (!std::isspace(buf[pos])) {
                        return false;
                    }
                }
                break;
            }
            if (ALLOW_EXPONENT) {
                if (buf[pos] == 'e' || buf[pos] == 'E') {
                    if (pos == start_pos) {
                        return false;
                    }
                    pos++;
                    if (pos >= len) {
                        return false;
                    }
                    using ExponentData = IntegerCastData<int32_t>;
                    ExponentData exponent{0, false};
                    int negative = buf[pos] == '-';
                    if (negative) {
                        if (!IntegerCastLoop<ExponentData, true, false, IntegerCastOperation, decimal_separator>(
                                buf + pos, len - pos, exponent, strict)) {
                            return false;
                        }
                    } else {
                        if (!IntegerCastLoop<ExponentData, false, false, IntegerCastOperation, decimal_separator>(
                                buf + pos, len - pos, exponent, strict)) {
                            return false;
                        }
                    }
                    return OP::template HandleExponent<T, NEGATIVE>(result, exponent.result);
                }
            }
            return false;
        }
        uint8_t digit = buf[pos++] - '0';
        if (!OP::template HandleDigit<T, NEGATIVE>(result, digit)) {
            return false;
        }
    }
    if (!OP::template Finalize<T, NEGATIVE>(result)) {
        return false;
    }
    return pos > start_pos;
}

template <class T, bool NEGATIVE, bool ALLOW_EXPONENT, class OP = IntegerCastOperation>
static bool IntegerHexCastLoop(const char *buf, size_t len, T &result, bool strict) {
    if (ALLOW_EXPONENT || NEGATIVE) {
        return false;
    }
    size_t start_pos = 1;
    size_t pos = start_pos;
    while (pos < len) {
        char current_char = std::tolower(buf[pos]);
        if (!std::isxdigit(current_char)) {
            return false;
        }
        uint8_t digit;
        if (current_char >= 'a') {
            digit = current_char - 'a' + 10;
        } else {
            digit = current_char - '0';
        }
        pos++;
        if (!OP::template HandleHexDigit<T, NEGATIVE>(result, digit)) {
            return false;
        }
    }
    if (!OP::template Finalize<T, NEGATIVE>(result)) {
        return false;
    }
    return pos > start_pos;
}

template <class T, bool NEGATIVE, bool ALLOW_EXPONENT, class OP = IntegerCastOperation>
static bool IntegerBinaryCastLoop(const char *buf, size_t len, T &result, bool strict) {
    if (ALLOW_EXPONENT || NEGATIVE) {
        return false;
    }
    size_t start_pos = 1;
    size_t pos = start_pos;
    uint8_t digit;
    while (pos < len) {
        char current_char = buf[pos];
        if (current_char == '_' && pos > start_pos) {
            // skip underscore, if it is not the first character
            pos++;
            if (pos == len) {
                // we cant end on an underscore either
                return false;
            }
            continue;
        } else if (current_char == '0') {
            digit = 0;
        } else if (current_char == '1') {
            digit = 1;
        } else {
            return false;
        }
        pos++;
        if (!OP::template HandleBinaryDigit<T, NEGATIVE>(result, digit)) {
            return false;
        }
    }
    if (!OP::template Finalize<T, NEGATIVE>(result)) {
        return false;
    }
    return pos > start_pos;
}

template <class T, bool IS_SIGNED = true, bool ALLOW_EXPONENT = true, class OP = IntegerCastOperation,
          bool ZERO_INITIALIZE = true, char decimal_separator = '.'>
static bool TryIntegerCast(const char *buf, size_t len, T &result, bool strict) {
    // skip any spaces at the start
    while (len > 0 && std::isspace(*buf)) {
        buf++;
        len--;
    }
    if (len == 0) {
        return false;
    }
    if (ZERO_INITIALIZE) {
        memset(&result, 0, sizeof(T));
    }
    // if the number is negative, we set the negative flag and skip the negative sign
    if (*buf == '-') {
        if (!IS_SIGNED) {
            // Need to check if its not -0
            size_t pos = 1;
            while (pos < len) {
                if (buf[pos++] != '0') {
                    return false;
                }
            }
        }
        return IntegerCastLoop<T, true, ALLOW_EXPONENT, OP, decimal_separator>(buf, len, result, strict);
    }
    if (len > 1 && *buf == '0') {
        if (buf[1] == 'x' || buf[1] == 'X') {
            // If it starts with 0x or 0X, we parse it as a hex value
            buf++;
            len--;
            return IntegerHexCastLoop<T, false, false, OP>(buf, len, result, strict);
        } else if (buf[1] == 'b' || buf[1] == 'B') {
            // If it starts with 0b or 0B, we parse it as a binary value
            buf++;
            len--;
            return IntegerBinaryCastLoop<T, false, false, OP>(buf, len, result, strict);
        } else if (strict && std::isdigit(buf[1])) {
            // leading zeros are not allowed in strict mode
            return false;
        }
    }
    return IntegerCastLoop<T, false, ALLOW_EXPONENT, OP, decimal_separator>(buf, len, result, strict);
}

template <typename T, bool IS_SIGNED = true>
static inline bool TrySimpleIntegerCast(const char *buf, size_t len, T &result, bool strict) {
    IntegerCastData<T> data;
    if (TryIntegerCast<IntegerCastData<T>, IS_SIGNED>(buf, len, data, strict)) {
        result = data.result;
        return true;
    }
    return false;
}

template <>
bool TryCast::Operation(std::string input, bool &result, bool strict) {
    auto input_data = input.data();
    auto input_size = input.size();

    // checkt input is all digits 
    int64_t r = 0;
    if(TryCast::Operation(input , r , strict)){
        result = r != 0;
        return true;
    }
    
    switch (input_size) {
        case 1: {
            char c = std::tolower(*input_data);
            if (c == 't' || (!strict && c == '1')) {
                result = true;
                return true;
            } else if (c == 'f' || (!strict && c == '0')) {
                result = false;
                return true;
            }
            return false;
        }
        case 4: {
            char t = std::tolower(input_data[0]);
            char r = std::tolower(input_data[1]);
            char u = std::tolower(input_data[2]);
            char e = std::tolower(input_data[3]);
            if (t == 't' && r == 'r' && u == 'u' && e == 'e') {
                result = true;
                return true;
            }
            return false;
        }
        case 5: {
            char f = std::tolower(input_data[0]);
            char a = std::tolower(input_data[1]);
            char l = std::tolower(input_data[2]);
            char s = std::tolower(input_data[3]);
            char e = std::tolower(input_data[4]);
            if (f == 'f' && a == 'a' && l == 'l' && s == 's' && e == 'e') {
                result = false;
                return true;
            }
            return false;
        }
        default:
            return false;
    }
}

template <>
bool TryCast::Operation(std::string input, int8_t &result, bool strict) {
    return TrySimpleIntegerCast<int8_t>(input.data(), input.size(), result, strict);
}
template <>
bool TryCast::Operation(std::string input, int16_t &result, bool strict) {
    return TrySimpleIntegerCast<int16_t>(input.data(), input.size(), result, strict);
}
template <>
bool TryCast::Operation(std::string input, int32_t &result, bool strict) {
    return TrySimpleIntegerCast<int32_t>(input.data(), input.size(), result, strict);
}
template <>
bool TryCast::Operation(std::string input, int64_t &result, bool strict) {
    return TrySimpleIntegerCast<int64_t>(input.data(), input.size(), result, strict);
}

template <>
bool TryCast::Operation(std::string input, Trivalent &result, bool strict) {
    bool bool_result;
    auto ret = TryCast::Operation<std::string, bool>(input, bool_result, strict);
    if (ret) {
        result = bool_result ? Trivalent::TRI_TRUE : Trivalent::TRI_FALSE;
        return true;
    }
    int64_t r = 0;
    ret = TrySimpleIntegerCast<int64_t>(input.data(), input.size(), r, true);
    result = ret && r != 0 ? Trivalent::TRI_TRUE : Trivalent::TRI_FALSE;
    return ret;
}

template <>
bool TryCast::Operation(std::string input, uint8_t &result, bool strict) {
    return TrySimpleIntegerCast<uint8_t, false>(input.data(), input.size(), result, strict);
}
template <>
bool TryCast::Operation(std::string input, uint16_t &result, bool strict) {
    return TrySimpleIntegerCast<uint16_t, false>(input.data(), input.size(), result, strict);
}
template <>
bool TryCast::Operation(std::string input, uint32_t &result, bool strict) {
    return TrySimpleIntegerCast<uint32_t, false>(input.data(), input.size(), result, strict);
}
template <>
bool TryCast::Operation(std::string input, uint64_t &result, bool strict) {
    return TrySimpleIntegerCast<uint64_t, false>(input.data(), input.size(), result, strict);
}

template <class T, char decimal_separator = '.'>
static bool TryDoubleCast(const char *buf, size_t len, T &result, bool strict) {
    // skip any spaces at the start
    while (len > 0 && std::isspace(*buf)) {
        buf++;
        len--;
    }
    if (len == 0) {
        return false;
    }
    if (*buf == '+') {
        if (strict) {
            // plus is not allowed in strict mode
            return false;
        }
        buf++;
        len--;
    }
    if (strict && len >= 2) {
        if (buf[0] == '0' && std::isdigit(buf[1])) {
            // leading zeros are not allowed in strict mode
            return false;
        }
    }
    auto endptr = buf + len;
    // use duckdb fast_float
    auto parse_result = duckdb_fast_float::from_chars(buf, buf + len, result, decimal_separator);
    if (parse_result.ec != std::errc()) {
        return false;
    }
    auto current_end = parse_result.ptr;
    if (!strict) {
        while (current_end < endptr && std::isspace(*current_end)) {
            current_end++;
        }
    }
    return current_end == endptr;
}

template <>
bool TryCast::Operation(std::string input, float &result, bool strict) {
    return TryDoubleCast<float>(input.data(), input.size(), result, strict);
}

template <>
bool TryCast::Operation(std::string input, double &result, bool strict) {
    return TryDoubleCast<double>(input.data(), input.size(), result, strict);
}
template <>
bool TryCast::Operation(std::string input, std::string &result, bool strict) {
    result = input;
    return true;
}

template <>
bool TryCast::Operation(std::string input, dec4_t &result, bool strict) {
    auto r = cm_str_to_dec4(input.c_str(), &result);
    if (r != GS_SUCCESS) {
        return false;
    }
    return true;
}

template <>
bool TryCast::Operation(std::string input, timestamp_stor_t &result, bool strict) {
    bool offset = false;
    std::string tz = "";
    auto res = TimeUtils::TryConvertTimestampTZ(input.c_str(), input.length(), result, offset, tz);
    if (!res) {
        return false;
    }
    if (result == DateUtils::Infinity().dates || result == DateUtils::Ninfinity().dates) {
        return true;
    }

    if (tz.size() == 0) {
        result.ts -= TIMEZONE_GET_MICROSECOND(cm_get_db_timezone());
        return true;
    }
    if (tz.size() == 3) {
        if ((tz[0] == 'U' || tz[0] == 'u') && (tz[1] == 'T' || tz[1] == 't') && (tz[2] == 'C' || tz[2] == 'c')) {
            return true;
        }
    }
    // 不支持非UTC时区名
    return false;
}

template <>
bool TryCast::Operation(std::string input, date_stor_t &result, bool strict) {
    bool special = false;
    size_t pos = 0;
    auto res = DateUtils::TryConvertDate(input.c_str(), input.length(), pos, result, special, false);
    if (res && (result == DateUtils::Infinity() || result == DateUtils::Ninfinity())) {
        return true;
    }
    if (!res) {
        return false;
    }
    return true;
}

//===--------------------------------------------------------------------===//
// Cast From Hugeint
//===--------------------------------------------------------------------===//
// parsing hugeint from string is done a bit differently for performance reasons
// for other integer types we keep track of a single value
// and multiply that value by 10 for every digit we read
// however, for hugeints, multiplication is very expensive (>20X as expensive as for int64)
// for that reason, we parse numbers first into an int64 value
// when that value is full, we perform a HUGEINT multiplication to flush it into the hugeint
// this takes the number of HUGEINT multiplications down from [0-38] to [0-2]
struct HugeIntCastData {
    hugeint_t hugeint;
    int64_t intermediate;
    uint8_t digits;
    bool decimal;

    bool Flush() {
        if (digits == 0 && intermediate == 0) {
            return true;
        }
        if (hugeint.lower != 0 || hugeint.upper != 0) {
            if (digits > 38) {
                return false;
            }
            if (!Hugeint::TryMultiply(hugeint, Hugeint::POWERS_OF_TEN[digits], hugeint)) {
                return false;
            }
        }
        if (!Hugeint::AddInPlace(hugeint, hugeint_t(intermediate))) {
            return false;
        }
        digits = 0;
        intermediate = 0;
        return true;
    }
};

struct HugeIntegerCastOperation {
    template <class T, bool NEGATIVE>
    static bool HandleDigit(T &result, uint8_t digit) {
        if (NEGATIVE) {
            if (result.intermediate < (NumericLimits<int64_t>::Minimum() + digit) / 10) {
                // intermediate is full: need to flush it
                if (!result.Flush()) {
                    return false;
                }
            }
            result.intermediate = result.intermediate * 10 - digit;
        } else {
            if (result.intermediate > (NumericLimits<int64_t>::Maximum() - digit) / 10) {
                if (!result.Flush()) {
                    return false;
                }
            }
            result.intermediate = result.intermediate * 10 + digit;
        }
        result.digits++;
        return true;
    }

    template <class T, bool NEGATIVE>
    static bool HandleHexDigit(T &result, uint8_t digit) {
        return false;
    }

    template <class T, bool NEGATIVE>
    static bool HandleBinaryDigit(T &result, uint8_t digit) {
        if (result.intermediate > (NumericLimits<int64_t>::Maximum() - digit) / 2) {
            // intermediate is full: need to flush it
            if (!result.Flush()) {
                return false;
            }
        }
        result.intermediate = result.intermediate * 2 + digit;
        result.digits++;
        return true;
    }

    template <class T, bool NEGATIVE>
    static bool HandleExponent(T &result, int32_t exponent) {
        if (!result.Flush()) {
            return false;
        }
        if (exponent < -38 || exponent > 38) {
            // out of range for exact exponent: use double and convert
            double dbl_res = Hugeint::Cast<double>(result.hugeint) * std::pow(10.0L, exponent);
            if (dbl_res < Hugeint::Cast<double>(NumericLimits<hugeint_t>::Minimum()) ||
                dbl_res > Hugeint::Cast<double>(NumericLimits<hugeint_t>::Maximum())) {
                return false;
            }
            result.hugeint = Hugeint::Convert(dbl_res);
            return true;
        }
        if (exponent < 0) {
            // negative exponent: divide by power of 10
            result.hugeint = Hugeint::Divide(result.hugeint, Hugeint::POWERS_OF_TEN[-exponent]);
            return true;
        } else {
            // positive exponent: multiply by power of 10
            return Hugeint::TryMultiply(result.hugeint, Hugeint::POWERS_OF_TEN[exponent], result.hugeint);
        }
    }

    template <class T, bool NEGATIVE, bool ALLOW_EXPONENT>
    static bool HandleDecimal(T &result, uint8_t digit) {
        // Integer casts round
        if (!result.decimal) {
            if (!result.Flush()) {
                return false;
            }
            if (NEGATIVE) {
                result.intermediate = -(digit >= 5);
            } else {
                result.intermediate = (digit >= 5);
            }
        }
        result.decimal = true;

        return true;
    }

    template <class T, bool NEGATIVE>
    static bool Finalize(T &result) {
        return result.Flush();
    }
};

// string to hugeint_t
template <>
bool TryCast::Operation(std::string input, hugeint_t &result, bool strict) {
    HugeIntCastData data;
    if (!TryIntegerCast<HugeIntCastData, true, true, HugeIntegerCastOperation>(input.data(), input.size(), data,
                                                                               strict)) {
        return false;
    }
    result = data.hugeint;
    return true;
}

template <>
bool TryCast::Operation(timestamp_stor_t input, timestamp_stor_t &result, bool strict) {
    result = input;
    return true;
}

template <>
bool TryCast::Operation(timestamp_stor_t input, int32_t &result, bool strict) {
    return NumericTryCast::Operation<int64_t, int32_t>(input.ts, result, strict);
}

template <>
bool TryCast::Operation(timestamp_stor_t input, int64_t &result, bool strict) {
    result = input.ts;
    return true;
}
template <>
bool TryCast::Operation(timestamp_stor_t input, uint32_t &result, bool strict) {
    return NumericTryCast::Operation<int64_t, uint32_t>(input.ts, result, strict);
}
template <>
bool TryCast::Operation(timestamp_stor_t input, uint64_t &result, bool strict) {
    return NumericTryCast::Operation<int64_t, uint64_t>(input.ts, result, strict);
}
template <>
bool TryCast::Operation(timestamp_stor_t input, hugeint_t &result, bool strict) {
    return Hugeint::TryConvert(input.ts, result);
}

template <>
bool TryCast::Operation(timestamp_stor_t input, std::string &result, bool strict) {
    if (input == TimeUtils::Infinity()) {
        result = DateUtils::PINF;
        return true;
    }
    if (input == TimeUtils::Ninfinity()) {
        result = DateUtils::NINF;
        return true;
    }
    date_detail_t detail{0};
    input.ts += TIMEZONE_GET_MICROSECOND(cm_get_db_timezone());
    input.ts += CM_UNIX_EPOCH;
    cm_decode_date(static_cast<date_t>(input.ts), &detail);
    char buf[37] = {'\0'};  // 37为理论上的最大长度 ， 一般30就够了
    sprintf(buf, "%04d-%02d-%02d %02d:%02d:%02d.%03d%03d", detail.year, detail.mon, detail.day, detail.hour, detail.min,
            detail.sec, detail.millisec, detail.microsec);
    result = std::string(buf);
    return true;
}

template <>
bool TryCast::Operation(timestamp_stor_t input, date_stor_t &result, bool strict) {
    if (input == TimeUtils::Infinity()) {
        result = DateUtils::Infinity();
        return true;
    }
    if (input == TimeUtils::Ninfinity()) {
        result = DateUtils::Ninfinity();
        return true;
    }
    // Date 是有时区的，Timestamp 是无时区的
    date_detail_t detail{0};
    input.ts += TIMEZONE_GET_MICROSECOND(cm_get_db_timezone());
    input.ts += CM_UNIX_EPOCH;
    cm_decode_date(static_cast<date_t>(input.ts), &detail);
    date_detail_t day_detail{0};
    day_detail.year = detail.year;
    day_detail.mon = detail.mon;
    day_detail.day = detail.day;
    auto d = cm_encode_date(&day_detail);
    d -= CM_UNIX_EPOCH;
    result = date_stor_t{timestamp_stor_t{static_cast<int64_t>(d)}};
    return true;
}

template <>
bool TryCast::Operation(date_stor_t input, std::string &result, bool strict) {
    if (input == DateUtils::Infinity()) {
        result = DateUtils::PINF;
        return true;
    }
    if (input == DateUtils::Ninfinity()) {
        result = DateUtils::NINF;
        return true;
    }
    date_detail_t detail;
    input.dates.ts += CM_UNIX_EPOCH;
    cm_decode_date(static_cast<date_t>(input.dates.ts), &detail);
    char buf[30] = {'\0'};
    sprintf(buf, "%04d-%02d-%02d", detail.year, detail.mon, detail.day);
    result = std::string(buf);
    return true;
}

template <>
bool TryCast::Operation(date_stor_t input, timestamp_stor_t &result, bool strict) {
    // Date 是有时区的，Timestamp 是无时区的
    if (input == DateUtils::Infinity()) {
        result = TimeUtils::Infinity();
        return true;
    }
    if (input == DateUtils::Ninfinity()) {
        result = TimeUtils::Ninfinity();
        return true;
    }
    result.ts = input.dates.ts - TIMEZONE_GET_MICROSECOND(cm_get_db_timezone());
    return true;
}

template <>
bool TryCast::Operation(date_stor_t input, date_stor_t &result, bool strict) {
    result = input;
    return true;
}
