//===----------------------------------------------------------------------===//
// Copyright 2018-2023 Stichting DuckDB Foundation
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

#include <math.h>

#include <cmath>
#include <limits>

#include "type/hugeint.h"
#include "type/limits.h"
#include "type/operator/cast_operators.h"
#include "type/valid.h"

template <typename SRC, typename DST>
static bool TryCastWithOverflowCheck(SRC value, DST &result) {
    if (!IsFinite(value)) {
        return false;
    }
    if (NumericLimits<SRC>::IsSigned() != NumericLimits<DST>::IsSigned()) {
        if (NumericLimits<SRC>::IsSigned()) {
            // signed to unsigned conversion
            if (NumericLimits<SRC>::Digits() > NumericLimits<DST>::Digits()) {
                if (value < 0 || value > (SRC)NumericLimits<DST>::Maximum()) {
                    return false;
                }
            } else {
                if (value < 0) {
                    return false;
                }
            }
            result = (DST)value;
            return true;
        } else {
            // unsigned to signed conversion
            if (NumericLimits<SRC>::Digits() >= NumericLimits<DST>::Digits()) {
                if (value <= (SRC)NumericLimits<DST>::Maximum()) {
                    result = (DST)value;
                    return true;
                }
                return false;
            } else {
                result = (DST)value;
                return true;
            }
        }
    } else {
        // same sign conversion
        if (NumericLimits<DST>::Digits() >= NumericLimits<SRC>::Digits()) {
            result = (DST)value;
            return true;
        } else {
            if (value < SRC(NumericLimits<DST>::Minimum()) || value > SRC(NumericLimits<DST>::Maximum())) {
                return false;
            }
            result = (DST)value;
            return true;
        }
    }
}

template <typename SRC, typename T>
bool TryCastWithOverflowCheckFloat(SRC value, T &result, SRC min, SRC max) {
    if (!IsFinite(value)) {
        return false;
    }
    if (!(value >= min && value <= max)) {
        return false;
    }
    // PG FLOAT => INT casts use statiical rounding
    result = std::nearbyint(value);
    return true;
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(float value, int8_t &result) {
    return TryCastWithOverflowCheckFloat<float, int8_t>(value, result, -128.0f, 128.0f);
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(float value, int16_t &result) {
    return TryCastWithOverflowCheckFloat<float, int16_t>(value, result, -32768.0f, 32768.0f);
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(float value, int32_t &result) {
    return TryCastWithOverflowCheckFloat<float, int32_t>(value, result, -2147483648.0f, 2147483648.0f);
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(float value, int64_t &result) {
    return TryCastWithOverflowCheckFloat<float, int64_t>(value, result, -9223372036854775808.0f,
                                                         9223372036854775808.0f);
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(float value, uint8_t &result) {
    return TryCastWithOverflowCheckFloat<float, uint8_t>(value, result, 0.0f, 256.0f);
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(float value, uint16_t &result) {
    return TryCastWithOverflowCheckFloat<float, uint16_t>(value, result, 0.0f, 65536.0f);
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(float value, uint32_t &result) {
    return TryCastWithOverflowCheckFloat<float, uint32_t>(value, result, 0.0f, 4294967296.0f);
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(float value, uint64_t &result) {
    return TryCastWithOverflowCheckFloat<float, uint64_t>(value, result, 0.0f, 18446744073709551616.0f);
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(double value, int8_t &result) {
    return TryCastWithOverflowCheckFloat<double, int8_t>(value, result, -128.0, 128.0);
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(double value, int16_t &result) {
    return TryCastWithOverflowCheckFloat<double, int16_t>(value, result, -32768.0, 32768.0);
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(double value, int32_t &result) {
    return TryCastWithOverflowCheckFloat<double, int32_t>(value, result, -2147483648.0, 2147483648.0);
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(double value, int64_t &result) {
    return TryCastWithOverflowCheckFloat<double, int64_t>(value, result, -9223372036854775808.0, 9223372036854775808.0);
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(double value, uint8_t &result) {
    return TryCastWithOverflowCheckFloat<double, uint8_t>(value, result, 0.0, 256.0);
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(double value, uint16_t &result) {
    return TryCastWithOverflowCheckFloat<double, uint16_t>(value, result, 0.0, 65536.0);
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(double value, uint32_t &result) {
    return TryCastWithOverflowCheckFloat<double, uint32_t>(value, result, 0.0, 4294967296.0);
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(double value, uint64_t &result) {
    return TryCastWithOverflowCheckFloat<double, uint64_t>(value, result, 0.0, 18446744073709551615.0);
}
template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(float input, float &result) {
    result = input;
    return true;
}
template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(float input, double &result) {
    result = double(input);
    return true;
}
template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(double input, double &result) {
    result = input;
    return true;
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(double input, float &result) {
    if (!IsFinite(input)) {
        result = float(input);
        return true;
    }
    auto res = float(input);
    if (!IsFinite(input)) {
        return false;
    }
    result = res;
    return true;
}

//===--------------------------------------------------------------------===//
// Cast Numeric -> bool
//===--------------------------------------------------------------------===//
// 函数不能偏特化
template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(bool value, bool &result) {
    result = bool(value);
    return true;
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(int8_t value, bool &result) {
    result = bool(value);
    return true;
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(int16_t value, bool &result) {
    result = bool(value);
    return true;
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(int32_t value, bool &result) {
    result = bool(value);
    return true;
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(int64_t value, bool &result) {
    result = bool(value);
    return true;
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(uint8_t value, bool &result) {
    result = bool(value);
    return true;
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(uint16_t value, bool &result) {
    result = bool(value);
    return true;
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(uint32_t value, bool &result) {
    result = bool(value);
    return true;
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(uint64_t value, bool &result) {
    result = bool(value);
    return true;
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(float value, bool &result) {
    result = bool(value);
    return true;
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(double value, bool &result) {
    result = bool(value);
    return true;
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(hugeint_t input, bool &result) {
    result = input.upper != 0 || input.lower != 0;
    return true;
}

//===--------------------------------------------------------------------===//
// Cast bool -> Numeric
//===--------------------------------------------------------------------===//
template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(bool value, int8_t &result) {
    result = int8_t(value);
    return true;
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(bool value, int16_t &result) {
    result = int16_t(value);
    return true;
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(bool value, int32_t &result) {
    result = int32_t(value);
    return true;
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(bool value, int64_t &result) {
    result = int64_t(value);
    return true;
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(bool value, uint8_t &result) {
    result = uint8_t(value);
    return true;
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(bool value, uint16_t &result) {
    result = uint16_t(value);
    return true;
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(bool value, uint32_t &result) {
    result = uint32_t(value);
    return true;
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(bool value, uint64_t &result) {
    result = uint64_t(value);
    return true;
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(bool value, float &result) {
    result = float(value);
    return true;
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(bool value, double &result) {
    result = double(value);
    return true;
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(bool input, hugeint_t &result) {
    result.upper = 0;
    result.lower = input ? 1 : 0;
    return true;
}

//===--------------------------------------------------------------------===//
// Cast Numeric -> hugeint
//===--------------------------------------------------------------------===//
template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(int8_t value, hugeint_t &result) {
    return Hugeint::TryConvert(value, result);
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(int16_t value, hugeint_t &result) {
    return Hugeint::TryConvert(value, result);
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(int32_t value, hugeint_t &result) {
    return Hugeint::TryConvert(value, result);
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(int64_t value, hugeint_t &result) {
    return Hugeint::TryConvert(value, result);
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(uint8_t value, hugeint_t &result) {
    return Hugeint::TryConvert(value, result);
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(uint16_t value, hugeint_t &result) {
    return Hugeint::TryConvert(value, result);
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(uint32_t value, hugeint_t &result) {
    return Hugeint::TryConvert(value, result);
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(uint64_t value, hugeint_t &result) {
    return Hugeint::TryConvert(value, result);
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(float value, hugeint_t &result) {
    return Hugeint::TryConvert(std::nearbyintf(value), result);
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(double value, hugeint_t &result) {
    return Hugeint::TryConvert(std::nearbyint(value), result);
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(hugeint_t value, hugeint_t &result) {
    result = value;
    return true;
}

//===--------------------------------------------------------------------===//
// Cast Hugeint -> Numeric
//===--------------------------------------------------------------------===//
template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(hugeint_t value, int8_t &result) {
    return Hugeint::TryCast(value, result);
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(hugeint_t value, int16_t &result) {
    return Hugeint::TryCast(value, result);
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(hugeint_t value, int32_t &result) {
    return Hugeint::TryCast(value, result);
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(hugeint_t value, int64_t &result) {
    return Hugeint::TryCast(value, result);
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(hugeint_t value, uint8_t &result) {
    return Hugeint::TryCast(value, result);
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(hugeint_t value, uint16_t &result) {
    return Hugeint::TryCast(value, result);
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(hugeint_t value, uint32_t &result) {
    return Hugeint::TryCast(value, result);
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(hugeint_t value, uint64_t &result) {
    return Hugeint::TryCast(value, result);
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(hugeint_t value, float &result) {
    return Hugeint::TryCast(value, result);
}

template <>
[[maybe_unused]] bool TryCastWithOverflowCheck(hugeint_t value, double &result) {
    return Hugeint::TryCast(value, result);
}

struct NumericTryCast {
    template <class SRC, class DST>
    static inline bool Operation(SRC input, DST &result, bool strict = false) {
        return TryCastWithOverflowCheck(input, result);
    }
};

struct NumericCast {
    template <class SRC, class DST>
    static inline DST Operation(SRC input) {
        DST result;
        if (!NumericTryCast::Operation(input, result)) {
            throw std::runtime_error("type cast error");
        }
        return result;
    }
};
