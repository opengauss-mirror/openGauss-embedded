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

#include "type/hugeint.h"
// Undef annoying windows macro
#undef max

#include <limits>

template <class T>
struct NumericLimits {
    static constexpr T Minimum() { return std::numeric_limits<T>::lowest(); };
    static constexpr T Maximum() { return std::numeric_limits<T>::max(); };
    static bool IsSigned();
    static uint64_t Digits();
};

template <>
struct NumericLimits<int8_t> {
    static constexpr int8_t Minimum() { return std::numeric_limits<int8_t>::lowest(); };
    static constexpr int8_t Maximum() { return std::numeric_limits<int8_t>::max(); };
    static bool IsSigned() { return true; }
    static uint64_t Digits() { return 3; }
};
template <>
struct NumericLimits<int16_t> {
    static constexpr int16_t Minimum() { return std::numeric_limits<int16_t>::lowest(); };
    static constexpr int16_t Maximum() { return std::numeric_limits<int16_t>::max(); };
    static bool IsSigned() { return true; }
    static uint64_t Digits() { return 5; }
};
template <>
struct NumericLimits<int32_t> {
    static constexpr int32_t Minimum() { return std::numeric_limits<int32_t>::lowest(); };
    static constexpr int32_t Maximum() { return std::numeric_limits<int32_t>::max(); };
    static bool IsSigned() { return true; }
    static uint64_t Digits() { return 10; }
};

template <>
struct NumericLimits<int64_t> {
    static constexpr int64_t Minimum() { return std::numeric_limits<int64_t>::lowest(); };
    static constexpr int64_t Maximum() { return std::numeric_limits<int64_t>::max(); };
    static bool IsSigned() { return true; }
    static uint64_t Digits() { return 19; }
};
template <>
struct NumericLimits<hugeint_t> {
    static constexpr hugeint_t Minimum() { return {std::numeric_limits<int64_t>::lowest(), 1}; };
    static constexpr hugeint_t Maximum() {
        return {std::numeric_limits<int64_t>::max(), std::numeric_limits<uint64_t>::max()};
    };
    static bool IsSigned() { return true; }
    static uint64_t Digits() { return 39; }
};

template <>
struct NumericLimits<uint8_t> {
    static constexpr uint8_t Minimum() { return std::numeric_limits<uint8_t>::lowest(); };
    static constexpr uint8_t Maximum() { return std::numeric_limits<uint8_t>::max(); };
    static bool IsSigned() { return false; }
    static uint64_t Digits() { return 3; }
};

template <>
struct NumericLimits<uint16_t> {
    static constexpr uint16_t Minimum() { return std::numeric_limits<uint16_t>::lowest(); };
    static constexpr uint16_t Maximum() { return std::numeric_limits<uint16_t>::max(); };
    static bool IsSigned() { return false; }
    static uint64_t Digits() { return 5; }
};
template <>
struct NumericLimits<uint32_t> {
    static constexpr uint32_t Minimum() { return std::numeric_limits<uint32_t>::lowest(); };
    static constexpr uint32_t Maximum() { return std::numeric_limits<uint32_t>::max(); };
    static bool IsSigned() { return false; }
    static uint64_t Digits() { return 10; }
};
template <>
struct NumericLimits<uint64_t> {
    static constexpr uint64_t Minimum() { return std::numeric_limits<uint64_t>::lowest(); };
    static constexpr uint64_t Maximum() { return std::numeric_limits<uint64_t>::max(); };
    static bool IsSigned() { return false; }
    static uint64_t Digits() { return 20; }
};
template <>
struct NumericLimits<float> {
    static constexpr float Minimum() { return std::numeric_limits<float>::lowest(); };
    static constexpr float Maximum() { return std::numeric_limits<float>::max(); };
    static bool IsSigned() { return true; }
    static uint64_t Digits() { return 127; }
};
template <>
struct NumericLimits<double> {
    static constexpr double Minimum() { return std::numeric_limits<double>::lowest(); };
    static constexpr double Maximum() { return std::numeric_limits<double>::max(); };
    static bool IsSigned() { return true; }
    static uint64_t Digits() { return 250; }
};
