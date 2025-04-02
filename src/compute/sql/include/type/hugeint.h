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

#include <stdint.h>

#include <limits>
#include <stdexcept>
#include <string>
#ifdef _MSC_VER
#define GS_MAX_INT64 LLONG_MAX
#define GS_MIN_INT64 LLONG_MIN
#endif
struct hugeint_t {
   public:
    uint64_t lower;
    int64_t upper;

   public:
    hugeint_t() = default;
    hugeint_t(int64_t value);
    constexpr hugeint_t(int64_t upper_v, uint64_t lower_v) : lower(lower_v), upper(upper_v) {}

    constexpr hugeint_t(const hugeint_t& rhs) = default;
    constexpr hugeint_t(hugeint_t&& rhs) = default;
    hugeint_t& operator=(const hugeint_t& rhs) = default;
    hugeint_t& operator=(hugeint_t&& rhs) = default;

    std::string ToString() const;

    // comparsion operators
    bool operator==(const hugeint_t& rhs) const;
    bool operator!=(const hugeint_t& rhs) const;
    bool operator<=(const hugeint_t& rhs) const;
    bool operator<(const hugeint_t& rhs) const;
    bool operator>(const hugeint_t& rhs) const;
    bool operator>=(const hugeint_t& rhs) const;

    // 数学运算
    hugeint_t operator+(const hugeint_t& rhs) const;
    hugeint_t operator-(const hugeint_t& rhs) const;
    hugeint_t operator*(const hugeint_t& rhs) const;
    hugeint_t operator/(const hugeint_t& rhs) const;
    hugeint_t operator%(const hugeint_t& rhs) const;
    hugeint_t operator-() const;

    // 位运算
    hugeint_t operator>>(const hugeint_t& rhs) const;
    hugeint_t operator<<(const hugeint_t& rhs) const;
    hugeint_t operator&(const hugeint_t& rhs) const;
    hugeint_t operator|(const hugeint_t& rhs) const;
    hugeint_t operator^(const hugeint_t& rhs) const;
    hugeint_t operator~() const;

    // 原地操作
    hugeint_t& operator+=(const hugeint_t& rhs);
    hugeint_t& operator-=(const hugeint_t& rhs);
    hugeint_t& operator*=(const hugeint_t& rhs);
    hugeint_t& operator/=(const hugeint_t& rhs);
    hugeint_t& operator%=(const hugeint_t& rhs);
    hugeint_t& operator>>=(const hugeint_t& rhs);
    hugeint_t& operator<<=(const hugeint_t& rhs);
    hugeint_t& operator&=(const hugeint_t& rhs);
    hugeint_t& operator|=(const hugeint_t& rhs);
    hugeint_t& operator^=(const hugeint_t& rhs);
};

//! The Hugeint class contains static operations for the INT128 type
class Hugeint {
   public:
    //! Convert a hugeint object to a string
    static std::string ToString(hugeint_t input);

    template <class T>
    static bool TryCast(hugeint_t input, T& result);

    template <class T>
    static T Cast(hugeint_t input) {
        T result = 0;
        TryCast(input, result);
        return result;
    }

    template <class T>
    static bool TryConvert(T value, hugeint_t& result);

    template <class T>
    static hugeint_t Convert(T value) {
        hugeint_t result;
        if (!TryConvert(value, result)) {  // LCOV_EXCL_START
            throw std::out_of_range("hugeint conver out of range");
        }  // LCOV_EXCL_STOP
        return result;
    }
#ifdef _MSC_VER
    static void NegateInPlace(hugeint_t& input) {
        if (input.upper == GS_MIN_INT64 && input.lower == 0) {
            throw std::out_of_range("hugeint is out of range");
        }
        input.lower = GS_MAX_INT64 - input.lower + 1;
        input.upper = -1 - input.upper + (input.lower == 0);
    }
#else
    static void NegateInPlace(hugeint_t& input) {
        if (input.upper == std::numeric_limits<int64_t>::min() && input.lower == 0) {
            throw std::out_of_range("hugeint is out of range");
        }
        input.lower = std::numeric_limits<uint64_t>::max() - input.lower + 1;
        input.upper = -1 - input.upper + (input.lower == 0);
    }
#endif
    static hugeint_t Negate(hugeint_t input) {
        NegateInPlace(input);
        return input;
    }

    static bool TryMultiply(hugeint_t lhs, hugeint_t rhs, hugeint_t& result);

    static hugeint_t Add(hugeint_t lhs, hugeint_t rhs);
    static hugeint_t Subtract(hugeint_t lhs, hugeint_t rhs);
    static hugeint_t Multiply(hugeint_t lhs, hugeint_t rhs);
    static hugeint_t Divide(hugeint_t lhs, hugeint_t rhs);
    static hugeint_t Modulo(hugeint_t lhs, hugeint_t rhs);

    // DivMod -> returns the result of the division (lhs / rhs), and fills up the remainder
    static hugeint_t DivMod(hugeint_t lhs, hugeint_t rhs, hugeint_t& remainder);
    // DivMod but lhs MUST be positive, and rhs is a uint64_t
    static hugeint_t DivModPositive(hugeint_t lhs, uint64_t rhs, uint64_t& remainder);

    static bool AddInPlace(hugeint_t& lhs, hugeint_t rhs);
    static bool SubtractInPlace(hugeint_t& lhs, hugeint_t rhs);

    // comparison operators
    // note that everywhere here we intentionally use bitwise ops
    // this is because they seem to be consistently much faster (benchmarked on a Macbook Pro)
    static bool Equals(hugeint_t lhs, hugeint_t rhs) {
        int lower_equals = lhs.lower == rhs.lower;
        int upper_equals = lhs.upper == rhs.upper;
        return lower_equals & upper_equals;
    }

    static bool NotEquals(hugeint_t lhs, hugeint_t rhs) {
        int lower_not_equals = lhs.lower != rhs.lower;
        int upper_not_equals = lhs.upper != rhs.upper;
        return lower_not_equals | upper_not_equals;
    }
    static bool GreaterThan(hugeint_t lhs, hugeint_t rhs) {
        int upper_bigger = lhs.upper > rhs.upper;
        int upper_equal = lhs.upper == rhs.upper;
        int lower_bigger = lhs.lower > rhs.lower;
        return upper_bigger | (upper_equal & lower_bigger);
    }

    static bool GreaterThanEquals(hugeint_t lhs, hugeint_t rhs) {
        int upper_bigger = lhs.upper > rhs.upper;
        int upper_equal = lhs.upper == rhs.upper;
        int lower_bigger_equals = lhs.lower >= rhs.lower;
        return upper_bigger | (upper_equal & lower_bigger_equals);
    }
    static bool LessThan(hugeint_t lhs, hugeint_t rhs) {
        int upper_smaller = lhs.upper < rhs.upper;
        int upper_equal = lhs.upper == rhs.upper;
        int lower_smaller = lhs.lower < rhs.lower;
        return upper_smaller | (upper_equal & lower_smaller);
    }
    static bool LessThanEquals(hugeint_t lhs, hugeint_t rhs) {
        int upper_smaller = lhs.upper < rhs.upper;
        int upper_equal = lhs.upper == rhs.upper;
        int lower_smaller_equals = lhs.lower <= rhs.lower;
        return upper_smaller | (upper_equal & lower_smaller_equals);
    }
    static const hugeint_t POWERS_OF_TEN[40];
};

template <>
bool Hugeint::TryCast(hugeint_t input, int8_t& result);
template <>
bool Hugeint::TryCast(hugeint_t input, int16_t& result);
template <>
bool Hugeint::TryCast(hugeint_t input, int32_t& result);
template <>
bool Hugeint::TryCast(hugeint_t input, int64_t& result);
template <>
bool Hugeint::TryCast(hugeint_t input, uint8_t& result);
template <>
bool Hugeint::TryCast(hugeint_t input, uint16_t& result);
template <>
bool Hugeint::TryCast(hugeint_t input, uint32_t& result);
template <>
bool Hugeint::TryCast(hugeint_t input, uint64_t& result);
template <>
bool Hugeint::TryCast(hugeint_t input, hugeint_t& result);
template <>
bool Hugeint::TryCast(hugeint_t input, float& result);
template <>
bool Hugeint::TryCast(hugeint_t input, double& result);
template <>
bool Hugeint::TryCast(hugeint_t input, long double& result);

template <>
bool Hugeint::TryConvert(int8_t value, hugeint_t& result);
template <>
bool Hugeint::TryConvert(int16_t value, hugeint_t& result);
template <>
bool Hugeint::TryConvert(int32_t value, hugeint_t& result);
template <>
bool Hugeint::TryConvert(int64_t value, hugeint_t& result);
template <>
bool Hugeint::TryConvert(uint8_t value, hugeint_t& result);
template <>
bool Hugeint::TryConvert(uint16_t value, hugeint_t& result);
template <>
bool Hugeint::TryConvert(uint32_t value, hugeint_t& result);
template <>
bool Hugeint::TryConvert(uint64_t value, hugeint_t& result);
template <>
bool Hugeint::TryConvert(float value, hugeint_t& result);
template <>
bool Hugeint::TryConvert(double value, hugeint_t& result);
template <>
bool Hugeint::TryConvert(long double value, hugeint_t& result);
template <>
bool Hugeint::TryConvert(const char* value, hugeint_t& result);
