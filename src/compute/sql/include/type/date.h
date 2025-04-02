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

#include <cstdlib>

#include "type/limits.h"
#include "type/timestamp_t.h"

struct date_stor_t {
    timestamp_stor_t dates;

    bool operator==(const date_stor_t &rhs) const { return dates == rhs.dates; }
    bool operator<(const date_stor_t &rhs) const { return dates < rhs.dates; }
    bool operator<=(const date_stor_t &rhs) const { return dates <= rhs.dates; }
};

class DateUtils {
   public:
    static const char *PINF;
    static const char *NINF;

    constexpr static const int64_t US_PER_DAY = 86400000000;
   private:
    static const char *EPOCH;

    static const char *MONTH_NAMES[12];
    static const char *MONTH_NAMES_ABBREVIATED[12];
    static const char *DAY_NAMES[7];
    static const char *DAY_NAMES_ABBREVIATED[7];
    static const int32_t NORMAL_DAYS[13];
    static const int32_t CUMULATIVE_DAYS[13];
    static const int32_t LEAP_DAYS[13];
    static const int32_t CUMULATIVE_LEAP_DAYS[13];
    static const int32_t CUMULATIVE_YEAR_DAYS[401];
    static const int8_t MONTH_PER_DAY_OF_YEAR[365];
    static const int8_t LEAP_MONTH_PER_DAY_OF_YEAR[366];

    constexpr static const int32_t EPOCH_YEAR = 1970;

    constexpr static const int32_t YEAR_INTERVAL = 400;
    constexpr static const int32_t DAYS_PER_YEAR_INTERVAL = 146097;

   public:
    static bool TryConvertDate(const char *buf, size_t len, size_t &pos, date_stor_t &result, bool &special,
                               bool strict = false);
    static bool ToDate(int32_t year, int32_t month, int32_t day, date_stor_t &result);

    static bool IsValid(int32_t year, int32_t month, int32_t day);

    // special values
    static inline date_stor_t Infinity() { return date_stor_t{timestamp_stor_t{NumericLimits<int64_t>::Maximum()}}; }
    static inline date_stor_t Ninfinity() { return date_stor_t{timestamp_stor_t{-NumericLimits<int64_t>::Maximum()}}; }
    static inline date_stor_t Epoch() { return date_stor_t{timestamp_stor_t{0}}; }

    static bool IsLeapYear(int32_t year);
    static bool ParseDoubleDigit(const char *buf, size_t len, size_t &pos, int32_t &result);

    static date_stor_t ToDate(timestamp_stor_t &t);
};
