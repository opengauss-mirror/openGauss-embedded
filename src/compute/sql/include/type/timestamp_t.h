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
* timestamp_t.h
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/include/type/timestamp_t.h
*
* -------------------------------------------------------------------------
*/
#pragma once

#include <stdint.h>

#include <string>

#include "type/limits.h"

struct timestamp_stor_t {
    int64_t ts;
    bool operator==(const timestamp_stor_t &rhs) const { return ts == rhs.ts; }
    bool operator<(const timestamp_stor_t &rhs) const { return ts < rhs.ts; }
    bool operator<=(const timestamp_stor_t &rhs) const { return ts <= rhs.ts; }
    bool operator>(const timestamp_stor_t &rhs) const { return ts > rhs.ts; }
    bool operator>=(const timestamp_stor_t &rhs) const { return ts >= rhs.ts; }
    timestamp_stor_t operator-(const timestamp_stor_t &rhs);
    timestamp_stor_t operator+(const timestamp_stor_t &rhs);
    bool operator==(int64_t rhs) const { return ts == rhs; }
    bool operator!=(const timestamp_stor_t& rhs) const { return ts != rhs.ts; }
    bool operator<(int64_t rhs) const { return ts < rhs; }
    bool operator>(int64_t rhs) const { return ts > rhs; }
    bool operator>=(int64_t rhs) const { return ts >= rhs; }
    timestamp_stor_t operator-(int64_t rhs);
    timestamp_stor_t operator+(int64_t rhs);
    friend bool operator==(int64_t lhs, timestamp_stor_t rhs);
    friend bool operator<(int64_t lhs, timestamp_stor_t rhs);
    friend bool operator>(int64_t lhs, timestamp_stor_t rhs);
    friend bool operator>=(int64_t lhs, timestamp_stor_t rhs);
};

bool operator==(int64_t lhs, timestamp_stor_t rhs);
bool operator<(int64_t lhs, timestamp_stor_t rhs);
bool operator>(int64_t lhs, timestamp_stor_t rhs);
bool operator>=(int64_t lhs, timestamp_stor_t rhs);

timestamp_stor_t TimestampNow(void);

class Interval {
   public:
    static constexpr const int32_t MONTHS_PER_MILLENIUM = 12000;
    static constexpr const int32_t MONTHS_PER_CENTURY = 1200;
    static constexpr const int32_t MONTHS_PER_DECADE = 120;
    static constexpr const int32_t MONTHS_PER_YEAR = 12;
    static constexpr const int32_t MONTHS_PER_QUARTER = 3;
    static constexpr const int32_t DAYS_ONE_WEEK = 7; // 区别重复定义
    //! only used for interval comparison/ordering purposes, in which case a month counts as 30 days
    static constexpr const int64_t DAYS_PER_MONTH = 30;
    static constexpr const int64_t DAYS_PER_YEAR = 365;
    static constexpr const int64_t MSECS_PER_SEC = 1000;
    static constexpr const int32_t SECS_PER_MINUTE = 60;
    static constexpr const int32_t MINS_PER_HOUR = 60;
    static constexpr const int32_t HOURS_PER_DAY = 24;
    static constexpr const int32_t SECS_PER_HOUR = SECS_PER_MINUTE * MINS_PER_HOUR;
    static constexpr const int32_t SECS_PER_DAY = SECS_PER_HOUR * HOURS_PER_DAY;
    static constexpr const int32_t SECS_PER_WEEK = SECS_PER_DAY * DAYS_ONE_WEEK;  // DAYS_PER_WEEK come from cm_date.h

    static constexpr const int64_t MICROS_PER_MSEC = 1000;
    static constexpr const int64_t MICROS_PER_SEC = MICROS_PER_MSEC * MSECS_PER_SEC;
    static constexpr const int64_t MICROS_PER_MINUTE = MICROS_PER_SEC * SECS_PER_MINUTE;
    static constexpr const int64_t MICROS_PER_HOUR = MICROS_PER_MINUTE * MINS_PER_HOUR;
    static constexpr const int64_t MICROS_PER_DAY = MICROS_PER_HOUR * HOURS_PER_DAY;
    static constexpr const int64_t MICROS_PER_WEEK = MICROS_PER_DAY * DAYS_ONE_WEEK;
    static constexpr const int64_t MICROS_PER_MONTH = MICROS_PER_DAY * DAYS_PER_MONTH;

    static constexpr const int64_t NANOS_PER_MICRO = 1000;
    static constexpr const int64_t NANOS_PER_MSEC = NANOS_PER_MICRO * MICROS_PER_MSEC;
    static constexpr const int64_t NANOS_PER_SEC = NANOS_PER_MSEC * MSECS_PER_SEC;
    static constexpr const int64_t NANOS_PER_MINUTE = NANOS_PER_SEC * SECS_PER_MINUTE;
    static constexpr const int64_t NANOS_PER_HOUR = NANOS_PER_MINUTE * MINS_PER_HOUR;
    static constexpr const int64_t NANOS_PER_DAY = NANOS_PER_HOUR * HOURS_PER_DAY;
    static constexpr const int64_t NANOS_PER_WEEK = NANOS_PER_DAY * DAYS_ONE_WEEK;
};

class TimeUtils {
   public:
    // 解析日期（包括时分秒）字符串为 timestamp_stor_t
    static bool TryConvertTimestampTZ(const char *str, size_t len, timestamp_stor_t &result, bool &has_offset,
                                      std::string &tz);

    // 转换时分秒格式字符串, 返回微秒到result
    static bool TryConvertTime(const char *str, size_t len, size_t &pos, int64_t &result, bool strict = false);

    static inline timestamp_stor_t Infinity() { return timestamp_stor_t{NumericLimits<int64_t>::Maximum()}; }
    static inline timestamp_stor_t Ninfinity() { return timestamp_stor_t{-NumericLimits<int64_t>::Maximum()}; }
    static inline timestamp_stor_t Epoch() { return timestamp_stor_t{0}; }

   private:
    static bool TryParseUTCOffset(const char *str, size_t &pos, size_t len, int &hour_offset, int &minute_offset);
};
