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
* timestamp_t.cpp
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/type/timestamp_t.cpp
*
* -------------------------------------------------------------------------
*/
#include "type/timestamp_t.h"

#include "common/string_util.h"
#include "storage/gstor/zekernel/common/cm_date.h"
#include "type/date.h"

bool operator==(int64_t lhs, timestamp_stor_t rhs) { return lhs == rhs.ts; }
bool operator<(int64_t lhs, timestamp_stor_t rhs) { return lhs < rhs.ts; }
bool operator>(int64_t lhs, timestamp_stor_t rhs) { return lhs > rhs.ts; }
bool operator>=(int64_t lhs, timestamp_stor_t rhs) { return lhs >= rhs.ts; }
timestamp_stor_t timestamp_stor_t::operator-(const timestamp_stor_t &rhs){
    timestamp_stor_t result;
    result.ts = ts - rhs.ts;
    return result;
}
timestamp_stor_t timestamp_stor_t::operator-(int64_t rhs) {
    timestamp_stor_t result;
    result.ts = ts - rhs;
    return result;
}
timestamp_stor_t timestamp_stor_t::operator+(const timestamp_stor_t &rhs){
    timestamp_stor_t result;
    result.ts = ts + rhs.ts;
    return result;
}
timestamp_stor_t timestamp_stor_t::operator+(int64_t rhs) {
    timestamp_stor_t result;
    result.ts = ts + rhs;
    return result;
}

timestamp_stor_t TimestampNow(void) {
    timestamp_stor_t t;
    t.ts = static_cast<int64_t>(cm_utc_now());
    t.ts -= CM_UNIX_EPOCH;
    return t;
}

// 是否属于时区的字符
static inline bool IsTimeZoneCharacter(char c) {
    return std::isalpha(c) || std::isdigit(c) || c == '_' || c == '/' || c == '+' || c == '-';
}

bool TimeUtils::TryConvertTimestampTZ(const char *str, size_t len, timestamp_stor_t &result, bool &has_offset,
                                      std::string &tz) {
    size_t pos;
    date_stor_t date;
    has_offset = false;
    if (!DateUtils::TryConvertDate(str, len, pos, date, has_offset)) {
        return false;
    }
    if (pos == len) {
        // 只指定了日期
        if (date == DateUtils::Infinity()) {
            result = TimeUtils::Infinity();
            return true;
        } else if (date == DateUtils::Ninfinity()) {
            result = TimeUtils::Ninfinity();
            return true;
        }
        result = date.dates;
        return true;
    }
    // 尝试解析时间部分
    if (str[pos] == ' ' || str[pos] == 'T') {
        pos++;
    }
    size_t time_pos = 0;
    int64_t time;
    // 解析时间部分
    if (!TimeUtils::TryConvertTime(str + pos, len - pos, time_pos, time)) {
        return false;
    }
    pos += time_pos;
    // TODO 判断转换的结果是否有溢出(存储的时间戳和unix时间戳不同，溢出判断比较复杂)
    result.ts = date.dates.ts + time;
    if (pos < len) {
        // skip a "Z" at the end (as per the ISO8601 specs)
        int hour_offset, minute_offset;
        if (str[pos] == 'Z') {
            pos++;
            has_offset = true;
        } else if (TryParseUTCOffset(str, pos, len, hour_offset, minute_offset)) {
            // 直接指明了时区偏移
            const int64_t delta = hour_offset * Interval::MICROS_PER_HOUR + minute_offset * Interval::MICROS_PER_MINUTE;
            // TODO 判断转换的结果是否有溢出(存储的时间戳和unix时间戳不同，溢出判断比较复杂)
            result.ts -= delta;
            has_offset = true;
        } else {
            // Parse a time zone: / [A-Za-z0-9/_]+/
            if (str[pos++] != ' ') {
                return false;
            }
            auto tz_name = str + pos;
            // 获取时区名字
            for (; pos < len && IsTimeZoneCharacter(str[pos]); ++pos) {
                continue;
            }
            auto tz_len = str + pos - tz_name;
            if (tz_len) {
                tz = std::string(tz_name, tz_len);
            }
            // Note that the caller must reinterpret the instant we return to the given time zone
        }

        // skip any spaces at the end
        while (pos < len && intarkdb::IsSpace(str[pos])) {
            pos++;
        }
        if (pos < len) {
            return false;
        }
    }
    return true;
}

constexpr int MAX_HOUR = 23;
constexpr int MIN_HOUR = 0;
constexpr int MAX_MINUTE = 59;
constexpr int MIN_MINUTE = 0;
constexpr int MAX_SECOND = 59;
constexpr int MIN_SECOND = 0;

// 解析时间部分
bool TimeUtils::TryConvertTime(const char *buf, size_t len, size_t &pos, int64_t &result, bool strict) {
    int32_t hour = -1, min = -1, sec = -1, micros = -1;
    pos = 0;

    if (len == 0) {
        return false;
    }

    int sep;

    // 移除空格
    while (pos < len && intarkdb::IsSpace(buf[pos])) {
        pos++;
    }

    if (pos >= len) {
        return false;
    }

    if (!std::isdigit(buf[pos])) {
        return false;
    }

    if (!DateUtils::ParseDoubleDigit(buf, len, pos, hour)) {
        return false;
    }
    if (hour < MIN_HOUR || hour > MAX_HOUR) {
        return false;
    }

    // 支持 YYYY-MM-DD HH
    if (pos == len) {
        min = 0;
        sec = 0;
        micros = 0;
        // 分、秒、毫秒置0
        result = (int64_t)(hour * 3600 + min * 60 + sec) * 1000000 + micros;
        return true;
    }
    if (pos > len) {
        return false;
    }

    // fetch the separator
    sep = buf[pos++];
    if (sep != ':') {
        // invalid separator
        return false;
    }

    if (!DateUtils::ParseDoubleDigit(buf, len, pos, min)) {
        return false;
    }
    if (min < MIN_MINUTE || min > MAX_MINUTE) {
        return false;
    }

    // 支持 YYYY-MM-DD HH:MI
    if (pos == len) {
        sec = 0;
        micros = 0;
        // 秒、毫秒置0
        result = (int64_t)(hour * 3600 + min * 60 + sec) * 1000000 + micros;
        return true;
    }
    if (pos > len) {
        return false;
    }

    if (buf[pos++] != sep) {
        return false;
    }

    if (!DateUtils::ParseDoubleDigit(buf, len, pos, sec)) {
        return false;
    }
    if (sec < MIN_SECOND || sec > MAX_SECOND) {
        return false;
    }

    micros = 0;
    if (pos < len && buf[pos] == '.') {
        pos++;
        // we expect some microseconds
        int32_t mult = 100000;
        for (; pos < len && std::isdigit(buf[pos]); pos++, mult /= 10) {
            if (mult > 0) {
                micros += (buf[pos] - '0') * mult;
            }
        }
    }

    // 在严格模式下，检查剩余的字符串是否有非空格字符
    if (strict) {
        // 移除空格
        while (pos < len && intarkdb::IsSpace(buf[pos])) {
            pos++;
        }
        // 检查是否已经到达字符串末尾
        if (pos < len) {
            return false;
        }
    }

    // 计算微秒数
    result = (int64_t)(hour * 3600 + min * 60 + sec) * 1000000 + micros;
    return true;
}

// 解析时区信息
static bool ParseHourOffset(const char *str, size_t &curpos, int &hour_offset, char sign_char) {
    if (!std::isdigit(str[curpos]) || !std::isdigit(str[curpos + 1])) {
        // expected +HH or -HH
        return false;
    }
    hour_offset = (str[curpos] - '0') * 10 + (str[curpos + 1] - '0');
    if (sign_char == '-') {
        hour_offset = -hour_offset;
    }
    return true;
}

static bool ParseMinuteOffset(const char *str, size_t len, size_t &curpos, int &minute_offset, char sign_char) {
    if (curpos >= len) {
        return true;  // 没有更多的字符来解析分钟偏移
    }
    if (str[curpos] == ':') {
        curpos++;
    }
    if (curpos + 2 > len || !std::isdigit(str[curpos]) || !std::isdigit(str[curpos + 1])) {
        return true;  // 没有MM偏移，但是我们需要返回true，因为函数需要继续执行
    }
    minute_offset = (str[curpos] - '0') * 10 + (str[curpos + 1] - '0');
    if (sign_char == '-') {
        minute_offset = -minute_offset;
    }
    curpos += 2;
    return true;
}

// 解析时区信息
bool TimeUtils::TryParseUTCOffset(const char *str, size_t &pos, size_t len, int &hour_offset, int &minute_offset) {
    minute_offset = 0;
    size_t curpos = pos;
    // parse the next 3 characters
    if (curpos + 3 > len) {
        // no characters left to parse
        return false;
    }
    char sign_char = str[curpos];
    if (sign_char != '+' && sign_char != '-') {
        // expected either + or -
        return false;
    }
    curpos++;
    if (!ParseHourOffset(str, curpos, hour_offset, sign_char)) {
        return false;
    }
    curpos += 2;
    pos = curpos;

    // optional minute specifier: expected either "MM" or ":MM"
    return ParseMinuteOffset(str, len, pos, minute_offset, sign_char);
}
