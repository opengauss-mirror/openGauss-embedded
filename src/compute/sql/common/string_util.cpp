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
 * string_util.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/common/string_util.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "common/string_util.h"

#include <sstream>
#include <string>

#include "common/exception.h"
#include "common/util.h"
#include "storage/gstor/zekernel/common/cm_log.h"
#include "storage/gstor/zekernel/common/cm_types.h"

namespace intarkdb {

std::string StringUtil::Replace(std::string source, const std::string& from, const std::string& to) {
    if (from.empty()) {
        throw Exception(ExceptionType::SYNTAX, "Invalid argument to StringReplace - empty FROM");
    }
    size_t start_pos = 0;
    while ((start_pos = source.find(from, start_pos)) != std::string::npos) {
        source.replace(start_pos, from.length(), to);
        start_pos += to.length();
    }
    return source;
}

static auto IsBlankChar(char c) { return c == ' ' || c == '\n' || c == '\t' || c == '\r'; }
// remove leading and trailing whitespaces and '\n\r\t'
std::string StringUtil::Trim(const std::string& str) {
    size_t start = 0;
    size_t end = str.size();
    while (start < end && IsBlankChar(str[start])) {
        start++;
    }
    while (end > start && IsBlankChar(str[end - 1])) {
        end--;
    }
    return str.substr(start, end - start);
}

std::vector<std::string> StringUtil::SplitAllString(const std::string& str, char delimiter) {
    std::vector<std::string> internal;
    std::stringstream ss(str);
    std::string tok;
    while (getline(ss, tok, delimiter)) {
        internal.push_back(tok);
    }
    return internal;
}

bool StringUtil::EndsWith(const std::string& str, const std::string& suffix) {
    if (suffix.size() > str.size()) {
        return false;
    }
    return equal(suffix.rbegin(), suffix.rend(), str.rbegin());
}

size_t UTF8Util::Length(const std::string& input) { return Length(input.c_str(), input.size()); }

size_t UTF8Util::Length(const char* input, size_t str_len) {
    size_t idx = 0;
    int len = 0;
    while (idx < str_len) {
        int sz = 0;
        intarkdb::utf8proc_codepoint(input + idx, sz);
        idx += sz;
        len++;
    }
    return len;
}

static bool IsUTF8Blank(utf8proc_category_t category) {
    return category == UTF8PROC_CATEGORY_ZS || category == UTF8PROC_CATEGORY_ZL || category == UTF8PROC_CATEGORY_ZP;
}

// remove utf-8 blank characters at the beginning
std::string UTF8Util::TrimLeft(const std::string& input) {
    size_t start = 0;
    size_t end = input.size();
    while (start < end) {
        int sz = 0;
        auto codepoint = intarkdb::utf8proc_codepoint(input.c_str() + start, sz);
        if (sz == 0) {
            break;
        }
        if (sz == 1 && IsBlankChar(*(input.c_str() + start))) {
            start++;
            continue;
        }
        if (sz > 1 && IsUTF8Blank(intarkdb::utf8proc_category(codepoint))) {
            start += sz;
            continue;
        }
        break;
    }
    return input.substr(start);
}

std::string UTF8Util::Substr(const std::string& input, size_t start, int len) {
    size_t idx = 0;
    size_t u_idx = 0;
    const char* start_str = input.c_str();
    while (idx < input.size() && u_idx < start) {
        int sz = 0;
        intarkdb::utf8proc_codepoint(input.c_str() + idx, sz);
        idx += sz;
        start_str = input.c_str() + idx;
        u_idx++;
    }

    if (idx >= input.size()) {
        return "";
    }
    size_t u_len = 0;
    size_t len_idx = 0;
    while (idx < input.size() && (int)u_len < len) {
        int sz = 0;
        intarkdb::utf8proc_codepoint(input.c_str() + idx, sz);
        idx += sz;
        len_idx += sz;
        u_len++;
    }
    return std::string(start_str, len_idx);
}

std::string UTF8Util::Substr(const std::string& input, size_t start) {
    auto len = Length(input);
    return Substr(input, start, len);
}

UTF8StringView::Iterator::Iterator(const char* str, size_t len, int idx) : str_(str), len_(len), idx_(idx) {
    if (idx_ < (int)len_) {
        int sz = 0;
        utf8proc_codepoint(str_ + idx_, sz);
        width_ = sz;
    }
}

std::string_view UTF8StringView::Iterator::operator*() const { return std::string_view(str_ + idx_, width_); }

UTF8StringView::Iterator& UTF8StringView::Iterator::operator++() {
    idx_ += width_;
    if (idx_ < (int)len_) {
        int sz = 0;
        utf8proc_codepoint(str_ + idx_, sz);
        width_ = sz;
    }
    return *this;
}

UTF8StringView::Iterator& UTF8StringView::Iterator::operator--() {
    idx_ -= width_;
    if ((uint8_t)(*(str_ + idx_)) < 0x80) {
        return *this;
    }
    while (((uint8_t)(*(str_ + idx_)) & 0xc0) == 0x80 && idx_ > 0) {
        --idx_;
    }
    int sz = 0;
    utf8proc_codepoint(str_ + idx_, sz);
    width_ = sz;
    return *this;
}

UTF8StringView::Iterator UTF8StringView::begin() const { return Iterator(str_, len_, 0); }

UTF8StringView::Iterator UTF8StringView::end() const { return Iterator(str_, len_, len_); }

bool UTF8StringView::Iterator::operator==(const Iterator& other) const {
    return str_ == other.str_ && len_ == other.len_ && idx_ == other.idx_;
}

bool UTF8StringView::Iterator::operator!=(const Iterator& other) const {
    return str_ != other.str_ || len_ != other.len_ || idx_ != other.idx_;
}

bool UTF8StringView::IsUTF8() const {
    size_t idx = 0;
    utf8proc_int32_t r;
    while (idx < len_) {
        int sz = 0;
        r = intarkdb::utf8proc_codepoint(str_ + idx, sz);
        if (r < 0) {
            // LOG
            GS_LOG_RUN_INF("handle string err=%s", intarkdb::utf8proc_errmsg(r));
            return false;
        }
        idx += sz;
    }
    return true;
}

std::string Lower(const std::string& str) {
    std::string copy(str);
    std::transform(copy.begin(), copy.end(), copy.begin(), [](unsigned char c) { return std::tolower(c); });
    return (copy);
}

void SplitString(const std::string& str, char delimiter, std::vector<std::string>& dest) {
    size_t found = str.rfind(delimiter);
    dest.push_back(str.substr(0, found));
    dest.push_back(str.substr(found + 1));
}

bool IsNumeric(const std::string& str) {
    for (char c : str) {
        if (!std::isdigit(c)) {
            return false;
        }
    }

    return true;
}

bool IsTime(const std::string& str) {
    std::regex pattern;
    if (str.length() == PART_NAME_SUFFIX_DAY_TIME_LEN) {
        pattern = "[0-9]{4}[0-9]{2}[0-9]{2}";
    } else if (str.length() == PART_NAME_SUFFIX_HOUR_TIME_LEN) {
        pattern = "[0-9]{4}[0-9]{2}[0-9]{2}[0-9]{2}";
    } else {
        GS_LOG_RUN_ERR("The time bound %s format error!", str.c_str());
        return GS_FALSE;
    }
    if (!regex_match(str, pattern)) {
        return GS_FALSE;
    }

    int32 year = stoi(str.substr(0, 4));
    int32 month = stoi(str.substr(4, 2));
    int32 day = stoi(str.substr(6, 2));
    int32 hour = 0;
    if (str.length() == PART_NAME_SUFFIX_HOUR_TIME_LEN) hour = stoi(str.substr(8, 2));

    if (year >= 1900 && month > 0 && month < 13 && hour <= 23 && hour >= 0) {
        switch (month) {
            case 1:
            case 3:
            case 5:
            case 7:
            case 8:
            case 10:
            case 12: {
                if (day > 31) {
                    return GS_FALSE;
                }
                break;
            }
            case 4:
            case 6:
            case 9:
            case 11: {
                if (day > 30) {
                    return GS_FALSE;
                }
                break;
            }
            case 2: {
                if (((year % 4 == 0 && year % 100 != 0) || year % 400 == 0)) {
                    if (day > 29) {
                        return GS_FALSE;
                    }
                } else {
                    if (day > 28) {
                        return GS_FALSE;
                    }
                }
                break;
            }
            default:
                return GS_FALSE;
        }
    } else {
        return GS_FALSE;
    }

    return GS_TRUE;
}

time_t StringToTime(char* pTime) {
    struct tm tm1 {
        0
    };
    time_t time1;
    if (strlen(pTime) == PART_NAME_SUFFIX_HOUR_TIME_LEN) {
        sscanf(pTime, "%4d%2d%2d%2d", &tm1.tm_year, &tm1.tm_mon, &tm1.tm_mday, &tm1.tm_hour);
    } else {
        sscanf(pTime, "%4d%2d%2d", &tm1.tm_year, &tm1.tm_mon, &tm1.tm_mday);
    }

    tm1.tm_year -= 1900;
    tm1.tm_mon -= 1;
    tm1.tm_sec = 0;
    tm1.tm_isdst = -1;
    time1 = mktime(&tm1);
    return time1;
}

bool IsSpace(char c) { return c == ' ' || c == '\t' || c == '\n' || c == '\v' || c == '\f' || c == '\r'; }

}  // namespace intarkdb
