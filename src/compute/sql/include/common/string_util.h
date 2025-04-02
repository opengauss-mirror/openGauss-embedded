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
 * string_util.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/common/string_util.h
 *
 * -------------------------------------------------------------------------
 */

#pragma once

#include <algorithm>
#include <iostream>
#include <regex>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "common/exception.h"
#include "utf8proc.hpp"

namespace intarkdb {

struct UTF8Util {
    static size_t Length(const std::string& input);
    static size_t Length(const char* input, size_t str_len);
    static std::string Substr(const std::string& input, size_t start, int len);
    static std::string Substr(const std::string& input, size_t start);
    static std::string TrimLeft(const std::string& input);
};

class StringUtil {
   public:
    static std::string Replace(std::string source, const std::string& from, const std::string& to);

    static bool EndsWith(const std::string& str, const std::string& suffix);

    static std::vector<std::string> SplitAllString(const std::string& str, char delimiter);

    static std::string Trim(const std::string& str);

    template <bool UPPER, typename T>
    static typename std::enable_if<std::is_convertible<T, std::string_view>::value, std::string>::type Convert(
        T&& str) {
        // support UTF-8
        std::string_view sv(str);
        std::vector<char> dest;
        dest.reserve(sv.length());
        auto input_length = sv.length();
        for (size_t i = 0; i < input_length;) {
            if (sv[i] & 0x80) {
                // non-ascii character
                int sz = 0, new_sz = 0;
                int codepoint = utf8proc_codepoint(sv.data() + i, sz);
                int converted_codepoint = UPPER ? utf8proc_toupper(codepoint) : utf8proc_tolower(codepoint);
                char buf[4];
                auto success = utf8proc_codepoint_to_utf8(converted_codepoint, new_sz, buf);
                if (!success) {
                    throw intarkdb::Exception(ExceptionType::EXECUTOR, "unknown utf-8 string");
                }
                // 因为存在大小写转换之后，字符长度会变化的可能，所以不可预分配内存
                // 比如字符 'ß' 转换为大写之后，长度变为3
                dest.insert(dest.end(), buf, buf + new_sz);
                i += sz;
            } else {
                // ascii
                dest.push_back(UPPER ? std::toupper(sv[i]) : std::tolower(sv[i]));
                i++;
            }
        }
        return std::string(dest.begin(), dest.end());
    }

    template <typename T>
    static std::string Lower(T&& str) {
        return Convert<false>(std::forward<T>(str));
    }

    template <typename T>
    static std::string Upper(T&& str) {
        return Convert<true>(std::forward<T>(str));
    }

    template <typename T, typename U>
    static typename std::enable_if<
        std::is_convertible<T, std::string_view>::value && std::is_convertible<U, std::string_view>::value, bool>::type
    IsEqualIgnoreCase(T& str1, U& str2) {
        std::string_view sv1(str1);
        std::string_view sv2(str2);
        if (sv1.size() != sv2.size()) {
            return false;
        }
        return std::equal(sv1.begin(), sv1.end(), sv2.begin(),
                          [](char c1, char c2) { return std::tolower(c1) == std::tolower(c2); });
    }

    static bool IsEqualIgnoreCase(const char* str1, size_t str_n, const char* str2, size_t str2_n) {
        std::string_view sv1(str1, str_n);
        std::string_view sv2(str2, str2_n);
        if (sv1.size() != sv2.size()) {
            return false;
        }
        return std::equal(sv1.begin(), sv1.end(), sv2.begin(),
                          [](char c1, char c2) { return std::tolower(c1) == std::tolower(c2); });
    }

    static size_t Length(const std::string& input) { return UTF8Util::Length(input); }
};

class UTF8StringView {
   public:
    explicit UTF8StringView(const std::string& s) {
        str_ = s.c_str();
        len_ = s.size();
    }
    explicit UTF8StringView(std::string_view s) {
        str_ = s.data();
        len_ = s.size();
    }
    // 判断是否合法的utf8字符串
    bool IsUTF8() const;
    size_t Length() const { return UTF8Util::Length(str_); }

    class Iterator {
       public:
        Iterator(const char* str, size_t len, int idx);

        std::string_view operator*() const;
        Iterator& operator++();
        Iterator& operator--();
        bool operator==(const Iterator& other) const;
        bool operator!=(const Iterator& other) const;

        int GetIdx() const { return idx_; }
        int GetWidth() const { return width_; }

       private:
        const char* str_ = nullptr;
        size_t len_ = 0;
        int idx_ = 0;
        size_t width_ = 1;
    };

    Iterator begin() const;
    Iterator end() const;

   private:
    const char* str_ = nullptr;
    size_t len_ = 0;
    size_t idx_ = 0;
};

void SplitString(const std::string& str, char delimiter, std::vector<std::string>& dest);

bool IsNumeric(const std::string& str);

bool IsTime(const std::string& str);

time_t StringToTime(char* pTime);

bool IsSpace(char c);

struct CaseInsensitiveCompare {
    bool operator()(const std::string& a, const std::string& b) const {
        return intarkdb::StringUtil::IsEqualIgnoreCase(a, b);
    }
};

struct CaseInsensitiveHash {
    size_t operator()(const std::string& s) const { return std::hash<std::string>{}(intarkdb::StringUtil::Lower(s)); }
};

using CaseInsensitiveSet = std::unordered_set<std::string, CaseInsensitiveHash, CaseInsensitiveCompare>;

template <typename T>
using CaseInsensitiveMap = std::unordered_map<std::string, T, CaseInsensitiveHash, CaseInsensitiveCompare>;

}  // namespace intarkdb
