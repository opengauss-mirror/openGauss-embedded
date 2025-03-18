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
* like_operator.cpp
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/type/operator/like_operator.cpp
*
* -------------------------------------------------------------------------
*/
#include "type/operator/like_operator.h"
#include "common/string_util.h"
#include <regex>

static auto GetChar(const std::string& val) -> char {
    if (val.size() > 1) {
        throw intarkdb::Exception(ExceptionType::EXECUTOR, "invalid escape string: " + val);
    }
    return val.size() > 0 ? val[0] : '\0';
}

constexpr char SINGLE_CHAR = '_';
constexpr char WILDCARD = '%';

static std::string ConvertLikePatternToRegex(const std::string& likePattern, char escapeChar) {
    std::string regexPattern;
    bool escapeNext = false;
    for (char c : likePattern) {
        if (escapeNext) {
            regexPattern += "\\";
            regexPattern += c;
            escapeNext = false;
        } else if (c == escapeChar) {
            escapeNext = true;
        } else {
            switch (c) {
                case WILDCARD:
                    regexPattern += "(.|\r|\n)*";
                    break;
                case SINGLE_CHAR:
                    regexPattern += "(.|\r|\n)";
                    break;
                case '.':
                case '*':
                case '+':
                case '?':
                case '(':
                case ')':
                case '[':
                case ']':
                case '{':
                case '}':
                case '^':
                case '$':
                case '|':
                case '\\':
                    // 以上字符在正则表达式中有特殊含义，需要转义
                    regexPattern += "\\";
                    regexPattern += c;
                    break;
                default:
                    regexPattern += c;
                    break;
            }
        }
    }
    if (escapeNext) {
        throw std::invalid_argument("Like pattern must not end with escape character!");
    }
    return regexPattern;
}

static bool HasMatchWithRegex(const std::string& target, const std::string& pattern) {
    std::regex re(pattern);
    return std::regex_match(target, re);
}

static bool HasMatchWithRegexCaseInsensitive(const std::string& target, const std::string& pattern) {
    std::string lower_target = intarkdb::StringUtil::Lower(target);
    std::string lower_pattern = intarkdb::StringUtil::Lower(pattern);
    std::regex re(lower_pattern);
    return std::regex_match(lower_target, re);
}

Value LikeOperator(LikeType type, const Value& left, const Value& right, const Value& escape) {
    if (left.IsNull() || right.IsNull() || escape.IsNull()) {
        // null boolean
        return ValueFactory::ValueNull();
    }
    auto target = left.GetCastAs<std::string>();
    auto pattern = right.GetCastAs<std::string>();
    bool is_match = false;
    auto regex_pattern = ConvertLikePatternToRegex(pattern,GetChar(escape.GetCastAs<std::string>()));
    switch (type) {
        case LikeType::Like:
        case LikeType::LikeEscape: {
            is_match = HasMatchWithRegex(target, regex_pattern);
            break;
        }
        case LikeType::NotLikeEscape:
        case LikeType::NotLike: {
            is_match = !HasMatchWithRegex(target, regex_pattern);
            break;
        }
        case LikeType::ILike:
        case LikeType::ILikeEscape: {
            is_match = HasMatchWithRegexCaseInsensitive(target, regex_pattern);
            break;
        }
        case LikeType::NotILike:
        case LikeType::NotILikeEscape: {
            is_match = !HasMatchWithRegexCaseInsensitive(target, regex_pattern);
            break;
        }
        case LikeType::Glob:
        default:
            throw std::runtime_error(fmt::format("this {} like type not impl", type));
    }
    return ValueFactory::ValueBool(is_match);
}
