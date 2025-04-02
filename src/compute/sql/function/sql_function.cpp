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
 * sql_function.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/function/sql_function.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "function/sql_function.h"

#include <algorithm>
#include <functional>
#include <random>
#include <unordered_set>

#include "cm_date.h"
#include "common/default_value.h"
#include "common/exception.h"
#include "common/string_util.h"
#include "function/date/date_function.h"
#include "type/type_str.h"
#include "utf8proc.hpp"

// TODO SQL 函数，考虑如何实现 相同函数名，不同参数类型和参数数量的重载和分发
auto current_date = [](const std::vector<Value>&) -> Value {
    int64_t date_ts = static_cast<int64_t>(cm_date_now());
    date_detail_t detail{0};
    cm_decode_date(static_cast<date_t>(date_ts), &detail);
    date_detail_t new_detail{0};
    new_detail.year = detail.year;
    new_detail.mon = detail.mon;
    new_detail.day = detail.day;
    date_ts = cm_encode_date(&new_detail);
    date_ts -= CM_UNIX_EPOCH;
    return ValueFactory::ValueDate(date_stor_t{date_ts});
};

auto now = [](const std::vector<Value>&) -> Value {
    timestamp_stor_t t;
    t.ts = static_cast<int64_t>(cm_utc_now());
    t.ts -= CM_UNIX_EPOCH;
    return ValueFactory::ValueTimeStamp(t);
};

static const int64_t SUPPORTED_UPPER_BOUND = NumericLimits<uint32_t>::Maximum();
static const int64_t SUPPORTED_LOWER_BOUND = -SUPPORTED_UPPER_BOUND - 1;

static inline void AssertInSupportedRange(uint64_t input_size, int64_t offset, int64_t length) {
    if (input_size > static_cast<uint64_t>(SUPPORTED_UPPER_BOUND)) {
        throw intarkdb::Exception(ExceptionType::OUT_OF_RANGE,
                                  fmt::format("Substring input size is too large (> {})", SUPPORTED_UPPER_BOUND));
    }
    if (offset < SUPPORTED_LOWER_BOUND) {
        throw intarkdb::Exception(
            ExceptionType::OUT_OF_RANGE,
            fmt::format("Substring offset outside of supported range (< {})", SUPPORTED_LOWER_BOUND));
    }
    if (offset > SUPPORTED_UPPER_BOUND) {
        throw intarkdb::Exception(ExceptionType::OUT_OF_RANGE,
                                  fmt::format("Substring offset outside of supported (> {})", SUPPORTED_UPPER_BOUND));
    }
    if (length < SUPPORTED_LOWER_BOUND) {
        throw intarkdb::Exception(
            ExceptionType::OUT_OF_RANGE,
            fmt::format("Substring length outside of supported range (< {})", SUPPORTED_LOWER_BOUND));
    }
    if (length > SUPPORTED_UPPER_BOUND) {
        throw intarkdb::Exception(
            ExceptionType::OUT_OF_RANGE,
            fmt::format("Substring length outside of supported range (> {})", SUPPORTED_UPPER_BOUND));
    }
}

bool SubstringStartEnd(int64_t input_size, int64_t offset, int64_t length, int64_t& start, int64_t& end) {
    if (length == 0) {
        return false;
    }
    size_t start_pos = 0;
    size_t end_pos = 0;
    if (offset < 0) {
        start_pos = 0;
        end_pos = std::string::npos;
        offset--;
        if (length < 0) {
            start = -offset - length;
            end = -offset;
        } else {
            start = -offset;
            end = -offset - length;
        }
        if (end <= 0) {
            end_pos = input_size;
        }
        int64_t current_character = 0;
        for (int64_t i = input_size; i > 0; --i) {
            ++current_character;
            if (current_character == start) {
                start_pos = i;
                break;
            } else if (current_character == end) {
                end_pos = i;
            }
        }
        if (end_pos == std::string::npos) {
            return false;
        }
    } else {
        // positive offset: scan from start
        start_pos = std::string::npos;
        end_pos = input_size;
        offset--;
        if (length < 0) {
            start = std::max<int64_t>(0, offset + length);
            end = offset;
        } else {
            start = std::max<int64_t>(0, offset);
            end = offset + length;
        }
        int64_t current_character = 0;
        for (int64_t i = 0; i < input_size; ++i) {
            if (current_character == end) {
                end_pos = i;
                break;
            } else if (current_character == start) {
                start_pos = i;
            }
            ++current_character;
        }
        if (start_pos == std::string::npos || end == 0 || end <= start) {
            return false;
        }
    }
    start = start_pos;
    end = end_pos;
    return true;
}

std::string substr(const std::string& str, int64_t pos, int64_t length) {
    size_t str_length = intarkdb::UTF8Util::Length(str);
    AssertInSupportedRange(str.size(), pos, length);
    if (length == 0) {
        return "";
    }

    int64_t start;
    int64_t end;
    if (!SubstringStartEnd(str_length, pos, length, start, end)) {
        return "";
    }
    return intarkdb::UTF8Util::Substr(str, start, end - start);
}

std::string substr(const std::string& str, int pos) { return substr(str, pos, std::numeric_limits<uint32>::max()); }

constexpr int FULL_SUBSTR_ARG_NUM = 3;
constexpr int SHORT_SUBSTR_ARG_NUM = 2;

auto sql_substr = [](const std::vector<Value>& values) -> Value {
    if (values.size() != FULL_SUBSTR_ARG_NUM && values.size() != SHORT_SUBSTR_ARG_NUM) {
        throw intarkdb::Exception(ExceptionType::EXECUTOR, "substr function parameter number error");
    }

    if (values[0].IsNull()) {
        return ValueFactory::ValueNull();
    }

    if (values[0].GetType() != GS_TYPE_STRING && values[0].GetType() != GS_TYPE_VARCHAR) {
        throw intarkdb::Exception(ExceptionType::EXECUTOR, "substr function parameter type error");
    }

    if (values.size() == SHORT_SUBSTR_ARG_NUM && values[1].IsNull()) {
        return ValueFactory::ValueNull();
    }

    if (values.size() == FULL_SUBSTR_ARG_NUM && (values[1].IsNull() || values[2].IsNull())) {
        return ValueFactory::ValueNull();
    }

    if (values.size() == FULL_SUBSTR_ARG_NUM && (!values[1].IsInteger() || !values[2].IsInteger())) {
        throw intarkdb::Exception(ExceptionType::EXECUTOR, "substr function parameter type error");
    }

    if (values.size() == SHORT_SUBSTR_ARG_NUM && (!values[1].IsInteger())) {
        throw intarkdb::Exception(ExceptionType::EXECUTOR, "substr function parameter type error");
    }

    auto str = values[0].GetCastAs<std::string>();
    auto start = values[1].GetCastAs<int32_t>();
    if (values.size() == FULL_SUBSTR_ARG_NUM) {
        auto length = values[2].GetCastAs<int32_t>();
        return ValueFactory::ValueVarchar(substr(str, start, length));
    } else {
        return ValueFactory::ValueVarchar(substr(str, start));
    }
};

auto sql_concat(const std::vector<Value>& values) -> Value {
    if (values.size() == 0) {
        throw intarkdb::Exception(ExceptionType::EXECUTOR, "concat function parameter number error");
    }
    std::string result;
    for (const auto& value : values) {
        if (value.IsNull()) {
            continue;
        }
        result.append(value.GetCastAs<std::string>());
    }
    return ValueFactory::ValueVarchar(std::move(result));
}

constexpr int CONCAT_WS_ARG_NUM = 2;
auto sql_concat_ws(const std::vector<Value>& values) -> Value {
    if (values.size() < CONCAT_WS_ARG_NUM) {
        throw intarkdb::Exception(ExceptionType::EXECUTOR, "concat_ws function parameter number error");
    }
    if (values[0].IsNull()) {
        return ValueFactory::ValueNull();
    }
    std::string sep = values[0].GetCastAs<std::string>();
    bool is_first_char = true;
    std::string result;
    for (size_t i = 1; i < values.size(); ++i) {
        if (values[i].IsNull()) {
            continue;
        }
        if (i > 1 && !is_first_char) {
            result.append(sep);
        }
        result.append(values[i].GetCastAs<std::string>());
        is_first_char = false;
    }
    return ValueFactory::ValueVarchar(std::move(result));
}

constexpr int LENGTH_ARG_NUM = 1;
auto sql_length(const std::vector<Value>& values) -> Value {
    if (values.size() != LENGTH_ARG_NUM) {
        throw intarkdb::Exception(ExceptionType::EXECUTOR, "function parameter number error");
    }
    if (values[0].IsNull()) {
        return ValueFactory::ValueNull();
    }
    return ValueFactory::ValueBigInt(intarkdb::UTF8Util::Length(values[0].GetCastAs<std::string>()));
}

constexpr int REPEAT_ARG_NUM = 2;
auto sql_repeat(const std::vector<Value>& values) -> Value {
    if (values.size() != REPEAT_ARG_NUM) {
        throw intarkdb::Exception(ExceptionType::EXECUTOR, "function parameter number error");
    }
    if (values[0].IsNull() || values[1].IsNull()) {
        return ValueFactory::ValueNull();
    }
    if (!values[1].IsInteger()) {
        throw intarkdb::Exception(ExceptionType::EXECUTOR, "function parameter type error");
    }
    auto str = values[0].GetCastAs<std::string>();
    auto count = values[1].GetCastAs<int64_t>();
    if (count < 0) {
        return ValueFactory::ValueVarchar("");
    }
    std::string result;
    for (int32_t i = 0; i < count; ++i) {
        result.append(str);
    }
    return ValueFactory::ValueVarchar(std::move(result));
}

auto ltrim_handle(std::string& str, const std::unordered_set<std::string_view>& sep_set) {
    intarkdb::UTF8StringView str_view(str);
    if (!str_view.IsUTF8()) {
        throw intarkdb::Exception(ExceptionType::EXECUTOR, "not supported non-utf-8 string");
    }
    for (auto it = str_view.begin(); it != str_view.end(); ++it) {
        if (sep_set.find(*it) == sep_set.end()) {
            str = str.substr(it.GetIdx());
            return;
        }
    }
    str = "";
}

auto rtrim_handle(std::string& str, const std::unordered_set<std::string_view>& sep_set) {
    intarkdb::UTF8StringView str_view(str);
    if (!str_view.IsUTF8()) {
        throw intarkdb::Exception(ExceptionType::EXECUTOR, "not supported non-utf-8 string");
    }
    for (auto it = str_view.end(); it != str_view.begin();) {
        --it;
        if (sep_set.find(*it) == sep_set.end()) {
            str = str.substr(0, it.GetIdx() + it.GetWidth());
            return;
        }
    }
    str = "";
}

constexpr int FULL_TRIM_ARG_NUM = 2;
constexpr int SHORT_TRIM_ARG_NUM = 1;
auto trim_handle(bool left, bool right, const std::vector<Value>& values) -> Value {
    if (values.size() != SHORT_TRIM_ARG_NUM && values.size() != FULL_TRIM_ARG_NUM) {
        throw intarkdb::Exception(ExceptionType::EXECUTOR, "function parameter number error");
    }
    if (values[0].IsNull() || (values.size() == FULL_TRIM_ARG_NUM && values[1].IsNull())) {
        return ValueFactory::ValueNull();
    }
    std::string result = values[0].GetCastAs<std::string>();
    std::string sep = " ";
    if (values.size() == FULL_TRIM_ARG_NUM) {
        sep = values[1].GetCastAs<std::string>();
    }

    intarkdb::UTF8StringView sep_view(sep);
    if (!sep_view.IsUTF8()) {
        throw intarkdb::Exception(ExceptionType::EXECUTOR, "not supported non-utf-8 string");
    }
    std::unordered_set<std::string_view> sep_set;
    for (const auto& s : sep_view) {
        sep_set.insert(s);
    }
    if (left) {
        ltrim_handle(result, sep_set);
    }
    if (right) {
        rtrim_handle(result, sep_set);
    }
    return ValueFactory::ValueVarchar(std::move(result));
}

auto sql_trim = [](const std::vector<Value>& values) -> Value { return trim_handle(true, true, values); };
auto sql_ltrim = [](const std::vector<Value>& values) -> Value { return trim_handle(true, false, values); };
auto sql_rtrim = [](const std::vector<Value>& values) -> Value { return trim_handle(false, true, values); };

constexpr int CONTAINS_ARG_NUM = 2;
auto sql_contains(const std::vector<Value>& values) -> Value {
    if (values.size() != CONTAINS_ARG_NUM) {
        throw intarkdb::Exception(ExceptionType::EXECUTOR, "function parameter number error");
    }
    if (values[0].IsNull() || values[1].IsNull()) {
        return ValueFactory::ValueNull();
    }
    const std::string& str = values[0].GetCastAs<std::string>();
    const std::string& target = values[1].GetCastAs<std::string>();
    return ValueFactory::ValueBool(str.find(target) != std::string::npos);
}

constexpr int STARTS_WITH_ARG_NUM = 2;
auto sql_starts_with(const std::vector<Value>& values) -> Value {
    if (values.size() != STARTS_WITH_ARG_NUM) {
        throw intarkdb::Exception(ExceptionType::EXECUTOR, "function parameter number error");
    }
    if (values[0].IsNull() || values[1].IsNull()) {
        return ValueFactory::ValueNull();
    }
    const std::string& str = values[0].GetCastAs<std::string>();
    const std::string& target = values[1].GetCastAs<std::string>();
    return ValueFactory::ValueBool(str.find(target) == 0);
}

constexpr int ENDS_WITH_ARG_NUM = 2;
auto sql_ends_with(const std::vector<Value>& values) -> Value {
    if (values.size() != ENDS_WITH_ARG_NUM) {
        throw intarkdb::Exception(ExceptionType::EXECUTOR, "function parameter number error");
    }
    if (values[0].IsNull() || values[1].IsNull()) {
        return ValueFactory::ValueNull();
    }
    const std::string& str = values[0].GetCastAs<std::string>();
    const std::string& target = values[1].GetCastAs<std::string>();
    return ValueFactory::ValueBool(str.rfind(target) == (str.length() - target.length()));
}

constexpr int REVERSE_ARG_NUM = 1;
auto sql_reverse(const std::vector<Value>& values) -> Value {
    if (values.size() != REVERSE_ARG_NUM) {
        throw intarkdb::Exception(ExceptionType::EXECUTOR, "function parameter number error");
    }
    if (values[0].IsNull()) {
        return ValueFactory::ValueNull();
    }
    const std::string& str = values[0].GetCastAs<std::string>();
    std::vector<std::string_view> items;
    intarkdb::UTF8StringView str_view(str);
    if (!str_view.IsUTF8()) {
        throw intarkdb::Exception(ExceptionType::EXECUTOR, "not supported non-utf-8 string");
    }
    for (auto iter = str_view.begin(); iter != str_view.end(); ++iter) {
        items.emplace_back(*iter);
    }
    std::string result;
    result.reserve(str.size());
    for (auto it = items.rbegin(); it != items.rend(); ++it) {
        result.append(*it);
    }
    return ValueFactory::ValueVarchar(std::move(result));
}

constexpr int REPLACE_ARG_NUM = 3;
constexpr int REPLACE_INPUT_IDX = 0;
constexpr int REPLACE_SOURCE_IDX = 1;
constexpr int REPLACE_TARGET_IDX = 2;
auto sql_replace(const std::vector<Value>& values) -> Value {
    if (values.size() != REPLACE_ARG_NUM) {
        throw intarkdb::Exception(ExceptionType::EXECUTOR, "function parameter number error");
    }
    if (values[REPLACE_INPUT_IDX].IsNull() || values[REPLACE_SOURCE_IDX].IsNull() ||
        values[REPLACE_TARGET_IDX].IsNull()) {
        return ValueFactory::ValueNull();
    }
    const std::string& str = values[REPLACE_INPUT_IDX].GetCastAs<std::string>();
    const std::string& source = values[REPLACE_SOURCE_IDX].GetCastAs<std::string>();
    const std::string& target = values[REPLACE_TARGET_IDX].GetCastAs<std::string>();
    std::string result = "";
    result.reserve(str.size());
    size_t pos = 0;
    while (pos < str.size()) {
        auto next_pos = str.find(source, pos);
        if (next_pos == std::string::npos) {
            result.append(str.substr(pos));
            break;
        }
        result.append(str.substr(pos, next_pos - pos));
        result.append(target);
        pos = next_pos + source.size();
    }
    return ValueFactory::ValueVarchar(std::move(result));
}

auto sql_random = [](const std::vector<Value>&) -> Value {
    // 创建随机数生成器，并设置种子
    std::mt19937 rng(std::random_device{}());

    // 创建分布函数，指定随机数范围
    std::uniform_real_distribution<double> dist(0.0, 1.0);

    // 生成随机数
    double random_number = dist(rng);
    return ValueFactory::ValueDouble(random_number);
};

auto get_year = [](const std::vector<Value>& values) -> Value {
    auto detail = CheckParamsAndDecodeDate(values);
    return ValueFactory::ValueBigInt(detail.year);
};

auto get_month = [](const std::vector<Value>& values) -> Value {
    auto detail = CheckParamsAndDecodeDate(values);
    return ValueFactory::ValueBigInt(detail.mon);
};

auto get_day = [](const std::vector<Value>& values) -> Value {
    auto detail = CheckParamsAndDecodeDate(values);
    return ValueFactory::ValueBigInt(detail.day);
};

auto get_hour = [](const std::vector<Value>& values) -> Value {
    auto detail = CheckParamsAndDecodeDate(values);
    return ValueFactory::ValueBigInt(detail.hour);
};

auto get_minute = [](const std::vector<Value>& values) -> Value {
    auto detail = CheckParamsAndDecodeDate(values);
    return ValueFactory::ValueBigInt(detail.min);
};

constexpr int UNIX_TIMESTAMP_ARG_NUM = 1;
auto sql_unix_timestamp = [](const std::vector<Value>& values) -> Value {
    if (values.size() != UNIX_TIMESTAMP_ARG_NUM) {
        throw intarkdb::Exception(ExceptionType::EXECUTOR, "parameter number error");
    }
    const auto& value = values[0];
    if (value.IsNull()) {
        throw intarkdb::Exception(ExceptionType::EXECUTOR, "parameter is null");
    }
    if (value.GetType() != GS_TYPE_DATE && value.GetType() != GS_TYPE_TIMESTAMP) {
        throw intarkdb::Exception(ExceptionType::EXECUTOR, "parameter type error");
    }
    auto date = value.GetCastAs<timestamp_stor_t>();
    return ValueFactory::ValueBigInt(date.ts);
};

constexpr int MAKE_DATE_ARG_NUM = 3;
auto make_date(const std::vector<Value>& values) -> Value {
    if (values.size() != MAKE_DATE_ARG_NUM) {
        throw intarkdb::Exception(ExceptionType::EXECUTOR, "parameter number error");
    }
    for (const auto& value : values) {
        if (value.IsNull()) {
            throw intarkdb::Exception(ExceptionType::EXECUTOR, "parameter is null");
        }
        if (!value.IsInteger()) {
            throw intarkdb::Exception(ExceptionType::EXECUTOR, "parameter type error");
        }
    }
    auto year = values[0].GetCastAs<int32_t>();
    auto month = values[1].GetCastAs<int32_t>();
    auto day = values[2].GetCastAs<int32_t>();
    date_stor_t date;
    if (!DateUtils::ToDate(year, month, day, date)) {
        throw intarkdb::Exception(ExceptionType::EXECUTOR, "parameter invalid value error");
    }
    // 默认make_date(year,mon,day)是一个带有期望时区的日期
    return ValueFactory::ValueDate(date);
}

// 接受单个字符串参数，且处理函数是逐个字符进行处理
constexpr int TRANSFORM_ARG_NUM = 1;
auto sql_string_lower(const std::vector<Value>& values) -> Value {
    if (values.size() != TRANSFORM_ARG_NUM) {
        throw intarkdb::Exception(ExceptionType::EXECUTOR, "lower function parameter number error");
    }
    if (values[0].IsNull()) {
        return values[0];  // null 不处理
    }
    if (values[0].GetType() != GS_TYPE_STRING && values[0].GetType() != GS_TYPE_VARCHAR) {
        throw intarkdb::Exception(ExceptionType::EXECUTOR, "lower function parameter type error");
    }
    auto str = values[0].GetCastAs<std::string>();
    return ValueFactory::ValueVarchar(intarkdb::StringUtil::Lower(str));
};

auto sql_string_upper(const std::vector<Value>& values) -> Value {
    if (values.size() != TRANSFORM_ARG_NUM) {
        throw intarkdb::Exception(ExceptionType::EXECUTOR, "upper function parameter number error");
    }
    if (values[0].IsNull()) {
        return values[0];  // null 不处理
    }
    if (values[0].GetType() != GS_TYPE_STRING && values[0].GetType() != GS_TYPE_VARCHAR) {
        throw intarkdb::Exception(ExceptionType::EXECUTOR, "upper function parameter type error");
    }
    auto str = values[0].GetCastAs<std::string>();
    return ValueFactory::ValueVarchar(intarkdb::StringUtil::Upper(str));
};

auto sql_to_type_string(const std::vector<Value>& values) -> Value {
    if (values.size() != 1) {
        throw intarkdb::Exception(ExceptionType::EXECUTOR, "to_type_string function parameter number error");
    }
    if (values[0].IsNull()) {
        return ValueFactory::ValueVarchar("");
    }
    return ValueFactory::ValueVarchar(fmt::format("{}", static_cast<GStorDataType>(values[0].GetCastAs<int32_t>())));
}

const int DEAL_DEFAULT_VALUE_ARG_NUM = 2;

auto sql_default_value_to_string(const std::vector<Value>& values) -> Value {
    if (values.size() != DEAL_DEFAULT_VALUE_ARG_NUM) {
        throw intarkdb::Exception(ExceptionType::EXECUTOR, "default_value_to_string function parameter number error");
    }
    if (values[0].IsNull() || values[1].IsNull()) {
        return ValueFactory::ValueVarchar("null");
    }
    DefaultValue* value = reinterpret_cast<DefaultValue*>(const_cast<char*>(values[0].GetRawBuff()));
    return ShowValue(*value, static_cast<GStorDataType>(values[1].GetCastAs<int32_t>()));
}

auto sql_to_null_string(const std::vector<Value>& values) -> Value {
    if (values.size() != 1) {
        throw intarkdb::Exception(ExceptionType::EXECUTOR, "to_null_string function parameter number error");
    }
    std::string result = "";
    if (values[0].IsNull()) {
        result = "null";
    } else {
        result = values[0].ToString();
    }
    return ValueFactory::ValueVarchar(result);
}

auto sql_to_empty_string(const std::vector<Value>& values) -> Value {
    if (values.size() != 1) {
        throw intarkdb::Exception(ExceptionType::EXECUTOR, "to_empty_string function parameter number error");
    }
    std::string result = "";
    if (values[0].IsNull()) {
        result = "";
    } else {
        result = values[0].ToString();
    }
    return ValueFactory::ValueVarchar(result);
}

auto sql_boolean_to_string(const std::vector<Value>& values) -> Value {
    if (values.size() != 1) {
        throw intarkdb::Exception(ExceptionType::EXECUTOR, "boolean_to_string function parameter number error");
    }
    if (values[0].IsNull()) {
        return ValueFactory::ValueVarchar("null");
    }
    return ValueFactory::ValueVarchar(values[0].GetCastAs<uint32_t>() == 0 ? "false" : "true");
}

auto sql_typeof(const std::vector<Value>& values) -> Value {
    if (values.size() != 1) {
        throw intarkdb::Exception(ExceptionType::EXECUTOR, "typeof function parameter number error");
    }
    return ValueFactory::ValueVarchar(fmt::format("{}", values[0].GetType()));
}

const char* INDEX_TYPE_PRI = "PRI";
const char* INDEX_TYPE_UNI = "UNI";
const char* INDEX_TYPE_MUL = "MUL";

const int FORMAT_KEY_TO_STRING_ARG_NUM = 2;

auto sql_format_key_to_string(const std::vector<Value>& values) -> Value {
    if (values.size() != FORMAT_KEY_TO_STRING_ARG_NUM) {
        throw intarkdb::Exception(ExceptionType::EXECUTOR, "format_key_to_string function parameter number error");
    }
    std::string result = "";
    if (values[0].IsNull() && values[1].IsNull()) {
        result = "";
    } else if (!values[0].IsNull() && values[0].GetCastAs<int32_t>() == 1) {
        result = INDEX_TYPE_PRI;
    } else if (!values[1].IsNull() && values[1].GetCastAs<int32_t>() == 1) {
        result = INDEX_TYPE_UNI;
    } else {
        result = INDEX_TYPE_MUL;
    }
    return ValueFactory::ValueVarchar(result);
}

auto sql_is_valid_col(const std::vector<Value>& values) -> Value {
    if (values.size() != 1) {
        throw intarkdb::Exception(ExceptionType::EXECUTOR, "is_deleted function parameter number error");
    }
    if (values[0].IsNull()) {
        return ValueFactory::ValueBool(false);
    }
    return ValueFactory::ValueBool((values[0].GetCastAs<int32_t>() & COLUMN_DELETED) == 0);
}

auto sql_is_auto_increment(const std::vector<Value>& values) -> Value {
    if (values.size() != 1) {
        throw intarkdb::Exception(ExceptionType::EXECUTOR, "is_auto_increment function parameter number error");
    }
    if (values[0].IsNull()) {
        return ValueFactory::ValueVarchar("");
    }
    return ValueFactory::ValueVarchar((values[0].GetCastAs<uint32_t>() & COLUMN_SERIAL) != 0 ? "autoincrement" : "");
}

const std::map<std::string, FuncInfo> SQLFunction::FUNC_MAP = {
    // date function
    {"current_date", {current_date, GS_TYPE_DATE}},
    {"year", {get_year, GS_TYPE_BIGINT}},
    {"month", {get_month, GS_TYPE_BIGINT}},
    {"day", {get_day, GS_TYPE_BIGINT}},
    {"hour", {get_hour, GS_TYPE_BIGINT}},
    {"minute", {get_minute, GS_TYPE_BIGINT}},
    {"make_date", {make_date, GS_TYPE_DATE}},
    {"datediff", {date_diff, GS_TYPE_INTEGER}},
    {"timestampdiff", {timestamp_diff, GS_TYPE_BIGINT}},
    {"date_add", {date_add, GS_TYPE_VARCHAR}},
    {"date_sub", {date_sub, GS_TYPE_VARCHAR}},
    {"date_format", {date_format, GS_TYPE_VARCHAR}},

    // timestamp function
    {"now", {now, GS_TYPE_TIMESTAMP}},
    {"unix_timestamp", {sql_unix_timestamp, GS_TYPE_BIGINT}},
    // string function
    {"substr", {sql_substr, GS_TYPE_VARCHAR}},
    {"tolower", {sql_string_lower, GS_TYPE_VARCHAR}},
    {"toupper", {sql_string_upper, GS_TYPE_VARCHAR}},
    {"lower", {sql_string_lower, GS_TYPE_VARCHAR}},
    {"upper", {sql_string_upper, GS_TYPE_VARCHAR}},
    {"concat", {sql_concat, GS_TYPE_VARCHAR}},
    {"concat_ws", {sql_concat_ws, GS_TYPE_VARCHAR}},
    {"length", {sql_length, GS_TYPE_BIGINT}},
    {"repeat", {sql_repeat, GS_TYPE_VARCHAR}},
    {"ltrim", {sql_ltrim, GS_TYPE_VARCHAR}},
    {"rtrim", {sql_rtrim, GS_TYPE_VARCHAR}},
    {"trim", {sql_trim, GS_TYPE_VARCHAR}},
    {"contains", {sql_contains, GS_TYPE_BOOLEAN}},
    {"starts_with", {sql_starts_with, GS_TYPE_BOOLEAN}},
    {"ends_with", {sql_ends_with, GS_TYPE_BOOLEAN}},
    {"reverse", {sql_reverse, GS_TYPE_VARCHAR}},
    {"replace", {sql_replace, GS_TYPE_VARCHAR}},
    // math function
    {"random", {sql_random, GS_TYPE_REAL}},
    // type function
    {"typeof",{sql_typeof,GS_TYPE_VARCHAR}},

    // 内置特殊函数
    {"_to_type_string", {sql_to_type_string, GS_TYPE_VARCHAR}},                    // typeId -> string
    {"_default_value_to_string", {sql_default_value_to_string, GS_TYPE_VARCHAR}},  // (default value,typeId) -> string
    {"_to_null_string", {sql_to_null_string, GS_TYPE_VARCHAR}},
    {"_to_empty_string", {sql_to_empty_string, GS_TYPE_VARCHAR}},
    {"_boolean_to_string", {sql_boolean_to_string, GS_TYPE_VARCHAR}},
    {"_format_key_to_string", {sql_format_key_to_string, GS_TYPE_VARCHAR}},
    {"_is_valid_col", {sql_is_valid_col, GS_TYPE_BOOLEAN}},
    {"_is_auto_increment", {sql_is_auto_increment, GS_TYPE_VARCHAR}},
};
