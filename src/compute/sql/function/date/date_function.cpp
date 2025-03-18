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
* date_function.cpp
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/include/function/date_function.cpp
*
* -------------------------------------------------------------------------
*/
#include <vector>

#include "type/value.h"
#include "common/string_util.h"
#include "type/type_system.h"
#include "function/date/date_function.h"

// YEAR/QUARTER/MONTH/WEEK/DAY/HOUR/MINUTE/SECOND/MILLISECOND/MICROSECOND
const std::string INTERVAL_TYPE_YEAR = "YEAR";
const std::string INTERVAL_TYPE_QUARTER = "QUARTER";
const std::string INTERVAL_TYPE_MONTH = "MONTH";
const std::string INTERVAL_TYPE_WEEK = "WEEK";
const std::string INTERVAL_TYPE_DAY = "DAY";
const std::string INTERVAL_TYPE_HOUR = "HOUR";
const std::string INTERVAL_TYPE_MINUTE = "MINUTE";
const std::string INTERVAL_TYPE_SECOND = "SECOND";
const std::string INTERVAL_TYPE_MILLISECOND = "MILLISECOND";
const std::string INTERVAL_TYPE_MICROSECOND = "MICROSECOND";

constexpr uint32_t DECODE_DATE_ARG_NUM = 1;
date_detail_t CheckParamsAndDecodeDate(const std::vector<Value>& values) {
    if (values.size() != DECODE_DATE_ARG_NUM) {
        throw intarkdb::Exception(ExceptionType::EXECUTOR, "parameter number error");
    }
    auto& value = const_cast<Value &>(values[0]);
    if (value.IsNull()) {
        throw intarkdb::Exception(ExceptionType::EXECUTOR, "parameter is null");
    }
    if (value.GetType() != GS_TYPE_DATE && value.GetType() != GS_TYPE_TIMESTAMP) {
        if (value.GetType() == GStorDataType::GS_TYPE_VARCHAR) {
            value = DataType::GetTypeInstance(GStorDataType::GS_TYPE_TIMESTAMP)->CastValue(value);
        } else {
            throw intarkdb::Exception(ExceptionType::EXECUTOR, "parameter type error");
        }
    }
    auto date = value.GetCastAs<timestamp_stor_t>();
    date_detail_t detail;
    date.ts += TIMEZONE_GET_MICROSECOND(cm_get_db_timezone());
    date.ts += CM_UNIX_EPOCH;
    cm_decode_date(date.ts, &detail);
    return detail;
}

// 增加年月时，需要对特殊月份的日期进行处理
constexpr int32_t THE_MONTH_2 = 2;
constexpr int32_t THE_MONTH_4 = 4;
constexpr int32_t THE_MONTH_6 = 6;
constexpr int32_t THE_MONTH_9 = 9;
constexpr int32_t THE_MONTH_11 = 11;
constexpr int32_t THE_MONTH_12 = 12;
constexpr int32_t THE_DAY_28 = 28;
constexpr int32_t THE_DAY_29 = 29;
constexpr int32_t THE_DAY_30 = 30;
constexpr int32_t THE_DAY_31 = 31;
constexpr int32_t MONTHS_OF_YEAR = 12;
// detail_2 - detail_1
int64_t DiffYearFromDetail(date_detail_t &detail_1, date_detail_t &detail_2) {
    auto diff_year = detail_2.year - detail_1.year;
    auto diff_mon = detail_2.mon - detail_1.mon;
    auto diff_day = detail_2.day - detail_1.day;
    auto diff_time = (detail_2.hour - detail_1.hour) * SECONDS_PER_HOUR * MICROSECS_PER_SECOND_LL + 
                   (detail_2.min - detail_1.min) * SECONDS_PER_MIN * MICROSECS_PER_SECOND_LL + 
                   (detail_2.sec - detail_1.sec) * MICROSECS_PER_SECOND_LL + 
                   (detail_2.millisec - detail_1.millisec) * MICROSECS_PER_MILLISEC + 
                   (detail_2.microsec - detail_1.microsec);
    if (diff_year > 0) {
        if (diff_mon < 0 || (diff_mon == 0 && diff_day < 0) || (diff_mon == 0 && diff_day == 0 && diff_time < 0)) {
            diff_year -= 1;
        }
    } else if (diff_year < 0) {
        if (diff_mon > 0 || (diff_mon == 0 && diff_day > 0) || (diff_mon == 0 && diff_day == 0 && diff_time > 0)) {
            diff_year += 1;
        }
    }
    return diff_year;
}

int64_t DiffMonFromDetail(date_detail_t &detail_1, date_detail_t &detail_2) {
    auto diff_year = detail_2.year - detail_1.year;
    auto diff_mon = detail_2.mon - detail_1.mon;
    auto diff_day = detail_2.day - detail_1.day;
    auto diff_time = (detail_2.hour - detail_1.hour) * SECONDS_PER_HOUR * MICROSECS_PER_SECOND_LL + 
                   (detail_2.min - detail_1.min) * SECONDS_PER_MIN * MICROSECS_PER_SECOND_LL + 
                   (detail_2.sec - detail_1.sec) * MICROSECS_PER_SECOND_LL + 
                   (detail_2.millisec - detail_1.millisec) * MICROSECS_PER_MILLISEC + 
                   (detail_2.microsec - detail_1.microsec);
    if ((diff_year * MONTHS_OF_YEAR) + diff_mon > 0) {
        if (diff_day < 0 || (diff_day == 0 && diff_time < 0)) {
            diff_mon -= 1;
        }
    } else if ((diff_year * MONTHS_OF_YEAR) + diff_mon < 0) {
        if (diff_day > 0 || (diff_day == 0 && diff_time > 0)) {
            diff_mon += 1;
        }
    }
    return (int64_t)diff_year * MONTHS_OF_YEAR + diff_mon;
}

void AddYearFromDetail(date_detail_t &detail, int64_t add_value) {
    detail.year += add_value;
    if (detail.year < CM_MIN_YEAR || detail.year > CM_MAX_YEAR) {
        throw intarkdb::Exception(ExceptionType::EXECUTOR, "Year out of range, must be in 1~9999");
    }
    if (detail.mon == 2 && detail.day >= 29) {
        if (!((detail.year % 4 == 0 && detail.year % 100 != 0) || detail.year % 400 == 0)) {
            detail.day = 28;
        } else {
            detail.day = 29;
        }
    } else if ((detail.mon == 4 || detail.mon == 6 || detail.mon == 9 || detail.mon == 11) && detail.day >= 31) {
        detail.day = 30;
    }
}

void AddMonthFromDetail(date_detail_t &detail, int64_t add_value) {
    int64_t add_year = 0;
    if (add_value >= 12 || add_value <= -12) {
        add_year = add_value / 12;
        add_value = add_value % 12;
    }
    if ((detail.mon + add_value) <= 0) {
        add_year -= 1;
        detail.mon = 12 + detail.mon + add_value;
    } else if ((detail.mon + add_value) <= 12) {
        detail.mon += add_value;
        if (detail.mon == 2 && detail.day >= 29) {
            if (!((detail.year % 4 == 0 && detail.year % 100 != 0) || detail.year % 400 == 0)) {
                detail.day = 28;
            } else {
                detail.day = 29;
            }
        } else if ((detail.mon == 4 || detail.mon == 6 || detail.mon == 9 || detail.mon == 11) && detail.day >= 31) {
            detail.day = 30;
        }
    } else {
        add_year += (detail.mon + add_value) / 12;
        detail.mon = (detail.mon + add_value) % 12;
    }
    AddYearFromDetail(detail, add_year);
}

constexpr int MONTHS_OF_QUARTER = 3;
void AddQuarterFromDetail(date_detail_t &detail, int64_t add_value) {
    AddMonthFromDetail(detail, MONTHS_OF_QUARTER * add_value);
}

// !-------------------------------------------------------------------------------------------------------

// eg : select datediff(d1,d2) from a;
// ag : select datediff(d1,'2024-05-16') from a;
// DATEDIFF(start_date, end_date)
// start_date/end_date 参数必须是一个合法的日期或时间值,
// 结果是 start_date - end_date 的天数差
constexpr int32_t DATEDIFF_ARG_NUM2 = 2;
auto date_diff(const std::vector<Value>& values) -> Value {
    if (values.size() != DATEDIFF_ARG_NUM2) {
        throw intarkdb::Exception(ExceptionType::EXECUTOR, "datediff function parameter number error, must be 2");
    }

    int64_t result = 0;
    auto date_1 = DataType::GetTypeInstance(GStorDataType::GS_TYPE_DATE)->CastValue(values[0]);
    auto date_2 = DataType::GetTypeInstance(GStorDataType::GS_TYPE_DATE)->CastValue(values[1]);
    auto ts_1 = date_1.GetCastAs<TimestampType::StorType>().ts;
    auto ts_2 = date_2.GetCastAs<TimestampType::StorType>().ts;
    result = (ts_1 - ts_2) / UNITS_PER_DAY;

    return ValueFactory::ValueBigInt(result);
}

// eg : select timestampdiff('DAY', d1, d2) from a;
// ag : select timestampdiff('DAY', d1, '2024-05-16 01:01:01') from a;
// TIMESTAMPDIFF(unit, start_date, end_date)
// unit 是时间间隔的单位，如MICROSECOND、SECOND、MINUTE、HOUR、DAY、WEEK、MONTH、QUARTER或YEAR。
// start_date/end_date 参数必须是一个合法的日期或时间值,
// 结果是 end_date - start_date 按unit为单位转换统计的差值
constexpr int32_t TIMESTAMPDIFF_ARG_NUM3 = 3;
constexpr int32_t DAYS_OF_WEEK = 7;
auto timestamp_diff(const std::vector<Value>& values) -> Value {
    if (values.size() != TIMESTAMPDIFF_ARG_NUM3) {
        throw intarkdb::Exception(ExceptionType::EXECUTOR, "timestampdiff function parameter number error, must be 3");
    }

    std::string diff_uint_string;
    Value date_1, date_2;
    diff_uint_string = intarkdb::StringUtil::Upper(values[0].ToString());
    date_1 = DataType::GetTypeInstance(GStorDataType::GS_TYPE_TIMESTAMP)->CastValue(values[1]);
    date_2 = DataType::GetTypeInstance(GStorDataType::GS_TYPE_TIMESTAMP)->CastValue(values[2]);
    auto ts_1 = date_1.GetCastAs<TimestampType::StorType>().ts;
    auto ts_2 = date_2.GetCastAs<TimestampType::StorType>().ts;

    // decode incoming date value
    std::vector<Value> date_values_1 = { date_1 };
    auto detail_1 = CheckParamsAndDecodeDate(date_values_1);
    std::vector<Value> date_values_2 = { date_2 };
    auto detail_2 = CheckParamsAndDecodeDate(date_values_2);

    int64_t result = 0;
    if (diff_uint_string == INTERVAL_TYPE_YEAR) {
        result = DiffYearFromDetail(detail_1, detail_2);
    } else if (diff_uint_string == INTERVAL_TYPE_QUARTER) {
        result = DiffMonFromDetail(detail_1, detail_2);
        result /= MONTHS_OF_QUARTER;
    } else if (diff_uint_string == INTERVAL_TYPE_MONTH) {
        result = DiffMonFromDetail(detail_1, detail_2);
    } else if (diff_uint_string == INTERVAL_TYPE_WEEK) {
        result = (ts_2 - ts_1) / UNITS_PER_DAY;
        result /= DAYS_OF_WEEK;
    } else if (diff_uint_string == INTERVAL_TYPE_DAY) {
        result = (ts_2 - ts_1) / UNITS_PER_DAY;
    } else if (diff_uint_string == INTERVAL_TYPE_HOUR) {
        result = (ts_2 - ts_1) / ((int64_t)SECONDS_PER_HOUR * (int64_t)MILLISECS_PER_SECOND * (int64_t)MICROSECS_PER_MILLISEC);
    } else if (diff_uint_string == INTERVAL_TYPE_MINUTE) {
        result = (ts_2 - ts_1) / ((int64_t)SECONDS_PER_MIN * (int64_t)MILLISECS_PER_SECOND * (int64_t)MICROSECS_PER_MILLISEC);
    } else if (diff_uint_string == INTERVAL_TYPE_SECOND) {
        result = (ts_2 - ts_1) / ((int64_t)MILLISECS_PER_SECOND * (int64_t)MICROSECS_PER_MILLISEC);
    } else if (diff_uint_string == INTERVAL_TYPE_MICROSECOND) {
        result = ts_2 - ts_1;
    } else {
        std::string errmsg = "timestampdiff function parameter error, not support : " + diff_uint_string;
        throw intarkdb::Exception(ExceptionType::EXECUTOR, errmsg.c_str());
    }

    return ValueFactory::ValueBigInt(result);
}

// statement eg : select date_add(d1, INTERVAL 1 hour) from a;
// DATE_ADD(date, INTERVAL, value, unit), 把INTERVAL表达式拆为3个value
// date参数必须是一个合法的日期或时间值,
// value参数必须是一个整数值，否则会使用其整数部分进行计算。
// unit是时间间隔的单位，如MICROSECOND、SECOND、MINUTE、HOUR、DAY、WEEK、MONTH、QUARTER或YEAR。
constexpr int32_t DATEADD_ARG_NUM4 = 4;
constexpr int32_t OUTPUT_FORMAT_LEN_10 = 10;   // YYYY-MM-DD
constexpr int32_t OUTPUT_FORMAT_LEN_19 = 19;   // YYYY-MM-DD HH:MI:SS
constexpr int32_t OUTPUT_FORMAT_LEN_26 = 26;   // YYYY-MM-DD HH:MI:SS.FFFFFF
constexpr int32_t OUTPUT_FORMAT_LEN_YYYY = 4;   // YYYY
constexpr int32_t OUTPUT_FORMAT_LEN_FFFFFF = 6;   // FFFFFF
auto date_add(const std::vector<Value>& values) -> Value {
    if (values.size() != DATEADD_ARG_NUM4) {
        throw intarkdb::Exception(ExceptionType::EXECUTOR, "date_add function parameter number error, must be 4");
    }

    // incoming args
    Value base_value = values[0];
    Value add_value = values[2];
    Value add_unit = values[3];
    GStorDataType base_value_type = base_value.GetType();
    uint32_t output_format_len = OUTPUT_FORMAT_LEN_10;
    if (base_value_type != GStorDataType::GS_TYPE_DATE && base_value_type != GStorDataType::GS_TYPE_TIMESTAMP) {
        std::string err_msg = "The first parameter is not a valid date! "
                    "Should be YYYY-MM-DD or YYYY-MM-DD HH:MI:SS or YYYY-MM-DD HH:MI:SS.FFFFFF";
        if (base_value_type != GStorDataType::GS_TYPE_VARCHAR) {
            throw intarkdb::Exception(ExceptionType::EXECUTOR, err_msg);
        }
        try {
            base_value = DataType::GetTypeInstance(GStorDataType::GS_TYPE_TIMESTAMP)->CastValue(base_value);
        } catch (std::exception &e) {
            throw intarkdb::Exception(ExceptionType::EXECUTOR, err_msg);
        }
    }
    auto ts_base = base_value.GetCastAs<TimestampType::StorType>().ts;
    int64_t add_value_int = DataType::GetTypeInstance(GStorDataType::GS_TYPE_INTEGER)->CastValue(add_value).GetCastAs<int32_t>();
    std::string add_unit_string = intarkdb::StringUtil::Upper(add_unit.ToString());

    // decode incoming date value
    std::vector<Value> date_values = { base_value };
    auto detail = CheckParamsAndDecodeDate(date_values);

    date_t ts_result;
    timestamp_stor_t result;
    if (add_unit_string == INTERVAL_TYPE_YEAR) {
        AddYearFromDetail(detail, add_value_int);
        ts_result = cm_encode_date(&detail) - TIMEZONE_GET_MICROSECOND(cm_get_db_timezone()) - CM_UNIX_EPOCH;
    } else if (add_unit_string == INTERVAL_TYPE_QUARTER) {
        AddQuarterFromDetail(detail, add_value_int);
        ts_result = cm_encode_date(&detail) - TIMEZONE_GET_MICROSECOND(cm_get_db_timezone()) - CM_UNIX_EPOCH;
    } else if (add_unit_string == INTERVAL_TYPE_MONTH) {
        AddMonthFromDetail(detail, add_value_int);
        ts_result = cm_encode_date(&detail) - TIMEZONE_GET_MICROSECOND(cm_get_db_timezone()) - CM_UNIX_EPOCH;
    } else if (add_unit_string == INTERVAL_TYPE_WEEK) {
        ts_result = ts_base + DAYS_OF_WEEK * add_value_int * UNITS_PER_DAY;
    } else if (add_unit_string == INTERVAL_TYPE_DAY) {
        ts_result = ts_base + add_value_int * UNITS_PER_DAY;
    } else if (add_unit_string == INTERVAL_TYPE_HOUR) {
        output_format_len = OUTPUT_FORMAT_LEN_19;
        ts_result = ts_base + add_value_int * ((int64_t)SECONDS_PER_HOUR * (int64_t)MILLISECS_PER_SECOND * (int64_t)MICROSECS_PER_MILLISEC);
    } else if (add_unit_string == INTERVAL_TYPE_MINUTE) {
        output_format_len = OUTPUT_FORMAT_LEN_19;
        ts_result = ts_base + add_value_int * ((int64_t)SECONDS_PER_MIN * (int64_t)MILLISECS_PER_SECOND * (int64_t)MICROSECS_PER_MILLISEC);
    } else if (add_unit_string == INTERVAL_TYPE_SECOND) {
        output_format_len = OUTPUT_FORMAT_LEN_19;
        ts_result = ts_base + add_value_int * ((int64_t)MILLISECS_PER_SECOND * (int64_t)MICROSECS_PER_MILLISEC);
    } else if (add_unit_string == INTERVAL_TYPE_MICROSECOND) {
        output_format_len = OUTPUT_FORMAT_LEN_26;
        ts_result = ts_base + add_value_int;
    } else {
        std::string errmsg = "date_add function parameter error, not support : " + add_unit_string;
        throw intarkdb::Exception(ExceptionType::EXECUTOR, errmsg.c_str());
    }

    // range : CM_MIN_DATETIME ~ CM_MAX_DATETIME
    auto check_range_ts = ts_result + TIMEZONE_GET_MICROSECOND(cm_get_db_timezone()) + CM_UNIX_EPOCH;
    if (check_range_ts < CM_MIN_DATETIME || check_range_ts > CM_MAX_DATETIME) {
        throw intarkdb::Exception(ExceptionType::EXECUTOR, "Year out of range, must be in 1~9999");
    }
    result = timestamp_stor_t{static_cast<int64_t>(ts_result)};

    // if YYYY-MM-DD HH:MI:SS
    if (detail.hour > 0 || detail.min > 0 || detail.sec > 0) {
        output_format_len = OUTPUT_FORMAT_LEN_19;
    }
    // if YYYY-MM-DD HH:MI:SS.FFFFFF
    if (detail.microsec > 0 || detail.millisec > 0) {
        output_format_len = OUTPUT_FORMAT_LEN_26;
    }
    auto result_2 = ValueFactory::ValueTimeStamp(result).ToString().substr(0, output_format_len);
    return ValueFactory::ValueVarchar(result_2);
}

// statement eg : select date_sub(d1, INTERVAL 1 hour) from a;
// DATE_SUB(date, INTERVAL, value, unit), 把INTERVAL表达式拆为3个value
// date参数必须是一个合法的日期或时间值,
// value参数必须是一个整数值，否则会使用其整数部分进行计算。
// unit是时间间隔的单位，如MICROSECOND、SECOND、MINUTE、HOUR、DAY、WEEK、MONTH、QUARTER或YEAR。
constexpr int32_t DATESUB_ARG_NUM4 = 4;
constexpr int32_t SUB_FACTOR_1 = -1;
auto date_sub(const std::vector<Value>& values) -> Value {
    if (values.size() != DATESUB_ARG_NUM4) {
        throw intarkdb::Exception(ExceptionType::EXECUTOR, "date_sub function parameter number error, must be 4");
    }

    // incoming args, trans sub value to add value
    std::vector<Value> add_values = values;
    Value add_value = add_values[2];
    auto sub_value_int = DataType::GetTypeInstance(GStorDataType::GS_TYPE_INTEGER)->CastValue(add_value).GetCastAs<int32_t>();
    auto add_value_int = sub_value_int * SUB_FACTOR_1;
    add_values[2] = ValueFactory::ValueInt(add_value_int);

    return date_add(add_values);
}

const char DATE_FORMAT_TOKEN = '%';
const std::string DATE_FORMAT_YEAR_YYYY = "%Y";
const std::string DATE_FORMAT_YEAR_yy = "%y";
const std::string DATE_FORMAT_MONTH_MM = "%m";
const std::string DATE_FORMAT_MONTH_m = "%c";
const std::string DATE_FORMAT_DAY_DD = "%d";
const std::string DATE_FORMAT_DAY_d = "%e";
const std::string DATE_FORMAT_HOUR_HH24 = "%H";
const std::string DATE_FORMAT_HOUR_hh12 = "%h";
const std::string DATE_FORMAT_MINUTE_MI = "%i";
const std::string DATE_FORMAT_SECOND_SS = "%S";
const std::string DATE_FORMAT_SECOND_ss = "%s";
const std::string DATE_FORMAT_MICROSECOND_f = "%f";
const std::string DATE_FORMAT_AM_PM = "%p";
bool IsDateFormatToken(const std::string &s) {
    if (s == DATE_FORMAT_YEAR_YYYY || s == DATE_FORMAT_YEAR_yy || 
        s == DATE_FORMAT_MONTH_MM || s == DATE_FORMAT_MONTH_m || 
        s == DATE_FORMAT_DAY_DD || s == DATE_FORMAT_DAY_d || 
        s == DATE_FORMAT_HOUR_HH24 || s == DATE_FORMAT_HOUR_hh12 || 
        s == DATE_FORMAT_MINUTE_MI || 
        s == DATE_FORMAT_SECOND_SS || s == DATE_FORMAT_SECOND_ss || 
        s == DATE_FORMAT_MICROSECOND_f || 
        s == DATE_FORMAT_AM_PM) {
        return true;
    }
    return false;
}
// statement eg : select date_format(d1, '%Y-%m-%d %H:%i:%s.%p') from a;
// statement eg : select date_format('2024-5-20 15:2:8', '%y-%c-%e %h:%i:%s.%p');
// DATE_FORMAT(date, format)
// date参数必须是一个合法的日期或时间值,
// format是格式
/*
    格式控制符      描述
    %Y          年份，四位数字
    %y	        年份，两位数字
    %m	        月份，两位数字
    %c	        月份，没有前导零
    %d	        月份中的第几天，两位数字
    %e	        月份中的第几天，没有前导零
    %H	        小时，24小时制，两位数字
    %h	        小时，12小时制，两位数字
    %i	        分钟，两位数字
    %s	        秒钟，两位数字
    %f	        微秒，六位数字，不足前面补0
    %p	        AM 或 PM
*/
constexpr int32_t DATEFORMAT_ARG_NUM2 = 2;
constexpr int32_t THE_NUMBER_10 = 10;
constexpr int32_t THE_HOUR_12 = 12;
auto date_format(const std::vector<Value>& values) -> Value {
    if (values.size() != DATEFORMAT_ARG_NUM2) {
        throw intarkdb::Exception(ExceptionType::EXECUTOR, "date_format function parameter number error, must be 2");
    }

    // incoming args
    Value base_value = values[0];
    Value format_value = values[1];
    std::string format_string = format_value.ToString();
    GStorDataType base_value_type = base_value.GetType();
    if (base_value_type != GStorDataType::GS_TYPE_DATE && base_value_type != GStorDataType::GS_TYPE_TIMESTAMP) {
        std::string err_msg = "The first parameter is not a valid date! "
                    "Should be YYYY-MM-DD or YYYY-MM-DD HH:MI:SS or YYYY-MM-DD HH:MI:SS.FFFFFF";
        if (base_value_type != GStorDataType::GS_TYPE_VARCHAR) {
            throw intarkdb::Exception(ExceptionType::EXECUTOR, err_msg);
        }
        try {
            base_value = DataType::GetTypeInstance(GStorDataType::GS_TYPE_TIMESTAMP)->CastValue(base_value);
        } catch (std::exception &e) {
            throw intarkdb::Exception(ExceptionType::EXECUTOR, err_msg);
        }
    }

    // decode incoming date value
    std::vector<Value> date_values = { base_value };
    auto detail = CheckParamsAndDecodeDate(date_values);

    // format
    std::vector<std::string> format_list;
    for (size_t i = 0; i < format_string.size(); i++) {
        if ((i + 1) < format_string.size()) {
            std::string format_one = format_string.substr(i, 2);
            if (IsDateFormatToken(format_one)) {
                format_list.push_back(format_one);
                i++;
            } else if (format_string[i] != DATE_FORMAT_TOKEN) {
                format_list.push_back(format_string.substr(i, 1));
            }
        } else if (format_string[i] != DATE_FORMAT_TOKEN) {
            format_list.push_back(format_string.substr(i, 1));
        }
    }

    // output
    std::string result;
    for (auto s : format_list) {
        if (s == DATE_FORMAT_YEAR_YYYY) {
            std::string year_yyyy = std::to_string(detail.year);
            auto YYYY_len = OUTPUT_FORMAT_LEN_YYYY - year_yyyy.length();
            std::string YYYY_0;
            if (YYYY_len > 0) {
                YYYY_0 = std::string(YYYY_len, '0');
            }
            result += YYYY_0 + year_yyyy;
        } else if (s == DATE_FORMAT_YEAR_yy) {
            std::string year_yy = std::to_string(detail.year % 100);
            if (year_yy.length() < 2) {
                year_yy = "0" + year_yy;
            }
            result += year_yy;
        } else if (s == DATE_FORMAT_MONTH_MM) {
            std::string month_mm = std::to_string(detail.mon);
            if (detail.mon <= THE_MONTH_9) {
                month_mm = "0" + month_mm;
            }
            result += month_mm;
        } else if (s == DATE_FORMAT_MONTH_m) {
            result += std::to_string(detail.mon);
        } else if (s == DATE_FORMAT_DAY_DD) {
            std::string day_dd = std::to_string(detail.day);
            if (detail.day < THE_NUMBER_10) {
                day_dd = "0" + day_dd;
            }
            result += day_dd;
        } else if (s == DATE_FORMAT_DAY_d) {
            result += std::to_string(detail.day);
        } else if (s == DATE_FORMAT_HOUR_HH24) {
            std::string hour_24 = std::to_string(detail.hour);
            if (detail.hour < THE_NUMBER_10) {
                hour_24 = "0" + hour_24;
            }
            result += hour_24;
        } else if (s == DATE_FORMAT_HOUR_hh12) {
            auto int_hour_12 = detail.hour;
            if (int_hour_12 > THE_HOUR_12) {
                int_hour_12 -= THE_HOUR_12;
            } else if (int_hour_12 == 0) {
                int_hour_12 = THE_HOUR_12;
            }
            std::string hour_12 = std::to_string(int_hour_12);
            if (int_hour_12 < THE_NUMBER_10) {
                hour_12 = "0" + hour_12;
            }
            result += hour_12;
        } else if (s == DATE_FORMAT_MINUTE_MI) {
            std::string minute_mi = std::to_string(detail.min);
            if (detail.min < THE_NUMBER_10) {
                minute_mi = "0" + minute_mi;
            }
            result += minute_mi;
        } else if (s == DATE_FORMAT_SECOND_SS || s == DATE_FORMAT_SECOND_ss) {
            std::string second_ss = std::to_string(detail.sec);
            if (detail.sec < THE_NUMBER_10) {
                second_ss = "0" + second_ss;
            }
            result += second_ss;
        } else if (s == DATE_FORMAT_MICROSECOND_f) {
            std::string micro_sec = std::to_string(detail.millisec * MICROSECS_PER_MILLISEC  + detail.microsec);
            auto FFFFFF_len = OUTPUT_FORMAT_LEN_FFFFFF - micro_sec.length();
            std::string FFFFFF_0;
            if (FFFFFF_len > 0) {
                FFFFFF_0 = std::string(FFFFFF_len, '0');
            }
            result += FFFFFF_0 + micro_sec;
        } else if (s == DATE_FORMAT_AM_PM) {
            std::string am_pm = "AM";
            if (detail.hour > THE_HOUR_12) {
                am_pm = "PM";
            }
            result += am_pm;
        } else {
            result += s;
        }
    }

    return ValueFactory::ValueVarchar(result);
}
