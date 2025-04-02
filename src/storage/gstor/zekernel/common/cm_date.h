/*
 * Copyright (c) 2022 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * cm_date.h
 *
 *
 * IDENTIFICATION
 *    src/storage/gstor/zekernel/common/cm_date.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef __CM_DATE_H_
#define __CM_DATE_H_

#include "cm_text.h"
#include "cm_timezone.h"

#include <time.h>

#ifndef WIN32
#include <sys/time.h>
#else
#include <winsock2.h>
#endif
#include <math.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * The date type is represented by a 64-bit signed integer. The minimum unit
 * is 1 microsecond. This indicates the precision can reach up to 6 digits after
 * the decimal point.
 */
typedef int64 date_t;

/*
 * seconds: '2019-01-01 00:00:00'UTC since Epoch ('1970-01-01 00:00:00' UTC)
 */
#define CM_GTS_BASETIME 1546300800

/*
 * Set the minimal and maximal years that are supported by this database system.
 * We set the BASELINE DATATIME by 2000-01-01 00:00:00.000000000, which corresponds
 * to the value (date_t)0. For practice, CM_MIN_YEAR and CM_MAX_YEAR are used to
 * restrict the year into a representable range. Here, we thus the `CM_MIN_YEAR`
 * should be greater than 1707 (= BASELINE DATATIME - 584.54/2), and the `CM_MAX_YEAR`
 * should be less than 2292 (= BASELINE DATATIME + 584.54/2).
 * **NOTE: ** The YEAR is not allowed to set back to BC, that is, the YEAR must be
 * greater than 0, since this program does not consider the chronology before BC yet.
 *  `CM_MAX_YEAR = CM_MIN_YEAR + maximal allowed range`
 */
#define CM_BASELINE_YEAY 2000
#define CM_MIN_YEAR      1
#define CM_MAX_YEAR      9999

#define CM_MIN_UTC 0                 /* 1970-01-01 00:00:00 UTC */
#define CM_MAX_UTC 2147483647.999999 /* 2038-01-19 03:14:07.999999 UTC */

#define CM_BASELINE_DAY ((int32)730120) /* == days_before_year(CM_BASELINE_YEAY) + 1 */

/* !
 * `CM_MIN_DATE` is the minimal date, corresponding to the date `CM_MIN_YEAR-01-01 00:00:00.000000`
 * `CM_MAX_DATE` is the maximal date, corresponding to the date `CM_MAX_YEAR-12-31 23:59:59.999999`
 */
#define CM_MIN_DATETIME ((date_t)-63082281600000000LL) /* == cm_encode_date(CM_MIN_YEAR-01-01 00:00:00.000000) */
#define CM_MAX_DATETIME ((date_t)252455615999999999LL) /* == cm_encode_date(CM_MAX_YEAR-12-31 23:59:59.999999) */
#define CM_MIN_DATE     ((int32)-730119)               /* == total_days_before_date(CM_MIN_YEAR-01-01) */
#define CM_MAX_DATE     ((int32)2921940)               /* == total_days_before_date((CM_MAX_YEAR+1)-01-01) */

/** Check whether the year is valid */
#define CM_IS_VALID_YEAR(year) ((year) >= CM_MIN_YEAR && (year) <= CM_MAX_YEAR)
#define CM_IS_VALID_MONTH(mon) ((mon) >= 1 && (mon) <= 12)
#define CM_IS_VALID_DAY(day) ((day) >= 1 && (day) <= 31)
#define CM_IS_VALID_HOUR(hour) ((hour) >= 0 && (hour) <= 23)
#define CM_IS_VALID_MINUTE(min) ((min) >= 0 && (min) <= 59)
#define CM_IS_VALID_SECOND(sec) ((sec) >= 0 && (sec) <= 59)
#define CM_IS_VALID_FRAC_SEC(fsec) ((fsec) >= 0 && (fsec) <= 999999999)
/** Check whether the julian date is valid */
#define CM_IS_VALID_DATE(d) ((d) >= CM_MIN_DATE && (d) < CM_MAX_DATE)
/** Check whether the julian timestamp is valid */
#define CM_IS_VALID_TIMESTAMP(t) ((t) >= CM_MIN_DATETIME && (t) <= CM_MAX_DATETIME)

#define SECONDS_PER_DAY         86400U
#define SECONDS_PER_HOUR        3600U
#define SECONDS_PER_MIN         60U
#define MILLISECS_PER_SECOND    1000U
#define MICROSECS_PER_MILLISEC  1000U
#define MICROSECS_PER_SECOND    1000000U
#define MICROSECS_PER_MIN       60000000U
#define NANOSECS_PER_MICROSEC   1000U
#define NANOSECS_PER_MILLISEC   1000000U
#define NANOSECS_PER_SECOND     1000000000U
#define MICROSECS_PER_SECOND_LL 1000000LL

#define DAYS_PER_WEEK           7U

/* the minimal units of a day == SECONDS_PER_DAY * MILLISECS_PER_SECOND * MICROSECS_PER_MILLISEC */
#define UNITS_PER_DAY 86400000000LL

/* the difference between 1970.01.01-2000.01.01 in microseconds */
/* FILETIME of Jan 1 1970 00:00:00 GMT, the Zenith epoch */
#define CM_UNIX_EPOCH (-946684800000000LL)

#define CM_IS_DATETIME_ADDTION_OVERFLOW(dt, val, res) \
    (!((val) >= 0 && (res) <= CM_MAX_DATETIME && (res) >= (dt)) &&  \
     !((val) < 0 && (res) >= CM_MIN_DATETIME && (res) <= (dt)))

#define GS_SET_ERROR_DATETIME_OVERFLOW() \
    GS_THROW_ERROR(ERR_TYPE_DATETIME_OVERFLOW, CM_MIN_YEAR, CM_MAX_YEAR);

#define GS_SET_ERROR_TIMESTAMP_OVERFLOW() \
    GS_THROW_ERROR(ERR_TYPE_TIMESTAMP_OVERFLOW, CM_MIN_YEAR, CM_MAX_YEAR);

/* !
* \brief A safe methods to calculate the addition between DATETIME TYPE and
* numerical types. It can avoid the overflow/underflow.
*/
static inline status_t cm_date_add_days(date_t dt, double day, date_t *res_dt)
{
    date_t new_dt = dt + (date_t)round((double)UNITS_PER_DAY * day);
    if (CM_IS_DATETIME_ADDTION_OVERFLOW(dt, day, new_dt)) {
        GS_SET_ERROR_DATETIME_OVERFLOW();
        return GS_ERROR;
    }

    *res_dt = new_dt;
    return GS_SUCCESS;
}

static inline status_t cm_date_add_seconds(date_t dt, uint64 second, date_t *res_dt)
{
    date_t new_dt = dt + (date_t)(MICROSECS_PER_SECOND * second);
    if (CM_IS_DATETIME_ADDTION_OVERFLOW(dt, second, new_dt)) {
        GS_SET_ERROR_DATETIME_OVERFLOW();
        return GS_ERROR;
    }

    *res_dt = new_dt;
    return GS_SUCCESS;
}

static inline status_t cm_date_sub_days(date_t dt, double day, date_t *res_dt)
{
    return cm_date_add_days(dt, -day, res_dt);
}

static inline int32 cm_date_diff_days(date_t dt1, date_t dt2)
{
    return (int32)((dt1 - dt2) / UNITS_PER_DAY);
}

#pragma pack(4)
/* To represent all parts of a date type */
typedef struct st_date_detail {
    uint16 year;
    uint8 mon;
    uint8 day;
    uint8 hour;
    uint8 min;
    uint8 sec;
    uint8 reserved;            /* reserved 8 bytes for byte alignment */
    uint16 millisec;           /* millisecond: 0~999, 1000 millisec = 1 sec */
    uint16 microsec;           /* microsecond: 0~999, 1000 microsec = 1 millisec */
    uint16 nanosec;            /* nanosecond:  0~999, 1000 nanoseconds = 1 millisec */
    timezone_info_t tz_offset; /* time zone */
} date_detail_t;
#pragma pack()

typedef date_t timestamp_t;

#pragma pack(4)
typedef struct st_timestamp_tz {
    timestamp_t tstamp;
    timezone_info_t tz_offset;  // minute uints
    int16 unused;              // reserved
} timestamp_tz_t;

typedef date_t timestamp_ltz_t;

typedef struct st_date_detail_ex {
    bool32 is_am;
    uint32 seconds;
    char ad;
    uint8 week;          // total weeks of current year
    uint8 quarter;       // quarter of current month
    uint8 day_of_week;   // (0..6 means Sun..Sat)
    uint16 day_of_year;  // total days of current year
    char reserve[2];     // not used, for byte alignment
} date_detail_ex_t;
#pragma pack()

static inline status_t cm_tstamp_add_days(timestamp_t ts, double day, date_t *res_ts)
{
    return cm_date_add_days(ts, day, res_ts);
}

static inline status_t cm_tstamp_sub_days(timestamp_t ts, double day, date_t *res_ts)
{
    return cm_tstamp_add_days(ts, -day, res_ts);
}

typedef enum en_format_id {
    FMT_AM_INDICATOR = 100,
    FMT_PM_INDICATOR = 101,
    FMT_SPACE = 102,
    FMT_MINUS = 103,
    FMT_SLASH = 104,
    FMT_BACK_SLASH = 105,
    FMT_COMMA = 106,
    FMT_DOT = 107,
    FMT_SEMI_COLON = 108,
    FMT_COLON = 109,
    FMT_X = 110,
    FMT_CENTURY = 201,
    FMT_DAY_OF_WEEK = 202,
    FMT_DAY_NAME = 203,
    FMT_DAY_ABBR_NAME = 204,
    FMT_DAY_OF_MONTH = 205,
    FMT_DAY_OF_YEAR = 206,
    FMT_FRAC_SECOND1 = 207,
    FMT_FRAC_SECOND2 = 208,
    FMT_FRAC_SECOND3 = 209,
    FMT_FRAC_SECOND4 = 210,
    FMT_FRAC_SECOND5 = 211,
    FMT_FRAC_SECOND6 = 212,
    FMT_FRAC_SECOND7 = 213,
    FMT_FRAC_SECOND8 = 214,
    FMT_FRAC_SECOND9 = 215,
    FMT_FRAC_SEC_VAR_LEN = 250,

    FMT_DQ_TEXT = 313, /* "text" is allowed in format */
    FMT_MINUTE = 314,
    FMT_MONTH = 315,
    FMT_MONTH_ABBR_NAME = 316,
    FMT_MONTH_NAME = 317,
    FMT_QUARTER = 318,
    FMT_SECOND = 319,
    FMT_SECOND_PASS = 320,
    FMT_WEEK_OF_YEAR = 321,
    FMT_WEEK_OF_MONTH = 322,
    /* The order of FMT_YEAR1, FMT_YEAR2, FMT_YEAR3 and FMT_YEAR4 can
     * not be changed */
    FMT_YEAR1 = 323,
    FMT_YEAR2 = 324,
    FMT_YEAR3 = 325,
    FMT_YEAR4 = 326,
    FMT_HOUR_OF_DAY12 = 328,
    FMT_HOUR_OF_DAY24 = 329,
    FMT_TZ_HOUR = 330,   /* time zone hour */
    FMT_TZ_MINUTE = 331, /* time zone minute */
    FMT_MONTH_RM = 332
} format_id_t;

typedef struct en_format_item {
    text_t name;
    format_id_t id;
    uint32 fmask; /* Added for parsing date/timestamp from text */
    int8 placer;  /* the length of the placers, -1 denoting unspecified or uncaring */
    bool8 reversible;
    bool8 dt_used; /* can the item be used in DATE_FORMAT */
} format_item_t;

#define MILL_SECOND1       (date_t)((double)1 / (double)86400000.0)
#define IS_LEAP_YEAR(year) (((year) % 4 == 0) && (((year) % 100 != 0) || ((year) % 400 == 0)) ? 1 : 0)
#define DAY2SECONDS(days)  (days) * 24 * 3600;

extern uint16 g_month_days[2][12];  // 12 months in leap year and 12 months in non-leap year
#define CM_MONTH_DAYS(year, mon) (g_month_days[IS_LEAP_YEAR(year)][(mon) - 1])

#define cm_check_special_char(text)                                         \
    do {                                                                    \
        if (*((text)->str) == '-' || *((text)->str) == ',' ||               \
            *((text)->str) == '.' || *((text)->str) == ';' ||               \
            *((text)->str) == ':' || *((text)->str) == '/') {               \
            --((text)->len);                                                \
            ++((text)->str);                                                \
        }                                                                   \
    } while (0)

#define cm_get_num_and_check(part_len, start, end)                               \
    do {                                                                         \
        uint16 item_len = cm_get_num_len_in_str(date_text, part_len, GS_FALSE);  \
        if (item_len == 0) {                                                     \
            return GS_ERROR;                                                     \
        }                                                                        \
                                                                                 \
        if (cm_check_number(date_text, item_len, start, end, &num_value) != 0) { \
            return GS_ERROR;                                                     \
        }                                                                        \
                                                                                 \
        date_text->len -= item_len;                                              \
        date_text->str += item_len;                                              \
    } while (0)

#define cm_get_num_and_check_with_sign(part_len, start, end)                                         \
    do {                                                                                             \
        uint16 item_len = cm_get_num_len_in_str(date_text, part_len, GS_TRUE);                       \
        if (item_len == 0) {                                                                         \
            return GS_ERROR;                                                                         \
        }                                                                                            \
                                                                                                     \
        if (cm_check_number_with_sign(date_text, item_len, start, end, &num_value_with_sign) != 0) { \
            return GS_ERROR;                                                                         \
        }                                                                                            \
                                                                                                     \
        date_text->len -= item_len;                                                                  \
        date_text->str += item_len;                                                                  \
    } while (0)

#define cm_check_mask(mask_id)          \
    do {                                \
        if ((*mask & (mask_id)) != 0) { \
            return GS_ERROR;            \
        }                               \
                                        \
        *mask |= (mask_id);             \
    } while (0)

#define cm_check_time_autofilled(mask_id, part_len, start, end) \
    do {                                                        \
        if (date_text->len > 0) {                               \
            cm_get_num_and_check(part_len, start, end);         \
        } else {                                                \
            num_value = start;                                  \
        }                                                       \
                                                                \
        cm_check_mask(mask_id);                                 \
    } while (0)

#define cm_check_time(mask_id, part_len, start, end) \
    do {                                             \
        cm_get_num_and_check(part_len, start, end);  \
        cm_check_mask(mask_id);                      \
    } while (0)

#define cm_check_time_with_sign(mask_id, part_len, start, end) \
    do {                                                       \
        cm_get_num_and_check_with_sign(part_len, start, end);  \
        cm_check_mask(mask_id);                                \
    } while (0)

static inline int32 cm_compare_date(date_t date1, date_t date2)
{
    /* use int64 to avoid overflow in unsigned type for representing negative values */
    int64 diff = date1 - date2;
    return diff > 0 ? 1 : (diff < 0 ? -1 : 0);
}

EXPORT_API date_t cm_now();
EXPORT_API date_t cm_utc_now();
EXPORT_API date_t cm_date_now();
EXPORT_API date_t cm_monotonic_now();
EXPORT_API status_t cm_adjust_timestamp(timestamp_t *ts, int32 precision);
EXPORT_API status_t cm_adjust_timestamp_tz(timestamp_tz_t *tstz, int32 precision);
EXPORT_API status_t cm_text2date(const text_t *text, const text_t *fmt, date_t *date);
EXPORT_API status_t cm_str2time(char *date, const text_t *fmt, time_t *time_stamp);
EXPORT_API status_t cm_check_tstz_is_valid(timestamp_tz_t *tstz);
EXPORT_API status_t cm_text2timestamp_tz(const text_t *text, const text_t *fmt, timezone_info_t defasult_tz, timestamp_tz_t *tstz);
EXPORT_API status_t cm_text2date_fixed(const text_t *text, const text_t *fmt, date_t *date);
EXPORT_API status_t cm_text2date_def(const text_t *text, date_t *date);
EXPORT_API status_t cm_text2timestamp_def(const text_t *text, date_t *date);
EXPORT_API status_t cm_text2date_flex(const text_t *text, date_t *date);
EXPORT_API bool32 cm_str2week(text_t *value, uint8 *week);
EXPORT_API time_t cm_current_time();
EXPORT_API time_t cm_date2time(date_t date);
EXPORT_API date_t cm_timestamp2date(date_t date);
EXPORT_API status_t cm_verify_date_fmt(const text_t *fmt);
EXPORT_API status_t cm_verify_timestamp_fmt(const text_t *fmt);
EXPORT_API status_t cm_date2text_ex(date_t date, text_t *fmt, uint32 precision, text_t *text, uint32 max_len);

EXPORT_API status_t cm_timestamp2text_ex(timestamp_t ts, text_t *fmt, uint32 precision, text_t *text,
                              uint32 max_len);
EXPORT_API status_t cm_timestamp_tz2text_ex(timestamp_tz_t *tstz, text_t *fmt, uint32 precision, text_t *text,
                                 uint32 max_len);

EXPORT_API int64 cm_get_unix_timestamp(timestamp_t ts, int64 time_zone_offset);
EXPORT_API int32 cm_tstz_cmp(timestamp_tz_t *tstz1, timestamp_tz_t *tstz2);
EXPORT_API int64 cm_tstz_sub(timestamp_tz_t *tstz1, timestamp_tz_t *tstz2);

static inline status_t cm_date2str_ex(date_t date, text_t *fmt_text, char *str, uint32 max_len)
{
    text_t date_text;
    date_text.str = str;
    date_text.len = 0;

    return cm_date2text_ex(date, fmt_text, 0, &date_text, max_len);
}

static inline status_t cm_timestamp2str_ex(timestamp_t ts, text_t *fmt_text, uint32 precision, char *str,
                                           uint32 max_len)
{
    text_t tmstamp_text;

    tmstamp_text.str = str;
    tmstamp_text.len = 0;

    return cm_timestamp2text_ex(ts, fmt_text, precision, &tmstamp_text, max_len);
}

static inline status_t cm_timestamp_tz2str_ex(timestamp_tz_t *tstz, text_t *fmt_text, uint32 precision, char *str,
                                              uint32 max_len)
{
    text_t tmstamp_text;

    tmstamp_text.str = str;
    tmstamp_text.len = 0;

    return cm_timestamp_tz2text_ex(tstz, fmt_text, precision, &tmstamp_text, max_len);
}

EXPORT_API status_t cm_time2text(time_t time, text_t *fmt, text_t *text, uint32 max_len);

EXPORT_API status_t cm_time2str(time_t time, const char *fmt, char *str, uint32 max_len);

EXPORT_API date_t cm_time2date(time_t time);

EXPORT_API void cm_now_detail(date_detail_t *detail);
/* decode a date type into a date_detail_t. */
EXPORT_API void cm_decode_date(date_t date, date_detail_t *detail);

/* decode a time type into  detail info. */
EXPORT_API void cm_decode_time(time_t time, date_detail_t *detail);

/* encode a date_detail type into a date type (i.e., a 64-bit integer) with
 * 10 nanoseconds as the minimum unit, that is, 1 = 10 nanoseconds. */
EXPORT_API date_t cm_encode_date(const date_detail_t *detail);

/* decode a date type into an ora date type (7 bytes) */
EXPORT_API void cm_decode_ora_date(date_t date, uint8 *ora_date);
/* encode an ora date type (7 bytes)) into a date type */
EXPORT_API date_t cm_encode_ora_date(uint8 *ora_date);

EXPORT_API time_t cm_encode_time(date_detail_t *detail);

static inline status_t cm_date2text(date_t date, text_t *fmt, text_t *text, uint32 max_len)
{
    return cm_date2text_ex(date, fmt, GS_MAX_DATETIME_PRECISION, text, max_len);
}

static inline status_t cm_date2str(date_t date, const char *fmt, char *str, uint32 max_len)
{
    text_t fmt_text;
    cm_str2text((char *)fmt, &fmt_text);
    return cm_date2str_ex(date, &fmt_text, str, max_len);
}

static inline status_t cm_timestamp2text(timestamp_t ts, text_t *fmt, text_t *text, uint32 max_len)
{
    return cm_timestamp2text_ex(ts, fmt, GS_MAX_DATETIME_PRECISION, text, max_len);
}

static inline status_t cm_timestamp2text_prec(timestamp_t ts, text_t *fmt, text_t *text, uint32 max_len,
                                              uint8 temestamp_prec)
{
    return cm_timestamp2text_ex(ts, fmt, (uint32)temestamp_prec, text, max_len);
}

static inline status_t cm_timestamp2str(timestamp_t ts, const char *fmt, char *str, uint32 max_len)
{
    text_t fmt_text;
    cm_str2text((char *)fmt, &fmt_text);
    return cm_timestamp2str_ex(ts, &fmt_text, GS_MAX_DATETIME_PRECISION, str, max_len);
}

static inline status_t cm_timestamp_tz2text(timestamp_tz_t *tstz, text_t *fmt, text_t *text,
                                            uint32 max_len)
{
    return cm_timestamp_tz2text_ex(tstz, fmt, GS_MAX_DATETIME_PRECISION, text, max_len);
}

static inline status_t cm_timestamp_tz2text_prec(timestamp_tz_t *tstz, text_t *fmt, text_t *text,
                                                 uint32 max_len, uint8 timestamp_prec)
{
    return cm_timestamp_tz2text_ex(tstz, fmt, (uint32)timestamp_prec, text, max_len);
}

static inline status_t cm_timestamp_tz2str(timestamp_tz_t *tstz, const char *fmt, char *str, uint32 max_len)
{
    text_t fmt_text;
    cm_str2text((char *)fmt, &fmt_text);
    return cm_timestamp_tz2str_ex(tstz, &fmt_text, GS_MAX_DATETIME_PRECISION, str, max_len);
}

static inline date_t cm_adjust_date(date_t date)
{
    return date / MICROSECS_PER_SECOND * MICROSECS_PER_SECOND;
}

/*
 * this function is used to  adjust time&date from src_tz to dest_tz
 */
static inline date_t cm_adjust_date_between_two_tzs(date_t src_time, timezone_info_t src_tz,
                                                    timezone_info_t dest_tz)
{
    return src_time + ((date_t)(dest_tz - src_tz)) * MICROSECS_PER_MIN;
}

static inline uint64 cm_day_usec()
{
#ifdef _WIN32
    uint64 usec;
    SYSTEMTIME sys_time;
    GetLocalTime(&sys_time);

    usec = sys_time.wHour * SECONDS_PER_HOUR * MICROSECS_PER_SECOND;
    usec += sys_time.wMinute * MICROSECS_PER_MIN;
    usec += sys_time.wSecond * MICROSECS_PER_SECOND;
    usec += sys_time.wMilliseconds * MICROSECS_PER_MILLISEC;
#else
    uint64 usec;
    struct timeval tv;
    gettimeofday(&tv, NULL);
    usec = (uint64)(tv.tv_sec * MICROSECS_PER_SECOND);
    usec += (uint64)tv.tv_usec;
#endif

    return usec;
}

#ifndef WIN32
#define cm_gettimeofday(a) gettimeofday(a, NULL)
#else

#if defined(_MSC_VER) || defined(_MSC_EXTENSIONS)
#define GS_DELTA_EPOCH_IN_MICROSECS 11644473600000000Ui64
#else
#define GS_DELTA_EPOCH_IN_MICROSECS 11644473600000000ULL
#endif

int cm_gettimeofday(struct timeval *tv);
#endif

#define timeval_t struct timeval

#define TIMEVAL_DIFF_US(t_start, t_end) (((t_end)->tv_sec - (t_start)->tv_sec) * 1000000ULL +  \
        (t_end)->tv_usec - (t_start)->tv_usec)
#define TIMEVAL_DIFF_S(t_start, t_end)  ((t_end)->tv_sec - (t_start)->tv_sec)

void cm_date2timeval(date_t date, struct timeval *val);
date_t cm_timeval2date(struct timeval tv);
date_t cm_timeval2realdate(struct timeval tv);

status_t cm_round_date(date_t date, text_t *fmt, date_t *result);
status_t cm_trunc_date(date_t date, text_t *fmt, date_t *result);
void cm_get_detail_ex(const date_detail_t *detail, date_detail_ex_t *detail_ex);
#ifdef __cplusplus
}
#endif

#endif
