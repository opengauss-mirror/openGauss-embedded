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
 * bind_interval.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/binder/bind_interval.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "binder/binder.h"

auto Binder::BindInterval(duckdb_libpgquery::PGIntervalConstant *node) -> std::unique_ptr<BoundExpression> {
    bind_location = node->location;
    Value interval_value;
    switch (node->val_type) {
        case duckdb_libpgquery::T_PGString:
            interval_value = ValueFactory::ValueVarchar(node->sval);
            break;
        case duckdb_libpgquery::T_PGInteger:
            interval_value = ValueFactory::ValueInt(node->ival);
            break;
        default:
            throw intarkdb::Exception(ExceptionType::BINDER, "Unsupported interval expression", bind_location);
    }
    if (!(node->typmods)) {
        throw intarkdb::Exception(ExceptionType::BINDER, "interval expression format error(eg : interval 1 hour)",
                                  bind_location);
    }
    int32_t interval_mask =
        reinterpret_cast<duckdb_libpgquery::PGAConst *>(node->typmods->head->data.ptr_value)->val.val.ival;
    // See: libpg_query/include/utils/datetime.hpp
    constexpr uint32_t MONTH_MASK = 1 << 1;
    constexpr uint32_t YEAR_MASK = 1 << 2;
    constexpr uint32_t DAY_MASK = 1 << 3;
    constexpr uint32_t HOUR_MASK = 1 << 10;
    constexpr uint32_t MINUTE_MASK = 1 << 11;
    constexpr uint32_t SECOND_MASK = 1 << 12;
    constexpr uint32_t MILLISECOND_MASK = 1 << 13;
    constexpr uint32_t MICROSECOND_MASK = 1 << 14;
    constexpr uint32_t WEEK_MASK = 1 << 24;
    constexpr uint32_t QUARTER_MASK = 1 << 29;

    std::string intarval_string;
    if (interval_mask & YEAR_MASK && interval_mask & MONTH_MASK) {
        throw intarkdb::Exception(ExceptionType::BINDER, "YEAR TO MONTH is not supported", bind_location);
    } else if (interval_mask & DAY_MASK && interval_mask & HOUR_MASK) {
        throw intarkdb::Exception(ExceptionType::BINDER, "DAY TO HOUR is not supported", bind_location);
    } else if (interval_mask & DAY_MASK && interval_mask & MINUTE_MASK) {
        throw intarkdb::Exception(ExceptionType::BINDER, "DAY TO MINUTE is not supported", bind_location);
    } else if (interval_mask & DAY_MASK && interval_mask & SECOND_MASK) {
        throw intarkdb::Exception(ExceptionType::BINDER, "DAY TO SECOND is not supported", bind_location);
    } else if (interval_mask & HOUR_MASK && interval_mask & MINUTE_MASK) {
        throw intarkdb::Exception(ExceptionType::BINDER, "HOUR TO MINUTE is not supported", bind_location);
    } else if (interval_mask & HOUR_MASK && interval_mask & SECOND_MASK) {
        throw intarkdb::Exception(ExceptionType::BINDER, "HOUR TO SECOND is not supported", bind_location);
    } else if (interval_mask & MINUTE_MASK && interval_mask & SECOND_MASK) {
        throw intarkdb::Exception(ExceptionType::BINDER, "MINUTE TO SECOND is not supported", bind_location);
    } else if (interval_mask & YEAR_MASK) {
        intarval_string = "YEAR";
    } else if (interval_mask & MONTH_MASK) {
        intarval_string = "MONTH";
    } else if (interval_mask & DAY_MASK) {
        intarval_string = "DAY";
    } else if (interval_mask & HOUR_MASK) {
        intarval_string = "HOUR";
    } else if (interval_mask & MINUTE_MASK) {
        intarval_string = "MINUTE";
    } else if (interval_mask & SECOND_MASK) {
        intarval_string = "SECOND";
    } else if (interval_mask & MILLISECOND_MASK) {
        intarval_string = "MILLISECOND";
    } else if (interval_mask & MICROSECOND_MASK) {
        intarval_string = "MICROSECOND";
    } else if (interval_mask & WEEK_MASK) {
        intarval_string = "WEEK";
    } else if (interval_mask & QUARTER_MASK) {
        intarval_string = "QUARTER";
    } else {
        throw intarkdb::Exception(ExceptionType::BINDER, "INTERVAL not supported", bind_location);
    }

    std::vector<Value> values;
    values.push_back(ValueFactory::ValueVarchar("INTERVAL"));
    values.push_back(interval_value);
    values.push_back(ValueFactory::ValueVarchar(intarval_string));

    return std::make_unique<BoundInterval>(values);
}
