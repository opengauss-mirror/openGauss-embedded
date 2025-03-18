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
* date_function.h
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/include/function/date/date_function.h
*
* -------------------------------------------------------------------------
*/
#pragma once

#include "type/value.h"
#include <vector>

date_detail_t CheckParamsAndDecodeDate(const std::vector<Value>& values);
int64_t DiffYearFromDetail(date_detail_t &detail_1, date_detail_t &detail_2);
int64_t DiffMonFromDetail(date_detail_t &detail_1, date_detail_t &detail_2);
void AddYearFromDetail(date_detail_t &detail, int64_t add_value);
void AddMonthFromDetail(date_detail_t &detail, int64_t add_value);
void AddQuarterFromDetail(date_detail_t &detail, int64_t add_value);
auto date_diff(const std::vector<Value>& values) -> Value;
auto timestamp_diff(const std::vector<Value>& values) -> Value;
auto date_add(const std::vector<Value>& values) -> Value;
auto date_sub(const std::vector<Value>& values) -> Value;
bool IsDateFormatToken(const std::string &s);
auto date_format(const std::vector<Value>& values) -> Value;
