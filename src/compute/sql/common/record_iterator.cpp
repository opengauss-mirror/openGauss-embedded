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
 * record_iterator.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/common/record_iterator.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "common/record_iterator.h"

#include "common/string_util.h"

RecordIterator::RecordIterator(RecordIteratorType iterator_type) : type_(iterator_type) {}

RecordIterator::~RecordIterator() {}

auto RecordIterator::GetHeader() -> std::vector<std::string> const {
    std::vector<std::string> headers_row;
    const auto& schema = GetSchema();
    const auto& headers = schema.GetColumnInfos();
    for (const auto& header : headers) {
        headers_row.emplace_back(intarkdb::StringUtil::Upper(header.GetColNameWithoutTableName()));
    }
    return headers_row;
}
