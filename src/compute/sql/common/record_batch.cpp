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
 * record_batch.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/common/record_batch.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "common/record_batch.h"

#include <fmt/color.h>
#include <fmt/core.h>
#include <fmt/format.h>

#include <stdexcept>
#include <unordered_map>
#include <vector>

#include "binder/statement_type.h"
#include "catalog/schema.h"
#include "common/row_container.h"
#include "common/string_util.h"
#include "common/util.h"
#include "type/value.h"

RecordBatch::RecordBatch(const Schema& schema) : RecordIterator(RecordIteratorType::Batch), schema_(schema) {}

RecordBatch::~RecordBatch() {}

std::tuple<Record, bool> RecordBatch::Next() {
    if (idx_ < (int64_t)records_.size()) {
        return {records_[idx_++], false};
    }
    return {Record(), true};
}

auto RecordToVector(const Record& record) -> std::vector<Value> { return record.row_container_->Values(); }

std::vector<std::vector<std::string>> RecordBatch::GetRecords(std::string null_str) const {
    std::vector<std::vector<std::string>> result;
    std::vector<std::string> headers_row;
    const auto& headers = schema_.GetColumnInfos();
    for (const auto& header : headers) {
        headers_row.emplace_back(header.GetColNameWithoutTableName());
    }
    result.push_back(headers_row);
    for (const auto& rec : records_) {
        std::vector<std::string> row;
        row.reserve(headers.size());
        for (size_t i = 0; i < headers.size(); ++i) {
            const auto& v = rec.Field(i);
            std::string col_value(v.IsNull() ? null_str : v.ToString());
            row.push_back(std::move(col_value));
        }
        result.push_back(std::move(row));
    }
    return result;
}

void RecordBatch::Print() {
    for (auto& ss : GetRecords()) {
        for (auto& s : ss) {
            std::cout << s << "|        |";
        }
        std::cout << std::endl;
    }
}
