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
 * record_batch.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/common/record_batch.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <fmt/color.h>
#include <fmt/core.h>
#include <fmt/format.h>

#include <iostream>
#include <stdexcept>
#include <unordered_map>
#include <vector>

#include "common/record_iterator.h"
#include "type/value.h"

auto RecordToVector(const Record& record) -> std::vector<Value>;

class RecordBatch : public RecordIterator {
   public:
    EXPORT_API RecordBatch(const Schema& schema);

    EXPORT_API virtual ~RecordBatch();

    EXPORT_API virtual std::tuple<Record, bool> Next();

    EXPORT_API virtual uint32_t ColumnCount() const { return schema_.GetColumnInfos().size(); }

    EXPORT_API virtual const Schema& GetSchema() const { return schema_; }

    EXPORT_API uint32_t RowCount() { return records_.size(); }

    EXPORT_API Record Row(uint32_t row_idx) { return records_[row_idx]; }
    EXPORT_API const Record& RowRef(uint32_t row_idx) const { return records_[row_idx]; }

    EXPORT_API void Clear() { records_.clear(); }
    EXPORT_API bool AddRecord(const Record& rec) {
        records_.push_back(rec);
        return true;
    }
    EXPORT_API bool AddRecord(Record&& rec) {
        records_.push_back(std::move(rec));
        return true;
    }

    EXPORT_API std::vector<std::vector<std::string>> GetRecords(std::string null_str = "") const;
    EXPORT_API void Print();

    void SetLastInsertRowid(int64_t rowid) { last_insert_rowid = rowid; }
    int64_t LastInsertRowid() { return last_insert_rowid; }

   private:
    int64_t idx_{0};
    std::vector<Record> records_;
    Schema schema_;

    // last insert rowid
    int64_t last_insert_rowid {0};
};
