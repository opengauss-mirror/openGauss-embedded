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
 * record.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/common/record.h
 *
 * -------------------------------------------------------------------------
 */

#pragma once

#include <vector>

#include "type/value.h"

namespace intarkdb {
class RowContainer;
}

class Record {
   public:
    EXPORT_API Record();
    EXPORT_API Record(const std::vector<Value>& v);
    EXPORT_API Record(std::vector<Value>&& v);
    EXPORT_API Record(std::unique_ptr<intarkdb::RowContainer> row_container);
    EXPORT_API Record(const Record& other);
    EXPORT_API Record(Record&& other) noexcept;
    EXPORT_API Record& operator=(const Record& other);
    EXPORT_API Record& operator=(Record&& other) noexcept;
    EXPORT_API ~Record();

    EXPORT_API uint32_t ColumnCount() const;

    // slot 为下标
    EXPORT_API Value Field(uint16_t slot) const;

    EXPORT_API Record Concat(const Record& other) const;

    friend auto RecordToVector(const Record& record) -> std::vector<Value>;

   private:
    std::unique_ptr<intarkdb::RowContainer> row_container_;
};
