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
 * record.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/common/record.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "common/record.h"

#include "common/row_container.h"

Record::Record() { row_container_ = std::make_unique<intarkdb::RowContainer>(); }

Record::Record(std::unique_ptr<intarkdb::RowContainer> row_container) {
    if (row_container != nullptr) {
        row_container_ = std::move(row_container);
    } else {
        row_container_ = std::make_unique<intarkdb::RowContainer>();
    }
}

Record::Record(const std::vector<Value>& v) { row_container_ = std::make_unique<intarkdb::RowContainer>(v); }

Record::Record(std::vector<Value>&& v) { row_container_ = std::make_unique<intarkdb::RowContainer>(std::move(v)); }

Record::Record(const Record& other) {
    row_container_ = std::make_unique<intarkdb::RowContainer>(*other.row_container_);
}

Record::Record(Record&& other) noexcept { row_container_ = std::move(other.row_container_); }

Record& Record::operator=(const Record& other) {
    if (this == &other) {
        return *this;
    }
    row_container_ = std::make_unique<intarkdb::RowContainer>(*other.row_container_);
    return *this;
}

Record& Record::operator=(Record&& other) noexcept {
    if (this == &other) {
        return *this;
    }
    row_container_ = std::move(other.row_container_);
    return *this;
}

Record::~Record() {}

uint32_t Record::ColumnCount() const { return row_container_->ColumnNumber(); }

// slot 为下标
Value Record::Field(uint16_t slot) const { return row_container_->Field(slot); }

Record Record::Concat(const Record& other) const {
    auto row_container_ptr = row_container_ ? row_container_.get() : nullptr;
    auto other_row_container_ptr = other.row_container_ ? other.row_container_.get() : nullptr;
    return Record(intarkdb::RowContainer::Concat(row_container_ptr, other_row_container_ptr));
}
