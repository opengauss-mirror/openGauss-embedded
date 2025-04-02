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
 * row_container.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/common/row_container.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <vector>

#include "common/memory/memory_manager.h"
#include "type/value.h"

namespace intarkdb {

struct RowItemView {
    uint32_t offset;
    GStorDataType type;
    uint8_t scale;
    uint8_t precision;
    bool is_null;
};

class RowBuffer {
   public:
    explicit RowBuffer(size_t size, int element_size);

    RowBuffer(RowBuffer&& other) noexcept;
    RowBuffer(const RowBuffer&) = delete;

    ~RowBuffer();

    bool AddItem(const void* value, uint32_t size, LogicalType type, bool is_null);

    bool GetItem(uint32_t index, Value& value) const;

    size_t ItemCount() const;

    std::unique_ptr<RowBuffer> Copy() const;

    size_t Hash() const;

    static std::unique_ptr<RowBuffer> Concat(RowBuffer* left, RowBuffer* right);

   private:
    char* buffer_{nullptr};
    uint32_t buffer_size_{0};
    uint32_t offset_{0};
    std::vector<RowItemView,intarkdb::Allocator<RowItemView>> items_;
    uint32_t items_idx_{0};
    intarkdb::Allocator<char> allocator_;
};

using RowBufferPtr = std::unique_ptr<RowBuffer>;

class RowContainer {
   public:
    RowContainer();
    RowContainer(const std::vector<Value>& v);
    RowContainer(const RowContainer& row);
    RowContainer(RowBuffer&& row);

    uint32_t ColumnNumber() const;
    Value Field(uint16_t slot) const;

    std::vector<Value> Values() const;

    size_t Hash() const;

    static std::unique_ptr<RowContainer> Concat(RowContainer* left, RowContainer* right);

   private:
    std::unique_ptr<RowBuffer> buffer_;
};

using RowContainerPtr = std::unique_ptr<RowContainer>;

};  // namespace intarkdb
