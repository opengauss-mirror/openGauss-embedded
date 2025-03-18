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
 * row_container.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/common/row_container.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "common/row_container.h"

#include "common/hash_util.h"

namespace intarkdb {

RowBuffer::RowBuffer(size_t size, int element_num) {
    items_.resize(element_num);
    items_idx_ = 0;
    buffer_ = size > 0 ? allocator_.allocate(size) : nullptr;
    buffer_size_ = size;
    offset_ = 0;
}

RowBuffer::RowBuffer(RowBuffer&& other) noexcept {
    auto temp = buffer_;
    auto temp_size = buffer_size_;

    buffer_ = other.buffer_;
    buffer_size_ = other.buffer_size_;
    offset_ = other.offset_;
    items_ = std::move(other.items_);
    items_idx_ = other.items_idx_;

    other.buffer_ = nullptr;
    other.buffer_size_ = 0;
    // 释放原来的内存
    if (temp) {
        allocator_.deallocate(temp, temp_size);
    }
}

RowBuffer::~RowBuffer() {
    if (buffer_ != nullptr) {
        allocator_.deallocate(buffer_, buffer_size_);
        buffer_ = nullptr;
    }
}

bool RowBuffer::AddItem(const void* value, uint32_t size, LogicalType type, bool is_null) {
    if (buffer_ == nullptr && items_.size() == 0) {  // record 可能只有一个 null 元素
        return false;
    }

    if (!is_null && offset_ + size > buffer_size_) {
        return false;
    }

    RowItemView item;
    item.type = type.TypeId();
    item.scale = type.Scale();
    item.precision = type.Precision();
    if (is_null) {
        // is null
        item.offset = offset_;
        item.is_null = true;
    } else {
        auto start = items_idx_ > 0 ? items_[items_idx_ - 1].offset : 0;
        item.is_null = false;
        memcpy(buffer_ + start, value, size);
        offset_ = offset_ + size;
        item.offset = offset_;
    }
    items_[items_idx_] = item;
    items_idx_++;
    return true;
}

bool RowBuffer::GetItem(uint32_t index, Value& value) const {
    if (index >= items_.size()) {
        return false;
    }
    const RowItemView& item = items_[index];
    if (!item.is_null) {
        col_text_t col_text;
        auto start = index > 0 ? items_[index - 1].offset : 0;
        col_text.str = buffer_ + start;
        col_text.len = item.offset - start;
        if (IsString(item.type)) {
            // FIMXE: 特殊处理 空字符串 但 不是 null 的情况
            value = Value(item.type, std::string(col_text.str, col_text.len));
        } else {
            value = Value(item.type, col_text, item.scale, item.precision);
        }
    } else {
        value = ValueFactory::ValueNull(item.type);
    }
    return true;
}

std::unique_ptr<RowBuffer> RowBuffer::Copy() const {
    auto new_buffer = std::make_unique<RowBuffer>(buffer_size_, items_.size());
    new_buffer->allocator_ = allocator_;
    if (new_buffer->buffer_) {
        memcpy(new_buffer->buffer_, buffer_, buffer_size_);
    }
    new_buffer->offset_ = offset_;
    new_buffer->items_ = items_;
    new_buffer->items_idx_ = items_idx_;
    return new_buffer;
}

size_t RowBuffer::ItemCount() const { return items_.size(); }

size_t RowBuffer::Hash() const {
    auto hash = buffer_ ? HashUtil::HashBytes(buffer_, buffer_size_) : 0;
    for (const auto& item : items_) {
        hash = HashUtil::CombineHash(hash, item.is_null);
        hash = HashUtil::CombineHash(hash, item.type);
    }
    return hash;
}

std::unique_ptr<RowBuffer> RowBuffer::Concat(RowBuffer* left, RowBuffer* right) {
    size_t new_buff_size = 0;
    new_buff_size += left ? left->buffer_size_ : 0;
    new_buff_size += right ? right->buffer_size_ : 0;
    size_t new_elem_num = 0;
    new_elem_num += left ? left->items_.size() : 0;
    new_elem_num += right ? right->items_.size() : 0;
    std::unique_ptr<RowBuffer> new_buff = std::make_unique<RowBuffer>(new_buff_size, new_elem_num);
    auto copy_offset = 0;
    if(left && left->buffer_size_ > 0) {
        memcpy(new_buff->buffer_, left->buffer_, left->buffer_size_);
        copy_offset = left->buffer_size_;
    }
    if( right && right->buffer_size_ > 0) {
        memcpy(new_buff->buffer_ + copy_offset , right->buffer_, right->buffer_size_);
    }

    if(left) {
        for(size_t i = 0 ; i < left->items_.size(); ++i) {
            new_buff->items_[i] = left->items_[i];
        }
    }
    
    if(right) {
        auto offset = left ? left->buffer_size_ : 0;
        auto idx_offset = left ? left->items_.size() : 0;
        for(size_t i = 0 ; i < right->items_.size(); ++i) {
            RowItemView new_item = right->items_[i];
            new_item.offset += offset;
            new_buff->items_[i + idx_offset] = new_item;
        }
    }
    return new_buff;
}

RowContainer::RowContainer() {}

RowContainer::RowContainer(const std::vector<Value>& v) {
    size_t total = 0;
    for (const auto& value : v) {
        total += value.Size();
    }
    buffer_ = std::make_unique<RowBuffer>(total, v.size());
    for (const auto& value : v) {
        buffer_->AddItem(value.GetRawBuff(), value.Size(), value.GetLogicalType(), value.IsNull());
    }
}

RowContainer::RowContainer(const RowContainer& row) {
    if (row.buffer_ != nullptr) {
        buffer_ = row.buffer_->Copy();
    } else {
        buffer_ = nullptr;
    }
}

RowContainer::RowContainer(RowBuffer&& row) { buffer_ = std::make_unique<RowBuffer>(std::move(row)); }

uint32_t RowContainer::ColumnNumber() const { return buffer_ ? buffer_->ItemCount() : 0; }

Value RowContainer::Field(uint16_t slot) const {
    Value value;
    if (buffer_ && buffer_->GetItem(slot, value)) {
        return value;
    } else {
        if (buffer_ == nullptr) {
            throw intarkdb::Exception(ExceptionType::EXECUTOR,
                                      "unfound slot=" + std::to_string(slot) + " values size=0");
        }
        throw intarkdb::Exception(ExceptionType::EXECUTOR, "unfound slot=" + std::to_string(slot) +
                                                               " values size=" + std::to_string(buffer_->ItemCount()));
    }
}

std::vector<Value> RowContainer::Values() const {
    std::vector<Value> values;
    if (buffer_) {
        for (size_t i = 0; i < buffer_->ItemCount(); i++) {
            Value value;
            buffer_->GetItem(i, value);
            values.push_back(value);
        }
    }
    return values;
}

size_t RowContainer::Hash() const { return buffer_ ? buffer_->Hash() : 0; }

std::unique_ptr<RowContainer> RowContainer::Concat(RowContainer* left, RowContainer* right) {
    auto left_row_buff = left ? left->buffer_.get() : nullptr;
    auto right_row_buff = right ? right->buffer_.get() : nullptr;
    return std::make_unique<RowContainer>(std::move(*RowBuffer::Concat(left_row_buff,right_row_buff)));
}

};  // namespace intarkdb
