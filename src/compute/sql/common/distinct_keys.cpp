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
 * distinct_keys.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/common/distinct_keys.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "common/distinct_keys.h"

#include "common/record_batch.h"

DistinctKey::DistinctKey(DistinctKey&& other) {
    keys_ = std::move(other.keys_);
    other.keys_ = nullptr;
}
#ifdef _MSC_VER
DistinctKey::DistinctKey(const DistinctKey& other) { keys_ = std::make_unique<intarkdb::RowContainer>(*other.keys_); }
#endif
DistinctKey::DistinctKey(const std::vector<Value>& values) { keys_ = std::make_unique<intarkdb::RowContainer>(values); }

bool DistinctKey::operator==(const DistinctKey& rhs) const {
    if (keys_ == nullptr || rhs.keys_ == nullptr) {
        return keys_ == nullptr && rhs.keys_ == nullptr ? true : false;
    }

    if (keys_->ColumnNumber() != rhs.keys_->ColumnNumber()) {
        return false;
    }

    auto columns_count = keys_->ColumnNumber();

    for (size_t i = 0; i < columns_count; ++i) {
        const auto& left = keys_->Field(i);
        const auto& right = rhs.keys_->Field(i);

        if (left.IsNull() || right.IsNull()) {
            if (left.IsNull() != right.IsNull()) {
                return false;
            }
            continue;  // 都是null ，跳过后续比较
        }
        if (left.GetType() != right.GetType()) {
            return false;
        }
        // 除了两个都是null之外，FALSE 和 UNKNOWN  都认为不相等
        auto res = left.Equal(right);
        if (res == Trivalent::TRI_FALSE || res == Trivalent::UNKNOWN) {
            return false;
        }
    }
    return true;
}

bool DistinctKey::operator<(const DistinctKey& rhs) const {
    if (keys_ == nullptr || rhs.keys_ == nullptr) {
        return keys_ == nullptr && rhs.keys_ == nullptr ? true : false;
    }

    if (keys_->ColumnNumber() != rhs.keys_->ColumnNumber()) {
        return keys_->ColumnNumber() < rhs.keys_->ColumnNumber();
    }

    auto columns_count = keys_->ColumnNumber();

    for (size_t i = 0; i < columns_count; ++i) {
        const auto& left = keys_->Field(i);
        const auto& right = rhs.keys_->Field(i);
        if (left.IsNull() || right.IsNull()) {
            if (left.IsNull() && !right.IsNull()) {
                return false;
            }
            if (!left.IsNull() && right.IsNull()) {
                return true;
            }
            continue;
        }

        if (left.GetType() < right.GetType()) {
            return false;
        }

        // TODO: LessThan 优化
        auto res = left.LessThan(right);
        if (res == Trivalent::TRI_FALSE || res == Trivalent::UNKNOWN) {
            if (left.Equal(right) == Trivalent::TRI_TRUE) {
                continue;
            }
            return false;
        } else {
            return true;
        }
    }
    return false;
}

std::vector<Value> DistinctKey::Keys() const { return keys_ ? keys_->Values() : std::vector<Value>(); }

auto DistinctKeyToRecord(DistinctKey&& key) -> Record { return Record(std::move(key.keys_)); }
