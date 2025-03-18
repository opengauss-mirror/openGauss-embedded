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
 * distinct_keys.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/common/distinct_keys.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <queue>

#include "common/hash_util.h"
#include "common/memory/memory_manager.h"
#include "common/row_container.h"
#include "type/troolean.h"
#include "type/value.h"
#include "function/function.h"

class Record;

// mulit key map
struct DistinctKey {
    bool operator==(const DistinctKey& rhs) const;

    bool operator<(const DistinctKey& rhs) const;

    DistinctKey(DistinctKey&& other);

    DistinctKey(const std::vector<Value>& values);
#ifdef _MSC_VER
    DistinctKey(const DistinctKey& other);
#else
    DistinctKey(const DistinctKey& other) = delete;
#endif
    size_t ItemSize() const { return keys_ ? keys_->ColumnNumber() : 0; }

    friend class std::hash<DistinctKey>;

    std::vector<Value> Keys() const;

    friend auto DistinctKeyToRecord(DistinctKey&& key) -> Record;

   private:
    std::unique_ptr<intarkdb::RowContainer> keys_;
};

auto DistinctKeyToRecord(DistinctKey&& key) -> Record;

namespace std {
template <>
struct hash<DistinctKey> {
    // hash function for DistinctKey
    auto operator()(const DistinctKey& key) const -> size_t { return key.keys_ ? key.keys_->Hash() : 0; }
};
}  // namespace std

using DistinctKeySet = std::unordered_set<DistinctKey, std::hash<DistinctKey>, std::equal_to<DistinctKey>,
                                          intarkdb::Allocator<DistinctKey>>;

template <typename T>
using DistinctKeyMap = std::unordered_map<DistinctKey, T, std::hash<DistinctKey>, std::equal_to<DistinctKey>,
                                          intarkdb::Allocator<std::pair<const DistinctKey, T>>>;

template <typename T>
using DistinctKeySortedMap =
    std::map<DistinctKey, T, std::less<DistinctKey>, intarkdb::Allocator<std::pair<const DistinctKey, T>>>;

// priority queue
struct TopCompareValues {
    bool operator()(const Value& a, const Value& b) {
        std::optional<intarkdb::Function> less_ = intarkdb::FunctionContext::GetCompareFunction("<", {a.GetLogicalType(), b.GetLogicalType()});
        return less_.value().func({a, b}).GetCastAs<bool>();
    }
};
using TopPriorityQueue = std::priority_queue<Value, std::vector<Value>, TopCompareValues>;

struct BottomCompareValues {
    bool operator()(const Value& a, const Value& b) {
        std::optional<intarkdb::Function> less_ = intarkdb::FunctionContext::GetCompareFunction("<", {a.GetLogicalType(), b.GetLogicalType()});
        return !less_.value().func({a, b}).GetCastAs<bool>();
    }
};
using BottomPriorityQueue = std::priority_queue<Value, std::vector<Value>, BottomCompareValues>;