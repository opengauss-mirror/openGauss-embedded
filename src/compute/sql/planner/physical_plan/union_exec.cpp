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
 * union_exec.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/planner/physical_plan/union_exec.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "planner/physical_plan/union_exec.h"

#include <list>

static auto TransformValueType(const Schema& schema, Record&& record) -> Record {
    std::vector<Value> values;
    auto columns = schema.GetColumnInfos();
    auto col_size = record.ColumnCount();
    for (size_t i = 0; i < col_size; ++i) {
        const auto& v = record.Field(i);
        const auto& type = columns[i].col_type;
        values.push_back(ValueCast::CastValue(v, type));
    }
    return values;
}

static auto RecordToDistinctKey(Record&& record) -> DistinctKey {
    auto col_nums = record.ColumnCount();
    std::vector<Value> keys;
    keys.reserve(col_nums);
    for (size_t i = 0; i < col_nums; ++i) {
        keys.push_back(record.Field(i));
    }
    return DistinctKey(keys);
}

auto UnionExec::IntersectInit() -> void {
    while (true) {
        auto&& [record, cursor, eof] = left_->Next();
        if (eof) {
            left_eof_ = true;
            break;
        }
        auto&& new_record = TransformValueType(schema_, std::move(record));
        DistinctKey key = RecordToDistinctKey(std::move(new_record));
        if (is_distinct_) {
            if (distinct_keys_.find(key) != distinct_keys_.end()) {
                continue;
            }
        }
        distinct_keys_.insert(std::move(key));
    }
}

auto UnionExec::ExceptInit() -> void {
    while (true) {
        auto&& [record, cursor, eof] = left_->Next();
        if (eof) {
            left_eof_ = true;
            break;
        }
        auto&& new_record = TransformValueType(schema_, std::move(record));
        DistinctKey key = RecordToDistinctKey(std::move(new_record));
        if (is_distinct_) {
            if (distinct_keys_.find(key) != distinct_keys_.end()) {
                continue;
            }
        }
        distinct_keys_.insert(std::move(key));
    }

    while (true) {
        auto&& [record, cursor, eof] = right_->Next();
        if (eof) {
            right_eof_ = true;
            break;
        }
        auto&& new_record = TransformValueType(schema_, std::move(record));
        DistinctKey key = RecordToDistinctKey(std::move(new_record));
        auto iter = distinct_keys_.find(key);
        if (iter != distinct_keys_.end()) {
            distinct_keys_.erase(iter);
        }
    }
}

void UnionExec::Init() {
    if (set_op_type_ == SetOperationType::INTERSECT) {
        IntersectInit();
    } else if (set_op_type_ == SetOperationType::EXCEPT) {
        ExceptInit();
    }
}

auto UnionExec::UnionNext() -> std::tuple<Record, knl_cursor_t*, bool> {
    if (!left_eof_) {
        while (true) {
            auto&& [record, cursor, eof] = left_->Next();
            if (eof) {
                break;
            }
            auto&& new_record = TransformValueType(schema_, std::move(record));
            if (is_distinct_) {
                DistinctKey key = RecordToDistinctKey(std::move(new_record));
                if (distinct_keys_.find(key) != distinct_keys_.end()) {
                    continue;
                }
                auto keys = key.Keys();  // need copy
                distinct_keys_.insert(std::move(key));
                return {std::move(keys), cursor, false};
            }
            return {std::move(new_record), cursor, false};
        }
    }
    left_eof_ = true;

    if (!right_eof_) {
        while (true) {
            auto&& [record, cursor, eof] = right_->Next();
            if (eof) {
                break;
            }
            auto&& new_record = TransformValueType(schema_, std::move(record));
            if (is_distinct_) {
                DistinctKey key = RecordToDistinctKey(std::move(new_record));
                if (distinct_keys_.find(key) != distinct_keys_.end()) {
                    continue;
                }
                auto keys = key.Keys();  // need copy
                distinct_keys_.insert(std::move(key));
                return {std::move(keys), cursor, false};
            }
            return {std::move(new_record), cursor, false};
        }
    }
    right_eof_ = true;
    return {Record(), nullptr, true};
}

auto UnionExec::IntersectNext() -> std::tuple<Record, knl_cursor_t*, bool> {
    if (!init_) {
        Init();
        init_ = true;
    }
    if (!right_eof_) {
        while (true) {
            auto&& [record, cursor, eof] = right_->Next();
            if (eof) {
                break;
            }
            auto&& new_record = TransformValueType(schema_, std::move(record));
            DistinctKey key = RecordToDistinctKey(std::move(new_record));
            auto iter = distinct_keys_.find(key);
            if (iter != distinct_keys_.end()) {
                distinct_keys_.erase(iter);
                return {Record(DistinctKeyToRecord(std::move(key))), cursor, false};
            } else {
                continue;
            }
        }
        right_eof_ = true;
    }
    return {Record(), nullptr, true};
}

auto UnionExec::ExceptNext() -> std::tuple<Record, knl_cursor_t*, bool> {
    if (!init_) {
        Init();
        init_ = true;
        idx_ = distinct_keys_.begin();
    }
    if (idx_ != distinct_keys_.end()) {
        // need copy
        Record record(idx_->Keys());
        ++idx_;
        return {std::move(record), nullptr, false};
    }
    return {Record(), nullptr, true};
}

auto UnionExec::Next() -> std::tuple<Record, knl_cursor_t*, bool> {
    if (set_op_type_ == SetOperationType::UNION) {
        return UnionNext();
    } else if (set_op_type_ == SetOperationType::INTERSECT) {
        return IntersectNext();
    } else if (set_op_type_ == SetOperationType::EXCEPT) {
        return ExceptNext();
    } else {
        throw intarkdb::Exception(ExceptionType::EXECUTOR, "unkown set op type");
    }
}

auto UnionJoinExec::Next() -> std::tuple<Record, knl_cursor_t*, bool> {
    if (!left_eof_) {
        while (true) {
            auto&& [record, cursor, eof] = left_->Next();
            if (eof) {
                break;
            }
            auto&& new_record = TransformValueType(schema_, std::move(record));
            DistinctKey key = RecordToDistinctKey(std::move(new_record));
            auto keys = key.Keys();  // need copy
            distinct_keys_.insert(std::move(key));
            return {std::move(new_record), cursor, false};
        }
    }
    left_eof_ = true;

    if (!right_eof_) {
        while (true) {
            auto&& [record, cursor, eof] = right_->Next();
            if (eof) {
                break;
            }
            auto&& new_record = TransformValueType(schema_, std::move(record));
            DistinctKey key = RecordToDistinctKey(std::move(new_record));
            if (distinct_keys_.find(key) != distinct_keys_.end()) {
                continue;
            }
            return {std::move(new_record), cursor, false};
        }
    }
    right_eof_ = true;
    ResetNext();
    return {Record(), nullptr, true};
}
