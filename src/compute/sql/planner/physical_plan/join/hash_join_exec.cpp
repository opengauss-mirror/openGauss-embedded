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
 * hash_join_exec.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/planner/physical_plan/join/hash_join_exec.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "planner/physical_plan/join/hash_join_exec.h"

#include "planner/physical_plan/join/join_util.h"
#include "planner/physical_plan/physical_plan.h"
#include "type/value.h"

static auto MakeHashKey(const Record& record, const std::vector<size_t>& key_idxs,
                        const std::vector<LogicalType>& types, bool& include_null_field) -> DistinctKey {
    std::vector<Value> values;
    values.reserve(key_idxs.size());
    for (size_t i = 0; i < key_idxs.size(); ++i) {
        auto idx = key_idxs[i];
        auto v = record.Field(idx);
        if (!(v.GetLogicalType() == types[i])) {
            v = ValueCast::CastValue(v, types[i]);
        }
        values.push_back(std::move(v));
        if (values.back().IsNull()) {
            include_null_field = true;
            break;
        }
    }
    return values;
}

auto HashJoinExec::Init(PhysicalPlanPtr plan, const std::vector<size_t>& idxs) -> void {
    while (true) {
        auto&& [record, _, eof] = plan->Next();
        if (eof) {
            break;
        }
        bool include_null_field = false;
        auto key = MakeHashKey(record, idxs, idxs_types_, include_null_field);
        if (include_null_field) {  // 含义null字段的记录，必不会相等
            continue;
        }
        auto iter = hash_table_.find(key);
        if (iter == hash_table_.end()) {
            hash_table_.emplace(std::move(key), std::vector<Record>{std::move(record)});
        } else {
            iter->second.push_back(std::move(record));
        }
    }
    padding_column_size_ = plan->GetSchema().GetColumnInfos().size();
}

auto HashJoinExec::LeftInit() -> void { Init(right_, inner_key_idxs_); }

// init for right join
auto HashJoinExec::RightInit() -> void { Init(left_, outer_key_idxs_); }

auto HashJoinExec::CrossJoinNext() -> std::tuple<Record, knl_cursor_t*, bool> {
    if (!init_) {
        LeftInit();
        init_ = true;
    }

    bool should_return = false;
    std::tuple<Record, knl_cursor_t*, bool> result;

    while (!should_return) {
        if (hash_idx_.idx == -1 || hash_idx_.idx == static_cast<int>(hash_idx_.iter->second.size())) {
            // need a new record
            auto&& [record, cursor, eof] = left_->Next();
            if (eof) {
                ResetNext();
                result = {Record(), nullptr, true};
                should_return = true;
                break;
            }
            bool include_null_field = false;
            auto key = MakeHashKey(record, outer_key_idxs_, idxs_types_, include_null_field);
            if (include_null_field) {
                continue;
            }
            auto iter = hash_table_.find(key);
            if (iter != hash_table_.end()) {
                hash_idx_.idx = 0;
                hash_idx_.iter = iter;
                curr_record_ = std::move(record);
            } else {
                continue;
            }
        }
        auto new_record = curr_record_.Concat(hash_idx_.iter->second[hash_idx_.idx++]);
        if (pred_ && pred_->Evaluate(new_record).GetCastAs<Trivalent>() != Trivalent::TRI_TRUE) {
            continue;
        }
        result = {std::move(new_record), nullptr, false};
        should_return = true;
    }
    return result;
}

auto HashJoinExec::LeftJoinNext() -> std::tuple<Record, knl_cursor_t*, bool> {
    if (!init_) {
        LeftInit();
        init_ = true;
    }

    bool should_return = false;
    std::tuple<Record, knl_cursor_t*, bool> result;

    while (!should_return) {
        if (hash_idx_.idx == -1 || hash_idx_.idx == static_cast<int>(hash_idx_.iter->second.size())) {
            // need a new record
            auto&& [record, cursor, eof] = left_->Next();
            if (eof) {
                ResetNext();
                result = {Record(), nullptr, true};
                should_return = true;
                break;
            }

            bool include_null_field = false;
            auto key = MakeHashKey(record, outer_key_idxs_, idxs_types_, include_null_field);
            if (include_null_field) {
                result = {PaddingNullRecord(record, padding_column_size_, false), nullptr, false};
                should_return = true;
                break;
            }
            auto iter = hash_table_.find(key);
            if (iter != hash_table_.end()) {
                hash_idx_.idx = 0;
                hash_idx_.iter = iter;
                curr_record_ = std::move(record);
            } else {
                should_return = true;
                result = {PaddingNullRecord(record, padding_column_size_, false), nullptr, false};
                break;
            }
        }
        auto new_record = curr_record_.Concat(hash_idx_.iter->second[hash_idx_.idx++]);
        if (pred_ && pred_->Evaluate(new_record).GetCastAs<Trivalent>() != Trivalent::TRI_TRUE) {
            if ((size_t)hash_idx_.idx == hash_idx_.iter->second.size()) {
                result = {PaddingNullRecord(curr_record_, padding_column_size_, false), nullptr, false};
                should_return = true;
                break;
            }
            continue;
        }
        result = {std::move(new_record), nullptr, false};
        should_return = true;
    }
    return result;
}

auto HashJoinExec::RightJoinNext() -> std::tuple<Record, knl_cursor_t*, bool> {
    if (!init_) {
        RightInit();
        init_ = true;
    }

    bool should_return = false;
    std::tuple<Record, knl_cursor_t*, bool> result;

    while (!should_return) {
        if (hash_idx_.idx == -1 || hash_idx_.idx == static_cast<int>(hash_idx_.iter->second.size())) {
            // need a new record
            auto&& [record, cursor, eof] = right_->Next();
            if (eof) {
                ResetNext();
                should_return = true;
                result = {Record(), nullptr, true};
                break;
            }

            bool include_null_field = false;
            auto key = MakeHashKey(record, inner_key_idxs_, idxs_types_, include_null_field);
            if (include_null_field) {
                result = {PaddingNullRecord(record, padding_column_size_, true), nullptr, false};
                should_return = true;
                break;
            }
            auto iter = hash_table_.find(key);
            if (iter != hash_table_.end()) {
                hash_idx_.idx = 0;
                hash_idx_.iter = iter;
                curr_record_ = std::move(record);
            } else {
                result = {PaddingNullRecord(record, padding_column_size_, true), nullptr, false};
                should_return = true;
                break;
            }
        }
        auto new_record = hash_idx_.iter->second[hash_idx_.idx++].Concat(curr_record_);
        if (pred_ && pred_->Evaluate(new_record).GetCastAs<Trivalent>() != Trivalent::TRI_TRUE) {
            if ((size_t)hash_idx_.idx == hash_idx_.iter->second.size()) {
                result = {PaddingNullRecord(curr_record_, padding_column_size_, true), nullptr, false};
                should_return = true;
                break;
            }
            continue;
        }
        result = {std::move(new_record), nullptr, false};
        should_return = true;
    }
    return result;
}

auto HashJoinExec::Next() -> std::tuple<Record, knl_cursor_t*, bool> {
    if (join_type_ == JoinType::CrossJoin) {
        return CrossJoinNext();
    } else if (join_type_ == JoinType::LeftJoin) {
        return LeftJoinNext();
    } else if (join_type_ == JoinType::RightJoin) {
        return RightJoinNext();
    } else {
        throw intarkdb::Exception(ExceptionType::FATAL, "Unsupported join type");
    }
}

auto HashJoinExec::ResetNext() -> void {
    left_->ResetNext();
    right_->ResetNext();
    hash_idx_.Clear();
    hash_table_.clear();
    init_ = false;
}
