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
* nested_loop_join_exec.cpp
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/planner/physical_plan/join/nested_loop_join_exec.cpp
*
* -------------------------------------------------------------------------
*/
#include "planner/physical_plan/join/nested_loop_join_exec.h"

auto NestedLoopJoinExec::Init() -> void {
    if (join_type_ != JoinType::CrossJoin && join_type_ != JoinType::LeftJoin && join_type_ != JoinType::SemiJoin &&
        join_type_ != JoinType::AntiJoin) {
        throw std::runtime_error(fmt::format("unsupported {} this type of join", join_type_));
    }
    while (true) {
        const auto& [record, _, eof] = right_->Next();
        if (eof) {
            break;
        }
        right_records_.push_back(record);
    }
    while (true) {
        auto&& [record, _, eof] = left_->Next();
        if (eof) {
            break;
        }
        bool found = false;
        bool all_fail = true;
        for (auto& r : right_records_) {
            auto tmp = record.Concat(r);

            if (pred_ == nullptr ||
                static_cast<Trivalent>(pred_->Evaluate(tmp).GetCastAs<uint32_t>()) == Trivalent::TRI_TRUE) {
                if (join_type_ != JoinType::AntiJoin) {
                    records_.emplace_back(std::move(tmp));
                }
                found = true;
                all_fail = false;
            }
            if (join_type_ == JoinType::SemiJoin && found) {
                break;
            }
        }
        if (join_type_ == JoinType::AntiJoin && all_fail) {
            // TODO: 有问题，不过目前应该没有 ANTI JOIN的类型
            records_.emplace_back(std::move(record));
        }
        if ((join_type_ == JoinType::LeftJoin || join_type_ == JoinType::SemiJoin) && !found) {
            const auto& cols = schema_.GetColumnInfos();
            auto values = RecordToVector(record);
            for (size_t i = 0; i < cols.size(); ++i) {
                values.push_back(ValueFactory::ValueNull());
            }
            records_.emplace_back(std::move(values));
        }
    }
    init_ = true;
}

auto NestedLoopJoinExec::Next() -> std::tuple<Record, knl_cursor_t*, bool> {
    if (!init_) {
        Init();
    }
    while (idx_ < records_.size()) {
        return std::make_tuple(records_[idx_++], nullptr, false);
    }
    ResetNext();
    return {{}, nullptr, true};
}

auto NestedLoopJoinExec::ResetNext() -> void {
    idx_ = 0;
    init_ = false;
    records_.clear();
    right_records_.clear();
    left_->ResetNext();
    right_->ResetNext();
    if (pred_) {
        pred_->Reset();
    }
}
