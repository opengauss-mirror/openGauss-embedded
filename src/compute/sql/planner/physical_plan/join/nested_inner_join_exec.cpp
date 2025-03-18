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
 * nested_inner_join_exec.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/planner/physical_plan/nested_inner_join_exec.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "planner/physical_plan/join/nested_inner_join_exec.h"

#include "planner/physical_plan/join/join_util.h"

auto InnerJoinExec::Next() -> std::tuple<Record, knl_cursor_t*, bool> {
    std::tuple<Record, knl_cursor_t*, bool> result;
    if (join_type_ == JoinType::CrossJoin) {
        result = CrossJoinNext();
    } else if (join_type_ == JoinType::LeftJoin) {
        result = LeftJoinNext();
    } else if (join_type_ == JoinType::RightJoin) {
        result = RightJoinNext();
    } else {
        throw intarkdb::Exception(ExceptionType::FATAL, "Unsupported join type");
    }
    return result;
}

auto InnerJoinExec::Init(PhysicalPlanPtr plan) -> void {
    records_.clear();
    while (true) {
        auto&& [record, _, eof] = plan->Next();
        if (eof) {
            break;
        }
        records_.push_back(std::move(record));
    }
    init_ = true;
    padding_column_size_ = plan->GetSchema().GetColumnInfos().size();
}

auto InnerJoinExec::LeftInit() -> void { Init(right_); }

auto InnerJoinExec::RightInit() -> void { Init(left_); }

auto InnerJoinExec::CrossJoinNext() -> std::tuple<Record, knl_cursor_t*, bool> {
    if (!init_) {
        LeftInit();
    }
    while (true) {
        if (right_eof_) {
            auto&& [record, _, eof] = left_->Next();
            if (eof) {
                left_eof_ = true;
                ResetNext();
                return {{}, nullptr, true};
            }
            curr_record_ = std::move(record);
        }
        if (idx_ == records_.size()) {
            right_eof_ = true;
            idx_ = 0;
            continue;
        }
        const auto& record = records_[idx_++];
        right_eof_ = false;
        auto result = curr_record_.Concat(record);
        if (pred_ && pred_->Evaluate(result).GetCastAs<Trivalent>() != Trivalent::TRI_TRUE) {
            continue;
        }
        return {std::move(result), nullptr, false};
    }
}

auto InnerJoinExec::LeftJoinNext() -> std::tuple<Record, knl_cursor_t*, bool> {
    if (!init_) {
        LeftInit();
    }

    bool is_first_record = false;
    while (true) {
        if (right_eof_) {
            is_first_record = true;
            auto&& [record, _, eof] = left_->Next();
            if (eof) {
                left_eof_ = true;
                ResetNext();
                return {{}, nullptr, true};
            }
            curr_record_ = std::move(record);
        }
        if (idx_ == records_.size()) {
            right_eof_ = true;
            idx_ = 0;
            if (is_first_record) {
                return {PaddingNullRecord(curr_record_, padding_column_size_, false), nullptr, false};
            }
            continue;
        }
        const auto& record = records_[idx_++];
        right_eof_ = false;
        auto result = curr_record_.Concat(record);
        if (pred_ && pred_->Evaluate(result).GetCastAs<Trivalent>() != Trivalent::TRI_TRUE) {
            continue;
        }
        return {std::move(result), nullptr, false};
    }
}

auto InnerJoinExec::RightJoinNext() -> std::tuple<Record, knl_cursor_t*, bool> {
    if (!init_) {
        RightInit();
    }
    bool is_first_record = false;
    while (true) {
        if (right_eof_) {
            is_first_record = true;
            auto&& [record, _, eof] = right_->Next();
            if (eof) {
                left_eof_ = true;
                ResetNext();
                return {{}, nullptr, true};
            }
            curr_record_ = std::move(record);
        }
        if (idx_ == records_.size()) {
            right_eof_ = true;
            idx_ = 0;
            if (is_first_record) {
                return {PaddingNullRecord(curr_record_, padding_column_size_, true), nullptr, false};
            }
            continue;
        }
        const auto& record = records_[idx_++];
        right_eof_ = false;
        auto result = record.Concat(curr_record_);
        if (pred_ && pred_->Evaluate(result).GetCastAs<Trivalent>() != Trivalent::TRI_TRUE) {
            continue;
        }
        return {std::move(result), nullptr, false};
    }
}

auto InnerJoinExec::ResetNext() -> void {
    idx_ = 0;
    left_eof_ = false;
    right_eof_ = true;
    records_.clear();
    left_->ResetNext();
    right_->ResetNext();
}
