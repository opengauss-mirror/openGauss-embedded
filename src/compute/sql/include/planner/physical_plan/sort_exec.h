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
 * sort_exec.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/planner/physical_plan/sort_exec.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <algorithm>

#include "binder/bound_sort.h"
#include "common/memory/memory_manager.h"
#include "planner/expressions/expression.h"
#include "planner/physical_plan/physical_plan.h"

struct OrderByInfo {
    SortType type;
    bool is_null_first;
};

class SortExec : public PhysicalPlan {
   public:
    SortExec(PhysicalPlanPtr child, std::vector<std::unique_ptr<Expression>> exprs, std::vector<OrderByInfo> order_info)
        : child_(child), exprs_(std::move(exprs)), order_info_(std::move(order_info)) {}
    ~SortExec() {}
    virtual Schema GetSchema() const override { return child_->GetSchema(); }

    // depercated
    virtual auto Execute() const -> RecordBatch override { return RecordBatch({}); }

    virtual std::vector<PhysicalPlanPtr> Children() const override { return {child_}; }

    virtual std::string ToString() const override { return "SortExec"; }

    void Init() {
        records_.clear();
        while (true) {
            auto [record, cursor, eof] = child_->Next();
            if (eof) {
                break;
            }
            records_.push_back(record);
        }
        // sort the record
        std::stable_sort(records_.begin(), records_.end(), [this](const Record &a, const Record &b) {
            for (size_t i = 0; i < exprs_.size() && i < order_info_.size(); i++) {
                const auto &expr = exprs_[i];
                auto type = order_info_[i].type;
                auto is_null_first = order_info_[i].is_null_first;
                auto val1 = expr->Evaluate(a);
                auto val2 = expr->Evaluate(b);
                if (val1.IsNull() && val2.IsNull()) {
                    continue;
                } else if (val1.IsNull()) {
                    return is_null_first ? true : false;
                } else if (val2.IsNull()) {
                    return is_null_first ? false : true;
                }
                auto result = DataType::GetTypeInstance(val1.GetType())->Compare(val1, val2);
                if (result == CMP_RESULT::EQUAL) {
                    continue;
                }
                if (result == CMP_RESULT::LESS) {
                    return type == SortType::DESC ? false : true;
                } else {
                    return type == SortType::DESC ? true : false;
                }
            }
            return false;
        });
        init_ = true;
    }

    virtual auto Next() -> std::tuple<Record, knl_cursor_t *, bool> override {
        if (!init_) {
            Init();
        }
        while (idx_ < records_.size()) {
            return std::make_tuple(records_[idx_++], nullptr, false);
        }
        init_ = false;
        return {Record{}, nullptr, true};
    }

    virtual void ResetNext() override {
        idx_ = 0;
        init_ = false;
        records_.clear();
        child_->ResetNext();
        for (auto &expr : exprs_) {
            expr->Reset();
        }
    }

   private:
    PhysicalPlanPtr child_;
    std::vector<std::unique_ptr<Expression>> exprs_;
    std::vector<OrderByInfo> order_info_;
    std::vector<Record, intarkdb::Allocator<Record>> records_;
    size_t idx_{0};
    bool init_{false};
};
