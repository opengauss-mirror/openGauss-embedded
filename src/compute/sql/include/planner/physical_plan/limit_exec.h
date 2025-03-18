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
 * limit_exec.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/planner/physical_plan/limit_exec.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <stdint.h>

#include <optional>

#include "planner/expressions/expression.h"
#include "planner/physical_plan/physical_plan.h"

class LimitExec : public PhysicalPlan {
   public:
    LimitExec(PhysicalPlanPtr child, std::unique_ptr<Expression> limit, std::unique_ptr<Expression> offset)
        : child_(child), limit_(std::move(limit)), offset_(std::move(offset)) {}
    ~LimitExec() {}
    virtual Schema GetSchema() const override { return child_->GetSchema(); }

    // depercated
    virtual auto Execute() const -> RecordBatch override { return RecordBatch({}); }

    virtual std::vector<PhysicalPlanPtr> Children() const override { return {child_}; }

    virtual std::string ToString() const override { return "LimitExec"; }

    virtual void ResetNext() override {
        init_ = false;
        offset_count_ = 0;
        limit_count_ = 0;
        child_->ResetNext();
    }

    void Init() {
        auto limit_value = limit_ ? limit_->Evaluate({}) : ValueFactory::ValueBigInt(0);  // limit must be a constant
        if (!limit_value.IsInteger()) {
            throw intarkdb::Exception(ExceptionType::EXECUTOR, "Limit must be an integer");
        }
        limit_value_ = limit_value.GetCastAs<uint64_t>();
        auto offset_value =
            offset_ ? offset_->Evaluate({}) : ValueFactory::ValueBigInt(0);  // offset must be a constant
        if (!offset_value.IsInteger()) {
            throw intarkdb::Exception(ExceptionType::EXECUTOR, "Offset must be an integer");
        }
        offset_value_ = offset_value.GetCastAs<uint64_t>();
        init_ = true;
    }

    virtual auto Next() -> std::tuple<Record, knl_cursor_t*, bool> override {
        if (!init_) {
            Init();
        }
        while (true) {
            auto&& [record, cursor, eof] = child_->Next();
            if (eof) {
                break;
            }
            if (offset_count_ < offset_value_) {
                offset_count_++;
                continue;
            }
            if (limit_count_ >= limit_value_) {
                break;
            }
            limit_count_++;
            return {std::move(record), cursor, eof};
        }

        return {Record{}, nullptr, true};
    }

   private:
    PhysicalPlanPtr child_;
    std::unique_ptr<Expression> limit_;
    std::unique_ptr<Expression> offset_;
    uint64_t limit_value_{0};
    uint64_t offset_value_{0};
    uint64_t limit_count_{0};
    uint64_t offset_count_{0};
    bool init_{false};
};
