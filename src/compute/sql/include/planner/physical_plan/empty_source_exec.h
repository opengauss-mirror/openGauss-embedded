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
 * empty_source_exec.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/planner/physical_plan/empty_source_exec.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <fmt/format.h>
#include <fmt/ranges.h>  // for fmt vector

#include <memory>
#include <string>
#include <vector>

#include "datasource/table_datasource.h"
#include "planner/logical_plan/logical_plan.h"
#include "planner/physical_plan/physical_plan.h"

// dummpy empty data source
class EmptySourceExec : public PhysicalPlan {
   public:
    explicit EmptySourceExec(const Schema& schema, int total_count = 0)
        : schema_(schema), total_count_(total_count),idx_{0} {}

    virtual Schema GetSchema() const override { return schema_; }

    virtual std::vector<PhysicalPlanPtr> Children() const override { return {}; }

    virtual std::string ToString() const override { return "EmptySourceExec"; }

    virtual auto Next() -> std::tuple<Record, knl_cursor_t*, bool> override {
        if (idx_ < total_count_) {
            ++idx_;
            return std::make_tuple(Record(), nullptr, false);
        }
        return std::make_tuple(Record(), nullptr, true);
    }

    void ResetNext() override { idx_ = 0; }

    // depercated
    virtual auto Execute() const -> RecordBatch override { return RecordBatch({}); }

   private:
    Schema schema_;
    int total_count_{0};
    int idx_{0};
};
