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
 * fast_scan_exec.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/planner/physical_plan/fast_scan_exec.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <memory>
#include <string>
#include <vector>

#include "datasource/table_datasource.h"
#include "planner/logical_plan/logical_plan.h"
#include "planner/physical_plan/physical_plan.h"
#include "type/type_id.h"

/** Scan a table with optional push-down projection. */
class FastScanExec : public PhysicalPlan {
   public:
    FastScanExec(const Schema& schema, std::unique_ptr<TableDataSource> data_source)
        : schema_(schema), source_(std::move(data_source)) {}

    virtual Schema GetSchema() const override { return schema_; }

    virtual std::vector<PhysicalPlanPtr> Children() const override { return {}; }

    virtual std::string ToString() const override { return "FastScanExec"; }

    virtual auto Next() -> std::tuple<Record, knl_cursor_t*, bool> override {
        if (first_) {
            auto val = ValueFactory::ValueBigInt(source_->Rows());
            first_ = false;
            std::vector<Value> values;
            values.push_back(val);
            return {Record{std::move(values)}, nullptr, false};
        }
        first_ = true;  // reset
        return {{}, nullptr, true};
    }

    void ResetNext() override {
        source_->Reset();
        first_ = true;
    }

    // depercated
    virtual auto Execute() const -> RecordBatch override { return RecordBatch({}); }

   private:
    Schema schema_;
    std::unique_ptr<TableDataSource> source_;
    bool first_{true};
};
