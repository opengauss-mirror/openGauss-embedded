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
 * seq_scan_exec.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/planner/physical_plan/seq_scan_exec.h
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

/** Scan a table with optional push-down projection. */
class SeqScanExec : public PhysicalPlan {
   public:
    SeqScanExec(std::unique_ptr<TableDataSource> data_source, const std::vector<std::string>& projection,
                std::vector<std::unique_ptr<Expression>> predicates)
        : source_(std::move(data_source)), projection_(projection), predicates_(std::move(predicates)) {}

    virtual Schema GetSchema() const override;

    virtual std::vector<PhysicalPlanPtr> Children() const override { return {}; }

    virtual std::string ToString() const override;

    virtual auto Next() -> std::tuple<Record, knl_cursor_t*, bool> override;

    void ResetNext() override {
        source_->Reset();
        for (auto& pred : predicates_) {
            pred->Reset();
        }
    }

    // depercated
    virtual auto Execute() const -> RecordBatch override { return RecordBatch({}); }

   private:
    void init();

   private:
    std::unique_ptr<TableDataSource> source_;
    std::vector<std::string> projection_;
    std::vector<std::unique_ptr<Expression>> predicates_;
    bool init_{false};
};
