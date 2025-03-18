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
 * fast_scan.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/planner/optimizer/fast_scan.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include "binder/expressions/bound_binary_op.h"
#include "planner/expressions/expression.h"
#include "planner/optimizer/optimizer.h"

namespace intarkdb {

class FastScan {
   public:
    explicit FastScan(Optimizer& optimizer);
    LogicalPlanPtr Rewrite(LogicalPlanPtr& op);

   private:
    LogicalPlanPtr Filter(LogicalPlanPtr& op);
    LogicalPlanPtr Scan(LogicalPlanPtr& op);
    LogicalPlanPtr Join(LogicalPlanPtr& op);
    LogicalPlanPtr Agg(LogicalPlanPtr& op);
    LogicalPlanPtr Pushdown(LogicalPlanPtr& op);
    LogicalPlanPtr Projection(LogicalPlanPtr& op);

    void ResetStatus() {
        has_filter_ = false;
        only_count_ = false;
    }

   private:
    Optimizer& optimizer_;
    bool has_filter_{false};
    bool only_count_{false};
};
}  // namespace intarkdb
