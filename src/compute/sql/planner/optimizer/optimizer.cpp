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
 * optimizer.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/planner/optimizer/optimizer.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "planner/optimizer/optimizer.h"

#include "planner/optimizer/expression_rewriter.h"
#include "planner/optimizer/fast_scan.h"
#include "planner/optimizer/filter_pushdown.h"
#include "planner/optimizer/projection_eliminate.h"
#include "storage/gstor/zekernel/common/cm_log.h"
namespace intarkdb {

Optimizer::Optimizer() {}

void Optimizer::RunOptimizer(OptRule type, const std::function<void()>& callback) { callback(); }

auto Optimizer::OptimizeLogicalPlan(LogicalPlanPtr& plan) -> LogicalPlanPtr {
    RunOptimizer(OptRule::REWRITE_EXPR, [&]() {
        ExpressionRewriter rewriter(*this);
        plan = rewriter.Rewrite(plan);
    });
    RunOptimizer(OptRule::FAST_COUNT, [&]() {
        FastScan fast_scan_opt(*this);
        plan = fast_scan_opt.Rewrite(plan);
    });
    RunOptimizer(OptRule::FILTER_PUSHDOWN, [&]() {
        FilterPushdown filter_pushdown(*this);
        plan = filter_pushdown.Rewrite(plan);
    });
    RunOptimizer(OptRule::PROJECTION_ELIMINATE, [&]() {
        ProjectionEliminate projection_eliminate(*this);
        plan = projection_eliminate.Rewrite(plan);
    });
    return plan;
}

}  // namespace intarkdb
