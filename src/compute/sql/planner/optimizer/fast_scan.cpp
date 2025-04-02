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
 * fast_scan.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/planner/optimizer/fast_scan.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "planner/optimizer/fast_scan.h"

#include "planner/logical_plan/aggregate_plan.h"
#include "planner/logical_plan/nested_loop_join_plan.h"
#include "planner/logical_plan/projection_plan.h"
#include "planner/logical_plan/scan_plan.h"

namespace intarkdb {

FastScan::FastScan(Optimizer& optimizer) : optimizer_(optimizer) {}

LogicalPlanPtr FastScan::Rewrite(LogicalPlanPtr& op) {
    switch (op->Type()) {
        case LogicalPlanType::Filter:
        case LogicalPlanType::Distinct:
            return Filter(op);
        case LogicalPlanType::Scan:
            return Scan(op);
        case LogicalPlanType::NestedLoopJoin:
        case LogicalPlanType::Union:
            return Join(op);
        case LogicalPlanType::Aggregation:
            return Agg(op);
        case LogicalPlanType::Projection:
            return Projection(op);
        default:
            return Pushdown(op);
    }
}

// TODO: 细化不同分支的filter的处理，目前是只要存在filter，都放弃 fast scan的优化
LogicalPlanPtr FastScan::Filter(LogicalPlanPtr& op) {
    has_filter_ = true;
    return op;
}

LogicalPlanPtr FastScan::Scan(LogicalPlanPtr& op) {
    if (only_count_ == true && has_filter_ == false) {
        std::shared_ptr<ScanPlan> scan_plan = std::dynamic_pointer_cast<ScanPlan>(op);
        scan_plan->SetOnlyCount(true);
    }
    return op;
}

LogicalPlanPtr FastScan::Join(LogicalPlanPtr& op) {
    auto children = op->Children();
    ResetStatus();
    auto left = Rewrite(children[0]);
    ResetStatus();
    auto right = Rewrite(children[1]);
    op->SetChildren({left, right});
    ResetStatus();
    return op;
}

LogicalPlanPtr FastScan::Agg(LogicalPlanPtr& op) {
    // 判断是否只有 count_star
    std::shared_ptr<AggregatePlan> agg_plan = std::dynamic_pointer_cast<AggregatePlan>(op);
    only_count_ = agg_plan->IsOnlyCountStar();
    auto child = agg_plan->GetLastPlan();
    auto new_child = Rewrite(child);
    // 观察后面是否有filter
    if (only_count_ && !has_filter_) {
        return new_child;
    }
    return op;
}

LogicalPlanPtr FastScan::Projection(LogicalPlanPtr& op) {
    ResetStatus();  // projection 是一个新的分支，需要重置状态
    auto children = op->Children();
    auto child = Rewrite(children[0]);
    if (only_count_ && !has_filter_) {
        ResetStatus();  // 结束分支，重置状态
        op->SetChildren({child});
    }
    return op;
}

LogicalPlanPtr FastScan::Pushdown(LogicalPlanPtr& op) {
    auto children = op->Children();
    std::vector<LogicalPlanPtr> new_children;
    for (auto& child : children) {
        new_children.emplace_back(Rewrite(child));
    }
    op->SetChildren(new_children);
    return op;
}

}  // namespace intarkdb
