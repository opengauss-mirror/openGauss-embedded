//===----------------------------------------------------------------------===//
// Copyright 2018-2023 Stichting DuckDB Foundation
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice (including the next paragraph)
// shall be included in all copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.
//===----------------------------------------------------------------------===//
#pragma once
#include <unordered_set>
#include <vector>

#include "binder/expressions/bound_binary_op.h"
#include "planner/expressions/expression.h"
#include "planner/logical_plan/nested_loop_join_plan.h"
#include "planner/optimizer/filter_combiner.h"
#include "planner/optimizer/optimizer.h"

namespace intarkdb {

class FilterPushdown {
   public:
    explicit FilterPushdown(Optimizer& optimizer);
    //! Perform filter pushdown
    LogicalPlanPtr Rewrite(LogicalPlanPtr& op);

   private:
    LogicalPlanPtr FinishPushdown(LogicalPlanPtr& op);

    LogicalPlanPtr PushdownFilter(LogicalPlanPtr& op);

    LogicalPlanPtr PushdownGet(LogicalPlanPtr& op);

    LogicalPlanPtr PushdownJoin(LogicalPlanPtr& op);

    LogicalPlanPtr PushFinalFilter(LogicalPlanPtr& op);

    FilterResult AddFilterToCombiner(std::unique_ptr<BoundExpression> expr);
    FilterResult AddFiltersToCombiner(std::vector<std::unique_ptr<BoundExpression>>&& predicates);

    auto FillCombiner() -> void;
    auto GenerateFromCombiner() -> void;

    auto ExtractJoinPredicates(NestedLoopJoinPlan& plan, std::vector<std::unique_ptr<BoundExpression>>& left_predicates,
                               std::vector<std::unique_ptr<BoundExpression>>& right_predicates) -> void;

   private:
    Optimizer& optimizer;
    std::vector<std::unique_ptr<BoundExpression>> predicates;
    FilterCombiner combiner;
};
}  // namespace intarkdb
