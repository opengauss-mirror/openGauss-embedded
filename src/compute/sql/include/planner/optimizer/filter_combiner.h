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

#include <functional>

#include "binder/bound_expression.h"
#include "binder/expressions/bound_binary_op.h"
#include "binder/expressions/bound_conjunctive.h"
#include "binder/expressions/bound_constant.h"
#include "common/compare_type.h"
#include "planner/optimizer/optimizer.h"

namespace intarkdb {

enum class ValueComparisonResult : uint8_t { PRUNE_LEFT, PRUNE_RIGHT, UNSATISFIABLE_CONDITION, PRUNE_NOTHING };

enum class FilterResult : uint8_t {
    UNSTATISFIED,  // 永远为false
    SUCCESS,
    UNSUPPORTED,
    IGNORE_EXPR,
};

class FilterCombiner {
   public:
    explicit FilterCombiner() {}
    auto AddFilter(std::unique_ptr<BoundExpression> expr) -> FilterResult;

    auto GenerateFilters(const std::function<void(std::unique_ptr<BoundExpression> filter)>& callback) -> void;

   private:
    struct ExpressionValueInfo {
        Value val;
        ComparisonType comparison_type;
    };

    using ExpressionValueInfoList = std::vector<ExpressionValueInfo>;

    auto AddFilter(BoundExpression& expr) -> FilterResult;
    auto AddConjunctiveFilter(BoundExpression& expr) -> FilterResult;
    auto AddComparisonFilter(BoundExpression& expr) -> FilterResult;
    auto AddConstantFilter(BoundExpression& expr) -> FilterResult;
    auto AddConstantComparison(std::vector<ExpressionValueInfo>& constant_set, ExpressionValueInfo& info)
        -> FilterResult;
    auto MergeValueInformation(ExpressionValueInfo& left, ExpressionValueInfo& right) -> ValueComparisonResult;
    auto MergeEqualAndComparison(ExpressionValueInfo& left, ExpressionValueInfo& right) -> ValueComparisonResult;
    auto MergeNotEqualAndComparison(ExpressionValueInfo& left, ExpressionValueInfo& right) -> ValueComparisonResult;

    auto AddTransitiveFilters(BoundBinaryOp& expr, bool is_root = true) -> FilterResult;
    auto FindTransitiveFilter(const BoundExpression& comparison) -> std::unique_ptr<BoundExpression>;
    auto GetNode(BoundExpression& expr) -> const BoundExpression&;
    auto GetEquivalenceSet(const BoundExpression& expr) -> size_t;
    auto HandleColumnCompareConstant(BoundBinaryOp& comparison) -> FilterResult;
    auto HandleColumnCompareColumn(BoundBinaryOp& comparison) -> FilterResult;

    auto FindRelatedConstantById(size_t id) -> ExpressionValueInfoList&;
    auto FindColumnsById(size_t id) -> std::vector<BoundExpressionRef>&;
    auto MergeUnionSet(size_t left_id, size_t right_id) -> FilterResult;

   private:
    std::vector<std::unique_ptr<BoundExpression>> remaining_filters_;
    BoundExpressionRefMap<std::unique_ptr<BoundExpression>> expr_set_;  // 缓存表达式，避免失效
    BoundExpressionRefMap<size_t> col2id_;                              // Expression -> ID
    std::unordered_map<size_t, std::vector<std::reference_wrapper<const BoundExpression>>>
        id2cols_;                                                                // ID -> Expression
    std::unordered_map<size_t, std::vector<ExpressionValueInfo>> id2constants_;  // ID -> constant values
    size_t idx_{0};
};
}  // namespace intarkdb
