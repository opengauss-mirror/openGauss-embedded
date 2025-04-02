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
#include "planner/optimizer/filter_combiner.h"

#include "binder/expressions/bound_binary_op.h"
#include "binder/expressions/bound_cast.h"
#include "binder/expressions/bound_constant.h"
#include "common/compare_type.h"
#include "planner/planner.h"

namespace intarkdb {

FilterResult FilterCombiner::AddFilter(std::unique_ptr<BoundExpression> expr) {
    auto result = AddFilter(*expr);
    if (result == FilterResult::UNSUPPORTED) {
        remaining_filters_.emplace_back(std::move(expr));  // 不支持的过滤条件，放到 unsupported_filters 中
        return FilterResult::SUCCESS;
    } else if (result == FilterResult::IGNORE_EXPR) {
        return FilterResult::SUCCESS;
    }
    return result;
}

FilterResult FilterCombiner::AddFilter(BoundExpression& expr) {
    if (expr.HasParameter()) {
        return FilterResult::UNSUPPORTED;
    }

    switch (expr.Type()) {
        case ExpressionType::CONJUNCTIVE:
            return AddConjunctiveFilter(expr);
        case ExpressionType::BINARY_OP:
            return AddComparisonFilter(expr);
        case ExpressionType::LITERAL:
            return AddConstantFilter(expr);
        case ExpressionType::TYPE_CAST:
            return AddFilter(*static_cast<BoundCast&>(expr).child);
        // TODO: 支持column_ref 类型的表达式进行合并
        default:
            return FilterResult::UNSUPPORTED;
    }
}

ValueComparisonResult InvertValueComparisonResult(ValueComparisonResult result) {
    if (result == ValueComparisonResult::PRUNE_RIGHT) {
        return ValueComparisonResult::PRUNE_LEFT;
    }
    if (result == ValueComparisonResult::PRUNE_LEFT) {
        return ValueComparisonResult::PRUNE_RIGHT;
    }
    return result;
}

auto FilterCombiner::MergeNotEqualAndComparison(ExpressionValueInfo& left, ExpressionValueInfo& right)
    -> ValueComparisonResult {
    // 裁剪左侧 or 不裁剪
    Trivalent prune_left_side = Trivalent::TRI_FALSE;
    switch (right.comparison_type) {
        case ComparisonType::LessThan:
            prune_left_side = right.val.LessThanOrEqual(left.val);
            break;
        case ComparisonType::LessThanOrEqual:
            prune_left_side = right.val.LessThan(left.val);
            break;
        case ComparisonType::GreaterThan:
            prune_left_side = left.val.LessThanOrEqual(right.val);
            break;
        case ComparisonType::GreaterThanOrEqual:
            prune_left_side = left.val.LessThan(right.val);
            break;
        case ComparisonType::NotEqual:
            prune_left_side = left.val.Equal(right.val);
            break;
        default:
            throw intarkdb::Exception(
                ExceptionType::EXECUTOR,
                fmt::format("unsupported comparison type: {}", intarkdb::CompareTypeToString(right.comparison_type)));
    }
    return prune_left_side == Trivalent::TRI_TRUE ? ValueComparisonResult::PRUNE_LEFT
                                                  : ValueComparisonResult::PRUNE_NOTHING;
}

// left is equal , try to prune right
auto FilterCombiner::MergeEqualAndComparison(ExpressionValueInfo& left, ExpressionValueInfo& right)
    -> ValueComparisonResult {
    // 裁剪右侧 or 返回不可满足
    Trivalent prune_right_side = Trivalent::TRI_FALSE;
    switch (right.comparison_type) {
        case ComparisonType::LessThan:
            prune_right_side = left.val.LessThan(right.val);
            break;
        case ComparisonType::LessThanOrEqual:
            prune_right_side = left.val.LessThanOrEqual(right.val);
            break;
        case ComparisonType::GreaterThan:
            prune_right_side = right.val.LessThan(left.val);
            break;
        case ComparisonType::GreaterThanOrEqual:
            prune_right_side = right.val.LessThanOrEqual(left.val);
            break;
        case ComparisonType::NotEqual:
            prune_right_side = TrivalentOper::Not(left.val.Equal(right.val));
            break;
        case ComparisonType::Equal:
            prune_right_side = left.val.Equal(right.val);
            break;
        default:
            throw intarkdb::Exception(ExceptionType::EXECUTOR, "Invalid comparison type");
    }
    return prune_right_side == Trivalent::TRI_TRUE ? ValueComparisonResult::PRUNE_RIGHT
                                                   : ValueComparisonResult::UNSATISFIABLE_CONDITION;
}

// 合并条件
ValueComparisonResult FilterCombiner::MergeValueInformation(ExpressionValueInfo& left, ExpressionValueInfo& right) {
    if (left.val.IsNull() || right.val.IsNull()) {
        return ValueComparisonResult::UNSATISFIABLE_CONDITION;
    }

    if (left.comparison_type == ComparisonType::Equal) {
        return MergeEqualAndComparison(left, right);
    } else if (right.comparison_type == ComparisonType::Equal) {
        // right is COMPARE_EQUAL
        return InvertValueComparisonResult(MergeValueInformation(right, left));
    } else if (left.comparison_type == ComparisonType::NotEqual) {
        return MergeNotEqualAndComparison(left, right);
    } else if (right.comparison_type == ComparisonType::NotEqual) {
        return InvertValueComparisonResult(MergeValueInformation(right, left));
    } else if ((left.comparison_type == ComparisonType::GreaterThan ||
                left.comparison_type == ComparisonType::GreaterThanOrEqual) &&
               (right.comparison_type == ComparisonType::GreaterThan ||
                right.comparison_type == ComparisonType::GreaterThanOrEqual)) {
        // 选择更窄的边界
        if (right.val.LessThan(left.val) == Trivalent::TRI_TRUE) {
            // 左边界更窄，裁剪右侧
            return ValueComparisonResult::PRUNE_RIGHT;
        } else if (left.val.LessThan(right.val) == Trivalent::TRI_TRUE) {
            // 右边界更窄，裁剪左侧
            return ValueComparisonResult::PRUNE_LEFT;
        } else {
            // constants are equivalent
            if (left.comparison_type == ComparisonType::GreaterThanOrEqual) {  // 左侧是闭区间，裁剪左侧
                return ValueComparisonResult::PRUNE_LEFT;
            } else {
                return ValueComparisonResult::PRUNE_RIGHT;
            }
        }
    } else if ((left.comparison_type == ComparisonType::LessThan ||
                left.comparison_type == ComparisonType::LessThanOrEqual) &&
               (right.comparison_type == ComparisonType::LessThan ||
                right.comparison_type == ComparisonType::LessThanOrEqual)) {
        if (left.val.LessThan(right.val) == Trivalent::TRI_TRUE) {
            // 左边界更窄，裁剪右侧
            return ValueComparisonResult::PRUNE_RIGHT;
        } else if (right.val.LessThan(left.val) == Trivalent::TRI_TRUE) {
            // 右边界更窄，裁剪左侧
            return ValueComparisonResult::PRUNE_LEFT;
        } else {
            // constants are equivalent
            if (left.comparison_type == ComparisonType::LessThanOrEqual) {  // 左侧是闭区间，裁剪左侧
                return ValueComparisonResult::PRUNE_LEFT;
            } else {
                return ValueComparisonResult::PRUNE_RIGHT;
            }
        }
    } else if (left.comparison_type == ComparisonType::LessThanOrEqual ||
               left.comparison_type == ComparisonType::LessThan) {
        // right is [>=] or [>]
        if (right.val.LessThanOrEqual(left.val) == Trivalent::TRI_TRUE) {
            return ValueComparisonResult::PRUNE_NOTHING;
        } else {
            return ValueComparisonResult::UNSATISFIABLE_CONDITION;
        }
    } else {
        // left is [>] and right is [<] or [!=]
        return InvertValueComparisonResult(MergeValueInformation(right, left));
    }
}

auto FilterCombiner::AddConstantComparison(std::vector<ExpressionValueInfo>& constant_set, ExpressionValueInfo& info)
    -> FilterResult {
    if (info.val.IsNull()) {
        return FilterResult::UNSTATISFIED;
    }
    for (size_t i = 0; i < constant_set.size(); ++i) {
        auto comparison = MergeValueInformation(constant_set[i], info);
        switch (comparison) {
            case ValueComparisonResult::PRUNE_LEFT:
                constant_set.erase(constant_set.begin() + i);  // 剔除左边的表达式
                i--;
                break;
            case ValueComparisonResult::PRUNE_RIGHT:
                return FilterResult::SUCCESS;
            case ValueComparisonResult::UNSATISFIABLE_CONDITION:
                return FilterResult::UNSTATISFIED;
            default:
                break;
        }
    }
    constant_set.push_back(info);
    return FilterResult::SUCCESS;
}

auto FilterCombiner::FindTransitiveFilter(const BoundExpression& expr) -> std::unique_ptr<BoundExpression> {
    // We only check for bound column ref
    if (expr.Type() != ExpressionType::COLUMN_REF) {
        return nullptr;
    }
    for (size_t i = 0; i < remaining_filters_.size(); i++) {
        if (remaining_filters_[i]->Type() == ExpressionType::BINARY_OP) {
            auto& comparison = static_cast<BoundBinaryOp&>(*remaining_filters_[i]);
            if (expr.Equals(comparison.Right()) && (comparison.OpName() != "!=" && comparison.OpName() != "<>")) {
                auto filter = std::move(remaining_filters_[i]);
                remaining_filters_.erase(remaining_filters_.begin() + i);
                return filter;
            }
        }
    }
    return nullptr;
}

FilterResult FilterCombiner::AddConjunctiveFilter(BoundExpression& expr) {
    BoundConjunctive& conjunctive = static_cast<BoundConjunctive&>(expr);
    for (auto& child : conjunctive.items) {
        auto result = AddFilter(*child);
        if (result != FilterResult::SUCCESS) {
            return result;
        }
    }
    return FilterResult::SUCCESS;
}

FilterResult FilterCombiner::AddConstantFilter(BoundExpression& expr) {
    BoundConstant& constant = static_cast<BoundConstant&>(expr);
    const auto& val = constant.ValRef();
    if (val.IsNull() || val.GetCastAs<bool>() != true) {
        return FilterResult::UNSTATISFIED;
    }
    return FilterResult::IGNORE_EXPR;  // 不必加入到 CNF 条件列表中
}

auto FilterCombiner::HandleColumnCompareConstant(BoundBinaryOp& binary_op) -> FilterResult {
    auto left_is_constant = binary_op.Left().Type() == ExpressionType::LITERAL;

    // 获取非常量表达式
    const BoundExpression& node = GetNode(left_is_constant ? binary_op.Right() : binary_op.Left());  //
    auto index = GetEquivalenceSet(node);                                                            // 获取列ID
    auto& constant = left_is_constant ? binary_op.Left() : binary_op.Right();
    const Value& val = static_cast<BoundConstant&>(constant).ValRef();
    if (val.IsNull()) {
        return FilterResult::UNSTATISFIED;  // 永不成立
    }

    ExpressionValueInfo info;
    auto comparison_type = intarkdb::ToComparisonType(binary_op.OpName());
    info.comparison_type = left_is_constant ? intarkdb::FilpComparisonType(comparison_type) : comparison_type;
    info.val = val;

    auto& constant_list = FindRelatedConstantById(index);

    auto ret = AddConstantComparison(constant_list, info);  // 添加常量比较条件

    // 处理传递性条件
    auto transitive_filter = FindTransitiveFilter(node);
    if (transitive_filter != nullptr) {
        // try to add transitive filters
        if (AddTransitiveFilters(static_cast<BoundBinaryOp&>(*transitive_filter)) == FilterResult::UNSUPPORTED) {
            remaining_filters_.push_back(std::move(transitive_filter));  // 重新填回到 unsupported_filters
        }
    }
    return ret;
}

auto FilterCombiner::HandleColumnCompareColumn(BoundBinaryOp& binary_op) -> FilterResult {
    auto comparison_type = intarkdb::ToComparisonType(binary_op.OpName());
    if (comparison_type != ComparisonType::Equal) {
        return FilterResult::UNSUPPORTED;  // not supported 
    }
    // get the LHS and RHS nodes
    auto& left_node = GetNode(binary_op.Left());
    auto& right_node = GetNode(binary_op.Right());

    if (left_node.Equals(right_node)) {
        return FilterResult::UNSUPPORTED;  // 保留
    }
    // get the equivalence sets of the LHS and RHS
    auto left_equivalence_id = GetEquivalenceSet(left_node);
    auto right_equivalence_id = GetEquivalenceSet(right_node);
    if (left_equivalence_id == right_equivalence_id) {
        return FilterResult::SUCCESS;
    }
    // add the right bucket into the left bucket
    return MergeUnionSet(left_equivalence_id, right_equivalence_id);
}

FilterResult FilterCombiner::AddComparisonFilter(BoundExpression& expr) {
    BoundBinaryOp& binary_op = static_cast<BoundBinaryOp&>(expr);
    if (!binary_op.IsCompareOp()) {
        // 不支持非比较操作符
        return FilterResult::UNSUPPORTED;
    }

    auto left_is_constant = binary_op.Left().Type() == ExpressionType::LITERAL;
    auto right_is_constant = binary_op.Right().Type() == ExpressionType::LITERAL;
    if (left_is_constant && right_is_constant) {  // TODO: 后续支持常量折叠
        return FilterResult::UNSUPPORTED;
    }
    if (left_is_constant || right_is_constant) {
        return HandleColumnCompareConstant(binary_op);
    } else {  // 两边都是变量
        return HandleColumnCompareColumn(binary_op);
    }
    return FilterResult::SUCCESS;
}

const BoundExpression& FilterCombiner::GetNode(BoundExpression& expr) {
    auto iter = expr_set_.find(expr);
    if (iter != expr_set_.end()) {
        return *iter->second;
    }
    auto expr_copy = expr.Copy();
    const auto& expr_ref = *expr_copy;
    expr_set_.insert({expr_ref, std::move(expr_copy)});
    return expr_ref;
}

size_t FilterCombiner::GetEquivalenceSet(const BoundExpression& expr) {
    auto iter = col2id_.find(expr);
    if (iter != col2id_.end()) {
        return iter->second;
    }
    auto index = idx_++;
    col2id_[expr] = index;
    id2cols_[index].push_back(expr);
    id2constants_.insert({index, {}});
    return index;
}

FilterResult FilterCombiner::AddTransitiveFilters(BoundBinaryOp& comparison, bool is_root) {
    // comparison type is > or >= or < or <=
    // get the LHS and RHS nodes
    auto& left_node = GetNode(comparison.Left());
    auto& right_node = GetNode(comparison.Right());

    if (left_node.Equals(right_node)) {
        return FilterResult::UNSUPPORTED;
    }

    // get the equivalence sets of the LHS and RHS
    size_t left_equivalence_id = GetEquivalenceSet(left_node);
    size_t right_equivalence_id = GetEquivalenceSet(right_node);
    if (left_equivalence_id == right_equivalence_id) {
        return FilterResult::UNSUPPORTED;
    }

    auto& left_constants = FindRelatedConstantById(left_equivalence_id);
    auto& right_constants = FindRelatedConstantById(right_equivalence_id);
    bool is_successful = false;
    bool is_inserted = false;
    auto comparison_type = intarkdb::ToComparisonType(comparison.OpName());
    // read every constant filters already inserted for the right scalar variable
    // and see if we can create new transitive filters, e.g., there is already a filter i > 10,
    // suppose that we have now the j >= i, then we can infer a new filter j > 10
    for (const auto& right_constant : right_constants) {
        ExpressionValueInfo info;
        info.val = right_constant.val;
        // there is already an equality filter, e.g., i = 10
        if (right_constant.comparison_type == ComparisonType::Equal) {
            // create filter j [>, >=, <, <=] 10
            // suppose the new comparison is j >= i and we have already a filter i = 10,
            // then we create a new filter j >= 10
            // and the filter j >= i can be pruned by not adding it into the remaining filters
            info.comparison_type = comparison_type;
        } else if ((comparison_type == ComparisonType::GreaterThanOrEqual &&
                    (right_constant.comparison_type == ComparisonType::GreaterThan ||
                     right_constant.comparison_type == ComparisonType::GreaterThanOrEqual)) ||
                   (comparison_type == ComparisonType::LessThanOrEqual &&
                    (right_constant.comparison_type == ComparisonType::LessThan ||
                     right_constant.comparison_type == ComparisonType::LessThanOrEqual))) {
            // filters (j >= i AND i [>, >=] 10) OR (j <= i AND i [<, <=] 10)
            // create filter j [>, >=] 10 and add the filter j [>=, <=] i into the remaining filters
            info.comparison_type = right_constant.comparison_type;  // create filter j [>, >=, <, <=] 10
            if (!is_inserted) {
                // Add the filter j >= i in the remaing filters
                auto filter = std::make_unique<BoundBinaryOp>(comparison.OpName(), comparison.Left().Copy(),
                                                              comparison.Right().Copy());
                remaining_filters_.push_back(std::move(filter));
                is_inserted = true;
            }
        } else if ((comparison_type == ComparisonType::GreaterThan &&
                    (right_constant.comparison_type == ComparisonType::GreaterThan ||
                     right_constant.comparison_type == ComparisonType::GreaterThanOrEqual)) ||
                   (comparison_type == ComparisonType::LessThan &&
                    (right_constant.comparison_type == ComparisonType::LessThan ||
                     right_constant.comparison_type == ComparisonType::LessThanOrEqual))) {
            // filters (j > i AND i [>, >=] 10) OR j < i AND i [<, <=] 10
            // create filter j [>, <] 10 and add the filter j [>, <] i into the remaining filters
            // the comparisons j > i and j < i are more restrictive
            info.comparison_type = comparison_type;
            if (!is_inserted) {
                // Add the filter j [>, <] i
                auto filter = std::make_unique<BoundBinaryOp>(comparison.OpName(), comparison.Left().Copy(),
                                                              comparison.Right().Copy());
                remaining_filters_.push_back(std::move(filter));
                is_inserted = true;
            }
        } else {
            // we cannot add a new filter
            continue;
        }
        // Add the new filer into the left set
        if (AddConstantComparison(left_constants, info) == FilterResult::UNSTATISFIED) {
            return FilterResult::UNSTATISFIED;
        }
        is_successful = true;
    }

    if (is_successful) {
        if (is_root) {
            // now check for remaining trasitive filters from the left column
            auto transitive_filter = FindTransitiveFilter(comparison.Left());
            if (transitive_filter != nullptr) {
                // try to add transitive filters
                if (AddTransitiveFilters(static_cast<BoundBinaryOp&>(*transitive_filter), false) ==
                    FilterResult::UNSUPPORTED) {
                    // in case of unsuccessful re-add filter into remaining ones
                    remaining_filters_.push_back(std::move(transitive_filter));
                }
            }
        }
        return FilterResult::SUCCESS;
    }

    return FilterResult::UNSUPPORTED;
}

void FilterCombiner::GenerateFilters(const std::function<void(std::unique_ptr<BoundExpression> filter)>& callback) {
    for (auto& filter : remaining_filters_) {
        callback(std::move(filter));
    }
    remaining_filters_.clear();

    for (auto& item : id2cols_) {
        auto idx = item.first;
        auto& exprs = item.second;

        auto& constant_list = FindRelatedConstantById(idx);

        for (size_t i = 0; i < exprs.size(); ++i) {
            // for each expr also create a comparison with each constant
            for (size_t k = 0; k < constant_list.size(); ++k) {
                auto& info = constant_list[k];
                auto expr = exprs[i].get().Copy();
                auto comparison =
                    std::make_unique<BoundBinaryOp>(CompareTypeToString(info.comparison_type), std::move(expr),
                                                    std::make_unique<BoundConstant>(info.val));
                callback(std::move(comparison));
            }
        }
    }
    expr_set_.clear();
    col2id_.clear();
    id2cols_.clear();
    id2constants_.clear();
    idx_ = 0;
}

auto FilterCombiner::FindRelatedConstantById(size_t id) -> ExpressionValueInfoList& {
    auto iter = id2constants_.find(id);
    if (iter == id2constants_.end()) {
        id2constants_.insert({id, {}});
        return id2constants_.find(id)->second;
    }
    return iter->second;
}

auto FilterCombiner::FindColumnsById(size_t id) -> std::vector<BoundExpressionRef>& {
    return id2cols_.find(id)->second;
}

auto FilterCombiner::MergeUnionSet(size_t left_equivalence_id, size_t right_equivalence_id) -> FilterResult {
    auto& left_bucket = FindColumnsById(left_equivalence_id);
    auto& right_bucket = FindColumnsById(right_equivalence_id);

    for (const auto& right_expr : right_bucket) {
        // rewrite the equivalence set mapping for this node
        col2id_[right_expr] = left_equivalence_id;
        // add the node to the left bucket
        left_bucket.push_back(right_expr);
    }

    // remove the right bucket
    id2cols_.erase(right_equivalence_id);

    // now add all constant values from the right bucket to the left bucket
    auto& left_constant_list = FindRelatedConstantById(left_equivalence_id);
    auto& right_constant_list = FindRelatedConstantById(right_equivalence_id);

    // O(n^2) comparison of all constant values
    for (auto& right_constant : right_constant_list) {
        if (AddConstantComparison(left_constant_list, right_constant) == FilterResult::UNSTATISFIED) {
            return FilterResult::UNSTATISFIED;
        }
    }
    return FilterResult::UNSUPPORTED;
}

}  // namespace intarkdb
