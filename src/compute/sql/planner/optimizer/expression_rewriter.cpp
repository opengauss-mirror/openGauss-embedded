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
 * expression_rewriter.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/planner/optimizer/expression_rewriter.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "planner/optimizer/expression_rewriter.h"

#include "binder/expressions/bound_binary_op.h"
#include "binder/expressions/bound_cast.h"
#include "binder/expressions/bound_constant.h"
#include "common/math_op_type.h"
#include "planner/expression_iterator.h"
#include "planner/logical_plan/filter_plan.h"
#include "planner/logical_plan/projection_plan.h"
#include "type/type_str.h"
#include "type/type_system.h"

namespace intarkdb {

ExpressionRewriter::ExpressionRewriter(Optimizer& optimizer) {}

static auto IsMathOpWithConstant(BoundBinaryOp& op) -> bool {
    auto math_type = ToMathOpType(op.OpName());
    // not support mod and div operation
    if (math_type == MathOpType::Mod || math_type == MathOpType::Divide || math_type == MathOpType::Invalid) {
        return false;
    }
    auto& left = op.LeftPtr();
    auto& right = op.RightPtr();
    if (left->Type() == ExpressionType::LITERAL && right->Type() == ExpressionType::COLUMN_REF) {
        return true;
    }
    if (left->Type() == ExpressionType::COLUMN_REF && right->Type() == ExpressionType::LITERAL) {
        return true;
    }
    return false;
}

static auto TryConvert(LogicalType type, Value& val) -> bool {
    // TODO: 避免try catch
    try {
        val = ValueCast::CastValue(val, type);
    } catch (intarkdb::Exception& intarkdb_ex) {
        return false;
    } catch (const std::exception& ex) {
        return false;
    }
    return true;
}

static auto MoveConstantInternal(const std::string& compare_op, BoundConstant& constants, BoundBinaryOp& math_op,
                                 bool left_is_constant, std::unique_ptr<BoundExpression>& origin_expr) -> void {
    auto math_type = ToMathOpType(math_op.OpName());
    const auto& val1 = constants.ValRef();
    const auto& val2 = math_op.LeftPtr()->Type() == ExpressionType::LITERAL
                           ? static_cast<BoundConstant&>(*math_op.LeftPtr()).ValRef()
                           : static_cast<BoundConstant&>(*math_op.RightPtr()).ValRef();
    if (val1.IsNull() || val2.IsNull()) {
        origin_expr = std::make_unique<BoundConstant>(ValueFactory::ValueNull(GS_TYPE_BOOLEAN));
        return;
    }
    if (!(val1.IsInteger() && val2.IsInteger())) {
        // only handle integer type
        return;
    }
    auto& column_ref = math_op.LeftPtr()->Type() == ExpressionType::LITERAL ? math_op.RightPtr() : math_op.LeftPtr();
    switch (math_type) {
        case MathOpType::Plus: {
            // val1 and val2 must be all integer , minus operation is safe
            auto result = val1.GetCastAs<hugeint_t>();
            if (!Hugeint::SubtractInPlace(result, val2.GetCastAs<hugeint_t>())) {
                return;
            }
            Value result_val = ValueFactory::ValueHugeInt(result);
            if (!TryConvert(column_ref->ReturnType(), result_val)) {
                // TODO: add constant_or_null when compare type is EQUAL
                return;
            }
            if (left_is_constant) {
                origin_expr = std::make_unique<BoundBinaryOp>(compare_op, std::make_unique<BoundConstant>(result_val),
                                                              std::move(column_ref));
            } else {
                origin_expr = std::make_unique<BoundBinaryOp>(compare_op, std::move(column_ref),
                                                              std::make_unique<BoundConstant>(result_val));
            }
            break;
        }
        case MathOpType::Minus: {
            // 区分 减数 和 被减数
            bool constant_be_minus = math_op.LeftPtr()->Type() == ExpressionType::LITERAL ? false : true;  // 常量被减
            if (constant_be_minus) {
                // val1 + val2
                auto result = val1.GetCastAs<hugeint_t>();
                if (!Hugeint::AddInPlace(result, val2.GetCastAs<hugeint_t>())) {
                    return;
                }
                Value result_val = ValueFactory::ValueHugeInt(result);
                if (!TryConvert(column_ref->ReturnType(), result_val)) {
                    return;
                }
                if (left_is_constant) {
                    origin_expr = std::make_unique<BoundBinaryOp>(
                        compare_op, std::make_unique<BoundConstant>(result_val), std::move(column_ref));
                } else {
                    origin_expr = std::make_unique<BoundBinaryOp>(compare_op, std::move(column_ref),
                                                                  std::make_unique<BoundConstant>(result_val));
                }
            } else {
                // val2 - val1
                auto result = val2.GetCastAs<hugeint_t>();
                if (!Hugeint::SubtractInPlace(result, val1.GetCastAs<hugeint_t>())) {
                    return;
                }
                Value result_val = ValueFactory::ValueHugeInt(result);
                if (!TryConvert(column_ref->ReturnType(), result_val)) {
                    return;
                }
                if (left_is_constant) {
                    origin_expr = std::make_unique<BoundBinaryOp>(compare_op, std::move(column_ref),
                                                                  std::make_unique<BoundConstant>(result_val));
                } else {
                    origin_expr = std::make_unique<BoundBinaryOp>(
                        compare_op, std::make_unique<BoundConstant>(result_val), std::move(column_ref));
                }
            }
            break;
        }
        case MathOpType::Multiply: {
            // val1 / val2
            auto val1_hugeint = val1.GetCastAs<hugeint_t>();
            auto val2_hugeint = val2.GetCastAs<hugeint_t>();  // trans to hugeint ，avoid overflow
            // compare val2 is zero or not , note val2_real is double , use speical method to compare
            if (val2_hugeint == 0) {
                // val2 is zero , expression result is 0 or null
                // TODO: return a new type expression constant or null depend on column_ref value is null or not
                // but now we just ignore it
                return;
            }

            // 判断是否有溢出
            if (val1_hugeint == NumericLimits<hugeint_t>::Minimum() && val2_hugeint == -1) {
                // overflow
                return;
            }

            // 判断是否可以整除
            if ((val1_hugeint % val2_hugeint) != 0) {
                // can't divide , no optimize
                return;
            }

            Value result_val = ValueFactory::ValueHugeInt(val1_hugeint / val2_hugeint);
            bool is_neg = val2_hugeint < 0;
            if (!TryConvert(column_ref->ReturnType(), result_val)) {
                return;
            }

            if ((left_is_constant && !is_neg) || (!left_is_constant && is_neg)) {
                origin_expr = std::make_unique<BoundBinaryOp>(compare_op, std::make_unique<BoundConstant>(result_val),
                                                              std::move(column_ref));
            } else {
                origin_expr = std::make_unique<BoundBinaryOp>(compare_op, std::move(column_ref),
                                                              std::make_unique<BoundConstant>(result_val));
            }
            break;
        }
        case MathOpType::Divide:
        case MathOpType::Mod: {
            throw intarkdb::Exception(ExceptionType::EXECUTOR,
                                      math_op.OpName() + " operation is not supported in move constant rewrite rule");
        }
        default:
            throw intarkdb::Exception(ExceptionType::EXECUTOR, "Invalid math operation type: " + math_op.OpName());
    }
}

static auto MoveConstant(std::unique_ptr<BoundExpression>& expr, bool is_root) -> void {
    // move constant
    if (expr->Type() != ExpressionType::BINARY_OP) {
        return;
    }
    BoundBinaryOp& binary_op = static_cast<BoundBinaryOp&>(*expr);
    if (!IsCompareOp(binary_op.OpName())) {
        return;
    }
    auto& left = binary_op.LeftPtr();
    auto& right = binary_op.RightPtr();
    if (left->Type() == ExpressionType::LITERAL && right->Type() == ExpressionType::BINARY_OP) {
        // move constant to right
        BoundBinaryOp& right_binary_op = static_cast<BoundBinaryOp&>(*right);
        if (!IsMathOpWithConstant(right_binary_op)) {
            return;
        }
        BoundConstant& constant = static_cast<BoundConstant&>(*left);
        MoveConstantInternal(binary_op.OpName(), constant, right_binary_op, true, expr);
    } else if (right->Type() == ExpressionType::LITERAL && left->Type() == ExpressionType::BINARY_OP) {
        // move constant to left
        BoundBinaryOp& left_binary_op = static_cast<BoundBinaryOp&>(*left);
        if (!IsMathOpWithConstant(left_binary_op)) {
            return;
        }
        BoundConstant& constant = static_cast<BoundConstant&>(*right);
        MoveConstantInternal(binary_op.OpName(), constant, left_binary_op, false, expr);
    }
}

static auto FoldConstant(std::unique_ptr<BoundExpression>& expr) -> void {
    auto type = expr->Type();
    switch (type) {
        case ExpressionType::TYPE_CAST: {
            const auto& cast_expr = static_cast<const BoundCast&>(*expr);
            if (cast_expr.child && cast_expr.child->Type() == ExpressionType::LITERAL) {
                auto& constant = static_cast<BoundConstant&>(*cast_expr.child);
                auto target_type = cast_expr.ReturnType();
                const auto& val = constant.ValRef();
                auto is_try_cast = cast_expr.TryCast();
                // TODO: 收敛这个函数逻辑
                try {
                    if (!val.IsNull()) {
                        auto v = ValueCast::CastValue(val, target_type);
                        expr = std::make_unique<BoundConstant>(v);
                    } else {
                        expr = std::make_unique<BoundConstant>(val);
                    }
                } catch (intarkdb::Exception& intarkdb_ex) {
                    if (is_try_cast) {
                        expr = std::make_unique<BoundConstant>(ValueFactory::ValueNull());
                    } else {
                        throw intarkdb_ex;
                    }
                } catch (const std::runtime_error& ex) {
                    if (is_try_cast) {
                        expr = std::make_unique<BoundConstant>(ValueFactory::ValueNull());
                    } else {
                        throw ex;
                    }
                }
            }
            break;
        }
        default:
            break;
    }
}

auto ExpressionRewriter::Rewrite(const LogicalPlanPtr& plan) -> LogicalPlanPtr {
    switch (plan->Type()) {
        case LogicalPlanType::Filter:
            return RewriteFilter(plan);
        default:
            break;
    }
    auto children = plan->Children();
    for (auto& child : children) {
        child = Rewrite(child);
    }
    plan->SetChildren(children);
    return plan;
}

auto ExpressionRewriter::RewriteFilter(const LogicalPlanPtr& plan) -> LogicalPlanPtr {
    FilterPlan& filter_plan = static_cast<FilterPlan&>(*plan);
    // fold constant
    ExpressionIterator::EnumerateExpression(filter_plan.expr,
                                            [](std::unique_ptr<BoundExpression>& child) { FoldConstant(child); });

    bool is_root = true;
    ExpressionIterator::EnumerateExpression(filter_plan.expr, [&is_root](std::unique_ptr<BoundExpression>& child) {
        MoveConstant(child, is_root);
        is_root = false;
    });
    return plan;
}

auto ExpressionRewriter::RewriteProjection(const LogicalPlanPtr& plan) -> LogicalPlanPtr {
    ProjectionPlan& project_plan = static_cast<ProjectionPlan&>(*plan);
    auto& exprs = project_plan.Exprs();
    for (auto& expr : exprs) {
        ExpressionIterator::EnumerateExpression(expr,
                                                [](std::unique_ptr<BoundExpression>& child) { FoldConstant(child); });
    }
    return plan;
}

}  // namespace intarkdb
