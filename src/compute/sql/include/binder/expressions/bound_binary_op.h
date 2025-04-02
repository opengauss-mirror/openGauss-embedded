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
 * bound_binary_op.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/binder/expressions/bound_binary_op.h
 *
 * -------------------------------------------------------------------------
 */

#pragma once

#include <fmt/core.h>
#include <fmt/format.h>

#include <memory>
#include <string>

#include "binder/bound_expression.h"
#include "common/compare_type.h"
#include "common/logic_op_type.h"
#include "function/function.h"
#include "type/type_id.h"

// 二元操作符
class BoundBinaryOp : public BoundExpression {
   public:
    explicit BoundBinaryOp(std::string op_name, std::unique_ptr<BoundExpression> larg,
                           std::unique_ptr<BoundExpression> rarg)
        : BoundExpression(ExpressionType::BINARY_OP),
          opname_(std::move(op_name)),
          left_arg_(std::move(larg)),
          right_arg_(std::move(rarg)) {
        is_compare_op_ = intarkdb::IsCompareOp(opname_);
        is_logical_op_ = intarkdb::IsBinaryLogicalOp(opname_);
        if (is_compare_op_) {
            func_info_ = intarkdb::FunctionContext::GetCompareFunction(
                opname_, {left_arg_->ReturnType(), right_arg_->ReturnType()});
        } else if (!is_logical_op_) {
            func_info_ =
                intarkdb::FunctionContext::GetFunction(opname_, {left_arg_->ReturnType(), right_arg_->ReturnType()});
        }
    }

    auto ToString() const -> std::string override { return fmt::format("({}{}{})", left_arg_, opname_, right_arg_); }

    auto HasAggregation() const -> bool override { return left_arg_->HasAggregation() || right_arg_->HasAggregation(); }

    virtual auto HasSubQuery() const -> bool override { return left_arg_->HasSubQuery() || right_arg_->HasSubQuery(); }

    virtual auto HasParameter() const -> bool override {
        return left_arg_->HasParameter() || right_arg_->HasParameter();
    }

    virtual auto HasWindow() const -> bool override { return left_arg_->HasWindow() || right_arg_->HasWindow(); }

    BoundExpression& Left() { return *left_arg_; }
    BoundExpression& Right() { return *right_arg_; }

    std::unique_ptr<BoundExpression>& LeftPtr() { return left_arg_; }
    std::unique_ptr<BoundExpression>& RightPtr() { return right_arg_; }

    const std::string OpName() const { return opname_; }

    virtual hash_t Hash() const override {
        auto h = HashUtil::HashBytes(opname_.c_str(), opname_.length());
        h = HashUtil::CombineHash(h, left_arg_->Hash());
        return HashUtil::CombineHash(h, right_arg_->Hash());
    }

    virtual std::unique_ptr<BoundExpression> Copy() const override {
        return std::make_unique<BoundBinaryOp>(opname_, left_arg_->Copy(), right_arg_->Copy());
    }

    virtual auto ReturnType() const -> LogicalType override {
        if (is_logical_op_ || is_compare_op_) {
            return GS_TYPE_BOOLEAN;
        }
        if (func_info_.has_value()) {
            return func_info_->sig.return_type;
        }
        throw intarkdb::Exception(ExceptionType::BINDER, fmt::format("binary op {} not supported", opname_));
    }

    virtual auto Equals(const BoundExpression& other) const -> bool override {
        if (other.Type() != ExpressionType::BINARY_OP) {
            return false;
        }
        auto& other_binary = static_cast<const BoundBinaryOp&>(other);
        return opname_ == other_binary.opname_ && left_arg_->Equals(*other_binary.left_arg_) &&
               right_arg_->Equals(*other_binary.right_arg_);
    }

    bool IsCompareOp() const { return is_compare_op_; }

    bool IsLogicalOp() const { return is_logical_op_; }

    auto FuncInfo() const -> const std::optional<intarkdb::Function>& { return func_info_; }

   private:
    std::string opname_;

    std::unique_ptr<BoundExpression> left_arg_;

    std::unique_ptr<BoundExpression> right_arg_;

    std::optional<intarkdb::Function> func_info_{std::nullopt};

    bool is_compare_op_{false};
    bool is_logical_op_{false};
};
