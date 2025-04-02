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
 * bound_unary_op.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/binder/expressions/bound_unary_op.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <fmt/core.h>
#include <fmt/format.h>

#include <memory>

#include "binder/bound_expression.h"
#include "common/hash_util.h"

// 一元操作符，! ;
class BoundUnaryOp : public BoundExpression {
   public:
    BoundUnaryOp(const std::string& op_name, std::unique_ptr<BoundExpression> expr)
        : BoundExpression(ExpressionType::UNARY_OP), op_name_(op_name), expr_(std::move(expr)) {}

    auto ToString() const -> std::string override { return fmt::format("({} {})", op_name_, expr_); }

    virtual bool HasAggregation() const override { return expr_->HasAggregation(); }

    virtual bool HasSubQuery() const override { return expr_->HasSubQuery(); }

    virtual bool HasParameter() const override { return expr_->HasParameter(); }

    virtual bool HasWindow() const override { return expr_->HasWindow(); }

    BoundExpression& Child() { return *expr_; }
    std::unique_ptr<BoundExpression>& ChildPtr() { return expr_; }

    const std::string OpName() const { return op_name_; }


    virtual hash_t Hash() const override {
        auto h1 = HashUtil::HashBytes(op_name_.c_str(), op_name_.size());
        return HashUtil::CombineHash(h1, expr_->Hash());
    }

    virtual std::unique_ptr<BoundExpression> Copy() const override {
        return std::make_unique<BoundUnaryOp>(op_name_, expr_->Copy());
    }

    virtual auto ReturnType() const -> LogicalType override { return expr_->ReturnType(); }

    virtual auto Equals(const BoundExpression& other) const -> bool override {
        if (other.Type() != ExpressionType::UNARY_OP) {
            return false;
        }
        auto& other_unary = static_cast<const BoundUnaryOp&>(other);
        return op_name_ == other_unary.op_name_ && expr_->Equals(*other_unary.expr_);
    }

   private:
    std::string op_name_;
    std::unique_ptr<BoundExpression> expr_;
};
