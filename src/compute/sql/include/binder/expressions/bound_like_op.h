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
 * bound_like_op.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/binder/expressions/bound_like_op.h
 *
 * -------------------------------------------------------------------------
 */

#pragma once

#include <fmt/core.h>
#include <fmt/format.h>

#include <memory>
#include <string>

#include "binder/bound_expression.h"
#include "binder/expressions/bound_constant.h"
#include "common/util.h"

/**
 * like 操作：like, not like, ilike, not ilike
 */
class BoundLikeOp : public BoundExpression {
   public:
    explicit BoundLikeOp(std::string op_name, std::vector<std::unique_ptr<BoundExpression> > arguments)
        : BoundExpression(ExpressionType::LIKE_OP), op_name_(std::move(op_name)), arguments_(std::move(arguments)) {}

    auto ToString() const -> std::string override {
        if (arguments_.size() == FULL_LIKE_ARG_NUM) {
            return fmt::format("({} {} {} escape {})", arguments_[0], op_name_, arguments_[1], arguments_[2]);
        } else {
            return fmt::format("({}{}{})", arguments_[0], op_name_, arguments_[1]);
        }
    }

    auto HasAggregation() const -> bool override {
        bool has_agg = false;
        for (auto& arg : arguments_) {
            has_agg |= arg->HasAggregation();
        }
        return has_agg;
    }

    virtual auto HasSubQuery() const -> bool override {
        bool has_sub = false;
        for (auto& arg : arguments_) {
            has_sub |= arg->HasSubQuery();
        }
        return has_sub;
    }

    virtual auto HasParameter() const -> bool override {
        bool has_parameter_ = false;
        for (auto& arg : arguments_) {
            has_parameter_ |= arg->HasParameter();
        }
        return has_parameter_;
    }

    virtual auto HasWindow() const -> bool override {
        bool has_window = false;
        for (auto& arg : arguments_) {
            has_window |= arg->HasWindow();
        }
        return has_window;
    }

    BoundExpression& Left() { return *arguments_[0]; }
    BoundExpression& Right() { return *arguments_[1]; }
    BoundExpression& Escape() { return arguments_.size() > SHORT_LIKE_ARG_NUM ? *arguments_[2] : default_escap_; }

    const std::string OpName() const { return op_name_; }


    virtual auto Equals(const BoundExpression& other) const -> bool override {
        if (other.Type() != ExpressionType::LIKE_OP) {
            return false;
        }
        const auto& other_like = static_cast<const BoundLikeOp&>(other);
        if (op_name_ != other_like.op_name_ || arguments_.size() != other_like.arguments_.size()) {
            return false;
        }
        for (size_t i = 0; i < arguments_.size(); ++i) {
            if (!arguments_[i]->Equals(*other_like.arguments_[i])) {
                return false;
            }
        }
        return true;
    }

    virtual hash_t Hash() const override {
        auto h = HashUtil::HashBytes(op_name_.c_str(), op_name_.length());
        for (auto& arg : arguments_) {
            h = HashUtil::CombineHash(h, arg->Hash());
        }
        return h;
    }

    virtual std::unique_ptr<BoundExpression> Copy() const override {
        std::vector<std::unique_ptr<BoundExpression> > new_args;
        for (auto& arg : arguments_) {
            new_args.push_back(arg->Copy());
        }
        return std::make_unique<BoundLikeOp>(op_name_, std::move(new_args));
    }

    std::vector<std::unique_ptr<BoundExpression> >& Arguments() { return arguments_; }


    virtual LogicalType ReturnType() const override { return LogicalType::Boolean(); }

   private:
    static constexpr int FULL_LIKE_ARG_NUM = 3;
    static constexpr int SHORT_LIKE_ARG_NUM = 2;

    // like op name
    std::string op_name_;

    // arguments for like op
    std::vector<std::unique_ptr<BoundExpression> > arguments_;

    BoundConstant default_escap_ = BoundConstant(ValueFactory::ValueVarchar("\0"));
};
