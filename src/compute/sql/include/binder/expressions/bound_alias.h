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
 * bound_alias.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/binder/expressions/bound_alias.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <fmt/core.h>

#include <memory>

#include "binder/bound_expression.h"

class BoundAlias : public BoundExpression {
   public:
    explicit BoundAlias(std::string alias, std::unique_ptr<BoundExpression> child)
        : BoundExpression(ExpressionType::ALIAS), alias_(std::move(alias)), child_(std::move(child)) {}

    virtual auto ToString() const -> std::string override { return fmt::format("{}", alias_); }

    virtual auto GetName() const -> std::vector<std::string> override { return {alias_}; }

    virtual auto HasAggregation() const -> bool override { return child_->HasAggregation(); }

    virtual auto HasSubQuery() const -> bool override { return child_->HasSubQuery(); }

    virtual auto HasParameter() const -> bool override { return child_->HasParameter(); }

    virtual auto HasWindow() const -> bool override { return child_->HasWindow(); }


    virtual hash_t Hash() const override { return child_->Hash(); }

    virtual std::unique_ptr<BoundExpression> Copy() const override {
        return std::make_unique<BoundAlias>(alias_, child_->Copy());
    }

    virtual auto Equals(const BoundExpression& other) const -> bool override {
        if (other.Type() != ExpressionType::ALIAS) {
            return false;
        }
        const auto& alias = static_cast<const BoundAlias&>(other);
        return alias_ == alias.alias_ && child_->Equals(*alias.child_);
    }


    virtual LogicalType ReturnType() const override { return child_->ReturnType(); }
    std::string alias_;

    std::unique_ptr<BoundExpression> child_;
};
