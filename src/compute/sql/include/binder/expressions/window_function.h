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
* window_function.h
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/include/binder/expressions/window_function.h
*
* -------------------------------------------------------------------------
*/
#pragma once

#include <string>

#include "binder/bound_expression.h"
#include "binder/over_clause.h"

namespace intarkdb {

class WindowFunction : public BoundExpression {
   public:
    WindowFunction(std::string name, std::unique_ptr<OverClause> over_clause)
        : BoundExpression(ExpressionType::WINDOW_FUNC_CALL),
          name_(std::move(name)),
          over_clause_(std::move(over_clause)) {}

    auto ToString() const -> std::string override { return name_; }

    auto GetName() const -> std::vector<std::string> override { return {name_}; }

    virtual bool HasSubQuery() const override { return false; }

    virtual bool HasAggregation() const override {
        // FIXME: over clause 中可能含有聚合函数
        return false;
    }

    virtual bool HasParameter() const override { return false; }

    virtual bool HasWindow() const override { return true; }

    virtual hash_t Hash() const override {
        hash_t h = HashUtil::HashBytes(name_.c_str(), name_.length());
        return HashUtil::CombineHash(h, over_clause_->Hash());
    }

    virtual LogicalType ReturnType() const override {
        // FIXME: 根据不同的 window function 返回不同的类型
        return LogicalType::Bigint();
    }

    virtual bool Equals(const BoundExpression &other) const override {
        if (Type() != other.Type()) {
            return false;
        }
        const auto &other_window = static_cast<const WindowFunction &>(other);
        return name_ == other_window.name_ && *over_clause_ == *other_window.over_clause_;
    }

    // copy
    virtual std::unique_ptr<BoundExpression> Copy() const override {
        return std::make_unique<WindowFunction>(name_, over_clause_->Copy());
    }

    const std::string GetFunctionName() const { return name_; }
    const OverClause& GetOverClause() const { return *over_clause_; }

   private:
    std::string name_;
    std::unique_ptr<OverClause> over_clause_;
};

}  // namespace intarkdb
