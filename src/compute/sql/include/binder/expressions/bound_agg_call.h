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
 * bound_agg_call.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/binder/expressions/bound_agg_call.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <fmt/format.h>
#include <fmt/ranges.h>

#include "binder/bound_expression.h"
#include "binder/expressions/bound_column_def.h"
#include "function/aggregate/aggregate_func_name.h"

/**
 * A bound aggregate call, e.g., `sum(x)`.
 */
class BoundAggCall : public BoundExpression {
   public:
    explicit BoundAggCall(const std::string& func_name, bool is_distinct,
                          std::vector<std::unique_ptr<BoundExpression>>&& args)
        : BoundExpression(ExpressionType::AGG_CALL),
          func_name_(func_name),
          is_distinct_(is_distinct),
          args_(std::move(args)) {}

    auto ToString() const -> std::string override {
        if (is_distinct_) {
            return fmt::format("{}_distinct({})", func_name_, args_);
        }
        std::string content;
        if (args_.size() == 1) {
            content = fmt::format("{}({})", func_name_, args_[0]->ToString());
        } else if (args_.size() == 2) {
            content = fmt::format("{}({}, {})", func_name_, args_[0]->ToString(), args_[1]->ToString());
        } else {
            content = fmt::format("{}", func_name_);
        }
        return content;
    }

    auto HasAggregation() const -> bool override { return true; }

    virtual auto HasSubQuery() const -> bool override {
        for (size_t i = 0; i < args_.size(); ++i) {
            if (args_[i]->HasSubQuery()) {
                return true;
            }
        }
        return false;
    }

    virtual auto HasParameter() const -> bool override {
        for (size_t i = 0; i < args_.size(); ++i) {
            if (args_[i]->HasParameter()) {
                return true;
            }
        }
        return false;
    }

    virtual auto HasWindow() const -> bool override { return false; }

    virtual hash_t Hash() const override {
        hash_t h = HashUtil::HashBytes(func_name_.c_str(), func_name_.length());
        for (auto& arg : args_) {
            h = HashUtil::CombineHash(h, arg->Hash());
        }
        return h;
    }

    virtual std::unique_ptr<BoundExpression> Copy() const override {
        std::vector<std::unique_ptr<BoundExpression>> args;
        args.reserve(args_.size());
        for (auto& arg : args_) {
            args.push_back(arg->Copy());
        }
        return std::make_unique<BoundAggCall>(func_name_, is_distinct_, std::move(args));
    }

    virtual LogicalType ReturnType() const override {
        // TODO: 优化函数名判断
        auto type = FindAggInfo(func_name_).return_type;
        switch (type) {
            case AggReturnType::BIGINT:
                return LogicalType::Bigint();
            case AggReturnType::DOUBLE:
                return LogicalType::Decimal(18, 4);  // TODO: 优化,旧规则,变更需要修改测试用例，先这种处理
            case AggReturnType::ARG:
                return args_[0]->ReturnType();
            case AggReturnType::INVALID:
                throw intarkdb::Exception(ExceptionType::BINDER, fmt::format("Invalid function name {}", func_name_));
            default:
                return LogicalType::Decimal(18, 4);
        }
    }

    virtual auto Equals(const BoundExpression& other) const -> bool override {
        if (other.Type() != ExpressionType::AGG_CALL) {
            return false;
        }
        auto& other_agg = static_cast<const BoundAggCall&>(other);
        if (func_name_ != other_agg.func_name_ || is_distinct_ != other_agg.is_distinct_ ||
            args_.size() != other_agg.args_.size()) {
            return false;
        }
        for (size_t i = 0; i < args_.size(); ++i) {
            if (!args_[i]->Equals(*other_agg.args_[i])) {
                return false;
            }
        }
        return true;
    }

    /** Function name. */
    std::string func_name_;

    /** Is distinct aggregation */
    bool is_distinct_;

    /** Arguments of the agg call. */
    std::vector<std::unique_ptr<BoundExpression>> args_;
};
