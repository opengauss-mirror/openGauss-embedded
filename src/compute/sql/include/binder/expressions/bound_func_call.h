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
 * bound_func_call.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/binder/expressions/bound_func_call.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <fmt/format.h>
#include <fmt/ranges.h>

#include "binder/bound_expression.h"
#include "function/sql_function.h"

class BoundFuncCall : public BoundExpression {
   public:
    // single argument
    explicit BoundFuncCall(const char* func_name, std::unique_ptr<BoundExpression> arg)
        : BoundExpression(ExpressionType::FUNC_CALL), func_name_(func_name) {
        args_.push_back(std::move(arg));
    }
    // two arguments 
    explicit BoundFuncCall(const char* func_name,std::unique_ptr<BoundExpression> arg1,std::unique_ptr<BoundExpression> arg2)
        : BoundExpression(ExpressionType::FUNC_CALL), func_name_(func_name) {
        args_.push_back(std::move(arg1));
        args_.push_back(std::move(arg2));
    }
    // multiple arguments 
    explicit BoundFuncCall(std::string func_name, std::vector<std::unique_ptr<BoundExpression>> args)
        : BoundExpression(ExpressionType::FUNC_CALL), func_name_(std::move(func_name)), args_(std::move(args)) {}

    auto ToString() const -> std::string override { return fmt::format("{}({})", func_name_, args_); }

    auto HasAggregation() const -> bool override {
        for (auto& arg : args_) {
            if (arg->HasAggregation()) {
                return true;
            }
        }
        return false;
    }

    auto HasSubQuery() const -> bool override {
        for (auto& arg : args_) {
            if (arg->HasSubQuery()) {
                return true;
            }
        }
        return false;
    }

    auto HasParameter() const -> bool override {
        for (auto& arg : args_) {
            if (arg->HasParameter()) {
                return true;
            }
        }
        return false;
    }

    auto HasWindow() const -> bool override {
        for (auto& arg : args_) {
            if (arg->HasWindow()) {
                return true;
            }
        }
        return false;
    }

    virtual hash_t Hash() const override {
        hash_t h = HashUtil::HashBytes(func_name_.c_str(), func_name_.length());
        for (auto& arg : args_) {
            h = HashUtil::CombineHash(h, arg->Hash());
        }
        return h;
    }


    virtual auto Equals(const BoundExpression& other) const -> bool override {
        if (other.Type() != ExpressionType::FUNC_CALL) {
            return false;
        }
        const auto& func_call = static_cast<const BoundFuncCall&>(other);
        if (func_name_ != func_call.func_name_ || args_.size() != func_call.args_.size()) {
            return false;
        }
        for (size_t i = 0; i < args_.size(); ++i) {
            if (!args_[i]->Equals(*func_call.args_[i])) {
                return false;
            }
        }
        return true;
    }

    // copy
    virtual std::unique_ptr<BoundExpression> Copy() const override {
        std::vector<std::unique_ptr<BoundExpression>> args;
        args.reserve(args_.size());
        for (auto& arg : args_) {
            args.push_back(arg->Copy());
        }
        return std::make_unique<BoundFuncCall>(func_name_, std::move(args));
    }

    virtual LogicalType ReturnType() const override {
        auto func_iter = SQLFunction::FUNC_MAP.find(func_name_);
        if (func_iter != SQLFunction::FUNC_MAP.end()) {
            return func_iter->second.return_type;
        }
        return intarkdb::NewLogicalType(GS_TYPE_NULL);
    }

    /** Function name. */
    std::string func_name_;

    /** Arguments of the func call. */
    std::vector<std::unique_ptr<BoundExpression>> args_;
};
