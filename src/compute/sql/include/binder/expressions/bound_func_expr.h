/*
 * 版权所有 (c) GBA-NCTI-ISDC 2022-2024
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
 * bound_func_expr.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/binder/expressions/bound_func_expr.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <fmt/format.h>
#include <fmt/ranges.h>

#include "binder/bound_expression.h"
#include "function/sql_function.h"

class BoundFuncExpr : public BoundExpression {
public:
    // single argument
    explicit BoundFuncExpr(const char* func_name, std::unique_ptr<BoundExpression> arg)
        : BoundExpression(ExpressionType::FUNC_CALL), funcname(func_name)
    {
        args.push_back(std::move(arg));
    }
    // two arguments
    explicit BoundFuncExpr(const char* func_name,
        std::unique_ptr<BoundExpression> arg1,
        std::unique_ptr<BoundExpression> arg2)
        : BoundExpression(ExpressionType::FUNC_CALL), funcname(func_name)
    {
        args.push_back(std::move(arg1));
        args.push_back(std::move(arg2));
    }
    // multiple arguments
    explicit BoundFuncExpr(const char* func_name, std::vector<std::unique_ptr<BoundExpression>> in_args)
        : BoundExpression(ExpressionType::FUNC_CALL), funcname(func_name), args(std::move(in_args)) {}

    auto ToString() const -> std::string override
    {
        return args.size()==0 ?
            fmt::format("{}()", funcname) :
            fmt::format("{}({})", funcname, args);
    }

    auto HasAggregation() const -> bool override
    {
        for (auto& arg : args) {
            if (arg->HasAggregation()) {
                return true;
            }
        }
        return false;
    }

    auto HasSubQuery() const -> bool override
    {
        for (auto& arg : args) {
            if (arg->HasSubQuery()) {
                return true;
            }
        }
        return false;
    }

    auto HasParameter() const -> bool override
    {
        for (auto& arg : args) {
            if (arg->HasParameter()) {
                return true;
            }
        }
        return false;
    }

    auto HasWindow() const -> bool override
    {
        for (auto& arg : args) {
            if (arg->HasWindow()) {
                return true;
            }
        }
        return false;
    }

    virtual hash_t Hash() const override
    {
        hash_t h = HashUtil::HashBytes(funcname.c_str(), funcname.length());
        for (auto& arg : args) {
            h = HashUtil::CombineHash(h, arg->Hash());
        }
        return h;
    }

    virtual auto Equals(const BoundExpression& other) const -> bool override
    {
        if (other.Type() != ExpressionType::FUNC_CALL) {
            return false;
        }
        const auto& func_call = static_cast<const BoundFuncExpr&>(other);
        if (funcname != func_call.funcname || args.size() != func_call.args.size()) {
            return false;
        }
        for (size_t i = 0; i < args.size(); ++i) {
            if (!args[i]->Equals(*func_call.args[i])) {
                return false;
            }
        }
        return true;
    }

    // copy
    virtual std::unique_ptr<BoundExpression> Copy() const override
    {
        std::vector<std::unique_ptr<BoundExpression>> new_args;
        new_args.reserve(args.size());
        for (auto& arg : args) {
            new_args.push_back(arg->Copy());
        }
        return std::make_unique<BoundFuncExpr>(funcname.c_str(), std::move(new_args));
    }

    virtual LogicalType ReturnType() const override
    {
        auto func_iter = SQLFunction::FUNC_MAP.find(funcname);
        if (func_iter != SQLFunction::FUNC_MAP.end()) {
            return func_iter->second.return_type;
        }
        return intarkdb::NewLogicalType(GS_TYPE_NULL);
    }

    std::string funcname;

    std::vector<std::unique_ptr<BoundExpression>> args;
};
