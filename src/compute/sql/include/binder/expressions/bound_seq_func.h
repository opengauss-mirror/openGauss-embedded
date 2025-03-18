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
 * bound_seq_func.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/binder/expressions/bound_seq_func.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include "binder/bound_expression.h"
#include "binder/expressions/bound_constant.h"
#include "catalog/catalog.h"

class BoundSequenceFunction : public BoundExpression {
   public:
    BoundSequenceFunction(const std::string& func, std::unique_ptr<BoundExpression> param, const Catalog& cata)
        : BoundExpression(ExpressionType::SEQ_FUNC), funcname(func), arg(std::move(param)), catalog(cata) {}

    auto ToString() const -> std::string override { return fmt::format("{}({})", funcname, arg); }

    auto HasAggregation() const -> bool override { return false; }

    auto HasSubQuery() const -> bool override { return false; }

    auto HasParameter() const -> bool override { return arg->HasParameter(); }

    auto HasWindow() const -> bool override { return arg->HasWindow(); }

    virtual hash_t Hash() const override {
        hash_t h = HashUtil::HashBytes(funcname.c_str(), funcname.length());
        return HashUtil::CombineHash(h, arg->Hash());
    }


    // copy
    virtual std::unique_ptr<BoundExpression> Copy() const override {
        return std::make_unique<BoundSequenceFunction>(funcname, arg->Copy(), catalog);
    }

    virtual auto Equals(const BoundExpression& other) const -> bool override {
        if (other.Type() != ExpressionType::SEQ_FUNC) {
            return false;
        }
        const auto& other_seq = static_cast<const BoundSequenceFunction&>(other);
        return funcname == other_seq.funcname && arg->Equals(*other_seq.arg);
    }

    virtual LogicalType ReturnType() const override { return LogicalType::Bigint(); }

    std::string funcname;
    std::unique_ptr<BoundExpression> arg;
    const Catalog& catalog;
};
