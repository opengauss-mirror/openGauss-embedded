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
* bound_in_expr.h
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/include/binder/expressions/bound_in_expr.h
*
* -------------------------------------------------------------------------
*/
#pragma once

#include "binder/bound_expression.h"

class BoundInExpr : public BoundExpression {
   public:
    BoundInExpr() : BoundExpression(ExpressionType::IN_EXPR) {}
    explicit BoundInExpr(std::unique_ptr<BoundExpression> in_ref, std::vector<std::unique_ptr<BoundExpression>>&& exprs,
                         bool is_not_in = false)
        : BoundExpression(ExpressionType::IN_EXPR),
          in_ref_expr(std::move(in_ref)),
          in_list(std::move(exprs)),
          is_not_in(is_not_in){};

    virtual auto ToString() const -> std::string {
        std::string str = "";
        if (is_not_in)
            str = fmt::format("{} NOT IN (", in_ref_expr->ToString());
        else
            str = fmt::format("{} IN (", in_ref_expr->ToString());
        for (const auto& expr : in_list) {
            str += fmt::format("{} ", expr->ToString());
        }
        str += ")";
        return str;
    }

    virtual auto HasSubQuery() const -> bool {
        if (in_ref_expr->HasSubQuery()) return true;
        for (const auto& expr : in_list) {
            if (expr->HasSubQuery()) return true;
        }
        return false;
    }

    virtual auto HasAggregation() const -> bool {
        if (in_ref_expr->HasAggregation()) return true;
        for (const auto& expr : in_list) {
            if (expr->HasAggregation()) return true;
        }
        return false;
    }

    virtual auto HasParameter() const -> bool {
        if (in_ref_expr->HasParameter()) return true;
        for (const auto& expr : in_list) {
            if (expr->HasParameter()) return true;
        }
        return false;
    }

    virtual auto HasWindow() const -> bool {
        if (in_ref_expr->HasWindow()) return true;
        for (const auto& expr : in_list) {
            if (expr->HasWindow()) return true;
        }
        return false;
    }

    virtual auto Hash() const -> hash_t {
        hash_t hash = in_ref_expr->Hash();
        for (const auto& expr : in_list) {
            hash = HashUtil::CombineHash(hash, expr->Hash());
        }
        return hash;
    }

    virtual auto ReturnType() const -> LogicalType { return LogicalType::Boolean(); }

    virtual auto Equals(const BoundExpression& other) const -> bool {
        if (other.Type() != ExpressionType::IN_EXPR) {
            return false;
        }
        const auto& other_in_expr = static_cast<const BoundInExpr&>(other);
        if (is_not_in != other_in_expr.is_not_in) {
            return false;
        }
        if (!in_ref_expr->Equals(*other_in_expr.in_ref_expr)) {
            return false;
        }
        if (in_list.size() != other_in_expr.in_list.size()) {
            return false;
        }
        for (size_t i = 0; i < in_list.size(); i++) {
            if (!in_list[i]->Equals(*other_in_expr.in_list[i])) {
                return false;
            }
        }
        return true;
    }

    virtual auto Copy() const -> std::unique_ptr<BoundExpression> {
        std::vector<std::unique_ptr<BoundExpression>> exprs;
        for (const auto& expr : in_list) {
            exprs.push_back(expr->Copy());
        }
        return std::make_unique<BoundInExpr>(in_ref_expr->Copy(), std::move(exprs), is_not_in);
    }

    std::unique_ptr<BoundExpression> in_ref_expr;
    std::vector<std::unique_ptr<BoundExpression>> in_list;
    bool is_not_in{false};
};
