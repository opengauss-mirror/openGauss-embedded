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
 * bound_sub_query.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/binder/expressions/bound_sub_query.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include "binder/bound_expression.h"
#include "binder/statement/select_statement.h"

enum class SubqueryType : uint8_t {
    INVALID = 0,
    SCALAR = 1,  // Regular scalar subquery
    EXISTS = 2,  // EXISTS (SELECT...)
    ANY = 3,     // x = ANY(SELECT...) OR x IN (SELECT...)
    ALL = 4,
};

class BoundSubqueryExpr : public BoundExpression {
   public:
    explicit BoundSubqueryExpr(SubqueryType subquery, std::shared_ptr<SelectStatement> stmt,
                               std::vector<std::unique_ptr<BoundExpression>>&& corr_cols,
                               std::unique_ptr<BoundExpression> expr, const std::string& op, int id)
        : BoundExpression(ExpressionType::SUBQUERY),
          subquery_type(subquery),
          select_statement(std::move(stmt)),
          correlated_columns(std::move(corr_cols)),
          child(std::move(expr)),
          op_name(op),
          subquery_id(id) {
        if (subquery_type == SubqueryType::SCALAR && select_statement->return_list.size() > 0) {
            col_type = select_statement->return_list[0].col_type;
        } else {
            col_type = GS_TYPE_BOOLEAN;
        }
    }

    explicit BoundSubqueryExpr(SubqueryType subquery, LogicalPlanPtr plan,
                               std::vector<std::unique_ptr<BoundExpression>>&& corr_cols,
                               std::unique_ptr<BoundExpression> expr, const std::string& op, int id)
        : BoundExpression(ExpressionType::SUBQUERY),
          subquery_type(subquery),
          correlated_columns(std::move(corr_cols)),
          child(std::move(expr)),
          plan_ptr(plan),
          op_name(op),
          subquery_id(id) {
        if (subquery_type == SubqueryType::SCALAR) {
            col_type = plan->GetSchema().GetColumnInfos()[0].col_type;
        } else {
            col_type = GS_TYPE_BOOLEAN;
        }
    }

    virtual ~BoundSubqueryExpr() {}

    virtual auto ToString() const -> std::string { return GetSubqueryName(); }

    virtual bool HasAggregation() const { return false; }

    virtual bool HasSubQuery() const { return true; }

    virtual bool HasParameter() const {
        if (child) {
            return child->HasParameter();
        } else {
            return false;
        }
    }

    virtual bool HasWindow() const {
        if (child) {
            return child->HasWindow();
        } else {
            return false;
        }
    }

    std::string GetSubqueryName() const { return fmt::format("__subquery_{}", subquery_id); }

    bool IsCorrelated() { return correlated_columns.size() > 0; }

    virtual hash_t Hash() const {
        auto subquery_id_str = GetSubqueryName();
        return HashUtil::HashBytes(subquery_id_str.c_str(), subquery_id_str.size());
    }

    // copy
    virtual std::unique_ptr<BoundExpression> Copy() const {
        std::unique_ptr<BoundExpression> new_child{nullptr};
        if (child) {
            new_child = child->Copy();
        }
        std::vector<std::unique_ptr<BoundExpression>> new_correlated_columns;
        for (auto& col : correlated_columns) {
            new_correlated_columns.push_back(col->Copy());
        }
        if (select_statement) {
            return std::make_unique<BoundSubqueryExpr>(subquery_type, select_statement,
                                                       std::move(new_correlated_columns), std::move(new_child), op_name,
                                                       subquery_id);
        }
        return std::make_unique<BoundSubqueryExpr>(subquery_type, plan_ptr, std::move(new_correlated_columns),
                                                   std::move(new_child), op_name, subquery_id);
    }

    virtual auto Equals(const BoundExpression& other) const -> bool {
        if (other.Type() != ExpressionType::SUBQUERY) {
            return false;
        }
        const auto& other_subquery = static_cast<const BoundSubqueryExpr&>(other);
        return subquery_type == other_subquery.subquery_type && subquery_id == other_subquery.subquery_id;
    }

    SubqueryType subquery_type{SubqueryType::INVALID};

    virtual LogicalType ReturnType() const { return col_type; }

    std::shared_ptr<SelectStatement> select_statement{nullptr};
    std::vector<std::unique_ptr<BoundExpression>> correlated_columns;
    std::unique_ptr<BoundExpression> child{nullptr};
    LogicalPlanPtr plan_ptr{nullptr};
    std::string op_name;
    int subquery_id{0};
    LogicalType col_type{GS_TYPE_BASE};
};

template <>
struct fmt::formatter<SubqueryType> : formatter<std::string_view> {
    template <typename FormatContext>
    auto format(SubqueryType c, FormatContext& ctx) const {
        std::string_view name;
        switch (c) {
            case SubqueryType::INVALID:
                name = "Invalid";
                break;
            case SubqueryType::ANY:
                name = "ANY";
                break;
            case SubqueryType::EXISTS:
                name = "EXISTS";
                break;
            case SubqueryType::SCALAR:
                name = "SCALAR";
                break;
            case SubqueryType::ALL:
                name = "ALL";
                break;
            default:
                name = "Unknown";
                break;
        }
        return formatter<std::string_view>::format(name, ctx);
    }
};
