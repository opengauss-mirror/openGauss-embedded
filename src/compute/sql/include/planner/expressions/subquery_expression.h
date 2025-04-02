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
 * subquery_expression.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/planner/expressions/subquery_expression.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <functional>
#include <set>

#include "binder/expressions/bound_sub_query.h"
#include "common/compare_type.h"
#include "planner/expressions/expression.h"
#include "planner/physical_plan/distinct_exec.h"
#include "planner/physical_plan/physical_plan.h"
#include "planner/planner.h"

struct cmpValue {
    bool operator()(const Value& v1, const Value& v2) const {
        if (v1.IsNull()) {
            return false;
        }
        if (v2.IsNull()) {
            return true;
        }
        auto res = v1.LessThan(v2);
        return res == Trivalent::TRI_TRUE;
    }
};

std::unique_ptr<Expression> SubQueryFactory(SubqueryType type, PhysicalPlanPtr plan, std::unique_ptr<Expression> expr,
                                            const std::string& opname);
// EXISTS 子查询
class SubQueryExistExpression : public Expression {
   public:
    SubQueryExistExpression(PhysicalPlanPtr plan);
    virtual auto Evaluate(const Record& record) const -> Value;
    virtual auto ReEvaluate(const Record& record) const -> Value;

    virtual auto Reset() -> void { evaluated_ = false; }
   private:
    void SubQueryResultInit() const;

   private:
    PhysicalPlanPtr sub_query_plan_;
    mutable bool exist_{false};
    mutable bool evaluated_{false};
};

// 标量子查询
class SubQueryScalarExpression : public Expression {
   public:
    SubQueryScalarExpression(PhysicalPlanPtr plan);
    virtual auto Evaluate(const Record& record) const -> Value;
    virtual auto ReEvaluate(const Record& record) const -> Value;

    virtual auto Reset() -> void { evaluated_ = false; }
   private:
    void SubQueryResultInit() const;

   private:
    PhysicalPlanPtr sub_query_plan_;
    mutable std::vector<Value> value_;
    mutable LogicalType data_type_;
    mutable bool evaluated_{false};
};

// ANY 类型的子查询
class SubQueryANYExpression : public Expression {
   public:
    SubQueryANYExpression(PhysicalPlanPtr plan, std::unique_ptr<Expression> expr, intarkdb::ComparisonType type);
    virtual auto Evaluate(const Record& record) const -> Value;
    virtual auto ReEvaluate(const Record& record) const -> Value;

    virtual auto Reset() -> void { evaluated_ = false; }

   private:
    void SubQueryResultInit() const;
    auto HandleEqual(const Value& v) const -> Value;
    auto HandleNotEqual(const Value& v) const -> Value;
    auto HandleLessThan(const Value& v) const -> Value;
    auto HandleLessThanOrEqual(const Value& v) const -> Value;
    auto HandleGreaterThan(const Value& v) const -> Value;
    auto HandleGreaterThanOrEqual(const Value& v) const -> Value;

   private:
    PhysicalPlanPtr sub_query_plan_;
    std::unique_ptr<Expression> expr_;
    mutable std::set<Value, cmpValue> values_;
    intarkdb::ComparisonType type_;
    mutable bool evaluated_{false};
};

// 关联子查询Expression
class SubQueryCorrelatedExpression : public Expression {
   public:
    SubQueryCorrelatedExpression(const std::vector<const ColumnParamExpression*> params,
                                 std::unique_ptr<Expression> sub_query_expr)
        : Expression(sub_query_expr->GetLogicalType()), params_(params), sub_query_expr_(std::move(sub_query_expr)) {}
    virtual auto Evaluate(const Record& record) const -> Value;

   private:
    std::vector<const ColumnParamExpression*> params_;
    std::unique_ptr<Expression> sub_query_expr_;
};
