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
* subquery_expression.cpp
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/planner/expressions/subquery_expression.cpp
*
* -------------------------------------------------------------------------
*/
#include "planner/expressions/subquery_expression.h"

#include "common/compare_type.h"
#include "common/exception.h"
#include "planner/expression_iterator.h"
#include "planner/expressions/logic_op_expression.h"
#include "type/value.h"

SubQueryExistExpression::SubQueryExistExpression(PhysicalPlanPtr plan)
    : Expression(GS_TYPE_BOOLEAN), sub_query_plan_(plan) {}

void SubQueryExistExpression::SubQueryResultInit() const {
    exist_ = false;
    auto [_, cur, eof] = sub_query_plan_->Next();
    if (!eof) {  // 子查询的结果不为空
        exist_ = true;
    }
    sub_query_plan_->ResetNext();
    evaluated_ = true;
}

auto SubQueryExistExpression::Evaluate(const Record& record) const -> Value {
    if (!evaluated_) {
        SubQueryResultInit();
    }
    return exist_ == true ? ValueFactory::ValueBool(Trivalent::TRI_TRUE)
                          : ValueFactory::ValueBool(Trivalent::TRI_FALSE);
}

auto SubQueryExistExpression::ReEvaluate(const Record& record) const -> Value {
    evaluated_ = false;
    return Evaluate(record);
}

SubQueryScalarExpression::SubQueryScalarExpression(PhysicalPlanPtr plan)
    : Expression(plan->GetSchema().GetColumnInfos()[0].col_type), sub_query_plan_(plan) {}

void SubQueryScalarExpression::SubQueryResultInit() const {
    const Schema& schema = sub_query_plan_->GetSchema();
    const auto& columns = schema.GetColumnInfos();
    if (columns.size() != 1) {
        throw intarkdb::Exception(ExceptionType::EXECUTOR,
                                  "SubQueryScalarExpression: subquery have more than one column");
    }
    data_type_ = columns[0].col_type;
    value_.clear();
    sub_query_plan_->ResetNext();
    while (true) {
        auto [r, _, eof] = sub_query_plan_->Next();
        if (eof) {
            break;
        }
        value_.push_back(r.Field(0));
    }
    evaluated_ = true;
}

auto SubQueryScalarExpression::Evaluate(const Record& record) const -> Value {
    if (!evaluated_) {
        SubQueryResultInit();
    }
    if (value_.size() == 0) {
        return ValueFactory::ValueNull();
    }
    return value_[0];
}

auto SubQueryScalarExpression::ReEvaluate(const Record& record) const -> Value {
    evaluated_ = false;
    return Evaluate(record);
}

SubQueryANYExpression::SubQueryANYExpression(PhysicalPlanPtr plan, std::unique_ptr<Expression> expr,
                                             intarkdb::ComparisonType type)
    : Expression(GS_TYPE_BOOLEAN), sub_query_plan_(plan), expr_(std::move(expr)), type_(type) {}

void SubQueryANYExpression::SubQueryResultInit() const {
    values_.clear();
    while (true) {
        auto [r, _, eof] = sub_query_plan_->Next();
        if (eof) {
            break;
        }
        if (r.ColumnCount() != 1) {
            throw intarkdb::Exception(ExceptionType::EXECUTOR,
                                      "SubQueryExpression: subquery have more than one column");
        }
        values_.insert(r.Field(0));
    }
    sub_query_plan_->ResetNext();
    evaluated_ = true;
}

auto SubQueryANYExpression::ReEvaluate(const Record& record) const -> Value {
    evaluated_ = false;
    return Evaluate(record);
}

auto SubQueryANYExpression::HandleEqual(const Value& v) const -> Value {
    bool have_unknow = false;
    for (auto iter = values_.cbegin(); iter != values_.cend(); ++iter) {
        auto res = v.Equal(*iter);
        if (res == Trivalent::TRI_TRUE) {
            return ValueFactory::ValueBool(Trivalent::TRI_TRUE);
        } else if (res == Trivalent::UNKNOWN) {
            have_unknow = true;
        }
    }
    return have_unknow ? ValueFactory::ValueBool(Trivalent::UNKNOWN) : ValueFactory::ValueBool(Trivalent::TRI_FALSE);
}

auto SubQueryANYExpression::HandleNotEqual(const Value& v) const -> Value {
    bool have_unknow = false;
    for (auto iter = values_.cbegin(); iter != values_.cend(); ++iter) {
        auto res = v.Equal(*iter);
        if (res == Trivalent::TRI_FALSE) {
            return ValueFactory::ValueBool(Trivalent::TRI_TRUE);
        } else if (res == Trivalent::UNKNOWN) {
            have_unknow = true;
        }
    }
    return have_unknow ? ValueFactory::ValueBool(Trivalent::UNKNOWN) : ValueFactory::ValueBool(Trivalent::TRI_FALSE);
}

auto SubQueryANYExpression::HandleLessThan(const Value& v) const -> Value {
    bool have_unknow = false;
    for (auto iter = values_.cbegin(); iter != values_.cend(); ++iter) {
        auto res = v.LessThan(*iter);
        if (res == Trivalent::TRI_TRUE) {
            return ValueFactory::ValueBool(Trivalent::TRI_TRUE);
        } else if (res == Trivalent::UNKNOWN) {
            have_unknow = true;
        }
    }
    return have_unknow ? ValueFactory::ValueBool(Trivalent::UNKNOWN) : ValueFactory::ValueBool(Trivalent::TRI_FALSE);
}

auto SubQueryANYExpression::HandleLessThanOrEqual(const Value& v) const -> Value {
    bool have_unknow = false;
    for (auto iter = values_.cbegin(); iter != values_.cend(); ++iter) {
        auto res = v.LessThanOrEqual(*iter);
        if (res == Trivalent::TRI_TRUE) {
            return ValueFactory::ValueBool(Trivalent::TRI_TRUE);
        } else if (res == Trivalent::UNKNOWN) {
            have_unknow = true;
        }
    }
    return have_unknow ? ValueFactory::ValueBool(Trivalent::UNKNOWN) : ValueFactory::ValueBool(Trivalent::TRI_FALSE);
}

auto SubQueryANYExpression::HandleGreaterThan(const Value& v) const -> Value {
    bool have_unknow = false;
    for (auto iter = values_.cbegin(); iter != values_.cend(); ++iter) {
        auto res = iter->LessThan(v);
        if (res == Trivalent::TRI_TRUE) {
            return ValueFactory::ValueBool(Trivalent::TRI_TRUE);
        } else if (res == Trivalent::UNKNOWN) {
            have_unknow = true;
        }
    }
    return have_unknow ? ValueFactory::ValueBool(Trivalent::UNKNOWN) : ValueFactory::ValueBool(Trivalent::TRI_FALSE);
}

auto SubQueryANYExpression::HandleGreaterThanOrEqual(const Value& v) const -> Value {
    bool have_unknow = false;
    for (auto iter = values_.cbegin(); iter != values_.cend(); ++iter) {
        auto res = iter->LessThanOrEqual(v);
        if (res == Trivalent::TRI_TRUE) {
            return ValueFactory::ValueBool(Trivalent::TRI_TRUE);
        } else if (res == Trivalent::UNKNOWN) {
            have_unknow = true;
        }
    }
    return have_unknow ? ValueFactory::ValueBool(Trivalent::UNKNOWN) : ValueFactory::ValueBool(Trivalent::TRI_FALSE);
}

auto SubQueryANYExpression::Evaluate(const Record& record) const -> Value {
    if (!evaluated_) {
        SubQueryResultInit();
    }
    auto v = expr_->Evaluate(record);
    switch (type_) {
        case intarkdb::ComparisonType::Equal: {
            return HandleEqual(v);
        }
        case intarkdb::ComparisonType::NotEqual: {
            return HandleNotEqual(v);
        }
        case intarkdb::ComparisonType::LessThan: {
            return HandleLessThan(v);
        }
        case intarkdb::ComparisonType::LessThanOrEqual: {
            return HandleLessThanOrEqual(v);
        }
        case intarkdb::ComparisonType::GreaterThan: {
            return HandleGreaterThan(v);
        }
        case intarkdb::ComparisonType::GreaterThanOrEqual: {
            return HandleGreaterThanOrEqual(v);
        }
        default:
            throw intarkdb::Exception(ExceptionType::EXECUTOR, "not suppored op name");
    }
}

using intarkdb::LogicOpType;

std::unique_ptr<Expression> SubQueryFactory(SubqueryType type, PhysicalPlanPtr plan, std::unique_ptr<Expression> expr,
                                            const std::string& opname) {
    switch (type) {
        case SubqueryType::EXISTS:
            return std::make_unique<SubQueryExistExpression>(plan);
        case SubqueryType::SCALAR:
            return std::make_unique<SubQueryScalarExpression>(plan);
        case SubqueryType::ANY:
            return std::make_unique<SubQueryANYExpression>(plan, std::move(expr), intarkdb::ToComparisonType(opname));
        case SubqueryType::ALL: {
            return std::make_unique<LogicUnaryOpExpression>(
                LogicOpType::Not,
                std::make_unique<SubQueryANYExpression>(
                    plan, std::move(expr), intarkdb::RevertComparisonType(intarkdb::ToComparisonType(opname))));
        }
        default:
            throw intarkdb::Exception(ExceptionType::PLANNER, "not supported subquery type");
    }
}

auto SubQueryCorrelatedExpression::Evaluate(const Record& record) const -> Value {
    // record 是外部 record
    for (auto& param : params_) {
        param->InitParam(record);
    }
    return sub_query_expr_->ReEvaluate(record);
}
