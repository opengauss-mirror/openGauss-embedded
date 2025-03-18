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
* function_expression.h
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/include/planner/expressions/function_expression.h
*
* -------------------------------------------------------------------------
*/
#pragma once

#include "function/sql_function.h"
#include "planner/expressions/expression.h"

class FunctionExpression : public Expression {
   public:
    FunctionExpression(const std::string& funcname, FuncType func, std::vector<std::unique_ptr<Expression>> args , LogicalType return_type)
        : Expression(return_type),funcname_(funcname), func_(func), args_(std::move(args)) {}

    virtual auto Evaluate(const Record& record) const -> Value {
        std::vector<Value> values;
        for (auto& arg : args_) {
            values.push_back(arg->Evaluate(record));
        }
        return func_(values);
    }

    virtual auto ToString() const -> std::string { return funcname_; }

    virtual auto Reset() -> void {
        for (auto& arg : args_) {
            arg->Reset();
        }
    }

   private:
    std::string funcname_;
    FuncType func_;
    std::vector<std::unique_ptr<Expression>> args_;
};
