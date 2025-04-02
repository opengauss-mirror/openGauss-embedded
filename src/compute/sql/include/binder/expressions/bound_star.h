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
* bound_star.h
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/include/binder/expressions/bound_star.h
*
* -------------------------------------------------------------------------
*/
#pragma once

#include <string_view>

#include "binder/bound_expression.h"

class BoundStar : public BoundExpression {
   public:
    BoundStar(const char* relation_name) : BoundExpression(ExpressionType::STAR) {
        if (relation_name != nullptr) {
            relation_name_ = relation_name;
        }
    }

    virtual bool HasAggregation() const override {
        throw std::invalid_argument("`HasAggregation` should not have been called on `BoundStar`.");
    }

    virtual std::string ToString() const override {
        return relation_name_.length() > 0 ? fmt::format("{}.*", relation_name_) : "*";
    }

    virtual const char* GetRelationName() const { return relation_name_.data(); }

   private:
    std::string_view relation_name_;
};
