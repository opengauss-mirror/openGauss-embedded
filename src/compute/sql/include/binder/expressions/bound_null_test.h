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
 * bound_null_test.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/binder/expressions/bound_null_test.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include "binder/bound_expression.h"

enum class NullTest : uint8_t {
    IS_NULL,
    IS_NOT_NULL,
};

class BoundNullTest : public BoundExpression {
   public:
    BoundNullTest(std::unique_ptr<BoundExpression> expression, NullTest null_test)
        : BoundExpression(ExpressionType::NULL_TEST), child(std::move(expression)), null_test_type(null_test) {}

    virtual auto ToString() const -> std::string override {
        return fmt::format("{} is {}", child, (null_test_type == NullTest::IS_NULL ? "null" : "not null"));
    };

    virtual bool HasAggregation() const override { return child->HasAggregation(); }

    virtual bool HasSubQuery() const override { return child->HasSubQuery(); }

    virtual bool HasParameter() const override { return child->HasParameter(); }

    virtual bool HasWindow() const override { return child->HasWindow(); }


    virtual hash_t Hash() const override {
        auto h1 =
            null_test_type == NullTest::IS_NULL ? HashUtil::HashBytes("null", 4) : HashUtil::HashBytes("notnull", 7);
        return HashUtil::CombineHash(h1, child->Hash());
    }

    virtual std::unique_ptr<BoundExpression> Copy() const override {
        return std::make_unique<BoundNullTest>(child->Copy(), null_test_type);
    }

    virtual LogicalType ReturnType() const override { return LogicalType::Boolean(); }

    virtual auto Equals(const BoundExpression& other) const -> bool override {
        if (other.Type() != ExpressionType::NULL_TEST) {
            return false;
        }
        auto& other_null_test = static_cast<const BoundNullTest&>(other);
        return null_test_type == other_null_test.null_test_type && child->Equals(*other_null_test.child);
    }

    std::unique_ptr<BoundExpression> child;
    NullTest null_test_type;
};
