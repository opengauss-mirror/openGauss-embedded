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
 * bound_constant.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/binder/expressions/bound_constant.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include "binder/bound_expression.h"
#include "type/value.h"

class BoundConstant : public BoundExpression {
   public:
    explicit BoundConstant(const Value& val) : BoundExpression(ExpressionType::LITERAL), val_(val) {}
    explicit BoundConstant(Value&& val) : BoundExpression(ExpressionType::LITERAL), val_(std::move(val)) {}

    auto ToString() const -> std::string override { return val_.ToString(); }

    auto HasAggregation() const -> bool override { return false; }

    auto HasSubQuery() const -> bool override { return false; }

    auto HasParameter() const -> bool override { return false; }

    auto HasWindow() const -> bool override { return false; }

    Value Val() const { return val_; }
    const Value& ValRef() const { return val_; }
    Value&& MoveableVal() { return std::move(val_); }


    virtual hash_t Hash() const override {
        hash_t h = 0;
        if (!val_.IsNull()) {
            if (val_.IsDecimal()) {
                auto decimal = val_.GetCastAs<dec4_t>();
                h = HashUtil::HashBytes(reinterpret_cast<const char*>(&decimal), cm_dec4_stor_sz(&decimal));
            } else {
                h = HashUtil::HashBytes(val_.GetRawBuff(), val_.Size());
            }
        }
        return h;
    }

    virtual std::unique_ptr<BoundExpression> Copy() const override { return std::make_unique<BoundConstant>(val_); }


    virtual LogicalType ReturnType() const override { return val_.GetLogicalType(); }

    virtual auto Equals(const BoundExpression& other) const -> bool override {
        if (other.Type() != ExpressionType::LITERAL) {
            return false;
        }
        const auto& other_literal = static_cast<const BoundConstant&>(other);
        return (val_.IsNull() && other_literal.val_.IsNull()) || val_.Equal(other_literal.val_) == Trivalent::TRI_TRUE;
    }

   private:
    /** 常量的值**/
    Value val_;
};
