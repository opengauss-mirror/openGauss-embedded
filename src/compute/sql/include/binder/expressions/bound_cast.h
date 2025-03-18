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
 * bound_cast.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/binder/expressions/bound_cast.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <memory>

#include "binder/bound_expression.h"
#include "type/type_str.h"

class BoundCast : public BoundExpression {
   public:
    BoundCast(LogicalType target_type, std::unique_ptr<BoundExpression> child, bool try_cast)
        : BoundExpression(ExpressionType::TYPE_CAST),
          target_type(target_type),
          child(std::move(child)),
          try_cast(try_cast) {}

    virtual auto ToString() const -> std::string override {
        if (try_cast) {
            return fmt::format("TRY_CAST ( {} AS {} )", child->ToString(), target_type);
        }
        return fmt::format("CAST ( {} AS {} )", child->ToString(), target_type);
    }

    virtual auto HasSubQuery() const -> bool override { return child->HasSubQuery(); }

    virtual auto HasAggregation() const -> bool override { return child->HasAggregation(); }

    virtual auto HasParameter() const -> bool override { return child->HasParameter(); }

    virtual auto HasWindow() const -> bool override { return child->HasWindow(); }

    virtual auto Hash() const -> hash_t override {
        auto h = HashUtil::HashBytes(reinterpret_cast<const char*>(&target_type), sizeof(target_type));
        return HashUtil::CombineHash(h, child->Hash());
    }

    // copy
    virtual auto Copy() const -> std::unique_ptr<BoundExpression> override {
        return std::make_unique<BoundCast>(target_type, child->Copy(), try_cast);
    }

    auto Child() const -> BoundExpression& { return *child; }

    auto TargetType() const -> GStorDataType { return target_type.type; }

    auto TryCast() const -> bool { return try_cast; }


    virtual auto ReturnType() const -> LogicalType override { return target_type; }

    virtual auto Equals(const BoundExpression& other) const -> bool override {
        if (other.Type() != ExpressionType::TYPE_CAST) {
            return false;
        }
        const auto& cast = static_cast<const BoundCast&>(other);
        return target_type.TypeId() == cast.target_type.TypeId() && try_cast == cast.try_cast &&
               child->Equals(*cast.child);
    }

    LogicalType target_type;
    std::unique_ptr<BoundExpression> child;
    bool try_cast{false};
};
