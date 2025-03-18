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
* bound_position_ref_expr.h
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/include/binder/expressions/bound_position_ref_expr.h
*
* -------------------------------------------------------------------------
*/
#pragma once

#include "binder/bound_expression.h"

class BoundPositionRef : public BoundExpression {
   public:
    explicit BoundPositionRef(int64_t index, LogicalType return_type)
        : BoundExpression(ExpressionType::POSITION_REF), index_(index), return_type_(return_type) {}
    virtual ~BoundPositionRef() = default;

    virtual auto ToString() const -> std::string { return fmt::format("PositionRef:{}", index_); }

    virtual bool HasSubQuery() const { return false; }

    virtual bool HasAggregation() const { return false; }

    virtual bool HasParameter() const { return false; }

    virtual bool HasWindow() const { return false; }


    virtual auto Equals(const BoundExpression& other) const -> bool {
        if (other.Type() != ExpressionType::POSITION_REF) {
            return false;
        }
        const auto& other_ref = static_cast<const BoundPositionRef&>(other);
        return index_ == other_ref.index_ && outer_level_ == other_ref.outer_level_;
    }

    virtual hash_t Hash() const {
        hash_t h = 0;
        h = HashUtil::HashBytes("PositionRef", sizeof("PositionRef"));
        return HashUtil::CombineHash(h, HashUtil::HashBytes((const char*)(&index_), sizeof(index_)));
    }


    virtual LogicalType ReturnType() const { return return_type_; }
    // copy
    virtual std::unique_ptr<BoundExpression> Copy() const {
        return std::make_unique<BoundPositionRef>(index_, return_type_);
    }

    int64_t GetIndex() const { return index_; }

    void AddLevel() { outer_level_++; }
    int GetLevel() const { return outer_level_; }

   private:
    int64_t index_;
    LogicalType return_type_;
    int outer_level_{0};
};
