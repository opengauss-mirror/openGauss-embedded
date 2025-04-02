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
 * bound_sort.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/binder/bound_sort.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <fmt/format.h>
#include <stdint.h>

#include <string_view>

#include "binder/bound_expression.h"

enum class SortType : uint8_t {
    INVALID,
    ASC,
    DESC,
};

static auto SortTypeToString(SortType type) -> std::string_view {
    switch (type) {
        case SortType::INVALID:
            return "Invalid";
        case SortType::ASC:
            return "Ascending";
        case SortType::DESC:
            return "Descending";
        default:
            return "Unknown";
    }
}

class BoundSortItem {
   public:
    explicit BoundSortItem(SortType type, bool is_null_first, std::unique_ptr<BoundExpression> expr)
        : sort_expr(std::move(expr)), type_(type), is_null_first_(is_null_first) {}

    auto ToString() const -> std::string {
        return fmt::format("BoundSortItem {{ sort type={} is_null_first={}, expr={} }}", SortTypeToString(type_),
                           is_null_first_, sort_expr);
    }

    auto GetSortType() const -> SortType { return type_; }

    auto IsNullFirst() const -> bool { return is_null_first_; }

    hash_t Hash() const {
        hash_t hash = 0;
        HashUtil::CombineHash(hash, static_cast<uint8_t>(type_));
        HashUtil::CombineHash(hash, is_null_first_);
        HashUtil::CombineHash(hash, sort_expr->Hash());
        return hash;
    }

    std::unique_ptr<BoundSortItem> Copy() const {
        return std::make_unique<BoundSortItem>(type_, is_null_first_, sort_expr->Copy());
    }

    bool operator==(const BoundSortItem& other) const {
        return type_ == other.type_ && is_null_first_ == other.is_null_first_ && ((sort_expr && other.sort_expr) ? sort_expr->Equals(*other.sort_expr) : (!sort_expr && !other.sort_expr));
    }

    std::unique_ptr<BoundExpression> sort_expr;

   private:
    SortType type_;
    bool is_null_first_;
};
