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
 * like_expression.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/planner/expressions/like_expression.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <fmt/core.h>
#include <fmt/format.h>

#include "planner/expressions/expression.h"
#include "type/value.h"

enum class LikeType : uint8_t {
    Invalid = 0,
    Like,
    NotLike,
    Glob,
    ILike,
    NotILike,
    LikeEscape,
    NotLikeEscape,
    ILikeEscape,
    NotILikeEscape
};  // ~~, !~~, ~~~, ~~*, !~~*

LikeType LikeTypeFromString(const std::string& str);

class LikeExpression : public Expression {
   public:
    LikeExpression(LikeType type, std::unique_ptr<Expression> lexp, std::unique_ptr<Expression> rexp,
                   std::unique_ptr<Expression> escape)
        : Expression(GS_TYPE_BOOLEAN),
          type_(type),
          lexp_(std::move(lexp)),
          rexp_(std::move(rexp)),
          escape_(std::move(escape)) {}

    virtual auto Evaluate(const Record& record) const -> Value;

    virtual auto ToString() const -> std::string {
        return fmt::format("{} {} {}", lexp_->ToString(), type_, rexp_->ToString());
    };

    virtual auto Reset() -> void {
        lexp_->Reset();
        rexp_->Reset();
        if (escape_) {
            escape_->Reset();
        }
    }

   private:
    LikeType type_;
    std::unique_ptr<Expression> lexp_;
    std::unique_ptr<Expression> rexp_;
    std::unique_ptr<Expression> escape_;
};

template <>
struct fmt::formatter<LikeType> : formatter<std::string_view> {
    template <typename FormatContext>
    auto format(LikeType c, FormatContext& ctx) const {
        std::string_view name;
        switch (c) {
            case LikeType::Like: {
                name = "~~";
                break;
            }
            case LikeType::NotLike: {
                name = "!~~";
                break;
            }
            case LikeType::Glob: {
                name = "~~~";
                break;
            }
            case LikeType::ILike: {
                name = "~~*";
                break;
            }
            case LikeType::NotILike: {
                name = "!~~*";
                break;
            }
            case LikeType::LikeEscape: {
                name = "like_escape";
                break;
            }
            case LikeType::NotLikeEscape: {
                name = "not_like_escape";
                break;
            }
            case LikeType::ILikeEscape: {
                name = "ilike_escape";
                break;
            }
            case LikeType::NotILikeEscape: {
                name = "not_ilike_escape";
                break;
            }
            default: {
                name = "Invalid Like";
            }
        }
        return formatter<std::string_view>::format(name, ctx);
    }
};
