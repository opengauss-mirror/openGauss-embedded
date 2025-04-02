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
 * bound_expression.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/binder/bound_expression.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <fmt/core.h>

#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_set>

#include "catalog/column.h"
#include "catalog/column_info.h"
#include "common/hash_util.h"
#include "planner/logical_plan/logical_plan.h"
#include "type/type_id.h"

/**
 * 支持的表达式类型
 */
enum class ExpressionType : uint8_t {
    INVALID = 0, /**< Invalid expression type. */
    LITERAL = 1, /**< 常量表达式 */
    COLUMN_REF,  /**< 列引用 */
    STAR,        /**< * 表达式 */
    ALIAS,       /**< 别名表达式 */
    BINARY_OP,
    UNARY_OP,
    AGG_CALL,
    FUNC_CALL,
    NULL_TEST,  // null test
    SUBQUERY,
    LIKE_OP,
    SEQ_FUNC,  // 序列函数
    TYPE_CAST,
    BOUND_PARAM,
    POSITION_REF,
    CONJUNCTIVE,
    IN_EXPR,
    INTERVAL,
    CASE,
    WINDOW_FUNC_CALL,
};

// 表达式基类
class BoundExpression {
   public:
    explicit BoundExpression(ExpressionType type) : type_(type) {}
    BoundExpression() = default;
    virtual ~BoundExpression() = default;

    ExpressionType Type() const { return type_; }

    virtual auto ToString() const -> std::string { return ""; }

    virtual auto GetName() const -> std::vector<std::string> { return {ToString()}; }

    bool IsInvalid() const { return type_ == ExpressionType::INVALID; }

    virtual bool HasSubQuery() const { throw std::runtime_error("has sub query should have been implemented"); }

    virtual bool HasAggregation() const { throw std::logic_error("has aggregation should have been implemented!"); }

    virtual bool HasParameter() const { throw std::logic_error("has parameter should have been implemented!"); }

    virtual bool HasWindow() const { throw std::logic_error("has window should have been implemented!"); }

    virtual hash_t Hash() const { throw std::runtime_error("hash need to implemented"); }

    virtual LogicalType ReturnType() const { throw std::runtime_error("col type need to implemented"); }

    virtual bool Equals(const BoundExpression &other) const { throw std::runtime_error("equals need to implemented"); }
    // copy
    virtual std::unique_ptr<BoundExpression> Copy() const {
        throw std::runtime_error(fmt::format("{} copy need to implemented", type_));
    }

   private:
    ExpressionType type_{ExpressionType::INVALID};
};

auto BoundExpressionToSchemaColumnInfo(const LogicalPlanPtr &plan, const BoundExpression &expr, uint32_t slot)
    -> SchemaColumnInfo;

struct BoundExpressionEquals {
    bool operator()(const std::unique_ptr<BoundExpression> &lhs, const std::unique_ptr<BoundExpression> &rhs) const {
        return lhs->Equals(*rhs);
    }
};
struct BoundExpressionHash {
    hash_t operator()(const std::unique_ptr<BoundExpression> &expr) const { return expr->Hash(); }
};

struct BoundExpressionRawPtrEquals {
    bool operator()(const BoundExpression *lhs, const BoundExpression *rhs) const { return lhs->Equals(*rhs); }
};

struct BoundExpressionRawPtrHash {
    hash_t operator()(const BoundExpression *expr) const { return expr->Hash(); }
};

struct BoundExpressionRefEquals {
    bool operator()(const std::reference_wrapper<const BoundExpression> &lhs,
                    const std::reference_wrapper<const BoundExpression> &rhs) const {
        return lhs.get().Equals(rhs.get());
    }
};

struct BoundExpressionRefHash {
    hash_t operator()(const std::reference_wrapper<const BoundExpression> &expr) const { return expr.get().Hash(); }
};

using BoundExpressionRef = std::reference_wrapper<const BoundExpression>;

using BoundExpressionRefSet =
    std::unordered_set<std::reference_wrapper<const BoundExpression>, BoundExpressionRefHash, BoundExpressionRefEquals>;

using BoundExpressionPtrSet =
    std::unordered_set<std::unique_ptr<BoundExpression>, BoundExpressionHash, BoundExpressionEquals>;

using BoundExpressionRawPtrSet =
    std::unordered_set<BoundExpression *, BoundExpressionRawPtrHash, BoundExpressionRawPtrEquals>;

template <typename T>
using BoundExpressionPtrMap =
    std::unordered_map<std::unique_ptr<BoundExpression>, T, BoundExpressionHash, BoundExpressionEquals>;

template <typename T>
using BoundExpressionRawPtrMap =
    std::unordered_map<BoundExpression *, T, BoundExpressionRawPtrHash, BoundExpressionRawPtrEquals>;

template <typename T>
using BoundExpressionRefMap = std::unordered_map<std::reference_wrapper<const BoundExpression>, T,
                                                 BoundExpressionRefHash, BoundExpressionRefEquals>;

template <typename T>
struct fmt::formatter<T, std::enable_if_t<std::is_base_of<BoundExpression, T>::value, char>>
    : fmt::formatter<std::string> {
    template <typename FormatCtx>
    auto format(const T &x, FormatCtx &ctx) const {
        return fmt::formatter<std::string>::format(x.ToString(), ctx);
    }
};

template <typename T>
struct fmt::formatter<std::unique_ptr<T>, std::enable_if_t<std::is_base_of<BoundExpression, T>::value, char>>
    : fmt::formatter<std::string> {
    template <typename FormatCtx>
    auto format(const std::unique_ptr<T> &x, FormatCtx &ctx) const {
        return fmt::formatter<std::string>::format(x->ToString(), ctx);
    }
};

template <>
struct fmt::formatter<ExpressionType> : formatter<std::string_view> {
    template <typename FormatContext>
    auto format(ExpressionType c, FormatContext &ctx) const {
        std::string_view name;
        switch (c) {
            case ExpressionType::INVALID:
                name = "Invalid";
                break;
            case ExpressionType::LITERAL:
                name = "Literal Value";
                break;
            case ExpressionType::COLUMN_REF:
                name = "Column Reference";
                break;
            case ExpressionType::STAR:
                name = "Star";
                break;
            case ExpressionType::ALIAS:
                name = "Alias";
                break;
            case ExpressionType::BINARY_OP:
                name = "Binary Operator";
                break;
            case ExpressionType::UNARY_OP:
                name = "Unary Operator";
                break;
            case ExpressionType::AGG_CALL:
                name = "Agg Call";
                break;
            case ExpressionType::FUNC_CALL:
                name = "Func Call";
                break;
            case ExpressionType::NULL_TEST:
                name = "Null Test";
                break;
            case ExpressionType::SUBQUERY:
                name = "SubQuery";
                break;
            case ExpressionType::LIKE_OP:
                name = "Like operator";
                break;
            case ExpressionType::SEQ_FUNC:
                name = "Sequence Function";
                break;
            case ExpressionType::TYPE_CAST:
                name = "TypeCast";
                break;
            case ExpressionType::BOUND_PARAM:
                name = "Bound Param";
                break;
            case ExpressionType::POSITION_REF:
                name = "Position Ref";
                break;
            case ExpressionType::CONJUNCTIVE:
                name = "Conjunctive";
                break;
            case ExpressionType::IN_EXPR:
                name = "In ";
                break;
            case ExpressionType::INTERVAL:
                name = "INTERVAL";
                break;
            case ExpressionType::CASE:
                name = "Case";
                break;
            case ExpressionType::WINDOW_FUNC_CALL:
                name = "Window Function Call";
                break;
        }
        return formatter<std::string_view>::format(name, ctx);
    }
};
