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
 * bind_expression.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/binder/bind_expression.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "binder/binder.h"
#include "binder/expressions/bound_agg_call.h"
#include "binder/expressions/bound_binary_op.h"
#include "binder/expressions/bound_cast.h"
#include "binder/expressions/bound_conjunctive.h"
#include "binder/expressions/bound_func_expr.h"
#include "binder/expressions/bound_in_expr.h"
#include "binder/expressions/bound_like_op.h"
#include "binder/expressions/bound_null_test.h"
#include "binder/expressions/bound_position_ref_expr.h"
#include "binder/expressions/bound_seq_func.h"
#include "binder/expressions/bound_star.h"
#include "binder/expressions/bound_sub_query.h"
#include "binder/expressions/bound_unary_op.h"
#include "binder/expressions/window_function.h"
#include "binder/transform_typename.h"
#include "common/aexpr_type.h"
#include "common/null_check_ptr.h"
#include "common/string_util.h"
#include "function/aggregate/aggregate_func_name.h"

using namespace duckdb_libpgquery;

const int MAX_EXPRESSION_DEPTH = 1000;
const int CATALOG_SCHEMA_NAME = 3;
const int SCHEMA_NAME = 2;
const int UNQUALIFIED_NAME = 1;

#define CHECK_EXPRESSION_DEPTH(depth)                                                                      \
    if ((depth) > MAX_EXPRESSION_DEPTH) {                                                                  \
        throw intarkdb::Exception(ExceptionType::BINDER, "expression tree is too large(maximum is 1000)"); \
    }

auto Binder::BindExpression(duckdb_libpgquery::PGNode *node, int depth) -> std::unique_ptr<BoundExpression> {
    CHECK_EXPRESSION_DEPTH(depth);

    if (!node) {
        return nullptr;
    }
    switch (node->type) {
        case duckdb_libpgquery::T_PGResTarget:
            // typeT_ResTarget表示一个列,ResTarget 再细分为其他类型(ColumnRef,Star等)
            return BindExprResTarget(reinterpret_cast<duckdb_libpgquery::PGResTarget *>(node), depth);
        case duckdb_libpgquery::T_PGColumnRef:
            return BindColumnRef(reinterpret_cast<duckdb_libpgquery::PGColumnRef *>(node));
        case duckdb_libpgquery::T_PGAConst:
            return BindConstanExpr(reinterpret_cast<duckdb_libpgquery::PGAConst *>(node));
        case duckdb_libpgquery::T_PGAStar:
            return BindStarExpr(reinterpret_cast<duckdb_libpgquery::PGAStar *>(node));
        case duckdb_libpgquery::T_PGFuncCall:
            return BindFuncExpression(reinterpret_cast<duckdb_libpgquery::PGFuncCall *>(node), depth);
        case duckdb_libpgquery::T_PGAExpr:
            return BindAExpr(reinterpret_cast<duckdb_libpgquery::PGAExpr *>(node), depth);
        case duckdb_libpgquery::T_PGBoolExpr: {
            return BindBoolExpr(reinterpret_cast<duckdb_libpgquery::PGBoolExpr *>(node), depth);
        }
        case duckdb_libpgquery::T_PGNullTest: {
            return BindNullTest(reinterpret_cast<duckdb_libpgquery::PGNullTest *>(node), depth);
        }
        case duckdb_libpgquery::T_PGSubLink: {
            return BindSubQueryExpr(reinterpret_cast<duckdb_libpgquery::PGSubLink *>(node));
        }
        case duckdb_libpgquery::T_PGParamRef: {
            return BindParamRef(reinterpret_cast<duckdb_libpgquery::PGParamRef *>(node));
        }
        case duckdb_libpgquery::T_PGTypeCast: {
            return BindTypeCast(reinterpret_cast<duckdb_libpgquery::PGTypeCast *>(node), depth);
        }
        case duckdb_libpgquery::T_PGIntervalConstant: {
            return BindInterval(reinterpret_cast<duckdb_libpgquery::PGIntervalConstant *>(node));
        }
        case duckdb_libpgquery::T_PGCaseExpr: {
            return BindCase(reinterpret_cast<duckdb_libpgquery::PGCaseExpr *>(node), depth);
        }
        default:
            break;
    }
    throw intarkdb::Exception(ExceptionType::BINDER,
                              fmt::format("type {} expr  not supported", Binder::ConvertNodeTagToString(node->type)));
}

auto Binder::BindExprResTarget(duckdb_libpgquery::PGResTarget *root, int depth) -> std::unique_ptr<BoundExpression>
{
    CHECK_EXPRESSION_DEPTH(depth);
    bind_location = root->location;
    auto expr = BindExpression(root->val, depth + 1);
    if (!expr) {
        return nullptr;
    }
    if (root->name != nullptr && strlen(root->name) > 0) {
        return std::make_unique<BoundAlias>(root->name, std::move(expr));
    }
    return expr;
}

auto Binder::BindStarExpr(duckdb_libpgquery::PGAStar *node) -> std::unique_ptr<BoundExpression>
{
    bind_location = node->location;
    if (node->except_list) {
        // e.g SELECT * EXCLUDE(y) FROM integers;
        throw intarkdb::Exception(ExceptionType::BINDER, "exclude list is not supported yet");
    }
    if (node->replace_list) {
        // e.g SELECT * REPLACE (y as z) FROM integers;
        throw intarkdb::Exception(ExceptionType::BINDER, "replace list is not supported yet");
    }
    if (node->expr || node->columns) {
        // e.g SELECT COLUMNS([x for x in (*) if x <> 'integers.i']) FROM integers;
        throw intarkdb::Exception(ExceptionType::BINDER, "star expr in column is not supported yet");
    }
    // SELECT * FROM tbl; 时 relation 为空
    // SELECT tbl.* from tbl; 时 relation 为 tbl
    return std::make_unique<BoundStar>(node->relation);
}

constexpr int MAX_COLUMN_ITEM_NUM = 2;
static auto TransformColumName(duckdb_libpgquery::PGList *fields) -> std::vector<std::string> {
    if (fields->length < 1) {
        throw intarkdb::Exception(ExceptionType::SYNTAX, "unexpected field length");
    }
    // fields 会有多段，比如 表前缀
    // e.g: select a.sid from student as a; columns_names wiil be ["a","sid"]
    // 如果没有表前缀，那么columns_names只有一段
    std::vector<std::string> column_names;
    for (auto node = fields->head; node != nullptr; node = node->next) {
        // TODO: 列名不可直接转为小写，会引起其他问题
        column_names.emplace_back(NullCheckPtrCast<duckdb_libpgquery::PGValue>(node->data.ptr_value)->val.str);
    }
    // 列名最多只能有两段
    if (column_names.size() > MAX_COLUMN_ITEM_NUM) {
        throw intarkdb::Exception(ExceptionType::BINDER, "column reference must have at most two identifiers");
    }
    return column_names;
}

auto Binder::BindColumnRefAlias(const std::vector<std::string> &column_name,
                                const std::unordered_map<std::string, AliasIdx> &alias_binding)
    -> std::unique_ptr<BoundExpression> {
    // 从别名中查找
    if (column_name.size() != 1) {
        return nullptr;
    }
    const auto &name = column_name[0];
    for (const auto &alias : alias_binding) {
        if (intarkdb::StringUtil::IsEqualIgnoreCase(alias.first, name)) {
            if (alias.second.idx != INVALID_IDX) {
                return std::make_unique<BoundPositionRef>(alias.second.idx, alias.second.type);
            }
            throw intarkdb::Exception(ExceptionType::BINDER, fmt::format("ambiguous alias {}", name));
        }
    }
    return nullptr;
}

auto Binder::BindCorrelatedColumn(const std::vector<std::string> &col_name, Binder *binder, BinderContext &ctx,
                                  bool root) -> std::unique_ptr<BoundExpression> {
    // 从上层中查找列信息
    if (binder) {
        auto expr = binder->BindColumnRef(col_name, false);
        // 考虑expr的类型 PositionRef or ColumnRef
        if (expr != nullptr) {
            if (expr->Type() == ExpressionType::COLUMN_REF) {
                auto &column_ref = static_cast<BoundColumnRef &>(*expr);
                column_ref.AddLevel();  // 增加列引用层级
            }
            if (expr->Type() == ExpressionType::POSITION_REF) {  // position ref 为对外层查询的引用
                // TODO: 暂不支持子查询中的别名引用
                throw intarkdb::Exception(ExceptionType::BINDER, "not supported alias reference in subquery");
            }
            if (root) {
                ctx.correlated_columns.push_back(expr->Copy());  // 递归的第一层才能加入到correlated_columns
            }
            return expr;
        }
    }
    return nullptr;
}

auto Binder::BindColumnRef(const std::vector<std::string> &column_name, bool root) -> std::unique_ptr<BoundExpression> {
    auto expr = BindColumnRefFromTableBinding(column_name, root);
    if (!expr) {
        expr = BindColumnRefAlias(column_name, ctx.alias_binding);
    }
    if (!expr) {
        expr = BindCorrelatedColumn(column_name, parent_binder_, ctx, root);
    }
    if (!expr && check_column_exist_) {
        throw intarkdb::Exception(ExceptionType::BINDER,
                                  fmt::format("column {} not found", fmt::join(column_name, ".")), bind_location);
    }
    if(!expr) {
        return std::make_unique<BoundColumnRef>(column_name, GS_TYPE_UNKNOWN);
    }
    return expr;
}

auto Binder::BindColumnRef(duckdb_libpgquery::PGColumnRef *node) -> std::unique_ptr<BoundExpression> {
    bind_location = node->location;
    std::unique_ptr<BoundExpression> expr = nullptr;
    auto column_fields = NullCheckPtrCast<duckdb_libpgquery::PGNode>(node->fields->head->data.ptr_value);
    if (column_fields->type == duckdb_libpgquery::T_PGString) {
        const auto &column_name = TransformColumName(node->fields);
        return BindColumnRef(column_name, true);
    }
    throw intarkdb::Exception(
        ExceptionType::NOT_IMPLEMENTED,
        fmt::format("ColumnRef type {} not implemented!",
            Binder::ConvertNodeTagToString(column_fields->type)), bind_location);
}

auto Binder::BindConstanExpr(duckdb_libpgquery::PGAConst *node) -> std::unique_ptr<BoundExpression>
{
    bind_location = node->location;
    auto &val = node->val;
    return BindValue(&val);
}

auto Binder::BindValue(duckdb_libpgquery::PGValue *node) -> std::unique_ptr<BoundExpression> {
    auto val = node->val;
    switch (node->type) {
        case duckdb_libpgquery::T_PGInteger: {
            if (val.ival > INT32_MAX) {
                throw intarkdb::Exception(ExceptionType::OUT_OF_RANGE, "integer out of range!", bind_location);
            }
            return std::make_unique<BoundConstant>(ValueFactory::ValueInt(val.ival));
        }
        case duckdb_libpgquery::T_PGBitString:
        case duckdb_libpgquery::T_PGString: {
            return std::make_unique<BoundConstant>(ValueFactory::ValueVarchar(val.str));
        }
        case duckdb_libpgquery::T_PGFloat: {
            auto len = strlen(val.str);
            if (len >= GS_MAX_DEC_OUTPUT_ALL_PREC) {
                len = GS_MAX_DEC_OUTPUT_ALL_PREC - 1;
            }
            std::string str_val(val.str, len);
            bool try_cast_as_integer = true;
            bool try_cast_as_decimal = true;
            int decimal_position = -1;
            for (size_t i = 0; i < str_val.length(); ++i) {
                if (str_val[i] == '.') {
                    try_cast_as_integer = false;
                    decimal_position = i;
                }
                if (str_val[i] == 'e' || str_val[i] == 'E') {
                    // 科学计数法，只能转换为double
                    try_cast_as_integer = false;
                    try_cast_as_decimal = false;
                }
            }
            if (try_cast_as_integer) {
                int64_t bigint_value;
                if (TryCast::Operation<std::string, int64_t>(str_val, bigint_value)) {
                    return std::make_unique<BoundConstant>(ValueFactory::ValueBigInt(bigint_value));
                }
                hugeint_t hugeint_value;
                if (TryCast::Operation<std::string, hugeint_t>(str_val, hugeint_value)) {
                    return std::make_unique<BoundConstant>(ValueFactory::ValueHugeInt(hugeint_value));
                }
            }
            // 计算decimal 的精度
            size_t decimal_offset = str_val[0] == '-' ? 3 : 2;
            if (try_cast_as_decimal && decimal_position >= 0 &&
                str_val.length() < DecimalPrecision::max + decimal_offset) {
                // figure out the width/scale based on the decimal position
                auto width = static_cast<uint8_t>(str_val.length() - 1);
                auto scale = static_cast<uint8_t>(width - decimal_position);
                if (str_val[0] == '-') {
                    width--;
                }
                if (width <= DecimalPrecision::max) {
                    // we can cast the value as a decimal
                    auto dec_value = ValueFactory::ValueDecimal(Cast::Operation<std::string, dec4_t>(str_val));
                    dec_value.SetScaleAndPrecision(scale, width);
                    return std::make_unique<BoundConstant>(dec_value);
                }
            }
            return std::make_unique<BoundConstant>(ValueFactory::ValueVarchar(str_val));  // 提高精度
        }
        case duckdb_libpgquery::T_PGNull: {
            return std::make_unique<BoundConstant>(ValueFactory::ValueNull());
        }
        default:
            break;
    }
    throw intarkdb::Exception(ExceptionType::BINDER,
                              fmt::format("unsupported value type: {}", Binder::ConvertNodeTagToString(node->type)),
                              bind_location);
}

static auto IsValidFunction(const std::string &funcname) -> bool {
    if (SQLFunction::FUNC_MAP.find(funcname) != SQLFunction::FUNC_MAP.end()) {
        return true;
    }
    return false;
}

static auto IsValidWindowFunction(const std::string &funcname) -> bool {
    static const intarkdb::CaseInsensitiveSet window_funcs = {"row_number"};
    return window_funcs.find(funcname) != window_funcs.end();
}

static auto IsValidParamType(const std::string &funcname, const AggFuncInfo &func_info,
                             const std::vector<std::unique_ptr<BoundExpression>> &children) -> bool {
    // FIXME: 特殊处理，后续优化
    if (funcname == "top" || funcname == "bottom") {
        if (children.size() != 2) {
            return false;
        }
        const auto &param = children[0];
        auto type = param->ReturnType().TypeId();
        if (intarkdb::IsNumeric(type)) {
            return true;
        }
        return false;
    }
    if(funcname == "mode" || funcname == "max" || funcname == "min") {
        if (children.size() != 1) {
            return false;
        }
        return true;
    }
    if (func_info.NeedNumericInput()) {
        if (children.size() != 1) {
            return false;
        }
        const auto &param = children[0];
        auto type = param->ReturnType().TypeId();
        if (intarkdb::IsNumeric(type)) {
            return true;
        }
        return false;
    }
    return true;
}

auto Binder::BindOverClause(const struct PGWindowDef &over) -> std::unique_ptr<intarkdb::OverClause> {
    std::unique_ptr<intarkdb::OverClause> over_clause = std::make_unique<intarkdb::OverClause>();
    if (over.partitionClause) {
        over_clause->partition_by = BindExpressionList(over.partitionClause,1);
        if(over_clause->partition_by.size() > 0) {
            // TODO: support other expr type later
            for(auto &expr : over_clause->partition_by) {
                if(expr->Type() != ExpressionType::COLUMN_REF) {
                    throw intarkdb::Exception(ExceptionType::BINDER, "partition by clause must be column reference", bind_location);
                }
            }
        }
    }
    if(over.orderClause) {
        // TODO: support constant expr later
        over_clause->order_by = BindSortItems(over.orderClause,{});
        for(auto &sort_item : over_clause->order_by) {
            if(sort_item->sort_expr->Type() != ExpressionType::COLUMN_REF) {
                throw intarkdb::Exception(ExceptionType::BINDER, "order by clause must be column reference", bind_location);
            }
        }
    }
    return over_clause;
}

auto Binder::BindFuncExpression(duckdb_libpgquery::PGFuncCall *root, int depth) -> std::unique_ptr<BoundExpression>
{
    CHECK_EXPRESSION_DEPTH(depth);
    bind_location = root->location;
    auto name = root->funcname;
    std::string catalog;
    std::string schema;
    std::string function_name;
    if (name->length == CATALOG_SCHEMA_NAME) {
        // catalog + schema + name
        catalog = reinterpret_cast<duckdb_libpgquery::PGValue *>(name->head->data.ptr_value)->val.str;
        schema = reinterpret_cast<duckdb_libpgquery::PGValue *>(name->head->next->data.ptr_value)->val.str;
        function_name = reinterpret_cast<duckdb_libpgquery::PGValue *>(name->head->next->next->data.ptr_value)->val.str;
    } else if (name->length == SCHEMA_NAME) {
        // schema + name
        schema = reinterpret_cast<duckdb_libpgquery::PGValue *>(name->head->data.ptr_value)->val.str;
        function_name = reinterpret_cast<duckdb_libpgquery::PGValue *>(name->head->next->data.ptr_value)->val.str;
    } else if (name->length == UNQUALIFIED_NAME) {
        // unqualified name
        function_name = reinterpret_cast<duckdb_libpgquery::PGValue *>(name->head->data.ptr_value)->val.str;
    } else {
        throw intarkdb::Exception(ExceptionType::BINDER, "BindFuncCall - Expected 1, 2 or 3 qualifications",
                                  bind_location);
    }
    // transfor function_name into lower
    function_name = intarkdb::StringUtil::Lower(function_name);

    std::vector<std::unique_ptr<BoundExpression>> children;
    if (root->args != nullptr) {
        for (auto node = root->args->head; node != nullptr; node = node->next) {
            auto child_expr = BindExpression(static_cast<duckdb_libpgquery::PGNode *>(node->data.ptr_value), depth + 1);
            if (child_expr->Type() == ExpressionType::INTERVAL) {
                auto interval = static_cast<BoundInterval *>(child_expr.get());
                for (auto &val : interval->values) {
                    children.push_back(std::make_unique<BoundConstant>(val));
                }
            } else {
                children.push_back(std::move(child_expr));
            }
        }
    }

    const auto &agg_info = FindAggInfo(function_name);
    if (agg_info.IsValid()) {
        // Rewrite count(*) , count(constant) to count_star()
        if (function_name == "count") {
            if (children.size() == 0 || (children.size() > 0 && children[0]->Type() == ExpressionType::STAR) ||
                (children.size() > 0 && children[0]->Type() == ExpressionType::LITERAL)) {
                function_name = "count_star";
                children.clear();
            }
        }
        // Bind function as agg call.
        if (IsValidParamType(function_name, agg_info, children)) {
            return std::make_unique<BoundAggCall>(function_name, root->agg_distinct, std::move(children));
        } else {
            throw intarkdb::Exception(ExceptionType::BINDER,
                                      fmt::format("Invalid param type for function {}", function_name), bind_location);
        }
    }

    if (IsLikeFunction(function_name)) {
        return std::make_unique<BoundLikeOp>(std::move(function_name), std::move(children));
    }

    if (function_name == "nextval" || function_name == "currval") {
        if (children.size() != 1) {
            throw intarkdb::Exception(ExceptionType::BINDER, fmt::format("{} should have one argument", function_name),
                                      bind_location);
        }
        return std::make_unique<BoundSequenceFunction>(function_name, std::move(children[0]), catalog_);
    }

    if (IsValidFunction(function_name)) {
        return std::make_unique<BoundFuncExpr>(function_name.c_str(), std::move(children));
    }
    if (IsValidWindowFunction(function_name)) {
        if (root->over == nullptr) {
            throw intarkdb::Exception(ExceptionType::BINDER,
                                      fmt::format("window function {} must have over clause", function_name),
                                      bind_location);
        }
        auto over_clause = BindOverClause(*(root->over));
        if (over_clause == nullptr) {
            throw intarkdb::Exception(ExceptionType::BINDER, "the over clause is invalid", bind_location);
        }
        return std::make_unique<intarkdb::WindowFunction>(function_name, std::move(over_clause));
    }
    throw intarkdb::Exception(ExceptionType::BINDER, fmt::format("unspport func name {}", function_name),
                              bind_location);
}

constexpr size_t CONJUNCTIVE_MAX_ITEM = 10;
constexpr size_t BOOL_EXPR_ARG_NUM = 2;

static auto MakeORExpr(BoundConjunctive &conjunctive_left, BoundConjunctive &conjunctive_right)
    -> std::unique_ptr<BoundConjunctive> {
    std::vector<std::unique_ptr<BoundExpression>> items;
    items.reserve(conjunctive_left.items.size() * conjunctive_right.items.size());
    for (size_t i = 0; i < conjunctive_left.items.size(); ++i) {
        for (size_t j = 0; j < conjunctive_right.items.size(); ++j) {
            items.push_back(std::make_unique<BoundBinaryOp>("or", conjunctive_left.items[i]->Copy(),
                                                            conjunctive_right.items[j]->Copy()));
        }
    }
    return std::make_unique<BoundConjunctive>(std::move(items));
}

static auto MakeORExpr(std::unique_ptr<BoundExpression> &&left, std::unique_ptr<BoundExpression> &&right)
    -> std::unique_ptr<BoundExpression> {
    if (left->Type() == ExpressionType::CONJUNCTIVE && right->Type() == ExpressionType::CONJUNCTIVE) {
        // 避免两个conunctive 太大，导致组合爆炸
        auto &conjunctive_left = static_cast<BoundConjunctive &>(*left);
        auto &conjunctive_right = static_cast<BoundConjunctive &>(*right);
        if (conjunctive_left.ItemCount() * conjunctive_right.ItemCount() >= CONJUNCTIVE_MAX_ITEM) {
            return std::make_unique<BoundBinaryOp>("or", std::move(left), std::move(right));
        }
        return MakeORExpr(conjunctive_left, conjunctive_right);
    } else {
        // note: (ConJUNCTIVE OR EXPR) 继续优化的耗时可能不值得
        return std::make_unique<BoundBinaryOp>("or", std::move(left), std::move(right));
    }
}

static auto MakeORExpr(std::vector<std::unique_ptr<BoundExpression>> &&exprs) -> std::unique_ptr<BoundExpression> {
    auto expr = MakeORExpr(std::move(exprs[0]), std::move(exprs[1]));
    for (size_t i = 2; i < exprs.size(); ++i) {
        expr = MakeORExpr(std::move(expr), std::move(exprs[i]));
    }
    return expr;
}

static auto MakeANDExpr(std::unique_ptr<BoundExpression> &&left, std::unique_ptr<BoundExpression> &&right)
    -> std::unique_ptr<BoundConjunctive> {
    std::vector<std::unique_ptr<BoundExpression>> items;
    if (left->Type() == ExpressionType::CONJUNCTIVE) {
        auto &conjunctive = static_cast<BoundConjunctive &>(*left);
        items = std::move(conjunctive.items);
    } else {
        items.push_back(std::move(left));
    }
    if (right->Type() == ExpressionType::CONJUNCTIVE) {
        auto &conjunctive = static_cast<BoundConjunctive &>(*right);
        std::copy(std::make_move_iterator(conjunctive.items.begin()), std::make_move_iterator(conjunctive.items.end()),
                  std::back_inserter(items));
    } else {
        items.push_back(std::move(right));
    }
    return std::make_unique<BoundConjunctive>(std::move(items));
}

static auto MakeANDExpr(std::vector<std::unique_ptr<BoundExpression>> &&exprs) -> std::unique_ptr<BoundConjunctive> {
    std::vector<std::unique_ptr<BoundExpression>> items;
    for (auto &expr : exprs) {
        if (expr->Type() == ExpressionType::CONJUNCTIVE) {
            auto &conjunctive = static_cast<BoundConjunctive &>(*expr);
            std::copy(std::make_move_iterator(conjunctive.items.begin()),
                      std::make_move_iterator(conjunctive.items.end()), std::back_inserter(items));
        } else {
            items.push_back(std::move(expr));
        }
    }
    return std::make_unique<BoundConjunctive>(std::move(items));
}

constexpr int BETWEEN_ARG_NUM = 2;

auto Binder::BindAExpr(duckdb_libpgquery::PGAExpr *root, int depth) -> std::unique_ptr<BoundExpression> {
    CHECK_EXPRESSION_DEPTH(depth);
    bind_location = root->location;
    if (root->kind == duckdb_libpgquery::PG_AEXPR_BETWEEN || root->kind == duckdb_libpgquery::PG_AEXPR_NOT_BETWEEN) {
        // 处理 between 表达式
        auto between_exprs = reinterpret_cast<duckdb_libpgquery::PGList *>(root->rexpr);
        if (between_exprs->length != BETWEEN_ARG_NUM) {
            throw intarkdb::Exception(ExceptionType::BINDER, "between expression need two args", bind_location);
        }

        auto between_left = BindExpression(
            reinterpret_cast<duckdb_libpgquery::PGNode *>(between_exprs->head->data.ptr_value), depth + 1);
        auto between_right = BindExpression(
            reinterpret_cast<duckdb_libpgquery::PGNode *>(between_exprs->tail->data.ptr_value), depth + 1);

        auto col_ref1 = BindExpression(root->lexpr, depth + 1);
        auto col_ref2 = col_ref1->Copy();

        auto left_half = std::make_unique<BoundBinaryOp>(">=", std::move(col_ref1), std::move(between_left));
        auto right_half = std::make_unique<BoundBinaryOp>("<=", std::move(col_ref2), std::move(between_right));

        auto result = MakeANDExpr(std::move(left_half), std::move(right_half));
        if (root->kind == duckdb_libpgquery::PG_AEXPR_NOT_BETWEEN) {
            return std::make_unique<BoundUnaryOp>("not", std::move(result));
        }
        return result;
    }

    if (root->kind == duckdb_libpgquery::PG_AEXPR_IN) {
        return BindInExpr(root, depth + 1);
    }

    // op name
    std::string_view name = NullCheckPtrCast<duckdb_libpgquery::PGValue>(root->name->head->data.ptr_value)->val.str;
    if (IsLikeExpression(root->kind)) {
        auto left_expr = BindExpression(root->lexpr, depth + 1);
        auto right_expr = BindExpression(root->rexpr, depth + 1);
        if (!left_expr || !right_expr) {
            throw intarkdb::Exception(ExceptionType::BINDER, "argument with LIKE expresion can't be empty",
                                      bind_location);
        }
        std::vector<std::unique_ptr<BoundExpression>> arguments;
        arguments.push_back(std::move(left_expr));
        arguments.push_back(std::move(right_expr));
        return std::make_unique<BoundLikeOp>(std::string(name), std::move(arguments));
    }

    if (root->kind != duckdb_libpgquery::PG_AEXPR_OP) {
        throw intarkdb::Exception(ExceptionType::BINDER,
                                  fmt::format("not supported this kind of expression {}", root->kind));
    }

    std::unique_ptr<BoundExpression> left_expr = nullptr;
    std::unique_ptr<BoundExpression> right_expr = nullptr;

    if (root->lexpr != nullptr) {
        left_expr = BindExpression(root->lexpr, depth + 1);
    }
    if (root->rexpr != nullptr) {
        right_expr = BindExpression(root->rexpr, depth + 1);
    }

    if (left_expr && right_expr) {
        // 双操作符
        if (name == "and") {
            return MakeANDExpr(std::move(left_expr), std::move(right_expr));
        } else if (name == "or") {
            return MakeORExpr(std::move(left_expr), std::move(right_expr));
        } else {
            return std::make_unique<BoundBinaryOp>(std::string(name), std::move(left_expr), std::move(right_expr));
        }
    }
    if (!left_expr && right_expr) {
        if (name == "+") {  // + 单目操作符，忽略
            return right_expr;
        }
        return std::make_unique<BoundUnaryOp>(std::string(name), std::move(right_expr));
    }
    throw intarkdb::Exception(ExceptionType::BINDER, "unsupported AExpr");
}

auto Binder::BindInExpr(duckdb_libpgquery::PGAExpr *root, int depth) -> std::unique_ptr<BoundExpression> {
    CHECK_EXPRESSION_DEPTH(depth);
    std::string_view name = NullCheckPtrCast<duckdb_libpgquery::PGValue>(root->name->head->data.ptr_value)->val.str;
    auto exprs = BindExpressionList(reinterpret_cast<duckdb_libpgquery::PGList *>(root->rexpr), depth + 1);
    if (exprs.size() < 0) {
        throw intarkdb::Exception(ExceptionType::BINDER, "argument list after IN expresion can't be empty");
    }
    auto col_ref = BindExpression(root->lexpr, depth + 1);
    auto in_expr = std::make_unique<BoundInExpr>(std::move(col_ref), std::move(exprs), name == "<>");
    return in_expr;
}

auto Binder::BindBoolExpr(duckdb_libpgquery::PGBoolExpr *expr_root, int depth) -> std::unique_ptr<BoundExpression> {
    CHECK_EXPRESSION_DEPTH(depth);
    bind_location = expr_root->location;
    if (expr_root->boolop == duckdb_libpgquery::PG_NOT_EXPR) {
        auto exprs = BindExpressionList(expr_root->args, depth + 1);
        if (exprs.size() != 1) {
            throw std::invalid_argument("NOT need only one arg");
        }
        // NOTE: expr为合取式时，是否需要改写表达式
        return std::make_unique<BoundUnaryOp>("not", std::move(exprs[0]));
    }

    std::string op_name;
    switch (expr_root->boolop) {
        case duckdb_libpgquery::PG_AND_EXPR: {
            op_name = "and";
            break;
        }
        case duckdb_libpgquery::PG_OR_EXPR: {
            op_name = "or";
            break;
        }
        default:
            throw intarkdb::Exception(ExceptionType::BINDER, "invalid bool expr");
    }

    auto exprs = BindExpressionList(expr_root->args, depth + 1);
    if (exprs.size() < BOOL_EXPR_ARG_NUM) {  // NOTE: 是否有三值逻辑
        throw intarkdb::Exception(ExceptionType::BINDER,
                                  fmt::format("{} need two arguments size={}", op_name, exprs.size()));
    }

    std::unique_ptr<BoundExpression> expr;
    if (op_name == "and") {
        expr = MakeANDExpr(std::move(exprs));
    } else {
        expr = MakeORExpr(std::move(exprs));
    }
    return expr;
}

auto Binder::BindExpressionList(duckdb_libpgquery::PGList *list, int depth)
    -> std::vector<std::unique_ptr<BoundExpression>> {
    CHECK_EXPRESSION_DEPTH(depth);
    auto select_list = std::vector<std::unique_ptr<BoundExpression>>{};
    for (auto node = list->head; node != nullptr; node = lnext(node)) {
        auto target = reinterpret_cast<duckdb_libpgquery::PGNode *>(node->data.ptr_value);
        auto expr = BindExpression(target, depth);  // no need to increase depth
        select_list.emplace_back(std::move(expr));
    }
    return select_list;
}

auto Binder::BindNullTest(duckdb_libpgquery::PGNullTest *root, int depth) -> std::unique_ptr<BoundExpression> {
    CHECK_EXPRESSION_DEPTH(depth);
    bind_location = root->location;
    auto target = reinterpret_cast<duckdb_libpgquery::PGNode *>(root->arg);
    if (target == nullptr) {
        throw intarkdb::Exception(ExceptionType::BINDER, "NullTest Expression should not be null argument");
    }
    auto expr = BindExpression(target, depth + 1);
    auto null_test_type =
        root->nulltesttype == duckdb_libpgquery::IS_NOT_NULL ? NullTest::IS_NOT_NULL : NullTest::IS_NULL;
    return std::make_unique<BoundNullTest>(std::move(expr), null_test_type);
}

using SubLinkType = duckdb_libpgquery::PGSubLinkType;
static auto CheckSubqueryColumns(size_t size, SubLinkType sublink_type) -> void {
    if (size == 0) {
        throw intarkdb::Exception(ExceptionType::BINDER, "subquery should have select list");
    }
    if ((sublink_type == duckdb_libpgquery::PG_ANY_SUBLINK || sublink_type == duckdb_libpgquery::PG_ALL_SUBLINK) &&
        size > 1) {
        throw intarkdb::Exception(ExceptionType::BINDER, fmt::format("subquery returns {} columns - expected 1", size));
    }
}

auto Binder::BindSubQueryExpr(duckdb_libpgquery::PGSubLink *root) -> std::unique_ptr<BoundExpression> {
    bind_location = root->location;
    // testexpr是外部查询的表达式，先绑定
    std::unique_ptr<BoundExpression> child = nullptr;
    if (root->subLinkType == duckdb_libpgquery::PG_ALL_SUBLINK ||
        root->subLinkType == duckdb_libpgquery::PG_ANY_SUBLINK) {
        child = BindExpression(root->testexpr, 0);
    }

    Binder subquery_binder(this);
    // 特殊需求，这里使用shated_ptr
    auto select_statment = std::shared_ptr<SelectStatement>(
        subquery_binder.BindSelectStmt(reinterpret_cast<duckdb_libpgquery::PGSelectStmt *>(root->subselect)));
    if (select_statment->return_list.size() == 0) {
        throw intarkdb::Exception(ExceptionType::BINDER, "subquery should have select list");
    }

    // ALL or ANY 子查询只能返回一个列
    CheckSubqueryColumns(select_statment->select_expr_list.size(), root->subLinkType);

    std::unique_ptr<BoundExpression> subquery;
    // note: 外部可能不存在其他 table , 比如 : insert into t3 values ((select b from t3));
    std::vector<std::unique_ptr<BoundExpression>> correlated_columns =
        std::move(subquery_binder.ctx.correlated_columns);

    switch (root->subLinkType) {
        case duckdb_libpgquery::PG_EXISTS_SUBLINK: {
            // eg select * from table1 where exists (select * from table2 where table1.id = table2.id);
            subquery =
                std::make_unique<BoundSubqueryExpr>(SubqueryType::EXISTS, std::move(select_statment),
                                                    std::move(correlated_columns), nullptr, "", GetNextUniversalID());
            break;
        }
        case duckdb_libpgquery::PG_EXPR_SUBLINK: {
            // e.g: select * from table1 where id = (select id from table2 where id is not null);
            // 子查询必只返回一个列, 但是不能确定是否一定是标量子查询
            // 显然的标量子查询：1.单列，没有from clause
            // 2. 单列，没有 group by,有聚合函数
            subquery =
                std::make_unique<BoundSubqueryExpr>(SubqueryType::SCALAR, std::move(select_statment),
                                                    std::move(correlated_columns), nullptr, "", GetNextUniversalID());
            break;
        }
        case duckdb_libpgquery::PG_ANY_SUBLINK:
        case duckdb_libpgquery::PG_ALL_SUBLINK: {
            // ANY e.g select * from table1 where id = ANY (select id from table2 where id is not null);
            // ALL e.g select * from table1 where id > ALL (select id from table2)
            // IN  e.g select * from table1 where id in (select id from table2)  == ANY
            std::string op_name = "";
            if (!root->operName) {
                // default is equal
                op_name = "=";
            } else {
                op_name = std::string(
                    (reinterpret_cast<duckdb_libpgquery::PGValue *>(root->operName->head->data.ptr_value))->val.str);
            }
            // check op_name is valid
            if (!intarkdb::IsCompareOp(op_name)) {
                throw intarkdb::Exception(ExceptionType::BINDER, fmt::format("op_name {} not supported", op_name));
            }

            subquery = std::make_unique<BoundSubqueryExpr>(
                root->subLinkType == duckdb_libpgquery::PG_ANY_SUBLINK ? SubqueryType::ANY : SubqueryType::ALL,
                std::move(select_statment), std::move(correlated_columns), std::move(child), op_name,
                GetNextUniversalID());
            break;
        }
        default:
            throw intarkdb::Exception(ExceptionType::BINDER,
                                      fmt::format("subquery type {} not supported", (int)root->subLinkType));
    }
    return subquery;
}

static auto GetParamIdentifier(duckdb_libpgquery::PGParamRef &node, int curr_param_num) -> int {
    if (node.name) {
        throw intarkdb::Exception(ExceptionType::BINDER, "Named parameter not support");
    }
    if (node.number < 0) {
        throw intarkdb::Exception(ExceptionType::BINDER, "Parameter numbers cannot be negative");
    }
    if (node.number > 0) {
        throw intarkdb::Exception(ExceptionType::BINDER, "Positional parameter not support");
    }
    return curr_param_num + 1;
}

auto Binder::BindParamRef(duckdb_libpgquery::PGParamRef *node) -> std::unique_ptr<BoundExpression> {
    bind_location = node->location;
    auto expr = std::make_unique<BoundParameter>();
    auto curr_param_num = ParamCount();
    expr->parameter_nr = GetParamIdentifier(*node, curr_param_num) - 1;  // start from 0
    SetParamCount(curr_param_num + 1);
    return expr;
}

auto Binder::BindTypeCast(duckdb_libpgquery::PGTypeCast *root, int depth) -> std::unique_ptr<BoundExpression> {
    bind_location = root->location;
    auto type = TransformTypeName(*root->typeName);
    return std::make_unique<BoundCast>(type, BindExpression(root->arg, depth + 1), root->tryCast);
}

auto Binder::BindCase(duckdb_libpgquery::PGCaseExpr *root, int depth) -> std::unique_ptr<BoundExpression> {
    CHECK_EXPRESSION_DEPTH(depth);
    bind_location = root->location;
    auto case_when_expr = std::make_unique<BoundCase>();
    auto root_arg = BindExpression(reinterpret_cast<duckdb_libpgquery::PGNode *>(root->arg), depth + 1);
    for (auto cell = root->args->head; cell != nullptr; cell = cell->next) {
        BoundCaseCheck bound_case_check;

        auto w = reinterpret_cast<duckdb_libpgquery::PGCaseWhen *>(cell->data.ptr_value);
        auto when_expr = BindExpression(reinterpret_cast<duckdb_libpgquery::PGNode *>(w->expr), depth + 1);
        if (root_arg) {
            bound_case_check.when_expr = std::make_unique<BoundBinaryOp>("=", root_arg->Copy(), std::move(when_expr));
        } else {
            bound_case_check.when_expr = std::move(when_expr);
        }
        bound_case_check.then_expr =
            BindExpression(reinterpret_cast<duckdb_libpgquery::PGNode *>(w->result), depth + 1);
        case_when_expr->bound_case_checks.push_back(std::move(bound_case_check));
    }

    if (root->defresult) {
        case_when_expr->else_expr =
            BindExpression(reinterpret_cast<duckdb_libpgquery::PGNode *>(root->defresult), depth + 1);
    } else {
        case_when_expr->else_expr = nullptr;
    }
    return std::move(case_when_expr);
}

auto Binder::BindSortExpression(duckdb_libpgquery::PGNode *node,
                                const std::vector<std::unique_ptr<BoundExpression>> &select_list)
    -> std::unique_ptr<BoundExpression> {
    auto expr = BindExpression(node, 1);
    if (expr) {
        if (expr->Type() == ExpressionType::COLUMN_REF) {
            // 再从alias map中查找，如果有别名命中，会优先使用别名[区别于where，orderby 会优先使用别名]
            // 如果已经是别名，不会走到这里
            const auto &column_ref_expr = static_cast<const BoundColumnRef &>(*expr);
            // 从OrderByClause获取的列名都是带有表名的，解析器会自动加上表名
            const auto &expr_name = column_ref_expr.GetName();
            const auto &alias = expr_name.back();  // 去掉表名 ， 只保留列名
            auto alias_expr = ctx.alias_binding.find(alias);
            if (alias_expr != ctx.alias_binding.end()) {
                expr = std::make_unique<BoundPositionRef>(alias_expr->second.idx, alias_expr->second.type);
            }
        } else if (expr->Type() == ExpressionType::LITERAL) {
            const auto &constant_expr = static_cast<const BoundConstant &>(*expr);
            const auto &val = constant_expr.ValRef();
            if (val.IsInteger()) {
                auto idx = val.GetCastAs<int32_t>();
                if (idx <= 0 || idx > (int32_t)select_list.size()) {
                    throw intarkdb::Exception(
                        ExceptionType::BINDER,
                        fmt::format("ORDER term out of range - should be between 1 and {}", select_list.size()));
                }
                expr = select_list[idx - 1]->Copy();
            } else {
                expr = nullptr;  // 常量非整数
            }
        } else if (expr->Type() == ExpressionType::BOUND_PARAM) {
            // 暂不支持 param表达式
            throw intarkdb::Exception(ExceptionType::BINDER, "not supported param expression in order by");
        } else if (expr->Type() == ExpressionType::STAR) {
            throw intarkdb::Exception(ExceptionType::SYNTAX, "not supported * expression in order by");
        }
    }
    return expr;
}
