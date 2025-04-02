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
 * binder.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/binder/binder.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "binder/binder.h"

#include <fmt/core.h>
#include <fmt/format.h>

#include <stdexcept>
#include <unordered_set>
#include <vector>

#include "binder/bound_expression.h"
#include "binder/expressions/bound_agg_call.h"
#include "binder/expressions/bound_alias.h"
#include "binder/expressions/bound_binary_op.h"
#include "binder/expressions/bound_cast.h"
#include "binder/expressions/bound_column_def.h"
#include "binder/expressions/bound_constant.h"
#include "binder/expressions/bound_func_expr.h"
#include "binder/expressions/bound_like_op.h"
#include "binder/expressions/bound_null_test.h"
#include "binder/expressions/bound_parameter.h"
#include "binder/expressions/bound_position_ref_expr.h"
#include "binder/expressions/bound_seq_func.h"
#include "binder/expressions/bound_star.h"
#include "binder/expressions/bound_sub_query.h"
#include "binder/expressions/bound_unary_op.h"
#include "binder/table_ref/bound_join.h"
#include "binder/transform_typename.h"
#include "common/compare_type.h"
#include "common/constrain_type.h"
#include "common/default_value.h"
#include "common/exception.h"
#include "common/gstor_exception.h"
#include "common/null_check_ptr.h"
#include "common/string_util.h"
#include "function/sql_function.h"
#include "nodes/nodes.hpp"
#include "nodes/pg_list.hpp"
#include "pg_functions.hpp"
#include "planner/expression_iterator.h"
#include "type/operator/cast_operators.h"
#include "type/type_system.h"

void Binder::ParseSQL(const std::string &query) 
{
    bind_location = -1;
    parser_.Parse(query);
    if (!parser_.success) {
        bind_location = parser_.error_location - 1;
        throw intarkdb::Exception(ExceptionType::SYNTAX, "[query fail to parse]" + parser_.error_message,
                                  bind_location);
    }
    SaveParseResult(parser_.parse_tree);
}

auto Binder::SaveParseResult(duckdb_libpgquery::PGList *tree) -> void {
    if (tree != nullptr) {
        for (auto entry = tree->head; entry != nullptr; entry = entry->next) {
            pg_statements_.push_back(reinterpret_cast<duckdb_libpgquery::PGNode *>(entry->data.ptr_value));
        }
    }
}

auto Binder::BindSQLStmt(duckdb_libpgquery::PGNode *stmt) -> std::unique_ptr<BoundStatement> 
{
    std::unique_ptr<BoundStatement> result;
    if (stmt == nullptr) {
        throw intarkdb::Exception(ExceptionType::BINDER, "statement is nullptr");
    }
    switch (stmt->type) {
        case duckdb_libpgquery::T_PGRawStmt:
            result = BindSQLStmt(NullCheckPtrCast<duckdb_libpgquery::PGRawStmt>(stmt)->stmt);
            break;
        case duckdb_libpgquery::T_PGCreateStmt:
            result = BindCreateStmt(reinterpret_cast<duckdb_libpgquery::PGCreateStmt *>(stmt));
            break;
        case duckdb_libpgquery::T_PGInsertStmt:
            result = BindInsertStmt(reinterpret_cast<duckdb_libpgquery::PGInsertStmt *>(stmt));
            break;
        case duckdb_libpgquery::T_PGSelectStmt:
            result = BindSelectStmt(reinterpret_cast<duckdb_libpgquery::PGSelectStmt *>(stmt));
            break;
        case duckdb_libpgquery::T_PGExplainStmt:
            result = BindExplainStmt(reinterpret_cast<duckdb_libpgquery::PGExplainStmt *>(stmt));
            break;
        case duckdb_libpgquery::T_PGCheckPointStmt:
            result = BindCheckPoint(reinterpret_cast<duckdb_libpgquery::PGCheckPointStmt *>(stmt));
            break;
        case duckdb_libpgquery::T_PGDeleteStmt:
            result = BindDeleteStmt(reinterpret_cast<duckdb_libpgquery::PGDeleteStmt *>(stmt));
            break;
        case duckdb_libpgquery::T_PGUpdateStmt:
            result = BindUpdateStmt(reinterpret_cast<duckdb_libpgquery::PGUpdateStmt *>(stmt));
            break;
        case duckdb_libpgquery::T_PGIndexStmt:
            result = BindCreateIndex(reinterpret_cast<duckdb_libpgquery::PGIndexStmt *>(stmt));
            break;
        case duckdb_libpgquery::T_PGVariableSetStmt:
            result = BindVariableSet(reinterpret_cast<duckdb_libpgquery::PGVariableSetStmt *>(stmt));
            break;
        case duckdb_libpgquery::T_PGVariableShowStmt:
            result = BindVariableShow(reinterpret_cast<duckdb_libpgquery::PGVariableShowStmt *>(stmt));
            break;
        case duckdb_libpgquery::T_PGTransactionStmt:
            result = BindTransaction(reinterpret_cast<duckdb_libpgquery::PGTransactionStmt *>(stmt));
            break;
        case duckdb_libpgquery::T_PGAlterTableStmt:
            result = BindAlter(reinterpret_cast<duckdb_libpgquery::PGAlterTableStmt *>(stmt));
            break;
        case duckdb_libpgquery::T_PGRenameStmt:
            result = BindRename(reinterpret_cast<duckdb_libpgquery::PGRenameStmt *>(stmt));
            break;
        case duckdb_libpgquery::T_PGDropStmt:
            result = BindDropStmt(reinterpret_cast<duckdb_libpgquery::PGDropStmt *>(stmt));
            break;
        case duckdb_libpgquery::T_PGCreateTableAsStmt:
            result = BindCtas(reinterpret_cast<duckdb_libpgquery::PGCreateTableAsStmt *>(stmt));
            break;
        case duckdb_libpgquery::T_PGViewStmt:
            result = BindCreateViewStmt(reinterpret_cast<duckdb_libpgquery::PGViewStmt *>(stmt));
            break;
        case duckdb_libpgquery::T_PGCreateSeqStmt:
            result = BindSequence(reinterpret_cast<duckdb_libpgquery::PGCreateSeqStmt *>(stmt));
            break;
        case duckdb_libpgquery::T_PGCopyStmt:
            result = BindCopy(reinterpret_cast<duckdb_libpgquery::PGCopyStmt *>(stmt));
            break;
        case duckdb_libpgquery::T_PGCommentStmt:
            result = BindCommentOn(reinterpret_cast<duckdb_libpgquery::PGCommentStmt *>(stmt));
            break;
        case duckdb_libpgquery::T_PGSynonymStmt:
            result = BindCreateSynonym(reinterpret_cast<duckdb_libpgquery::PGSynonymStmt *>(stmt));
            break;
        default:
            throw intarkdb::Exception(ExceptionType::NOT_IMPLEMENTED,
                                      "[not support statement]" + ConvertNodeTagToString(stmt->type));
    }

    result->props = stmt_props;

    return result;
}

auto Binder::BoundExpressionToDefaultValue(BoundExpression &expr, Column &column) -> std::vector<uint8_t> {
    std::vector<uint8_t> default_value;
    switch (expr.Type()) {
        case ExpressionType::LITERAL: {
            auto const_expr = static_cast<BoundConstant &>(expr);
            if (const_expr.ValRef().IsNull()) {  // NULL
                column.SetNullable(true);
            } else {
                auto &d_val = const_cast<Value &>(const_expr.ValRef());
                auto col_type = column.GetLogicalType();
                if (col_type.TypeId() != d_val.GetLogicalType().TypeId()) {
                    d_val = DataType::GetTypeInstance(col_type.TypeId())->CastValue(d_val);
                }
                auto default_value_ptr =
                    CreateDefaultValue(DefaultValueType::DEFAULT_VALUE_TYPE_LITERAL, d_val.Size(), d_val.GetRawBuff());
                default_value = std::vector<uint8_t>(
                    (uint8_t *)default_value_ptr.get(),
                    (uint8_t *)default_value_ptr.get() + DefaultValueSize(default_value_ptr.get()));
            }
            break;
        }
        case ExpressionType::UNARY_OP: {  // NOT NULL
            column.SetNullable(false);
            break;
        }
        case ExpressionType::SEQ_FUNC: {
            auto &seq_func_expr = static_cast<BoundSequenceFunction &>(expr);
            const std::string &func_name = seq_func_expr.ToString();
            const std::string &seq_name = seq_func_expr.arg->ToString();
            if (!catalog_.IsSequenceExists(seq_name)) {
                throw intarkdb::Exception(ExceptionType::CATALOG,
                                          fmt::format("Sequence with name {} does not exist!", seq_name));
            }
            auto default_value_ptr =
                CreateDefaultValue(DefaultValueType::DEFAULT_VALUE_TYPE_FUNC, func_name.length(), func_name.c_str());
            default_value =
                std::vector<uint8_t>((uint8_t *)default_value_ptr.get(),
                                     (uint8_t *)default_value_ptr.get() + DefaultValueSize(default_value_ptr.get()));
            break;
        }
        case ExpressionType::FUNC_CALL: {
            auto &func_call_expr = static_cast<BoundFuncExpr &>(expr);
            const std::string &func_name = func_call_expr.ToString();
            if (func_call_expr.funcname == "now" || func_call_expr.funcname == "current_date" ||
                func_call_expr.funcname == "random") {
                // 当前只支持上述可变值的函数
                auto default_value_ptr = CreateDefaultValue(DefaultValueType::DEFAULT_VALUE_TYPE_FUNC,
                                                            func_name.length(), func_name.c_str());
                default_value = std::vector<uint8_t>(
                    (uint8_t *)default_value_ptr.get(),
                    (uint8_t *)default_value_ptr.get() + DefaultValueSize(default_value_ptr.get()));
            } else {
                throw intarkdb::Exception(
                    ExceptionType::CATALOG,
                    fmt::format("Column's({}) default expression type {} func_name {} is not supported ", column.Name(),
                                expr.Type(), func_call_expr.funcname));
            }
            break;
        }
        default:
            throw intarkdb::Exception(
                ExceptionType::CATALOG,
                fmt::format("Column's({}) default expression type {} is not supported", column.Name(), expr.Type()));
    }
    return default_value;
}

static auto CreateCrossJoin(std::unique_ptr<BoundQuerySource> left, std::unique_ptr<BoundQuerySource> right)
    -> std::unique_ptr<BoundJoin> {
    return std::make_unique<BoundJoin>(JoinType::CrossJoin, std::move(left), std::move(right), nullptr);
}

auto Binder::BindFromClause(duckdb_libpgquery::PGList *list) -> std::unique_ptr<BoundQuerySource> {
    if (list == nullptr) {
        // not table
        return std::make_unique<BoundQuerySource>(DataSourceType::DUAL);
    }

    if (list->length == 1) {
        return BindTableRef(*NullCheckPtrCast<duckdb_libpgquery::PGNode>(list->head->data.ptr_value));
    }

    auto curr = list->head;
    auto left_table = BindTableRef(*NullCheckPtrCast<duckdb_libpgquery::PGNode>(curr->data.ptr_value));
    curr = lnext(curr);

    auto right_table = BindTableRef(*NullCheckPtrCast<duckdb_libpgquery::PGNode>(curr->data.ptr_value));
    curr = lnext(curr);

    auto result = CreateCrossJoin(std::move(left_table), std::move(right_table));

    for (; curr != nullptr; curr = lnext(curr)) {
        result = CreateCrossJoin(std::move(result),
                                 BindTableRef(*NullCheckPtrCast<duckdb_libpgquery::PGNode>(curr->data.ptr_value)));
    }
    return result;
}

auto Binder::BindTableAllColumns(const std::string &table_name) -> std::vector<std::unique_ptr<BoundExpression>> {
    auto columns = std::vector<std::unique_ptr<BoundExpression>>{};
    auto binding = ctx.GetTableBinding(table_name);
    if (!binding) {
        throw intarkdb::Exception(ExceptionType::BINDER, fmt::format("reference table {} not found", table_name));
    }
    const auto &col_names = binding->GetColNames();
    const auto &col_types = binding->GetColTypes();

    columns.reserve(col_names.size());
    for (size_t i = 0; i < col_names.size(); ++i) {
        columns.emplace_back(std::make_unique<BoundColumnRef>(std::vector{table_name, col_names[i]}, col_types[i]));
    }
    return columns;
}

auto Binder::BindAllColumnRefs(const char *expect_relation_name) -> std::vector<std::unique_ptr<BoundExpression>> {
    if (expect_relation_name) {
        auto columns = BindTableAllColumns(expect_relation_name);
        if (columns.empty()) {
            throw intarkdb::Exception(ExceptionType::BINDER,
                                      fmt::format("reference table {} not found", expect_relation_name));
        }
        return columns;
    } else {
        const auto &table_bindings_vec = ctx.GetAllTableBindings();
        std::vector<std::unique_ptr<BoundExpression>> columns;
        for (const auto &table_bindings : table_bindings_vec) {
            const auto &table_name = table_bindings.GetTableName();
            auto table_columns = BindTableAllColumns(table_name);
            std::copy(std::make_move_iterator(table_columns.begin()), std::make_move_iterator(table_columns.end()),
                      std::back_inserter(columns));
        }
        return columns;
    }
}

// 从表达式中查找 star 表达式
static auto SearchAndHandleStarExpression(BoundExpression &expr, BoundStar **star_expr, bool is_root_expr) -> bool {
    bool found_star_expr = false;
    if (expr.Type() == ExpressionType::STAR) {
        // star 表达式只能是根表达式
        if (is_root_expr) {
            *star_expr = static_cast<BoundStar *>(&expr);
            return true;
        }
        // 比如 * + 1 ，这种情况下 star 表达式不是根表达式
        throw intarkdb::Exception(ExceptionType::BINDER, "star expression must be root expression");
    }
    if (expr.Type() == ExpressionType::ALIAS) {  // fix for: select * as a from test
        auto &alias_expr = static_cast<BoundAlias &>(expr);
        return SearchAndHandleStarExpression(*alias_expr.child_, star_expr, true);  // 别名特殊处理，root状态不变
    }
    ExpressionIterator::EnumerateChildren(expr, [&](BoundExpression &child) {
        if (SearchAndHandleStarExpression(child, star_expr, false)) {
            found_star_expr = true;
        }
    });
    return found_star_expr;
}

/** 绑定 select 子句 */
auto Binder::BindSelectListExprs(duckdb_libpgquery::PGList *list) -> std::vector<std::unique_ptr<BoundExpression>> {
    auto select_list = std::vector<std::unique_ptr<BoundExpression>>{};
    int count = 0;
    for (auto node = list->head; node != nullptr; node = lnext(node), count++) {
        auto target = reinterpret_cast<duckdb_libpgquery::PGNode *>(node->data.ptr_value);
        // 表达式解释
        BoundStar *star_expr = nullptr;
        auto expr = BindExpression(target, 1);
        if (SearchAndHandleStarExpression(*expr, &star_expr, true)) {
            auto all_select_list = BindAllColumnRefs(star_expr->GetRelationName());
            if (all_select_list.empty()) {
                // select *
                throw intarkdb::Exception(ExceptionType::SYNTAX, "no valid table name for star expression");
            }
            intarkdb::CaseInsensitiveMap<int> column_using_map;
            for (size_t i = 0; i < all_select_list.size(); ++i) {
                auto &expr = all_select_list[i];
                if (expr->Type() == ExpressionType::COLUMN_REF) {
                    auto &column_ref = static_cast<BoundColumnRef &>(*expr);
                    auto in_using_clause =
                        ctx.using_column_names.find(column_ref.GetColName()) != ctx.using_column_names.end();
                    auto is_primary_table = ctx.primary_using_table == column_ref.GetTableName();
                    auto idx = column_using_map.find(column_ref.GetColName());
                    if (in_using_clause && is_primary_table && idx != column_using_map.end()) {
                        // replace
                        select_list[idx->second] = std::move(expr);
                        continue;
                    }
                    if (idx == column_using_map.end()) {
                        // not show
                        column_using_map[column_ref.GetColName()] = i;
                    }
                    if (in_using_clause && !is_primary_table && idx != column_using_map.end()) {
                        continue;
                    }
                }
                select_list.emplace_back(std::move(expr));
            }
        } else {
            // build alias map in bind select list stage
            select_list.emplace_back(std::move(expr));
        }
    }

    if (select_list.size() > 1) {
        // 检查是否有TOP或BOTTOM函数
        bool has_top_or_bottom = false;
        for (size_t i = 0; i < select_list.size(); ++i) {
            if (select_list[i]->Type() == ExpressionType::AGG_CALL) {
                auto &agg_call = static_cast<BoundFuncExpr &>(*select_list[i]);
                if (agg_call.funcname == "top" || agg_call.funcname == "bottom") {
                    has_top_or_bottom = true;
                }
            }
            if (has_top_or_bottom) {
                throw intarkdb::Exception(ExceptionType::BINDER, 
                    "Some functions are allowed only in the SELECT list of a query. And, cannot be mixed with other functions or columns.");
                break;
            }
        }
    }

    return select_list;
}

auto Binder::BindColumnRefFromTableBinding(const std::vector<std::string> &col_name, bool root)
    -> std::unique_ptr<BoundExpression> {
    if (col_name.size() > 1) {
        // with table name
        const std::string &table_name = col_name[0];
        const std::string &name = col_name[1];
        auto binding = ctx.GetTableBinding(table_name);
        if (binding) {
            const auto &col_names = binding->GetColNames();
            const auto &col_types = binding->GetColTypes();
            for (size_t i = 0; i < col_names.size(); i++) {
                if (intarkdb::StringUtil::IsEqualIgnoreCase(col_names[i], name)) {
                    return std::make_unique<BoundColumnRef>(std::vector<std::string>{table_name, col_names[i]},
                                                            col_types[i]);
                }
            }
        }
    } else {
        // no table name
        const std::string &name = col_name[0];
        bool in_using_clause = ctx.using_column_names.find(name) != ctx.using_column_names.end();
        if (in_using_clause && !ctx.primary_using_table.empty()) {
            std::vector<std::string> col_name = {ctx.primary_using_table, name};
            return BindColumnRefFromTableBinding(col_name, root);
        }
        std::unique_ptr<BoundExpression> expr = nullptr;
        for (const auto &binding : ctx.table_bindings_vec) {
            const std::string &table_name = binding.GetTableName();
            const auto &col_names = binding.GetColNames();
            const auto &col_types = binding.GetColTypes();
            for (size_t i = 0; i < col_names.size(); i++) {
                if (intarkdb::StringUtil::IsEqualIgnoreCase(col_names[i], name)) {
                    if (!expr) {
                        expr = std::make_unique<BoundColumnRef>(std::vector<std::string>{table_name, col_names[i]},
                                                                col_types[i]);
                    } else {
                        throw intarkdb::Exception(ExceptionType::BINDER, fmt::format("ambiguous column {}", name));
                    }
                }
            }
        }
        if (expr) {
            return expr;
        }
    }
    return nullptr;
}

auto Binder::BindTransaction(duckdb_libpgquery::PGTransactionStmt *pg_stmt) -> std::unique_ptr<TransactionStatement> {
    switch (pg_stmt->kind) {
        case duckdb_libpgquery::PG_TRANS_STMT_BEGIN:
        case duckdb_libpgquery::PG_TRANS_STMT_START:
            return std::make_unique<TransactionStatement>(TransactionType::BEGIN_TRANSACTION);
        case duckdb_libpgquery::PG_TRANS_STMT_COMMIT:
            return std::make_unique<TransactionStatement>(TransactionType::COMMIT);
        case duckdb_libpgquery::PG_TRANS_STMT_ROLLBACK:
            return std::make_unique<TransactionStatement>(TransactionType::ROLLBACK);
        default:
            throw intarkdb::Exception(
                ExceptionType::NOT_IMPLEMENTED,
                fmt::format("Transaction type {} not implemented yet", static_cast<int>(pg_stmt->kind)));
    }
}

auto Binder::BindDefault(duckdb_libpgquery::PGNode *node) -> std::unique_ptr<BoundExpression> {
    // TODO: 检查默认值表达式，支持 函数 & 常量 ，其他类型要支持吗?
    return node ? BindExpression(node, 1) : nullptr;
}

auto Binder::BindSequence(duckdb_libpgquery::PGCreateSeqStmt *stmt) -> std::unique_ptr<CreateSequenceStatement> {
    // CheckSysPrivilege
    if (catalog_.CheckSysPrivilege(CREATE_SEQUENCE) != GS_TRUE) {
        throw intarkdb::Exception(ExceptionType::BINDER,
                                  fmt::format("user {} create sequence permission denied!", user_));
    }

    auto result = std::make_unique<CreateSequenceStatement>();
    result->ignore_conflict = stmt->onconflict == duckdb_libpgquery::PG_IGNORE_ON_CONFLICT;
    if (stmt->sequence->relname) {
        result->name = stmt->sequence->relname;
    } else {
        throw intarkdb::Exception(ExceptionType::SYNTAX, "Missing Sequence name.", bind_location);
    }

    // schema_name
    std::string schema_name = stmt->sequence->schemaname ? stmt->sequence->schemaname : user_;
    result->schema_name = schema_name;

    if (stmt->sequence->relpersistence == duckdb_libpgquery::PG_RELPERSISTENCE_TEMP) {
        throw intarkdb::Exception(ExceptionType::SYNTAX, "Temporary Sequence is not supported.");
    }

    if (stmt->options) {
        std::unordered_set<SequenceInfo> used;
        duckdb_libpgquery::PGListCell *cell = nullptr;
        for_each_cell(cell, stmt->options->head) {
            auto *def_elem = reinterpret_cast<duckdb_libpgquery::PGDefElem *>(cell->data.ptr_value);
            std::string opt_name = std::string(def_elem->defname);
            auto val = (duckdb_libpgquery::PGValue *)def_elem->arg;
            bool nodef = def_elem->defaction == duckdb_libpgquery::PG_DEFELEM_UNSPEC && !val;  // e.g. NO MINVALUE
            int64_t opt_value = 0;

            if (val) {
                if (val->type == duckdb_libpgquery::T_PGInteger) {
                    opt_value = val->val.ival;
                } else if (val->type == duckdb_libpgquery::T_PGFloat) {
                    if (!TryCast::Operation<std::string, int64_t>(std::string(val->val.str), opt_value)) {
                        throw std::invalid_argument(
                            fmt::format("Expected an integer argument for option {}", opt_name));
                    }
                } else {
                    throw std::invalid_argument(fmt::format("Expected an integer argument for option {}", opt_name));
                }
            }
            if (opt_name == "increment") {
                if (used.find(SequenceInfo::SEQ_INC) != used.end()) {
                    throw std::invalid_argument(fmt::format("Increment value should be passed as most once"));
                }
                used.insert(SequenceInfo::SEQ_INC);
                if (nodef) {
                    continue;
                }

                result->increment = opt_value;
                if (result->increment == 0) {
                    throw std::invalid_argument(fmt::format("Increment must not be zero"));
                }
                if (result->increment < 0) {
                    result->start_value = result->max_value = -1;
                    result->min_value = NumericLimits<int64_t>::Minimum();
                } else {
                    result->start_value = result->min_value = 1;
                    result->max_value = NumericLimits<int64_t>::Maximum();
                }
            } else if (opt_name == "minvalue") {
                if (used.find(SequenceInfo::SEQ_MIN) != used.end()) {
                    throw std::invalid_argument(fmt::format("Minvalue should be passed as most once"));
                }
                used.insert(SequenceInfo::SEQ_MIN);
                if (nodef) {
                    continue;
                }

                result->min_value = opt_value;
                if (result->increment > 0) {
                    result->start_value = result->min_value;
                }
            } else if (opt_name == "maxvalue") {
                if (used.find(SequenceInfo::SEQ_MAX) != used.end()) {
                    throw std::invalid_argument(fmt::format("Maxvalue should be passed as most once"));
                }
                used.insert(SequenceInfo::SEQ_MAX);
                if (nodef) {
                    continue;
                }

                result->max_value = opt_value;
                if (result->increment < 0) {
                    result->start_value = result->max_value;
                }
            } else if (opt_name == "start") {
                if (used.find(SequenceInfo::SEQ_START) != used.end()) {
                    throw std::invalid_argument(fmt::format("Start value should be passed as most once"));
                }
                used.insert(SequenceInfo::SEQ_START);
                if (nodef) {
                    continue;
                }

                result->start_value = opt_value;
            } else if (opt_name == "cycle") {
                if (used.find(SequenceInfo::SEQ_CYCLE) != used.end()) {
                    throw std::invalid_argument(fmt::format("Cycle value should be passed as most once"));
                }
                used.insert(SequenceInfo::SEQ_CYCLE);
                if (nodef) {
                    continue;
                }

                result->cycle = opt_value > 0;
            } else {
                throw std::invalid_argument(fmt::format("Unrecognized option {} for CREATE SEQUENCE.", opt_name));
            }
        }
    }

    if (result->max_value <= result->min_value) {
        throw std::invalid_argument(
            fmt::format("MINVALUE {} must be less than MAXVALUE {}.", result->min_value, result->max_value));
    }
    if (result->start_value < result->min_value) {
        throw std::invalid_argument(
            fmt::format("START value {} cannot be less than MINVALUE {}.", result->start_value, result->min_value));
    }
    if (result->start_value > result->max_value) {
        throw std::invalid_argument(
            fmt::format("START value {} cannot be greater than MAXVALUE {}.", result->start_value, result->max_value));
    }
    return result;
}

auto Binder::BindCheckPoint(duckdb_libpgquery::PGCheckPointStmt *stmt) -> std::unique_ptr<CheckpointStatement> {
    auto result = std::make_unique<CheckpointStatement>();
    return result;
}

auto Binder::BindExplainStmt(duckdb_libpgquery::PGExplainStmt *stmt) -> std::unique_ptr<ExplainStatement> 
{
    auto result = std::make_unique<ExplainStatement>();
    auto explain_type = ExplainType::EXPLAIN_STANDARD;

    if (stmt->options) {
        for (auto n = stmt->options->head; n; n = n->next) {
            auto def_elem = reinterpret_cast<duckdb_libpgquery::PGDefElem *>(n->data.ptr_value)->defname;
            std::string elem(def_elem);
            if (elem == "analyze") {
                explain_type = ExplainType::EXPLAIN_ANALYZE;
            } else {
                throw std::invalid_argument("unspported explain statement defname:" + elem);
            }
        }
    }
    result->explain_type = explain_type;
    result->stmt = BindSQLStmt(stmt->query);
    return result;
}

// COMMENT ON COLUMN <object> IS <text>
// COMMENT ON TABLE <object> IS <text>
auto Binder::BindCommentOn(duckdb_libpgquery::PGCommentStmt *stmt) -> std::unique_ptr<CommentStatement> {
    auto result = std::make_unique<CommentStatement>();

    std::vector<std::string> obj_decode_list;
    if (stmt->object->type == duckdb_libpgquery::T_PGList) {
        auto obj_list = reinterpret_cast<duckdb_libpgquery::PGList *>(stmt->object);
        for (auto c = obj_list->head; c != nullptr; c = lnext(c)) {
            auto target = reinterpret_cast<duckdb_libpgquery::PGResTarget *>(c->data.ptr_value);
            obj_decode_list.push_back(std::string(target->name));
        }
    }
    result->comment = stmt->comment;

    if (stmt->objtype == duckdb_libpgquery::PG_OBJECT_COLUMN) {
        result->object_type = ObjectType::COLUMN;
        // support <object> format : [user_name.]table_name.column_name
        if (obj_decode_list.size() == OBJ_LIST_2ARG) {
            result->user_name = user_;
            result->table_name = obj_decode_list[0];
            result->column_name = obj_decode_list[1];

        } else if (obj_decode_list.size() == OBJ_LIST_3ARG) {
            result->user_name = obj_decode_list[0];
            result->table_name = obj_decode_list[1];
            result->column_name = obj_decode_list[2];
        } else {
            throw std::invalid_argument("Bad format of object input!!, must be : [user_name.]table_name.column_name");
        }
    } else if (stmt->objtype == duckdb_libpgquery::PG_OBJECT_TABLE) {
        result->object_type = ObjectType::TABLE;
        // support <object> format : [user_name.]table_name
        if (obj_decode_list.size() == OBJ_LIST_1ARG) {
            result->user_name = user_;
            result->table_name = obj_decode_list[0];

        } else if (obj_decode_list.size() == OBJ_LIST_2ARG) {
            result->user_name = obj_decode_list[0];
            result->table_name = obj_decode_list[1];
        } else {
            throw std::invalid_argument("Bad format of object input!!, must be : [user_name.]table_name");
        }
    } else {
        throw std::invalid_argument("unspported comment statement objtype:" + std::to_string(stmt->objtype));
    }
    return result;
}

auto Binder::AddRelatedTables(const std::string &name, bool is_timescale, bool is_modified) -> void {
    if (is_modified) {
        RootBinder()->stmt_props.modify_tables.insert(std::make_pair(
            name, StatementProps::TableInfo{name, is_timescale ? StatementProps::TableType::TABLE_TYPE_TIMESCALE
                                                               : StatementProps::TableType::TABLE_TYPE_NORMAL}));
    } else {
        RootBinder()->stmt_props.read_tables.insert(std::make_pair(
            name, StatementProps::TableInfo{name, is_timescale ? StatementProps::TableType::TABLE_TYPE_TIMESCALE
                                                               : StatementProps::TableType::TABLE_TYPE_NORMAL}));
    }
}
