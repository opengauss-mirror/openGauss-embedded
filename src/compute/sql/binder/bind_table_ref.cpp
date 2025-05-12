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
 * bind_table_ref.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/binder/bind_table_ref.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "binder/binder.h"
#include "binder/expressions/bound_binary_op.h"
#include "binder/expressions/bound_column_def.h"
#include "binder/expressions/bound_conjunctive.h"
#include "binder/table_ref/bound_value_clause.h"
#include "common/null_check_ptr.h"
#include "common/string_util.h"

using namespace duckdb_libpgquery;

auto Binder::BindTableRef(const PGNode &node) -> std::unique_ptr<BoundQuerySource>
{
    switch (node.type) {
        case T_PGRangeVar: {
            // 表 或 视图 类型，比如 select * from student;
            return BindRangeVarTableRef((const PGRangeVar &)node);
        }
        case T_PGJoinExpr: {
            // 连接表达式类型，比如SELECT * FROM employees JOIN departments ON employees.department_id = departments.id;
            return BindJoinTableRef((const PGJoinExpr &)node);
        }
        case T_PGRangeSubselect: {
            // 子查询类型，比如SELECT department_id FROM (SELECCT * FROM emplyees ) WHERE salary > 10;
            // 就是T_PGRangeSubselect类型的节点
            return BindRangeSubselectTableRef((const PGRangeSubselect &)node);
        }
        default:
            throw intarkdb::Exception(ExceptionType::NOT_IMPLEMENTED,
                fmt::format("unsupported node type: {}", Binder::ConvertNodeTagToString(node.type)));
    }
}

static auto CheckBindRangVar(const PGRangeVar &table_ref) -> void {
    if (table_ref.catalogname != nullptr) {
        throw intarkdb::Exception(ExceptionType::NOT_IMPLEMENTED, "catalog name is not supported yet");
    }
    if (table_ref.sample != nullptr) {
        throw intarkdb::Exception(ExceptionType::NOT_IMPLEMENTED, "sample is not supported yet");
    }
    if (table_ref.alias != nullptr && table_ref.alias->colnames != nullptr) {
        throw intarkdb::Exception(ExceptionType::NOT_IMPLEMENTED, "alias(colnames) is not supported yet");
    }
    if (table_ref.relname == nullptr) {
        throw intarkdb::Exception(ExceptionType::BINDER, "table name is nullptr");
    }
}

auto Binder::BindRangeVarTableRef(const PGRangeVar &table_ref, bool is_select) -> std::unique_ptr<BoundQuerySource>
{
    CheckBindRangVar(table_ref);

    std::string schema_name = table_ref.schemaname ? std::string(table_ref.schemaname) : user_;
    if (is_system_command_) {
        schema_name = SYS_USER_NAME;
    }
    std::unique_ptr<BoundBaseTable> base_table =
        BindBaseTableRef(schema_name, table_ref.relname,
                         table_ref.alias ? std::make_optional(table_ref.alias->aliasname) : std::nullopt);
    if (!base_table) {
        throw intarkdb::Exception(ExceptionType::BINDER,
            fmt::format("table {}.{} not exists!", schema_name, table_ref.relname));
    }

    if (is_select) {
        // check privileges
        std::string obj_user = base_table->GetSchema();
        std::string obj_name = base_table->GetBoundTableName();
        if (!is_system_command_ &&
            catalog_.CheckPrivilege(obj_user, obj_name, OBJ_TYPE_TABLE, GS_PRIV_SELECT) != GS_TRUE) {
            throw intarkdb::Exception(ExceptionType::BINDER,
                fmt::format("{}.{} Permission denied!", obj_user, obj_name));
        }
        // check privileges again (for example : synonym)
        const auto &table_info = base_table->GetTableInfo();
        std::string real_obj_name = std::string(table_info.GetTableName());
        if (real_obj_name != obj_name) {
            auto uid = table_info.user_id;
            std::string real_obj_user = catalog_.GetUserName(uid);
            if (catalog_.CheckPrivilege(real_obj_user, real_obj_name, OBJ_TYPE_TABLE, GS_PRIV_SELECT) != GS_TRUE) {
                throw intarkdb::Exception(ExceptionType::BINDER,
                    fmt::format("{}.{} permission denied!", real_obj_user, real_obj_name));
            }
        }
    }

    const auto &table_info = base_table->GetTableInfo();
    switch (table_info.GetObjectType()) {
        case DIC_TYPE_TABLE:
            base_table->SetDictType(DIC_TYPE_TABLE);
            if (is_select) {
                ctx.AddTableBinding(base_table->GetTableNameOrAlias(), table_info.columns);
                AddRelatedTables(std::string(table_info.GetTableName()), table_info.IsTimeScale(), false);
            }
            return base_table;
        case DIC_TYPE_VIEW: {
            auto query_sql = table_info.GetTableMetaInfo().sql;
            Binder view_binder(this);
            view_binder.ParseSQL(query_sql.str);
            const auto &stmts = view_binder.GetStatementNodes();
            auto node = reinterpret_cast<PGNode *>(stmts[0]);
            if (node->type == T_PGRawStmt) {
                node = reinterpret_cast<PGRawStmt *>(node)->stmt;
            }
            if (node->type != T_PGViewStmt && node->type != T_PGRawStmt) {
                throw intarkdb::Exception(ExceptionType::BINDER,
                                          fmt::format("invalid view statement {}", ConvertNodeTagToString(node->type)));
            }
            auto view_node = reinterpret_cast<PGViewStmt *>(node);
            return BindSubqueryTableRef(reinterpret_cast<PGSelectStmt *>(view_node->query), view_node->view->relname);
        }
        default:
            throw std::invalid_argument(
                fmt::format("unsupported dictionary type: {}", (int)table_info.GetObjectType()));
    }
}

auto Binder::BindBaseTableRef(const std::string &schema_name, const std::string &table_name,
                              std::optional<std::string> &&alias, bool if_exists) -> std::unique_ptr<BoundBaseTable> {
    // 查找表是否存
    auto table_info = catalog_.GetTable(schema_name, table_name);
    if (!table_info) {
        if (if_exists) {
            GS_LOG_RUN_WAR("%s", fmt::format("table {}.{} not exists!", schema_name, table_name).c_str());
            return nullptr;
        }
        // no table
        GS_LOG_RUN_WAR("%s", fmt::format("table {}.{} not exists!", schema_name, table_name).c_str());
        throw intarkdb::Exception(ExceptionType::BINDER,
                                  fmt::format("table {}.{} not exists!", schema_name, table_name));
    }
    return std::make_unique<BoundBaseTable>(schema_name, table_name, std::move(alias), std::move(table_info));
}

static auto TransformJoinType(PGJoinType type) -> JoinType {
    JoinType join_type = JoinType::Invalid;
    switch (type) {
        case PG_JOIN_INNER:
            join_type = JoinType::CrossJoin;
            break;
        case PG_JOIN_LEFT:
            join_type = JoinType::LeftJoin;
            break;
        case PG_JOIN_RIGHT:
            join_type = JoinType::RightJoin;
            break;
        case PG_JOIN_FULL:
            join_type = JoinType::FullJoin;
            break;
        default:
            throw intarkdb::Exception(ExceptionType::BINDER, fmt::format("Join type {} not supported", type));
    }
    return join_type;
}

static auto HandleUsingClause(Binder &left_table_binder, Binder &right_table_binder, BinderContext &curr_ctx,
                              JoinType join_type) -> std::unique_ptr<BoundExpression> {
    if (!curr_ctx.using_column_names.empty()) {
        std::vector<std::unique_ptr<BoundExpression>> condition_exprs;
        // check left tableref and right tableref column exist
        std::set<std::string> left_table_set;
        std::set<std::string> right_table_set;
        for (const auto &column_name : curr_ctx.using_column_names) {
            if (left_table_binder.ctx.primary_using_table.empty()) {
                left_table_set = left_table_binder.ctx.FindBindingByColumnName(column_name);
                if (left_table_set.empty()) {
                    throw intarkdb::Exception(ExceptionType::BINDER,
                                              fmt::format("column {} not exists in left table", column_name));
                }
                if (left_table_set.size() > 1) {
                    throw intarkdb::Exception(ExceptionType::BINDER,
                                              fmt::format("column {} exists in multiple tables", column_name));
                }
            } else {
                auto binding = left_table_binder.ctx.GetTableBinding(left_table_binder.ctx.primary_using_table);
                if (!binding) {
                    throw intarkdb::Exception(
                        ExceptionType::BINDER,
                        fmt::format("table {} not exists in left side", left_table_binder.ctx.primary_using_table));
                }
                if (!binding->CheckColumnExist(column_name)) {
                    throw intarkdb::Exception(ExceptionType::BINDER,
                                              fmt::format("column {} not exists in left table", column_name));
                }
                left_table_set.insert(left_table_binder.ctx.primary_using_table);
            }

            if (right_table_binder.ctx.primary_using_table.empty()) {
                right_table_set = right_table_binder.ctx.FindBindingByColumnName(column_name);
                if (right_table_set.empty()) {
                    throw intarkdb::Exception(ExceptionType::BINDER,
                                              fmt::format("column {} not exists in right table", column_name));
                }
                if (right_table_set.size() > 1) {
                    throw intarkdb::Exception(ExceptionType::BINDER,
                                              fmt::format("column {} exists in multiple tables", column_name));
                }
            } else {
                auto binding = right_table_binder.ctx.GetTableBinding(right_table_binder.ctx.primary_using_table);
                if (!binding) {
                    throw intarkdb::Exception(
                        ExceptionType::BINDER,
                        fmt::format("table {} not exists in right side", right_table_binder.ctx.primary_using_table));
                }
                if (!binding->CheckColumnExist(column_name)) {
                    throw intarkdb::Exception(ExceptionType::BINDER,
                                              fmt::format("column {} not exists in right table", column_name));
                }
                right_table_set.insert(right_table_binder.ctx.primary_using_table);
            }
            std::unique_ptr<BoundColumnRef> left_column_ptr = nullptr;
            auto left_binding = left_table_binder.ctx.GetTableBinding(*left_table_set.begin());
            for (size_t i = 0; i < left_binding->GetColNames().size(); ++i) {
                if (left_binding->GetColNames()[i] == column_name) {
                    left_column_ptr = std::make_unique<BoundColumnRef>(
                        std::vector{left_binding->GetTableName(), column_name}, left_binding->GetColTypes()[i]);
                    break;
                }
            }
            auto right_binding = right_table_binder.ctx.GetTableBinding(*right_table_set.begin());
            std::unique_ptr<BoundColumnRef> right_column_ptr = nullptr;
            for (size_t i = 0; i < right_binding->GetColNames().size(); ++i) {
                if (right_binding->GetColNames()[i] == column_name) {
                    right_column_ptr = std::make_unique<BoundColumnRef>(
                        std::vector{right_binding->GetTableName(), column_name}, right_binding->GetColTypes()[i]);
                    break;
                }
            }
            condition_exprs.emplace_back(
                std::make_unique<BoundBinaryOp>("=", std::move(left_column_ptr), std::move(right_column_ptr)));
            if (join_type == JoinType::RightJoin) {
                curr_ctx.primary_using_table = right_binding->GetTableName();
            } else if (join_type == JoinType::LeftJoin || join_type == JoinType::CrossJoin) {
                curr_ctx.primary_using_table = left_binding->GetTableName();
            } else {
                throw intarkdb::Exception(ExceptionType::BINDER,
                                          fmt::format("Join type {} with using clause not supported", join_type));
            }
        }
        return std::make_unique<BoundConjunctive>(std::move(condition_exprs));
    }
    return nullptr;
}

auto Binder::BindJoinTableRef(const PGJoinExpr &node) -> std::unique_ptr<BoundQuerySource>
{
    if (node.isNatural) {
        throw intarkdb::Exception(ExceptionType::NOT_IMPLEMENTED, "NATURAL JOIN is not supported yet");
    }

    if (node.alias && node.alias->colnames) {
        throw intarkdb::Exception(ExceptionType::NOT_IMPLEMENTED, "join with alias(colnames) is not supported yet");
    }

    JoinType join_type = TransformJoinType(node.jointype);

    Binder left_table_binder(this);
    Binder right_table_binder(this);
    auto left_table = left_table_binder.BindTableRef(*NullCheckPtrCast<PGNode>(node.larg));
    auto right_table = right_table_binder.BindTableRef(*NullCheckPtrCast<PGNode>(node.rarg));

    if (node.usingClause && node.usingClause->length > 0) {
        for (auto curr = node.usingClause->head; curr != nullptr; curr = curr->next) {
            auto target = reinterpret_cast<duckdb_libpgquery::PGNode *>(curr->data.ptr_value);
            // asume target is String
            std::string name =
                intarkdb::StringUtil::Lower(reinterpret_cast<duckdb_libpgquery::PGValue *>(target)->val.str);
            ctx.using_column_names.insert(name);
        }
    }

    std::unique_ptr<BoundExpression> on_condition =
        HandleUsingClause(left_table_binder, right_table_binder, ctx, join_type);

    ctx.MergeTableBinding(left_table_binder.ctx);
    ctx.MergeTableBinding(right_table_binder.ctx);

    if (node.quals != nullptr) {
        on_condition = BindExpression(node.quals, 1);
    }
    return std::make_unique<BoundJoin>(join_type, std::move(left_table), std::move(right_table),
                                       std::move(on_condition));
}

auto Binder::BindRangeSubselectTableRef(const PGRangeSubselect &node) -> std::unique_ptr<BoundQuerySource>
{
    if (node.lateral) {
        throw intarkdb::Exception(ExceptionType::NOT_IMPLEMENTED, "LATERNAL in subquery is not supported yet");
    }
    if (node.sample) {
        throw intarkdb::Exception(ExceptionType::NOT_IMPLEMENTED, "SAMPLE in subquery is not supported yet");
    }
    // 暂不支持如下语法 e.g: select * from (select 1) as t(a);
    if (node.alias != nullptr && node.alias->colnames != nullptr) {
        throw intarkdb::Exception(ExceptionType::NOT_IMPLEMENTED, "subquery with alias(colnames) is not supported yet");
    }

    // 子查询别名
    std::string subquery_name = node.alias ? node.alias->aliasname : fmt::format("__subquery_{}", GetNextUniversalID());
    // 将子查询的列绑定到当前上下文
    return BindSubqueryTableRef(reinterpret_cast<PGSelectStmt *>(node.subquery), subquery_name);
}

auto Binder::BindSubqueryTableRef(PGSelectStmt *node, const std::string &subquery_name)
    -> std::unique_ptr<BoundSubquery>
{
    // 绑定select statement
    auto subquery_binder = Binder(this);
    auto subquery = subquery_binder.BindSelectStmt(node);
    // 查询列表
    std::vector<SchemaColumnInfo> col_infos;
    int slot = 0;
    for (const auto &item : subquery->return_list) {
        col_infos.emplace_back(item.col_name, "", item.col_type, slot++);
    }
    if (!col_infos.empty()) {
        // note: 注意子查询导出的列名可能重复，需要重新命名
        Schema schema = Schema::RenameSchemaColumnIfExistSameNameColumns(Schema(std::move(col_infos)));
        ctx.AddTableBinding(subquery_name, schema.GetColumnInfos());
    }
    return std::make_unique<BoundSubquery>(std::move(subquery), subquery_name);
}

auto Binder::BindValueList(PGList *list, std::vector<std::unique_ptr<BoundExpression>> &select_list)
    -> std::unique_ptr<BoundQuerySource>
{
    auto value_clause = std::make_unique<BoundValueClause>();
    for (auto value_list = list->head; value_list != nullptr; value_list = value_list->next) {
        std::vector<std::unique_ptr<BoundExpression>> row_values;
        auto target_list = NullCheckPtrCast<PGList>(value_list->data.ptr_value);
        for (auto node = target_list->head; node != nullptr; node = node->next) {
            auto target = reinterpret_cast<PGNode *>(node->data.ptr_value);
            // 关闭检查列存在性  // 双引号会被判断为列
            check_column_exist_ = false;
            auto expr = BindExpression(target, 1);
            check_column_exist_ = true;
            if (expr->Type() == ExpressionType::COLUMN_REF) {
                throw intarkdb::Exception(
                    ExceptionType::BINDER,
                    fmt::format("Bad input at ({}), please use singlequotes ' ' for string values", expr->ToString()));
            }
            row_values.push_back(std::move(expr));
        }
        if (!value_clause->values.empty() && value_clause->values[0].size() != row_values.size()) {
            throw intarkdb::Exception(ExceptionType::BINDER, "VALUES lists must all be the same length");
        }
        value_clause->values.push_back(std::move(row_values));
    }

    if (value_clause->values.empty()) {
        throw intarkdb::Exception(ExceptionType::BINDER, "VALUES lists must not be empty");
    }

    std::vector<SchemaColumnInfo> &col_infos = value_clause->col_infos;
    // TODO 优化
    static const std::string value_list_name = "valuelist";
    int col_count = 0;
    size_t col_num = value_clause->values[0].size();
    for (size_t i = 0; i < col_num; ++i) {
        auto type = value_clause->values[0][i]->ReturnType();
        for (size_t j = 1; j < value_clause->values.size(); ++j) {
            type = intarkdb::GetCompatibleType(type, value_clause->values[j][i]->ReturnType());
        }
        SchemaColumnInfo col_info;
        col_info.col_name = std::vector{value_list_name, fmt::format("col{}", col_count++)};
        col_info.col_type = type;
        col_info.slot = i;
        col_infos.push_back(col_info);
    }
    ctx.AddTableBinding(value_list_name, col_infos);

    select_list = BindAllColumnRefs(value_list_name.c_str());
    return value_clause;
}
