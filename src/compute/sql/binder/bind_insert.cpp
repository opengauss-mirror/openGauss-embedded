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
 * bind_insert.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/binder/bind_insert.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "binder/binder.h"
#include "binder/expressions/bound_binary_op.h"
#include "binder/expressions/bound_constant.h"
#include "binder/expressions/bound_unary_op.h"
#include "common/null_check_ptr.h"
#include "common/string_util.h"

auto Binder::BindInsert(duckdb_libpgquery::PGInsertStmt *pg_stmt) -> std::unique_ptr<InsertStatement> {
    if (!pg_stmt->selectStmt) {
        throw intarkdb::Exception(ExceptionType::NOT_IMPLEMENTED, "DEFAULT VALUES clause is not supported!");
    }

    if (pg_stmt->returningList) {
        throw intarkdb::Exception(ExceptionType::NOT_IMPLEMENTED, "returning is not supported!");
    }

    if (pg_stmt->withClause) {
        throw intarkdb::Exception(ExceptionType::NOT_IMPLEMENTED, "WITH clause is not supported!");
    }

    InsertOnConflictActionAlias opt_or_action = ONCONFLICT_ALIAS_NONE;
    if (pg_stmt->onConflictAlias) {
        if (pg_stmt->onConflictAlias == duckdb_libpgquery::PG_ONCONFLICT_ALIAS_IGNORE) {
            opt_or_action = ONCONFLICT_ALIAS_IGNORE;
        } else {
            opt_or_action = ONCONFLICT_ALIAS_NONE;
            // throw intarkdb::Exception(ExceptionType::BINDER, "Bad onconflict alias, not support!");  // TODO
        }
    }

    // be dependent on select statement
    auto pg_stmt_sel = reinterpret_cast<duckdb_libpgquery::PGSelectStmt *>(pg_stmt->selectStmt);
    auto select_statement = BindSelect(pg_stmt_sel);

    // columns specified
    intarkdb::CaseInsensitiveSet column_names_set;
    std::vector<std::string> column_names;
    if (pg_stmt->cols) {
        for (auto c = pg_stmt->cols->head; c != nullptr; c = lnext(c)) {
            auto target = (duckdb_libpgquery::PGResTarget *)(c->data.ptr_value);
            if (column_names_set.find(target->name) != column_names_set.end()) {
                throw intarkdb::Exception(ExceptionType::BINDER, "Duplicate column name: " + std::string(target->name));
            }
            column_names_set.insert(target->name);
            column_names.emplace_back(intarkdb::StringUtil::Lower(target->name));
        }
    }

    // Bind insert columns
    auto table_ref = BindRangeVar(*NullCheckPtrCast<duckdb_libpgquery::PGRangeVar>(pg_stmt->relation), false);
    auto dict_type = table_ref->DictType();
    if (dict_type != DIC_TYPE_TABLE) {
        throw std::runtime_error("Can't insert, entry type not support!");
    }
    auto &table_info = (static_cast<BoundBaseTable *>(table_ref.get()))->GetTableInfo();
    auto &meta_info = table_info.GetTableMetaInfo();
    if (meta_info.space_id != SQL_SPACE_TYPE_USERS) {
        throw std::runtime_error("Cannot insert into system table : " + std::string(meta_info.name));
    }

    AddRelatedTables(std::string(table_info.GetTableName()), table_info.IsTimeScale(), true);

    auto &columns = table_info.columns;

    // check privileges
    auto base_table = static_cast<BoundBaseTable *>(table_ref.get());
    std::string obj_user = base_table->GetSchema();
    std::string obj_name = base_table->GetBoundTableName();
    if (catalog_.CheckPrivilege(obj_user, obj_name, OBJ_TYPE_TABLE, GS_PRIV_INSERT) != GS_TRUE) {
        throw intarkdb::Exception(ExceptionType::BINDER,
                                fmt::format("{}.{} insert permission denied!", obj_user, obj_name));
    }
    // check privileges again (for example : synonym)
    std::string real_obj_name = std::string(table_info.GetTableName());
    if (real_obj_name != obj_name) {
        auto uid = table_info.user_id;
        std::string real_obj_user = catalog_.GetUserName(uid);
        if (catalog_.CheckPrivilege(real_obj_user, real_obj_name, OBJ_TYPE_TABLE, GS_PRIV_INSERT) != GS_TRUE) {
            throw intarkdb::Exception(ExceptionType::BINDER,
                                    fmt::format("{}.{} insert permission denied!", real_obj_user, real_obj_name));
        }
    }

    std::vector<Column> bound_columns;
    std::vector<Column> unbound_defaults;
    std::vector<Column> bound_defaults;
    if (!column_names.empty()) {
        // Columns specified
        // bound_columns
        for (auto &col_name : column_names) {
            for (auto &column : columns) {
                if (column.Name() == col_name) {
                    bound_columns.push_back(column);
                    break;
                }
            }
        }
        if (bound_columns.size() != column_names.size()) {
            // FIXME: 找不到列名也会报这个错误
            throw std::runtime_error("Cannot explicitly insert columns");
        }
        // unbound defaults & bound defaults
        for (auto &column : columns) {
            bool is_bound_col = false;
            for (auto &col_name : column_names) {
                if (column.Name() == col_name) {
                    is_bound_col = true;
                    if (meta_info.has_autoincrement && column.GetRaw().is_autoincrement) {
                        // do nothing
                    } else if (column.GetRaw().is_default) {
                        bound_defaults.push_back(column);
                    }
                    break;
                }
            }
            if (!is_bound_col) {
                if (meta_info.has_autoincrement && column.GetRaw().is_autoincrement) {
                    // do nothing
                } else if (column.GetRaw().is_default) {
                    unbound_defaults.push_back(column);
                } else {
                    // unbound
                    if (!column.GetRaw().nullable) {
                        throw std::runtime_error(
                            fmt::format("unbound column ( {} ) not nullable!", column.GetRaw().name.str));
                    }
                }
            }
        }
    } else {
        // No columns specified
        for (auto &column : columns) {
            column_names.push_back(column.Name());
        }

        bound_columns = columns;
    }

    if (column_names.size() != select_statement->select_expr_list.size()) {
        throw std::runtime_error("Number of columns does not match");
    }

    auto insert_statement = std::make_unique<InsertStatement>(std::move(table_ref), std::move(select_statement));
    insert_statement->bound_columns_ = bound_columns;
    insert_statement->unbound_defaults_ = unbound_defaults;
    insert_statement->bound_defaults_ = bound_defaults;
    insert_statement->opt_or_action_ = opt_or_action;

    return insert_statement;
}
