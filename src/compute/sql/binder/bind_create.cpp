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
 * bind_create.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/binder/bind_create.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "binder/binder.h"
#include "binder/transform_typename.h"
#include "common/null_check_ptr.h"
#include "common/string_util.h"
#include "common/constrain_type.h"

// PGColumnDef -> Column , 列描述 -> 列实体
auto Binder::BindColumnWtihDef(duckdb_libpgquery::PGColumnDef &cdef, uint16_t slot,
                               std::vector<Constraint> &constraints) -> Column
{
    if (cdef.collClause != nullptr) {
        // 表示列的排序规则，先不支持
        throw intarkdb::Exception(ExceptionType::NOT_IMPLEMENTED, "col clause on column is not supported");
    }

    if (cdef.category == duckdb_libpgquery::COL_GENERATED || cdef.typeName == nullptr) {
        throw intarkdb::Exception(ExceptionType::NOT_IMPLEMENTED, "generated column is not supported yet!");
    }

    // 列名检查
    std::string colname = cdef.colname;
    colname = intarkdb::UTF8Util::TrimLeft(colname);
    if (colname.length() >= GS_MAX_NAME_LEN) {
        throw intarkdb::Exception(ExceptionType::BINDER,
                                  fmt::format("column name is too long, max length:{}", GS_MAX_NAME_LEN));
    }
    BindName(colname);

    colname = intarkdb::StringUtil::Lower(colname);
    Column col(colname, {});
    // TransformTypeName 会返回匹配的类型，如果没有匹配的类型，会抛出异常
    LogicalType gstor_type = TransformTypeName(*cdef.typeName);
    col.SetColType(gstor_type);
    col.SetSlot(slot);
    col.SetNullable(true);  // 默认为可空
    BindSingleColConstraintList(cdef.constraints, col, constraints);
    return col;
}

static auto CheckValidAutoIncrement(const Column &col) -> void {
    if (col.IsAutoIncrement()) {
        if (!intarkdb::IsInteger(col.GetLogicalType().TypeId())) {
            throw intarkdb::Exception(ExceptionType::BINDER, "autoincrement column must be integer type!");
        }
        if (col.HasDefault()) {
            throw intarkdb::Exception(ExceptionType::BINDER, "autoincrement column must not have default value!");
        }
    }
}

auto Binder::BindSingleColConstraint(duckdb_libpgquery::PGListCell *cell, Column &col) -> std::unique_ptr<Constraint> {
    auto constraint = NullCheckPtrCast<duckdb_libpgquery::PGConstraint>(cell->data.ptr_value);
    switch (constraint->contype) {
        case duckdb_libpgquery::PG_CONSTR_NOTNULL: {
            col.SetNullable(false);
            return nullptr;
        }
        case duckdb_libpgquery::PG_CONSTR_NULL: {
            col.SetNullable(true);
            return nullptr;
        }
        case duckdb_libpgquery::PG_CONSTR_DEFAULT: {
            if (col.IsAutoIncrement()) {
                throw intarkdb::Exception(ExceptionType::BINDER, "autoincrement column must not have default value!");
            }
            auto expr = BindDefault(constraint->raw_expr);
            if (expr) {
                auto default_value = BoundExpressionToDefaultValue(*expr, col);
                if (default_value.size() > 0) {
                    col.SetDefault(default_value);
                }
            }
            return nullptr;
        }
        case duckdb_libpgquery::PG_CONSTR_PRIMARY: {
            col.SetIsPrimaryKey(true);
            col.SetNullable(false);
            return std::make_unique<Constraint>(CONS_TYPE_PRIMARY, std::vector<std::string>{col.Name()});
        }
        case duckdb_libpgquery::PG_CONSTR_UNIQUE: {
            col.SetIsUnique(true);
            return std::make_unique<Constraint>(CONS_TYPE_UNIQUE, std::vector<std::string>{col.Name()});
        }
        case duckdb_libpgquery::PG_CONSTR_GENERATED_VIRTUAL:
        case duckdb_libpgquery::PG_CONSTR_GENERATED_STORED:
        case duckdb_libpgquery::PG_CONSTR_CHECK:
        case duckdb_libpgquery::PG_CONSTR_COMPRESSION:
        case duckdb_libpgquery::PG_CONSTR_FOREIGN: {
            throw intarkdb::Exception(ExceptionType::NOT_IMPLEMENTED,
                                      fmt::format("not support {} constraint yet!", constraint->contype));
        }
        case duckdb_libpgquery::PG_CONSTR_EXTENSION_COMMENT: {
            const std::string &comment = BindComment(constraint->raw_expr);
            if (!comment.empty()) {
                col.SetComment(comment);
            }
            return nullptr;
        }
        case duckdb_libpgquery::PG_CONSTR_EXTENSION_AUTOINCREMENT: {
            col.SetAutoIncrement(true);
            CheckValidAutoIncrement(col);
            return nullptr;
        }
        default:
            throw intarkdb::Exception(ExceptionType::BINDER, "Constraint type not handled yet!");
    }
}

auto Binder::BindMultiColConstraint(const duckdb_libpgquery::PGConstraint &constraint) -> Constraint {
    switch (constraint.contype) {
        case duckdb_libpgquery::PG_CONSTR_UNIQUE:
        case duckdb_libpgquery::PG_CONSTR_PRIMARY: {
            bool is_primary_key = constraint.contype == duckdb_libpgquery::PG_CONSTR_PRIMARY;
            std::vector<std::string> columns;
            for (auto kc = constraint.keys->head; kc != nullptr; kc = kc->next) {
                columns.emplace_back(intarkdb::StringUtil::Lower(
                    reinterpret_cast<duckdb_libpgquery::PGValue *>(kc->data.ptr_value)->val.str));
            }
            return Constraint(is_primary_key ? CONS_TYPE_PRIMARY : CONS_TYPE_UNIQUE, columns, constraint.conname);
        }
        case duckdb_libpgquery::PG_CONSTR_CHECK: {
            throw intarkdb::Exception(ExceptionType::NOT_IMPLEMENTED, "not support check constraint yet!");
        }
        case duckdb_libpgquery::PG_CONSTR_FOREIGN: {
            auto table_ref =
                BindRangeVarTableRef(*NullCheckPtrCast<duckdb_libpgquery::PGRangeVar>(constraint.pktable), false);
            auto base_table = static_cast<BoundBaseTable *>(table_ref.get());
            auto pk_user = base_table->GetSchema();
            auto pk_table = base_table->GetBoundTableName();
            char fk_matchtype = constraint.fk_matchtype;
            char fk_upd_action = constraint.fk_upd_action;
            char fk_del_action = constraint.fk_del_action;
            if (fk_upd_action != 'a') {
                throw intarkdb::Exception(ExceptionType::BINDER, "ON UPDATE ACTION not support!");
            }

            std::vector<std::string> fk_columns;
            if (constraint.fk_attrs) {
                for (auto kc = constraint.fk_attrs->head; kc != nullptr; kc = kc->next) {
                    fk_columns.emplace_back(intarkdb::StringUtil::Lower(
                        reinterpret_cast<duckdb_libpgquery::PGValue *>(kc->data.ptr_value)->val.str));
                }
            }
            std::vector<std::string> fk_ref_columns;
            if (constraint.pk_attrs) {
                for (auto kc = constraint.pk_attrs->head; kc != nullptr; kc = kc->next) {
                    fk_ref_columns.emplace_back(intarkdb::StringUtil::Lower(
                        reinterpret_cast<duckdb_libpgquery::PGValue *>(kc->data.ptr_value)->val.str));
                }
            }
            if (fk_columns.size() != fk_ref_columns.size()) {
                throw intarkdb::Exception(ExceptionType::BINDER,
                    "Key reference and table reference don't match!");
            }

            Constraint cons_fk(CONS_TYPE_REFERENCE, fk_columns, fk_ref_columns, pk_user, pk_table, constraint.conname);
            cons_fk.fk_matchtype_ = fk_matchtype;
            cons_fk.fk_upd_action_ = REF_DEL_NOT_ALLOWED;
            if (fk_del_action == 'c') {
                cons_fk.fk_del_action_ = REF_DEL_CASCADE;
            } else if (fk_del_action == 'n') {
                cons_fk.fk_del_action_ = REF_DEL_SET_NULL;
            } else if (fk_del_action == 'd') {
                throw intarkdb::Exception(ExceptionType::BINDER, "FKCONSTR_ACTION_SETDEFAULT not support!");
            } else {
                cons_fk.fk_del_action_ = REF_DEL_NOT_ALLOWED;
            }
            cons_fk.def_.fk_upd_action = cons_fk.fk_upd_action_;
            cons_fk.def_.fk_del_action = cons_fk.fk_del_action_;
            return cons_fk;
        }
        default:
            throw intarkdb::Exception(ExceptionType::NOT_IMPLEMENTED, "Constraint type not handled yet!");
    }
}

auto Binder::BindName(const std::string &name) -> void {
    if (name.length() < 1) {
        throw intarkdb::Exception(ExceptionType::BINDER, "name length is 0!");
    }

    auto c = name[0];
    if (c >= 48 && c <= 57) {
        throw intarkdb::Exception(ExceptionType::BINDER, "the first character cannot be a number!");
    }

    auto lower_name = intarkdb::StringUtil::Lower(name);
    for (auto c : lower_name) {
        if (c != '_' && !(c >= 48 && c <= 57) && !(c >= 97 && c <= 122)) {
            throw intarkdb::Exception(ExceptionType::BINDER,
                fmt::format("'{}', the character of name must be (A~Z) or (a~z) or (0~9) or '_'", c));
        }
    }
}

static void CheckUnSupportedFeature(duckdb_libpgquery::PGCreateStmt *pg_stmt) {
    if (!pg_stmt) {
        throw intarkdb::Exception(ExceptionType::BINDER, "create table statement is nullptr");
    }
    if (pg_stmt->relation == nullptr) {
        throw intarkdb::Exception(ExceptionType::BINDER, "create table relation is nullptr");
    }
    std::string table_name = pg_stmt->relation->relname ? pg_stmt->relation->relname : "";
    if (table_name.length() >= GS_MAX_NAME_LEN) {
        throw intarkdb::Exception(ExceptionType::BINDER,
                                  fmt::format("table name is too long, max length:{}", GS_MAX_NAME_LEN));
    }
    if (pg_stmt->onconflict == duckdb_libpgquery::PG_REPLACE_ON_CONFLICT) {
        throw intarkdb::Exception(ExceptionType::NOT_IMPLEMENTED, "replace expr is not supported in create table sql!");
    }
    if (pg_stmt->inhRelations) {
        throw intarkdb::Exception(ExceptionType::NOT_IMPLEMENTED, "not support inheritance relations");
    }

    if (!pg_stmt->tableElts || !pg_stmt->tableElts->head) {
        throw intarkdb::Exception(ExceptionType::BINDER, "should have at least 1 column");
    }
    if (pg_stmt->relation->relpersistence == duckdb_libpgquery::PG_RELPERSISTENCE_TEMP) {  // create temp table ....
        throw intarkdb::Exception(ExceptionType::NOT_IMPLEMENTED, "not support temp table");
    }
}

auto Binder::BindSingleColConstraintList(duckdb_libpgquery::PGList *constraints_node, Column &column,
                                         std::vector<Constraint> &constraints) -> void {
    if (constraints_node) {
        for (auto constr = constraints_node->head; constr != nullptr; constr = constr->next) {
            auto constraint = BindSingleColConstraint(constr, column);
            if (constraint) {
                constraints.push_back(std::move(*constraint));
            }
        }
    }
}

auto Binder::BindCreateColumnList(duckdb_libpgquery::PGList *tableElts, const std::string &table_name,
                                  std::vector<Column> &columns, std::vector<Constraint> &constraints) -> void {
    if (tableElts == nullptr) {
        throw intarkdb::Exception(ExceptionType::BINDER, "tableElts is nullptr");
    }

    size_t column_slots = 0;
    intarkdb::CaseInsensitiveSet lower_case_col_set;
    bool has_auto_increment = false;
    for (auto c = tableElts->head; c != nullptr; c = lnext(c)) {
        auto node = NullCheckPtrCast<duckdb_libpgquery::PGNode>(c->data.ptr_value);
        switch (node->type) {
            case duckdb_libpgquery::T_PGColumnDef: {
                auto cdef = NullCheckPtrCast<duckdb_libpgquery::PGColumnDef>(c->data.ptr_value);
                // create column
                auto col = BindColumnWtihDef(*cdef, column_slots, constraints);
                // 检查列名是否重复
                if (lower_case_col_set.find(col.Name()) != lower_case_col_set.end()) {
                    throw intarkdb::Exception(ExceptionType::BINDER,
                                              fmt::format("Column {} already exists!", col.Name()));
                }
                lower_case_col_set.insert(col.Name());
                if (col.IsAutoIncrement()) {
                    if (has_auto_increment) {
                        throw intarkdb::Exception(ExceptionType::BINDER,
                                                  "not support more than one autoincrement column!");
                    }
                    has_auto_increment = true;
                }
                columns.push_back(std::move(col));
                ++column_slots;
                break;
            }
            case duckdb_libpgquery::T_PGConstraint: {
                // node 已经检查过了，不会为空
                auto constr = reinterpret_cast<duckdb_libpgquery::PGConstraint *>(c->data.ptr_value);
                // 绑定多列约束
                constraints.emplace_back(BindMultiColConstraint(*constr));  // 多列约束
                break;
            }
            default:
                throw intarkdb::Exception(ExceptionType::NOT_IMPLEMENTED, "ColumnDef type not handled yet");
        }
    }
}

static auto HandleCreatePartionAndTimescale(duckdb_libpgquery::PGCreateStmt *pg_stmt,
                                            std::unique_ptr<CreateStatement> &result) -> void {
    if (pg_stmt->partspec != NULL) {
        if (pg_stmt->partspec->strategy != duckdb_libpgquery::PARTITION_STRATEGY_RANGE) {
            throw std::runtime_error("part table do not support list or hash strategy yet!");
        }
        if (!pg_stmt->timescale) {
            throw std::runtime_error("we do not support NON-TIMESCALE part table yet!");
        }
        GS_LOG_RUN_INF("partition:strategy-range");
        result->SetPartition(GS_TRUE);
        auto &part_obj = result->GetPartObjDefMutable();
        part_obj.part_type = PART_TYPE_RANGE;
        uint32_t part_col_idx = 0;
        for (auto c = pg_stmt->partspec->partParams->head; c != nullptr; c = lnext(c)) {
            auto part_elem = reinterpret_cast<duckdb_libpgquery::PGPartitionElem *>(c->data.ptr_value);
            GS_LOG_RUN_INF("partition:column name-%s", part_elem->name);
            if (part_elem->name == NULL) {
                throw std::runtime_error("part column name is NULL!");
            }
            bool found = GS_FALSE;
            for (const auto &col : result->GetColumns()) {
                if (col.Name() == part_elem->name) {
                    if (col.GetRaw().col_type != GS_TYPE_TIMESTAMP) {
                        GS_LOG_RUN_ERR("part key column type(%d) is wrong, must be timestamp!", col.GetRaw().col_type);
                        throw std::runtime_error("part key column type is wrong, must be timestamp!");
                    }
                    part_obj.part_keys[part_col_idx].column_id = col.GetRaw().col_slot;
                    part_obj.part_keys[part_col_idx].datatype = col.GetRaw().col_type;
                    part_obj.part_keys[part_col_idx].size = col.GetRaw().size;
                    part_obj.part_keys[part_col_idx].precision = col.GetRaw().precision;
                    part_obj.part_keys[part_col_idx].scale = col.GetRaw().scale;
                    part_col_idx++;
                    found = GS_TRUE;
                    break;
                }
            }
            if (!found) {
                throw std::runtime_error(fmt::format("part column name {} is not found!", part_elem->name));
            }
        }
        part_obj.part_col_count = part_col_idx;
        part_obj.auto_addpart = pg_stmt->autopart;
        part_obj.is_crosspart = pg_stmt->crosspart;
        part_obj.is_interval = GS_FALSE;
        if (pg_stmt->interval) {
            GS_LOG_RUN_INF("partition interval:%s", pg_stmt->interval);
            std::string interv(pg_stmt->interval);
            std::regex pattern("1[hdHD]"); // 1h or 1d,
            if (!std::regex_match(interv, pattern)) {
                throw std::runtime_error(
                    fmt::format("the interval must be  1h/1d, but now is {}!",
                                pg_stmt->interval));
            }
            part_obj.is_interval = GS_TRUE;
            part_obj.interval.str = pg_stmt->interval;
            part_obj.interval.len = strlen(pg_stmt->interval);
        } else {
            if (pg_stmt->timescale) {
                throw std::runtime_error("timescale table missing interval!");
            }
        }
    }
    if (pg_stmt->timescale) {
        GS_LOG_RUN_INF("timescale table");
        result->SetTimescale(GS_TRUE);
        if (!result->Partition()) {
            throw std::runtime_error("timescale table must be parted!");
        }

        for (auto &cons : result->GetConstraints()) {
            if (cons.Type() == constraint_type_t::CONS_TYPE_PRIMARY ||
                cons.Type() == constraint_type_t::CONS_TYPE_UNIQUE) {
                GS_LOG_RUN_ERR("timescale table can not have the primary key or unique key!");
                throw intarkdb::Exception(ExceptionType::BINDER,
                                          "timescale table can not have the primary key or unique key!");
            }
        }
    }
    if (pg_stmt->retention) {
        GS_LOG_RUN_INF("partition retention time:%s", pg_stmt->retention);
        std::string retent(pg_stmt->retention);
        std::regex pattern("[1-9][0-9]*[hdHD]");
        if (!std::regex_match(retent, pattern)) {
            throw std::runtime_error(
                fmt::format("the number prefix of retention {} must be a positive integer, and suffix must be h/d!",
                            pg_stmt->retention));
        }
        result->SetRetention(pg_stmt->retention);
    } else {
        if (pg_stmt->timescale) {
            GS_LOG_RUN_INF("You do not set the retention for timescale, default 7 days!");
            result->SetRetention((char *)RETENTION_DEFAULT.c_str());
        }
    }
}

auto Binder::BindCreateStmt(duckdb_libpgquery::PGCreateStmt *pg_stmt) -> std::unique_ptr<CreateStatement>
{
    // CheckSysPrivilege
    if (catalog_.CheckSysPrivilege(CREATE_TABLE) != GS_TRUE) {
        throw intarkdb::Exception(ExceptionType::BINDER, fmt::format("user {} create table permission denied!", user_));
    }

    // 检查是否有不支持的特性
    CheckUnSupportedFeature(pg_stmt);

    auto columns = std::vector<Column>{};
    auto constraints = std::vector<Constraint>{};
    std::string table_name = pg_stmt->relation->relname ? pg_stmt->relation->relname : "";
    table_name = intarkdb::StringUtil::Lower(table_name);
    table_name = intarkdb::UTF8Util::TrimLeft(table_name);
    auto result = std::make_unique<CreateStatement>(table_name);

    BindName(table_name);

    // tableElts 为列定义，一般为 PGColumnDef(列定义) 或 PGConstraint(索引定义)
    BindCreateColumnList(pg_stmt->tableElts, table_name, columns, constraints);

    result->SetColumns(std::move(columns));
    result->SetConstraints(std::move(constraints));

    // 处理分区表
    HandleCreatePartionAndTimescale(pg_stmt, result);

    AddRelatedTables(table_name, result->Timescale(), true);

    // TABLE COMMENT
    result->SetComment(pg_stmt->comment);

#ifdef ENABLE_PG_QUERY
    result->SetConflictOpt(pg_stmt->onconflict);
#endif

    return result;
}

auto Binder::BindComment(duckdb_libpgquery::PGNode *node) -> std::string {
    if (node) {
        auto expr = BindExpression(node, 1);
        if (expr && expr->Type() == ExpressionType::LITERAL) {
            return expr->ToString();
        }
    }
    return "";
}
