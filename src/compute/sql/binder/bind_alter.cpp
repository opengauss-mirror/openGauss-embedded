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
 * bind_alter.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/binder/bind_alter.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <fmt/core.h>
#include <fmt/format.h>

#include <cstdlib>

#include "binder/alter_table_info.h"
#include "binder/binder.h"
#include "binder/expressions/bound_constant.h"
#include "binder/transform_typename.h"
#include "common/alter_type.h"
#include "common/constrain_type.h"
#include "common/exception.h"
#include "common/object_type.h"
#include "common/string_util.h"

static auto TransformConstraintType(duckdb_libpgquery::PGConstrType type) -> constraint_type_t {
    switch (type) {
        case duckdb_libpgquery::PG_CONSTR_PRIMARY:
            return CONS_TYPE_PRIMARY;
        case duckdb_libpgquery::PG_CONSTR_UNIQUE:
            return CONS_TYPE_UNIQUE;
        default:
            throw intarkdb::Exception(ExceptionType::BINDER,
                                      fmt::format("Constraint type {} not supported yet!", type));
    }
}

auto Binder::BindAlter(duckdb_libpgquery::PGAlterTableStmt *stmt) -> std::unique_ptr<AlterStatement> {
    auto schema_name = stmt->relation->schemaname ? std::string(stmt->relation->schemaname) : user_;
    auto table_name = std::string(stmt->relation->relname);
    auto result = std::make_unique<AlterStatement>(schema_name, table_name);
    auto &al_cols = result->al_cols;
    std::vector<col_text_t> old_names;
    std::vector<col_text_t> new_names;

    auto table = BindBaseTableRef(schema_name, table_name, std::nullopt);
    const auto &table_info = table->GetTableInfo();
    if (table_info.GetSpaceId() != SQL_SPACE_TYPE_USERS) {
        throw intarkdb::Exception(ExceptionType::BINDER,
                                  fmt::format("Cannot alter system tables : {}", table_info.GetTableName()));
    }

    AddRelatedTables(std::string(table_info.GetTableName()), table_info.IsTimeScale(), true);
    // check privileges
    std::string obj_user = table->GetSchema();
    std::string obj_name = table->GetBoundTableName();
    if (catalog_.CheckPrivilege(obj_user, obj_name, OBJ_TYPE_TABLE, GS_PRIV_ALTER) != GS_TRUE) {
        throw intarkdb::Exception(ExceptionType::BINDER,
                                fmt::format("{}.{} alter permission denied!", obj_user, obj_name));
    }

    // alter table comment
    if (stmt->comment) {
        result->info = std::make_unique<AlterTableInfo>(GsAlterTableType::ALTABLE_MODIFY_TABLE_COMMENT,
                                                        stmt->relation->relname, stmt->comment);
        return result;
    }
    if (!stmt->cmds) {
        return result;
    }

    // first we check the type of ALTER
    for (auto c = stmt->cmds->head; c != nullptr; c = c->next) {
        auto command = reinterpret_cast<duckdb_libpgquery::PGAlterTableCmd *>(lfirst(c));
        switch (command->subtype) {
            case duckdb_libpgquery::PG_AT_AddColumn: {
                auto cdef = (duckdb_libpgquery::PGColumnDef *)command->def;

                if (stmt->relkind != duckdb_libpgquery::PG_OBJECT_TABLE) {
                    throw intarkdb::Exception(ExceptionType::BINDER, "Adding columns is only supported for tables");
                }
                if (cdef->category == duckdb_libpgquery::COL_GENERATED) {
                    throw intarkdb::Exception(ExceptionType::BINDER,
                                              "Adding generated columns after table creation is not supported yet");
                }
                auto col = BindColumnDefinition(*cdef, -1, result->constraints);
                if (table_info.IsTimeScale() && col.HasPrimaryOrUniqueKey()) {
                    throw intarkdb::Exception(ExceptionType::BINDER,
                                              "Primary key or unique key is not supported for time scale table");
                }
                al_cols.emplace_back(std::move(col));
                result->info = std::make_unique<AlterTableInfo>(GsAlterTableType::ALTABLE_ADD_COLUMN,
                                                                stmt->relation->relname, old_names, new_names, al_cols);
                break;
            }
            case duckdb_libpgquery::PG_AT_DropColumn: {
                if (stmt->relkind != duckdb_libpgquery::PG_OBJECT_TABLE) {
                    throw intarkdb::Exception(ExceptionType::BINDER, "Dropping columns is only supported for tables");
                }
#ifdef _MSC_VER
                col_text_t col_text_instance;
                col_text_instance.str = command->name;
                col_text_instance.len = static_cast<uint32_t>(strlen(command->name));
                col_text_instance.assign = ASSIGN_TYPE_EQUAL;

                exp_column_def_t exp_column_def_instance;
                exp_column_def_instance.name = col_text_instance;

                Column col(std::string(command->name, strlen(command->name)), exp_column_def_instance);
#else
                Column col(std::string(command->name, strlen(command->name)),
                           exp_column_def_t{.name = col_text_t{.str = command->name,
                                                               .len = static_cast<uint32_t>(strlen(command->name)),
                                                               .assign = ASSIGN_TYPE_EQUAL}});
#endif
                al_cols.emplace_back(std::move(col));
                result->info = std::make_unique<AlterTableInfo>(GsAlterTableType::ALTABLE_DROP_COLUMN,
                                                                stmt->relation->relname, old_names, new_names, al_cols);
                break;
            }
            case duckdb_libpgquery::PG_AT_ColumnDefault: {
                auto table_info = catalog_.GetTable(schema_name, table_name);
                if (table_info == nullptr) {
                    GS_LOG_RUN_ERR("%s", fmt::format("Alter table {} not exists! command->subtype :{}", table_name,
                                                     command->subtype)
                                             .c_str());
                    throw intarkdb::Exception(ExceptionType::BINDER,
                                              fmt::format("Alter table {} not exists!", table_name));
                }
                auto old_col = table_info->GetColumnByName(command->name);
                if (old_col == NULL) {
                    throw intarkdb::Exception(ExceptionType::BINDER,
                                              fmt::format("Alter column {} not exists!", command->name));
                }
                Column col(*old_col);
                auto expr = BindDefault(command->def);
                if (expr) {
                    auto default_value = BoundExpressionToDefaultValue(*expr, col);
                    col.SetDefault(default_value);
                } else {
                    col.SetHasDefault(false);
                }
                al_cols.emplace_back(std::move(col));
                result->info = std::make_unique<AlterTableInfo>(GsAlterTableType::ALTABLE_MODIFY_COLUMN,
                                                                stmt->relation->relname, old_names, new_names, al_cols);
                break;
            }
            case duckdb_libpgquery::PG_AT_AlterColumnType: {
                auto cdef = (duckdb_libpgquery::PGColumnDef *)command->def;
                cdef->colname = command->name;  // 补全列名,防止BindColumnDefinition执行错误
                auto table_info = catalog_.GetTable(schema_name, table_name);
                if (table_info == nullptr) {
                    GS_LOG_RUN_ERR("%s", fmt::format("Alter table {} not exists! command->subtype :{}", table_name,
                                                     command->subtype)
                                             .c_str());
                    throw intarkdb::Exception(ExceptionType::BINDER,
                                              fmt::format("Alter table {} not exists!", table_name));
                }
                auto old_col = table_info->GetColumnByName(command->name);
                if (old_col == NULL) {
                    throw intarkdb::Exception(ExceptionType::BINDER,
                                              fmt::format("Alter column {} not exists!", command->name));
                }
                Column col(*old_col);
                // TODO: 优化这个函数
#ifdef ENABLE_PG_QUERY
                col.UpdateColumnDefinition(cdef);
#endif
                if (stmt->relkind != duckdb_libpgquery::PG_OBJECT_TABLE) {
                    throw intarkdb::Exception(ExceptionType::BINDER,
                                              "Alter column's type is only supported for tables");
                }
                if (table_info->IsTimeScale() && col.HasPrimaryOrUniqueKey()) {
                    throw intarkdb::Exception(ExceptionType::BINDER,
                                              "Primary key or unique key is not supported for time scale table");
                }
                auto expr = BindDefault(cdef->raw_default);
                if (expr) {
                    auto default_value = BoundExpressionToDefaultValue(*expr, col);
                    col.SetDefault(default_value);
                } else {
                    col.SetHasDefault(false);
                }
                al_cols.emplace_back(std::move(col));
                result->info = std::make_unique<AlterTableInfo>(GsAlterTableType::ALTABLE_MODIFY_COLUMN,
                                                                stmt->relation->relname, old_names, new_names, al_cols);
                break;
            }
            case duckdb_libpgquery::PG_AT_AttachPartition: {
                if (stmt->relkind != duckdb_libpgquery::PG_OBJECT_TABLE) {
                    throw intarkdb::Exception(ExceptionType::BINDER, "Adding partition is only supported for tables");
                }
                auto part_cmd = reinterpret_cast<duckdb_libpgquery::PGPartitionCmd *>(command->def);
                GS_LOG_RUN_INF("add partition name:%s\n", part_cmd->name->relname);
                if (strlen(part_cmd->name->relname) >= GS_TS_PART_NAME_BUFFER_SIZE) {
                    throw intarkdb::Exception(ExceptionType::BINDER, "add Partition name is too long");
                }
                auto table_info = catalog_.GetTable(schema_name, table_name);
                if (table_info == nullptr) {
                    GS_LOG_RUN_ERR("%s", fmt::format("Alter table {} not exists! command->subtype :{}", table_name,
                                                     command->subtype)
                                             .c_str());
                    throw intarkdb::Exception(ExceptionType::BINDER,
                                              fmt::format("Alter table {} not exists!", table_name));
                }
                if (!table_info->GetTableMetaInfo().parted) {
                    throw intarkdb::Exception(ExceptionType::BINDER,
                                              "Adding partition is only supported for partition table!");
                }
                const auto &part_desc = table_info->GetTableMetaInfo().part_table.desc;
                if (part_desc.interval.len == 0 || part_desc.interval.str == NULL) {
                    throw intarkdb::Exception(ExceptionType::BINDER, "only support interval partition table!");
                }
                std::vector<std::string> tokens;
                intarkdb::SplitString(std::string(part_cmd->name->relname), '_', tokens);
                if (tokens.size() != 2 || tokens[0] != table_name || !intarkdb::IsNumeric(tokens[1])) {
                    throw intarkdb::Exception(
                        ExceptionType::BINDER,
                        fmt::format("Partition name {} format error, <table name>_20230821 or <table name>_2023083116!",
                                    part_cmd->name->relname));
                }
                if (!intarkdb::IsTime(tokens[1])) {
                    throw intarkdb::Exception(ExceptionType::BINDER, "part name suffix is not time!");
                }
                if ((tokens[1].length() == PART_NAME_SUFFIX_DAY_TIME_LEN &&
                     part_desc.interval.str[part_desc.interval.len - 1] != GS_TIME_SUFFIX_DAY) ||
                    (tokens[1].length() == PART_NAME_SUFFIX_HOUR_TIME_LEN &&
                     part_desc.interval.str[part_desc.interval.len - 1] != GS_TIME_SUFFIX_HOUR)) {
                    throw intarkdb::Exception(ExceptionType::BINDER,
                                              "part high bound value is not mismatching with table!");
                }
                auto hpartbound = intarkdb::StringToTime((char *)tokens[1].c_str());  // seconds
                if (tokens[1].length() == PART_NAME_SUFFIX_HOUR_TIME_LEN) {
                    hpartbound += SECONDS_PER_HOUR;
                } else {
                    hpartbound += SECONDS_PER_DAY;
                }
                GS_LOG_RUN_INF("hpartbound:%ld s\n", hpartbound);
                result->hpartbound_ = std::to_string(hpartbound * MICROSECS_PER_SECOND_LL);  // microseconds
#ifdef _MSC_VER
                col_text_t part_name_instance;
                part_name_instance.str = part_cmd->name->relname;
                part_name_instance.len = static_cast<uint32>(strlen(part_cmd->name->relname));
                part_name_instance.assign = ASSIGN_TYPE_EQUAL;

                col_text_t hiboundval_instance;
                hiboundval_instance.str = (char *)result->hpartbound_.c_str();
                hiboundval_instance.len = (uint32)result->hpartbound_.length();
                hiboundval_instance.assign = ASSIGN_TYPE_EQUAL;

                exp_alt_table_part_t part_opt;
                part_opt.part_name = part_name_instance;
                part_opt.part_type = part_desc.parttype;
                part_opt.hiboundval = hiboundval_instance;
#else
                exp_alt_table_part_t part_opt{.part_name = {.str = part_cmd->name->relname,
                                                            .len = static_cast<uint32>(strlen(part_cmd->name->relname)),
                                                            .assign = ASSIGN_TYPE_EQUAL},
                                              .part_type = part_desc.parttype,
                                              .hiboundval = {.str = (char *)result->hpartbound_.c_str(),
                                                             .len = (uint32)result->hpartbound_.length(),
                                                             .assign = ASSIGN_TYPE_EQUAL}};
#endif
                result->info = std::make_unique<AlterTableInfo>(GsAlterTableType::ALTABLE_ADD_PARTITION,
                                                                stmt->relation->relname, std::move(part_opt));
                break;
            }
            case duckdb_libpgquery::PG_AT_DetachPartition: {
                if (stmt->relkind != duckdb_libpgquery::PG_OBJECT_TABLE) {
                    throw intarkdb::Exception(ExceptionType::BINDER, "Droping partition is only supported for tables!");
                }
                auto part_cmd = reinterpret_cast<duckdb_libpgquery::PGPartitionCmd *>(command->def);
                GS_LOG_RUN_INF("drop partition name:%s\n", part_cmd->name->relname);
                if (strlen(part_cmd->name->relname) >= GS_TS_PART_NAME_BUFFER_SIZE) {
                    throw intarkdb::Exception(ExceptionType::BINDER, "drop Partition name is too long");
                }
                auto table_info = catalog_.GetTable(schema_name, table_name);
                if (table_info == nullptr) {
                    GS_LOG_RUN_ERR("%s", fmt::format("Alter table {} not exists! command->subtype :{}", table_name,
                                                     command->subtype)
                                             .c_str());
                    throw intarkdb::Exception(ExceptionType::BINDER,
                                              fmt::format("Alter table {} not exists!", table_name));
                }
                if (!table_info->GetTableMetaInfo().parted) {
                    throw intarkdb::Exception(ExceptionType::BINDER,
                                              "Droping partition is only supported for partition table!");
                }
                const auto &part_desc = table_info->GetTableMetaInfo().part_table.desc;
                if (table_info->GetTablePartByName(part_cmd->name->relname) == NULL) {
                    throw intarkdb::Exception(ExceptionType::BINDER,
                                              fmt::format("part {} not exists!", part_cmd->name->relname));
                }
#ifdef _MSC_VER
                // 创建 part_name 实例
                col_text_t part_name_instance;
                part_name_instance.str = part_cmd->name->relname;
                part_name_instance.len = static_cast<uint32>(strlen(part_cmd->name->relname));
                part_name_instance.assign = ASSIGN_TYPE_EQUAL;

                // 创建 exp_alt_table_part_t 实例
                exp_alt_table_part_t part_opt;
                part_opt.part_name = part_name_instance;
                part_opt.part_type = part_desc.parttype;
#else
                exp_alt_table_part_t part_opt{.part_name = {.str = part_cmd->name->relname,
                                                            .len = static_cast<uint32>(strlen(part_cmd->name->relname)),
                                                            .assign = ASSIGN_TYPE_EQUAL},
                                              .part_type = part_desc.parttype};
#endif
                result->info = std::make_unique<AlterTableInfo>(GsAlterTableType::ALTABLE_DROP_PARTITION,
                                                                stmt->relation->relname, std::move(part_opt));
                break;
            }
            case duckdb_libpgquery::PG_AT_AddConstraint: {
                auto cstr_cmd = reinterpret_cast<duckdb_libpgquery::PGConstraint *>(command->def);
                BindMultiColConstraint(*cstr_cmd);
                std::vector<std::string> col_names;
                col_names.reserve(cstr_cmd->keys->length);
                auto cur = cstr_cmd->keys->head;
                while (cur != NULL) {
                    col_names.emplace_back(
                        reinterpret_cast<duckdb_libpgquery::PGColumnDef *>(cur->data.ptr_value)->colname);
                    cur = cur->next;
                }
                constraint_type_t new_type = TransformConstraintType(cstr_cmd->contype);

                if (table_info.IsTimeScale() && (new_type == constraint_type_t::CONS_TYPE_PRIMARY ||
                                                 new_type == constraint_type_t::CONS_TYPE_UNIQUE)) {
                    throw intarkdb::Exception(ExceptionType::BINDER,
                                              "Primary key or unique key is not supported for time scale table");
                }

                result->info =
                    std::make_unique<AlterTableInfo>(GsAlterTableType::ALTABLE_ADD_CONSTRAINT, stmt->relation->relname,
                                                     cstr_cmd->conname, new_type, std::move(col_names));
                break;
            }
            case duckdb_libpgquery::PG_AT_SetNotNull:
            case duckdb_libpgquery::PG_AT_DropNotNull:
            case duckdb_libpgquery::PG_AT_DropConstraint:
            default:
                throw intarkdb::Exception(ExceptionType::BINDER,
                                          fmt::format("ALTER TABLE option {} not supported yet!", command->subtype));
        }
    }

    return result;
}

auto Binder::BindRename(duckdb_libpgquery::PGRenameStmt *stmt) -> std::unique_ptr<AlterStatement> {
    auto schema_name = stmt->relation->schemaname ? std::string(stmt->relation->schemaname) : user_;
    auto table_name = std::string(stmt->relation->relname);
    auto result = std::make_unique<AlterStatement>(schema_name, table_name);
    std::vector<Column> al_cols;
    std::vector<col_text_t> old_names;
    std::vector<col_text_t> new_names;

    auto table = BindBaseTableRef(schema_name, table_name, std::nullopt);
    auto metaInfo = table->GetTableInfo().GetTableMetaInfo();
    if (metaInfo.space_id != SQL_SPACE_TYPE_USERS) {
        throw std::runtime_error("Cannot rename system table : " + std::string(metaInfo.name));
    }

    // first we check the type of ALTER
    switch (stmt->renameType) {
        case duckdb_libpgquery::PG_OBJECT_COLUMN: {
            // change column name
            // get the old name and the new name
            if (strlen(stmt->newname) >= GS_MAX_NAME_LEN) {
                throw std::runtime_error(
                    fmt::format("column name is too long, max length:{}", GS_MAX_NAME_LEN));
            }
            BindName(stmt->newname);
#ifdef _MSC_VER
            col_text_t old_name_instance;
            old_name_instance.str = stmt->subname;
            old_name_instance.len = static_cast<uint32>(strlen(stmt->subname));
            old_name_instance.assign = ASSIGN_TYPE_EQUAL;
            old_names.emplace_back(old_name_instance);

            col_text_t new_name_instance;
            new_name_instance.str = stmt->newname;
            new_name_instance.len = static_cast<uint32>(strlen(stmt->newname));
            new_name_instance.assign = ASSIGN_TYPE_EQUAL;
            new_names.emplace_back(new_name_instance);
#else
            old_names.emplace_back(col_text_t{
                .str = stmt->subname, .len = static_cast<uint32>(strlen(stmt->subname)), .assign = ASSIGN_TYPE_EQUAL});
            new_names.emplace_back(col_text_t{
                .str = stmt->newname, .len = static_cast<uint32>(strlen(stmt->newname)), .assign = ASSIGN_TYPE_EQUAL});
#endif
            result->info =
                std::make_unique<AlterTableInfo>(GsAlterTableType::ALTABLE_RENAME_COLUMN, stmt->relation->relname,
                                                 old_names, new_names, std::move(al_cols));
            break;
        }
        case duckdb_libpgquery::PG_OBJECT_TABLE: {
            // change table name
            if (strlen(stmt->newname) >= GS_MAX_NAME_LEN) {
                throw std::runtime_error(fmt::format("table name is too long, max length:{}", GS_MAX_NAME_LEN));
            }
            BindName(stmt->newname);
#ifdef _MSC_VER
            col_text_t new_name_instance;
            new_name_instance.str = stmt->newname;
            new_name_instance.len = static_cast<uint32>(strlen(stmt->newname));
            new_name_instance.assign = ASSIGN_TYPE_EQUAL;

            exp_alt_table_prop_t altable;
            altable.new_name = new_name_instance;
#else
            exp_alt_table_prop_t altable{.new_name = {.str = stmt->newname,
                                                      .len = static_cast<uint32>(strlen(stmt->newname)),
                                                      .assign = ASSIGN_TYPE_EQUAL}};
#endif
            result->info = std::make_unique<AlterTableInfo>(GsAlterTableType::ALTABLE_RENAME_TABLE,
                                                            stmt->relation->relname, std::move(altable));
            break;
        }
        case duckdb_libpgquery::PG_OBJECT_VIEW:
        case duckdb_libpgquery::PG_OBJECT_DATABASE:
        default:
            throw intarkdb::Exception(ExceptionType::BINDER,
                                      fmt::format("Schema element {} not supported yet!", stmt->renameType));
    }

    return result;
}
