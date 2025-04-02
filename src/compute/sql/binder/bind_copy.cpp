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
 * bind_copy.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/binder/bind_copy.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "binder/binder.h"
#include "binder/expressions/bound_star.h"
#include "binder/statement/select_statement.h"
#include "binder/table_ref/bound_base_table.h"
#include "common/gstor_exception.h"
#include "common/string_util.h"
#include "planner/planner.h"
#include "type/value.h"

static auto IsValidOptionType(duckdb_libpgquery::PGNodeTag type) -> bool {
    switch (type) {
        case duckdb_libpgquery::T_PGList:
        case duckdb_libpgquery::T_PGAStar:
            return false;
        default:
            return true;
    }
}

// 获取文件格式, 如果没有指定文件格式，则默认为csv, 标准输入统一默认为csv
static auto TransformFileFormat(const std::string &file_path) -> std::string {
    std::string file_format = "csv";
    if (intarkdb::StringUtil::EndsWith(file_path, ".parquet")) {
        file_format = "parquet";
    } else if (intarkdb::StringUtil::EndsWith(file_path, ".json") ||
               intarkdb::StringUtil::EndsWith(file_path, ".ndjson")) {
        file_format = "json";
    }
    return file_format;
}

// 获取文件路径，如果没有指定文件路径，则默认为标准输入输出
static auto TransformFilePath(const duckdb_libpgquery::PGCopyStmt &stmt) -> std::string {
    std::string file_path = "";
    if (!stmt.filename) {
        file_path = stmt.is_from ? "/dev/stdin" : "/dev/stdout";
    } else {
        file_path = stmt.filename;
    }
    return file_path;
}

// 目前只支持csv格式
static auto IsValidCopyFormat(const std::string &format) -> bool { return format == "csv"; }

static auto IsDuplicateOption(const std::string &option_name,
                              const std::unordered_map<std::string, std::vector<Value>> &options) -> bool {
    return options.find(option_name) != options.end();
}

static auto IsSupportedOption(const std::string &option_name) -> bool {
    return option_name == "format" || option_name == "delimiter";
}

auto Binder::BindCopyOptionWithNoArg(const std::string &option_name, CopyInfo &info) -> void {
    if (option_name == "format") {
        throw intarkdb::Exception(ExceptionType::BINDER, "format option should have an argument");
    }
    // default
    info.options[option_name] = std::vector<Value>();
}

auto Binder::BindCopyOptionWithArg(const std::string &option_name, CopyInfo &info,
                                   duckdb_libpgquery::PGDefElem *def_elem) -> void {
    // check valid option type
    if (!IsValidOptionType(def_elem->arg->type)) {
        throw intarkdb::Exception(ExceptionType::BINDER, "Unsupported option expression");
    }
    auto arg_val = BindValue(reinterpret_cast<duckdb_libpgquery::PGValue *>(def_elem->arg));
    const auto &bound_constant = static_cast<BoundConstant &>(*arg_val);
    info.options[option_name].push_back(bound_constant.Val());

    // format option check
    if (option_name == "format") {
        info.format = intarkdb::StringUtil::Lower(bound_constant.Val().ToString());
        if (!IsValidCopyFormat(info.format)) {
            throw intarkdb::Exception(ExceptionType::BINDER, "Unsupported file format: " + info.format);
        }
    }
}

auto Binder::BindCopyOptions(CopyInfo &info, duckdb_libpgquery::PGList *options) -> void {
    if (!options) {
        return;
    }
    duckdb_libpgquery::PGListCell *cell = nullptr;

    // iterate over each option
    for_each_cell(cell, options->head) {
        auto *def_elem = reinterpret_cast<duckdb_libpgquery::PGDefElem *>(cell->data.ptr_value);
        if (!def_elem) {
            throw intarkdb::Exception(ExceptionType::BINDER, "Unexpected option expression");
        }
        auto option_name_lower = intarkdb::StringUtil::Lower(def_elem->defname);

        if (!IsSupportedOption(option_name_lower)) {
            throw intarkdb::Exception(ExceptionType::BINDER, "Unsupported option: " + option_name_lower);
        }

        // check if the option already exists
        if (IsDuplicateOption(option_name_lower, info.options)) {
            throw intarkdb::Exception(ExceptionType::BINDER, "duplicate option");
        }

        if (!def_elem->arg) {
            BindCopyOptionWithNoArg(option_name_lower, info);
        } else {
            BindCopyOptionWithArg(option_name_lower, info, def_elem);
        }
    }
}

auto Binder::BindCopy(duckdb_libpgquery::PGCopyStmt *stmt) -> std::unique_ptr<CopyStatement> {
    auto result = std::make_unique<CopyStatement>();
    auto &info = *result->info;
    info.is_from = stmt->is_from;
    info.file_path = TransformFilePath(*stmt);
    info.format = TransformFileFormat(info.file_path);

    if (!IsValidCopyFormat(info.format)) {
        throw intarkdb::Exception(ExceptionType::NOT_IMPLEMENTED, "Unsupported file format: " + info.format);
    }
    BindCopyOptions(info, stmt->options);
    if (stmt->is_from) {  // copy from
        BindCopyFrom(*stmt, info);
    } else {  // copy to
        result->select_statement = BindCopyTo(*stmt, info);
    }
    return result;
}

auto Binder::BindCopySelectList(const duckdb_libpgquery::PGCopyStmt &stmt, const BoundBaseTable &table)
    -> std::vector<std::unique_ptr<BoundExpression>> {
    std::vector<std::unique_ptr<BoundExpression>> select_list;
    if (stmt.attlist) {
        select_list = BindSelectListExprs(stmt.attlist);
    } else {
        select_list = BindAllColumnRefs(nullptr);
    }
    return select_list;
}

auto Binder::BindCopyFrom(const duckdb_libpgquery::PGCopyStmt &stmt, CopyInfo &info) -> void {
    if (stmt.relation) {
        auto table = BindRangeVarTableRef(*stmt.relation);
        info.table_ref = std::unique_ptr<BoundBaseTable>(static_cast<BoundBaseTable *>(table.release()));
        info.select_list = BindCopySelectList(stmt, *info.table_ref);
    } else {
        throw intarkdb::Exception(ExceptionType::BINDER, "unspecific copy table name");
    }
}

auto Binder::BindCopyTo(const duckdb_libpgquery::PGCopyStmt &stmt, CopyInfo &info) -> std::unique_ptr<SelectStatement> {
    std::unique_ptr<SelectStatement> select_statement;
    if (stmt.relation) {
        select_statement = std::make_unique<SelectStatement>();
        select_statement->table_ref = BindRangeVarTableRef(*stmt.relation);
        auto &table_ref = static_cast<BoundBaseTable &>(*select_statement->table_ref);
        select_statement->select_expr_list = BindCopySelectList(stmt, table_ref);
    } else if (stmt.query) {
        select_statement = BindSelectStmt((duckdb_libpgquery::PGSelectStmt *)stmt.query);
    } else {
        throw intarkdb::Exception(ExceptionType::BINDER, "unspecific copy table name");
    }
    return select_statement;
}
