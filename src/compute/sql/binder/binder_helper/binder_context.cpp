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
 * binder_context.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/binder/binder_context.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "binder/binder_context.h"

auto BinderContext::AddTableBinding(const std::string &table_name_or_alias, const std::vector<Column> &cols) -> void {
    std::vector<std::string> col_names;
    std::vector<LogicalType> col_types;
    for (auto &col : cols) {
        col_names.push_back(col.NameWithoutPrefix());
        col_types.push_back(col.GetLogicalType());
    }
    return AddTableBinding(table_name_or_alias, std::move(col_names), std::move(col_types));
}

auto BinderContext::AddTableBinding(const std::string &table_name_or_alias,
                                    const std::vector<SchemaColumnInfo> &col_infos) -> void {
    std::vector<std::string> col_names;
    std::vector<LogicalType> col_types;
    for (auto &col_info : col_infos) {
        col_names.push_back(col_info.col_name.back());  // 只保留列名
        col_types.push_back(col_info.col_type);
    }
    return AddTableBinding(table_name_or_alias, std::move(col_names), std::move(col_types));
}

auto BinderContext::AddTableBinding(const std::string &table_name_or_alias, std::vector<std::string> &&col_name,
                                    std::vector<LogicalType> &&col_type) -> void {
    auto binding_iter = table_bindings.find(table_name_or_alias);
    if (binding_iter != table_bindings.end()) {
        throw intarkdb::Exception(ExceptionType::BINDER, fmt::format("Duplicate table {} ", table_name_or_alias));
    }
    table_bindings_vec.emplace_back(table_name_or_alias, std::move(col_name), std::move(col_type));
    table_bindings[table_name_or_alias] = table_bindings_vec.size() - 1;
}

auto BinderContext::MergeTableBinding(BinderContext &other) -> void {
    for (auto &binding : other.table_bindings_vec) {
        std::string table_name = binding.GetTableName();
        auto binding_iter = table_bindings.find(table_name);
        if (binding_iter != table_bindings.end()) {
            throw intarkdb::Exception(ExceptionType::BINDER, fmt::format("Duplicate table {} ", table_name));
        }
        table_bindings_vec.emplace_back(std::move(binding));
        table_bindings[table_name] = table_bindings_vec.size() - 1;
    }
}

auto BinderContext::GetTableBinding(const std::string &table_name) const -> const TableBinding * {
    auto it = table_bindings.find(table_name);
    if (it == table_bindings.end() || (size_t)it->second >= table_bindings_vec.size()) {
        return nullptr;
    }
    return &table_bindings_vec[it->second];
}

auto BinderContext::BuildProjectionAndAlias(const std::vector<std::unique_ptr<BoundExpression>> &select_list) -> void {
    for (size_t i = 0; i < select_list.size(); i++) {
        auto &expr = select_list[i];
        if (expr->Type() == ExpressionType::ALIAS) {
            const auto &name = expr->GetName();
            const auto &alias = name.back();
            auto alias_iter = alias_binding.find(alias);
            if (alias_iter == alias_binding.end()) {
                alias_binding[alias] = {(int)i, expr->ReturnType()};
            }
        }
    }
}

auto BinderContext::FindBindingByColumnName(const std::string &col_name) -> std::set<std::string> {
    std::set<std::string> result;
    for (auto &table_binding : table_bindings_vec) {
        const auto &col_names = table_binding.GetColNames();
        for (auto &col : col_names) {
            if (col == col_name) {
                result.insert(table_binding.GetTableName());
            }
        }
    }
    return result;
}
