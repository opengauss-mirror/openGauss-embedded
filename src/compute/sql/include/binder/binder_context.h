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
 * binder_context.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/binder/binder_context.h
 *
 * -------------------------------------------------------------------------
 */

#pragma once

#include <stdint.h>

#include <algorithm>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "binder/bound_expression.h"
#include "type/type_id.h"

// 实体表的列绑定
class TableBinding {
    std::string table_name_or_alias_;
    std::vector<std::string> col_names_;
    std::vector<LogicalType> col_types_;

   public:
    TableBinding(const std::string &table_name_or_alias, std::vector<std::string> &&col_names,
                 std::vector<LogicalType> &&col_types)
        : table_name_or_alias_(table_name_or_alias),
          col_names_(std::move(col_names)),
          col_types_(std::move(col_types)) {}
    const std::string &GetTableName() const { return table_name_or_alias_; }
    const std::vector<std::string> &GetColNames() const { return col_names_; }
    const std::vector<LogicalType> &GetColTypes() const { return col_types_; }

    bool CheckColumnExist(const std::string &col_name) const {
        return std::find(col_names_.begin(), col_names_.end(), col_name) != col_names_.end();
    }
};

struct AliasIdx {
    int idx;
    LogicalType type;
};

struct BinderContext {
    std::unordered_map<std::string, AliasIdx> alias_binding;  // alias -> idx
    std::vector<std::unique_ptr<BoundExpression>> correlated_columns;
    std::unordered_map<std::string, int> table_bindings;  // table_name -> table_binding
    std::vector<TableBinding> table_bindings_vec;
    std::set<std::string> using_column_names;
    std::string primary_using_table;

    auto AddTableBinding(const std::string &table_name_or_alias, const std::vector<Column> &cols) -> void;

    auto AddTableBinding(const std::string &table_name_or_alias, const std::vector<SchemaColumnInfo> &col_infos)
        -> void;

    auto AddTableBinding(const std::string &table_name_or_alias, std::vector<std::string> &&col_name,
                         std::vector<LogicalType> &&col_type) -> void;

    auto MergeTableBinding(BinderContext &other) -> void;

    auto GetTableBinding(const std::string &table_name) const -> const TableBinding *;

    auto FindBindingByColumnName(const std::string &col_name) -> std::set<std::string>;

    auto GetAllTableBindings() const -> const std::vector<TableBinding> & { return table_bindings_vec; }

    auto BuildProjectionAndAlias(const std::vector<std::unique_ptr<BoundExpression>> &select_list) -> void;

    auto AddAliasBinding(const std::string &alias, int idx, LogicalType type) -> void {
        alias_binding[alias] = {idx, type};
    }
};
