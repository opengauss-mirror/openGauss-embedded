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
 * bound_base_table.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/binder/table_ref/bound_base_table.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <fmt/core.h>

#include <optional>
#include <string>

#include "binder/bound_query_source.h"
#include "catalog/table_info.h"

class BoundBaseTable : public BoundQuerySource {
   public:
    explicit BoundBaseTable(const std::string &schema_name, std::string table, std::optional<std::string> alias,
                               std::unique_ptr<TableInfo> table_info)
        : BoundQuerySource(DataSourceType::BASE_TABLE),
          schema_(schema_name),
          table_(std::move(table)),
          alias_(std::move(alias)),
          meta_(std::move(table_info)) {}

    const std::string& GetBoundTableName() const { return table_; }


    virtual std::string ToString() const override {
        if (alias_ == std::nullopt) {
            return fmt::format("BoundBaseTable {{ table={} }}", table_);
        }
        return fmt::format("BoundBaseTable {{ table={}, alias={} }}", table_, *alias_);
    }

    // 过期 deprecated
    const TableInfo& GetTableInfo() const { return *meta_; }

    const std::optional<std::string>& GetAlias() const { return alias_; }

    std::string GetTableNameOrAlias() const { return alias_.value_or(table_); }

    // 获取表类型
    exp_dict_type_t GetObjectType() const { return meta_->GetObjectType(); }

    uint32_t GetSpaceId() const { return meta_->GetSpaceId(); }

    // user
    std::string GetSchema() { return schema_; }
   private:
    // schema
    std::string schema_;
    // 表名
    std::string table_;

    // 表名别名
    std::optional<std::string> alias_;

    // 表元信息
    std::unique_ptr<TableInfo> meta_;
};
