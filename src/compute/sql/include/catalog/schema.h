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
 * schema.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/catalog/schema.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <fmt/format.h>

#include <memory>
#include <stdexcept>
#include <vector>

#include "catalog/column.h"
#include "catalog/column_info.h"


constexpr uint32_t INVALID_COLUMN_INDEX = UINT32_MAX;

class Schema {
   public:
    Schema() = default;
    // EXPORT_API for test
    EXPORT_API explicit Schema(const std::string& table_name, const std::vector<Column>& columns);
    EXPORT_API explicit Schema(std::vector<SchemaColumnInfo>&& columns);

    // 拷贝schema中的某几列
    static Schema CopySchema(const Schema* from, const std::vector<uint32_t>& attrs) {
        std::vector<SchemaColumnInfo> cols;
        cols.reserve(attrs.size());
        if (from) {
            for (const auto i : attrs) {
                cols.emplace_back(from->columns_[i]);
            }
        }
        return Schema{std::move(cols)};
    }

    // TODO: 可能需要重写
    Schema Select(const std::vector<std::string>& projs) const {
        Schema schema;
        for (const auto& proj : projs) {
            for (const auto& col : columns_) {
                if (col.col_name.back() == proj) {  // 只取列名，但是proj可能包含表名
                    schema.columns_.push_back(col);
                }
            }
        }
        return schema;
    }

    // for 兼容
    EXPORT_API auto GetColumns() const -> std::vector<Column>;

    EXPORT_API auto GetColumn(const uint32_t idx) const -> Column;

    uint32_t GetIdxByName(const std::vector<std::string>& name) const {
        if (auto idx = GetIdxByNameWithoutException(name); idx != INVALID_COLUMN_INDEX) {
            return idx;
        }
        throw intarkdb::Exception(ExceptionType::PLANNER,
                                  fmt::format("get column idx by name {} failed", fmt::join(name, ".")));
    }

    uint32_t GetIdxByNameWithoutException(const std::vector<std::string>& name) const {
        for (uint32_t i = 0; i < columns_.size(); ++i) {
            if (cmp_col_name(name, columns_[i].col_name)) {
                return i;
            }
            if (cmp_col_alias(name, columns_[i].alias)) {
                return i;
            }
        }
        return INVALID_COLUMN_INDEX;
    }

    EXPORT_API static auto RenameSchemaColumnIfExistSameNameColumns(const Schema&) -> Schema;

    EXPORT_API auto GetColumnInfos() const -> const std::vector<SchemaColumnInfo>& { return columns_; }

    EXPORT_API auto GetColumnInfoByIdx(size_t idx) const -> const SchemaColumnInfo&;

   private:
    static auto cmp_col_alias(const std::vector<std::string>& name, const std::string& alias) -> bool;

    static auto cmp_col_name(const std::vector<std::string>& name, const std::vector<std::string>& col_name) -> bool;

   private:
    // schema 所有列
    std::vector<SchemaColumnInfo> columns_;
};

using SchemaPtr = std::shared_ptr<const Schema>;
