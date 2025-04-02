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
 * column.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/catalog/column.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <string>
#include <vector>

#include "common/winapi.h"
#ifdef ENABLE_PG_QUERY
#include "nodes/parsenodes.hpp"
#endif
#include "storage/gstor/gstor_executor.h"
#include "type/type_id.h"
#include "type/value.h"

// 列定义
class Column {
   public:
    EXPORT_API Column(const Column& other);
    EXPORT_API Column& operator=(const Column& other);
    EXPORT_API explicit Column(const exp_column_def_t& col);
    EXPORT_API explicit Column(const std::string& name, const exp_column_def_t& col);
    EXPORT_API Column(Column&& other) noexcept;
    EXPORT_API Column& operator=(Column&& other) noexcept;
    EXPORT_API ~Column();

    // 列宽字节数
    EXPORT_API uint32_t GetSize() const;

    EXPORT_API const std::string& Name() const;
    EXPORT_API const std::string& NameWithoutPrefix() const;

    void SetName(const std::string& name) {
        name_ = name;
        def_.name.str = (char*)name_.c_str();
        def_.name.len = name_.length();
        name_without_prefix_ = "";
    }

    void SetName(std::string&& name) {
        name_ = std::move(name);
        def_.name.str = (char*)name_.c_str();
        def_.name.len = name_.length();
        name_without_prefix_ = "";
    }

    // FIXME: 是否可以去掉 ?  不泄露抽象的内部实现
    const exp_column_def_t& GetRaw() const;

    EXPORT_API uint16_t Slot() const;

    EXPORT_API LogicalType GetLogicalType() const { return intarkdb::NewLogicalType(def_); }

    void SetHasDefault(bool is_default);

    void SetHasComment(bool is_comment) { def_.is_comment = is_comment ? GS_TRUE : GS_FALSE; }

    bool HasDefault() const { return def_.is_default == GS_TRUE; }

    void SetDefault(const std::vector<uint8_t>& default_value);

    void SetComment(const std::string& comment);

    // 临时使用，后续需要去掉
    void SetCrud(char* str, uint32 len, assign_type_t assign) {
        def_.crud_value.str = str;
        def_.crud_value.len = len;
        def_.crud_value.assign = assign;
    }

    void SetNullable(bool nullable) { def_.nullable = nullable ? GS_TRUE : GS_FALSE; }

    void SetIsPrimaryKey(bool primary_key) { def_.is_primary = primary_key ? GS_TRUE : GS_FALSE; }

    void SetIsUnique(bool unique) { def_.is_unique = unique ? GS_TRUE : GS_FALSE; }

    bool HasPrimaryOrUniqueKey() { return def_.is_unique || def_.is_primary; }

    void SetColType(LogicalType type) {
        def_.col_type = type.TypeId();
        def_.size = type.Length();
        def_.scale = type.Scale();
        def_.precision = type.Precision();
    }

    void SetSlot(uint16_t slot) { def_.col_slot = slot; }

    bool IsAutoIncrement() const { return def_.is_autoincrement == GS_TRUE; }
    auto SetAutoIncrement(bool auto_increment) -> void { def_.is_autoincrement = auto_increment ? GS_TRUE : GS_FALSE; }

#ifdef ENABLE_PG_QUERY
    void UpdateColumnDefinition(duckdb_libpgquery::PGColumnDef* cdef);
#endif

    static auto TransformColumnVecToDefs(const std::vector<Column>& columns) -> std::vector<exp_column_def_t>;

   private:
    std::string name_;
    mutable std::string name_without_prefix_{""};
    exp_column_def_t def_;
    std::string comment_;
    std::vector<uint8_t> default_value_;
};
