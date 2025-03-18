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
 * column.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/catalog/column.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "catalog/column.h"

#include <fmt/format.h>

#include "binder/transform_typename.h"
#include "catalog/table_info.h"
#include "common/default_value.h"
#include "common/null_check_ptr.h"
#include "common/string_util.h"

Column::Column(const Column& other) {
    name_ = other.name_;
    comment_ = other.comment_;
    default_value_ = other.default_value_;

    def_ = other.def_;
    def_.name.str = (char*)name_.c_str();
    def_.name.len = name_.length();

    def_.comment.str = (char*)comment_.c_str();
    def_.comment.len = comment_.length();

    def_.default_val.str = (char*)default_value_.data();
    def_.default_val.len = default_value_.size();
}

Column::Column(Column&& other) noexcept {
    name_ = std::move(other.name_);
    comment_ = std::move(other.comment_);
    default_value_ = std::move(other.default_value_);

    def_ = other.def_;

    def_.name.str = (char*)name_.c_str();
    def_.name.len = name_.length();

    def_.comment.str = (char*)comment_.c_str();
    def_.comment.len = comment_.length();

    def_.default_val.str = (char*)default_value_.data();
    def_.default_val.len = default_value_.size();

    other.def_ = {};
}

Column& Column::operator=(const Column& other) {
    if (this != &other) {
        def_ = other.def_;
        name_ = other.name_;
        comment_ = other.comment_;
        default_value_ = other.default_value_;

        def_.name.str = (char*)name_.c_str();
        def_.name.len = name_.length();

        def_.comment.str = (char*)comment_.c_str();
        def_.comment.len = comment_.length();

        def_.default_val.str = (char*)default_value_.data();
        def_.default_val.len = default_value_.size();
    }
    return *this;
}

Column& Column::operator=(Column&& other) noexcept {
    def_ = other.def_;
    name_ = std::move(other.name_);
    comment_ = std::move(other.comment_);
    default_value_ = std::move(other.default_value_);

    def_.name.str = (char*)name_.c_str();
    def_.name.len = name_.length();

    def_.comment.str = (char*)comment_.c_str();
    def_.comment.len = comment_.length();

    def_.default_val.str = (char*)default_value_.data();
    def_.default_val.len = default_value_.size();

    other.def_ = {};
    return *this;
}

Column::Column(const exp_column_def_t& col) : def_(col) {
    if (def_.name.str != nullptr && def_.name.len > 0) {
        name_ = std::string(def_.name.str);
        def_.name.str = (char*)name_.c_str();
        def_.name.len = name_.length();
    }

    if (def_.default_val.str != nullptr && def_.default_val.len > 0) {
        default_value_ = std::vector<uint8_t>(def_.default_val.str, def_.default_val.str + def_.default_val.len);
        def_.default_val.str = (char*)default_value_.data();
        def_.default_val.len = default_value_.size();
    }

    if (def_.comment.str != nullptr && def_.comment.len > 0) {
        comment_ = std::string(def_.comment.str);
        def_.comment.str = (char*)comment_.c_str();
        def_.comment.len = comment_.length();
    }
}

Column::Column(const std::string& name, const exp_column_def_t& col) : name_(name), def_(col) {
    def_.name.str = (char*)name_.c_str();
    def_.name.len = name_.length();

    if (def_.default_val.str != nullptr && def_.default_val.len > 0) {
        default_value_ = std::vector<uint8_t>(def_.default_val.str, def_.default_val.str + def_.default_val.len);
        def_.default_val.str = (char*)default_value_.data();
        def_.default_val.len = default_value_.size();
    }

    if (def_.comment.str != nullptr && def_.comment.len > 0) {
        comment_ = std::string(def_.comment.str);
        def_.comment.str = (char*)comment_.c_str();
        def_.comment.len = comment_.length();
    }
}

Column::~Column() {}

// 列宽字节数
uint32_t Column::GetSize() const { return def_.size; }

const std::string& Column::Name() const { return name_; }

const std::string& Column::NameWithoutPrefix() const {
    if (name_without_prefix_.empty()) {
        std::string_view namesv_ = name_;
        auto pos = namesv_.find(".");
        if (pos != std::string::npos) {
            std::string_view tablename = namesv_.substr(0, pos);
            if (!IsValidTableName(tablename)) {
                name_without_prefix_ = name_;
            } else {
                // substr the rest string
                name_without_prefix_ = name_.substr(pos + 1, name_.length() - pos - 1);
            }
        } else {
            name_without_prefix_ = name_;
        }
    }
    return name_without_prefix_;
}

const exp_column_def_t& Column::GetRaw() const { return def_; }

uint16_t Column::Slot() const { return def_.col_slot; }

void Column::SetHasDefault(bool is_default) { def_.is_default = is_default; }

void Column::SetDefault(const std::vector<uint8_t>& default_value) {
    default_value_ = default_value;
    def_.default_val.str = (char*)default_value_.data();
    def_.default_val.len = default_value_.size();
    def_.default_val.assign = ASSIGN_TYPE_EQUAL;
    def_.is_default = true;
}

void Column::SetComment(const std::string& comment) {
    comment_ = comment;

    def_.comment.str = (char*)comment_.c_str();
    def_.comment.len = comment_.length();
    def_.comment.assign = ASSIGN_TYPE_EQUAL;

    def_.is_comment = true;
}

#ifdef ENABLE_PG_QUERY
void Column::UpdateColumnDefinition(duckdb_libpgquery::PGColumnDef* cdef) {
    if (cdef == nullptr) {
        return;
    }
    if (cdef->collClause != nullptr) {
        // 表示列的排序规则，先不支持
        throw intarkdb::Exception(ExceptionType::BINDER, "col clause on column is not supported");
    }
    if (cdef->typeName == nullptr) {
        throw intarkdb::Exception(ExceptionType::BINDER, "column type is null");
    }
    LogicalType gstor_type = TransformTypeName(*(cdef->typeName));
    def_.col_type = gstor_type.TypeId();
    def_.size = gstor_type.Length();
    def_.precision = gstor_type.Precision();
    def_.scale = gstor_type.Scale();
    if (cdef->constraints != nullptr) {
        for (auto constr = cdef->constraints->head; constr != nullptr; constr = constr->next) {
            auto constraint = NullCheckPtrCast<duckdb_libpgquery::PGConstraint>(constr->data.ptr_value);
            switch (constraint->contype) {
                case duckdb_libpgquery::PG_CONSTR_NOTNULL:
                    // default is null
                    def_.nullable = GS_FALSE;
                    break;
                case duckdb_libpgquery::PG_CONSTR_NULL:
                    def_.nullable = GS_TRUE;
                    break;
                case duckdb_libpgquery::PG_CONSTR_PRIMARY:
                    def_.is_primary = GS_TRUE;
                    break;
                case duckdb_libpgquery::PG_CONSTR_UNIQUE:
                    def_.is_unique = GS_TRUE;
                    break;
                default:
                    // static_cast 的作用是 fix 编译器的 warning
                    throw intarkdb::Exception(
                        ExceptionType::BINDER,
                        fmt::format("Constraint {} is not implemented", static_cast<int32_t>(constraint->contype)));
            }
        }
    }
}
#endif

auto Column::TransformColumnVecToDefs(const std::vector<Column>& columns) -> std::vector<exp_column_def_t> {
    std::vector<exp_column_def_t> column_defs;
    column_defs.reserve(columns.size());
    for (auto& col : columns) {
        column_defs.push_back(col.GetRaw());
    }
    return column_defs;
}
