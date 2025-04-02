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
 * bound_column_def.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/binder/expressions/bound_column_def.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <fmt/core.h>
#include <fmt/format.h>
#include <fmt/ranges.h>

#include <algorithm>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "binder/bound_expression.h"
#include "common/gstor_exception.h"
#include "type/type_id.h"

class BoundColumnRef : public BoundExpression {
   public:
    explicit BoundColumnRef(std::vector<std::string> col_name, LogicalType type, int outer_level = 0)
        : BoundExpression(ExpressionType::COLUMN_REF),
          col_name_(std::move(col_name)),
          return_type_(type),
          outer_level_(outer_level) {}

    std::string ToString() const override { return col_name_.back(); }

    virtual auto GetName() const -> std::vector<std::string> override { return col_name_; }

    virtual bool HasAggregation() const override { return false; }

    virtual bool HasSubQuery() const override { return false; }

    virtual bool HasParameter() const override { return false; }

    virtual bool HasWindow() const override { return false; }


    virtual auto Equals(const BoundExpression& other) const -> bool override {
        if (other.Type() != ExpressionType::COLUMN_REF) {
            return false;
        }
        const auto& other_col = static_cast<const BoundColumnRef&>(other);
        return col_name_ == other_col.col_name_ && return_type_.TypeId() == other_col.return_type_.TypeId() &&
            outer_level_ == other_col.outer_level_;
    }

    bool IsQualified() const { return col_name_.size() > 1; }

    const std::string& GetColName() const { return col_name_.back(); }

    const std::string& GetTableName() const {
        if (col_name_.size() > 1) {
            return col_name_.front();
        }
        throw intarkdb::Exception(ExceptionType::BINDER, "no table name");
    }

    const std::vector<std::string>& Name() const { return col_name_; }

    void SetName(std::vector<std::string>&& names) { col_name_ = std::move(names); }


    virtual LogicalType ReturnType() const override { return return_type_; }

    virtual hash_t Hash() const override {
        hash_t h = 0;
        for (auto& col : col_name_) {
            h = HashUtil::CombineHash(h, HashUtil::HashBytes(col.c_str(), col.length()));
        }
        return h;
    }

    virtual std::unique_ptr<BoundExpression> Copy() const override {
        return std::make_unique<BoundColumnRef>(col_name_, return_type_, outer_level_);
    }

    bool IsOuter() const { return outer_level_ > 0; }
    void SetOuterFlag(int outer_level ) { outer_level_ = outer_level; }
    void AddLevel() { outer_level_++; }
    int GetLevel() const { return outer_level_; }

   private:
    std::vector<std::string> col_name_;  // [table_name,column_name]
    LogicalType return_type_;            // 列类型
    int outer_level_{0};
};
