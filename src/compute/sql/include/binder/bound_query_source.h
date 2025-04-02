/*
 * 版权所有 (c) GBA-NCTI-ISDC 2022-2024
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
 * bound_query_source.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/binder/bound_query_source.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <fmt/format.h>

#include <cstdint>
#include <string>
#include <string_view>

#include "storage/gstor/gstor_executor.h"

enum class DataSourceType : uint8_t {
    INVALID = 0,
    DUAL = 1,             // 空 from 子句( placeholder ,like insert statement)
    BASE_TABLE = 2,       // 普通表
    JOIN_RESULT = 3,      // join的中间结果
    SUBQUERY_RESULT = 4,  // 子查询
    VALUE_CLAUSE = 5,     // value子句
};

auto DataSourceTypeToString(DataSourceType type) -> std::string_view;

class BoundQuerySource {
public:
    explicit BoundQuerySource(DataSourceType type) : type_(type) {}
    BoundQuerySource() = default;
    virtual ~BoundQuerySource() = default;

    bool IsInvalid() const { return type_ == DataSourceType::INVALID; }

    DataSourceType Type() const { return type_; }

    void SetDictType(exp_dict_type_t dict_type) { dict_type_ = dict_type; }
    exp_dict_type_t DictType() { return dict_type_; }

    virtual std::string ToString() const { return "BoundQuerySource"; }

private:
    DataSourceType type_{DataSourceType::INVALID};
    exp_dict_type_t dict_type_{en_exp_dict_type::DIC_TYPE_UNKNOWN};
};
