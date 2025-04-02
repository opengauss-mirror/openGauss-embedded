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
 * transform_typename.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/binder/transform_typename.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "binder/transform_typename.h"

#include <fmt/format.h>

#include <algorithm>

#include "common/exception.h"
#include "common/null_check_ptr.h"
#include "common/string_util.h"
#include "common/util.h"
#include "nodes/parsenodes.hpp"

using namespace intarkdb;

LogicalType GetDefaultType(std::string_view name) {
    int32_t width = 0;
    auto type = GStorDataType::GS_TYPE_UNKNOWN;
    for (uint32_t index = 0; internal_types[index].name != nullptr; index++) {
        if (intarkdb::StringUtil::IsEqualIgnoreCase(internal_types[index].name, name)) {
            type = internal_types[index].type;
            width = internal_types[index].width;  // 获取类型的宽度
            break;
        }
    }
    auto t = intarkdb::NewLogicalType(type);
    t.width = width;
    return t;
}

LogicalType TransformTypeName(duckdb_libpgquery::PGTypeName& type_name) {
    if (type_name.type != duckdb_libpgquery::T_PGTypeName) {
        throw intarkdb::Exception(ExceptionType::PARSER, "Expected a type");
    }

    std::string_view name =
        NullCheckPtrCast<duckdb_libpgquery::PGValue>(type_name.names->tail->data.ptr_value)->val.str;

    // transform it to the SQL type
    auto result_type = GetDefaultType(name);
    if (result_type.TypeId() == GStorDataType::GS_TYPE_UNKNOWN) {
        throw intarkdb::Exception(ExceptionType::BINDER, fmt::format("Unknown type : {}", name));
    }

    // check any modifiers
    int modifier_idx = 0;
    if (type_name.typmods) {
        for (auto node = type_name.typmods->head; node != nullptr; node = node->next) {
            auto& const_val = *NullCheckPtrCast<duckdb_libpgquery::PGAConst>(node->data.ptr_value);
            if (const_val.type != duckdb_libpgquery::T_PGAConst ||
                const_val.val.type != duckdb_libpgquery::T_PGInteger) {  // e.g varchar(20) , decimal(10,2)
                throw intarkdb::Exception(ExceptionType::PARSER, "Expected an integer constant as type modifier");
            }
            if (const_val.val.val.ival < 0) {
                throw intarkdb::Exception(ExceptionType::PARSER, "Negative modifier not supported");
            }

            if (const_val.val.val.ival > COLUMN_VARCHAR_SIZE_DEFAULT) {
                throw intarkdb::Exception(
                    ExceptionType::PARSER,
                    fmt::format("column size/precision/scale out of range({})!", COLUMN_VARCHAR_SIZE_DEFAULT));
            }
            if (result_type.type == GStorDataType::GS_TYPE_VARCHAR) {
                result_type.length = const_val.val.val.ival;  // varchar(20)
                result_type.width = const_val.val.val.ival;
            } else if (IsDecimal(result_type.type)) {  // decimal(10,2)
                if (modifier_idx == 0) {
                    result_type.precision = const_val.val.val.ival;
                } else if (modifier_idx == 1) {
                    result_type.scale = const_val.val.val.ival;
                } else {
                    throw intarkdb::Exception("A maximum of two modifiers is supported");
                }
            }
            modifier_idx++;
        }
    }

    switch (result_type.type) {
        case GStorDataType::GS_TYPE_VARCHAR:
            if (modifier_idx == 0) {  // 没有填写长度
                result_type.length = COLUMN_VARCHAR_SIZE_DEFAULT;
                result_type.width = COLUMN_VARCHAR_SIZE_DEFAULT;
            }
            if (modifier_idx > 1) {
                throw intarkdb::Exception(ExceptionType::PARSER, "varchar only support one modifier");
            }
            if (result_type.length > COLUMN_VARCHAR_SIZE_DEFAULT) {
                throw intarkdb::Exception(
                    ExceptionType::PARSER,
                    fmt::format("column size/precision/scale out of range({})!", COLUMN_VARCHAR_SIZE_DEFAULT));
            }
            break;
        case GStorDataType::GS_TYPE_NUMBER:
        case GStorDataType::GS_TYPE_DECIMAL:
            if (modifier_idx == 0) {
                result_type.precision = DEFALUT_DECIMAL_PRECISION;
                result_type.scale = DEFALUT_DECIMAL_SCALE;
            }
            if (result_type.precision > DecimalPrecision::max || result_type.precision <= 0) {
                throw intarkdb::Exception(ExceptionType::PARSER,
                                          fmt::format("precision must be between 1 and {}!", DecimalPrecision::max));
            }
            if (result_type.scale > result_type.precision) {
                throw intarkdb::Exception(ExceptionType::PARSER, "scale cannot be bigger than precision");
            }
            break;

        default:
            if (modifier_idx > 0) {
                throw intarkdb::Exception(ExceptionType::PARSER,
                                          fmt::format("Type {} does not support any modifiers!", result_type));
            }
    }
    if (type_name.arrayBounds) {  // 不支持数组类型
        throw intarkdb::Exception(ExceptionType::PARSER, "Array type not supported!");
    }
    return result_type;
}
