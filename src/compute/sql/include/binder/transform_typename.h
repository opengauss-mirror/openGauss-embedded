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
* transform_typename.h
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/include/binder/transform_typename.h
*
* -------------------------------------------------------------------------
*/
#pragma once
#include "nodes/parsenodes.hpp"
#include "type/type_id.h"
#include "type/type_system.h"

struct DefaultType {
    const char* name;
    GStorDataType type;
    int width;
    std::string_view gs_type_name;
};



static const DefaultType internal_types[] = {
    {"int", GStorDataType::GS_TYPE_INTEGER, 4, "GS_TYPE_INTEGER"},
    {"int4", GStorDataType::GS_TYPE_INTEGER, 4, "GS_TYPE_INTEGER"},
    {"signed", GStorDataType::GS_TYPE_INTEGER, 4, "GS_TYPE_INTEGER"},
    {"integer", GStorDataType::GS_TYPE_INTEGER, 4, "GS_TYPE_INTEGER"},
    {"int32", GStorDataType::GS_TYPE_INTEGER, 4, "GS_TYPE_INTEGER"},
    {"mediumint", GStorDataType::GS_TYPE_INTEGER, 4, "GS_TYPE_INTEGER"},
    {"varchar", GStorDataType::GS_TYPE_VARCHAR, 0, "GS_TYPE_VARCHAR"},
    {"bpchar", GStorDataType::GS_TYPE_VARCHAR, 0, "GS_TYPE_VARCHAR"},  // 视作等价于varchar
    {"text", GStorDataType::GS_TYPE_VARCHAR, 0, "GS_TYPE_VARCHAR"},
    {"char", GStorDataType::GS_TYPE_VARCHAR, 0, "GS_TYPE_VARCHAR"},
    // {"nvarchar", GStorDataType::GS_TYPE_VARCHAR, "GS_TYPE_VARCHAR"},
    {"string", GStorDataType::GS_TYPE_VARCHAR, 65535,
     "GS_TYPE_STRING"},  // string 的长度是65535,宽字节会导致存储的字符个数变短
    // {"bytea", GStorDataType::GS_TYPE_BLOB, 0, "GS_TYPE_BLOB"},   // 暂不支持
    {"blob", GStorDataType::GS_TYPE_BLOB, 0, "GS_TYPE_BLOB"},
    {"clob", GStorDataType::GS_TYPE_CLOB, 0, "GS_TYPE_CLOB"},
    // {"varbinary", GStorDataType::GS_TYPE_VARBINARY, "GS_TYPE_VARBINARY"},
    {"binary", GStorDataType::GS_TYPE_BINARY, 0, "GS_TYPE_BINARY"},
    {"int8", GStorDataType::GS_TYPE_BIGINT, 8, "GS_TYPE_BIGINT"},
    {"bigint", GStorDataType::GS_TYPE_BIGINT, 8, "GS_TYPE_BIGINT"},
    {"int64", GStorDataType::GS_TYPE_BIGINT, 8, "GS_TYPE_BIGINT"},
    {"long", GStorDataType::GS_TYPE_BIGINT, 8, "GS_TYPE_BIGINT"},
    // {"oid", GStorDataType::GS_TYPE_BIGINT, "GS_TYPE_BIGINT"},
    {"int2", GStorDataType::GS_TYPE_SMALLINT, 4, "GS_TYPE_SMALLINT"},
    {"smallint", GStorDataType::GS_TYPE_SMALLINT, 4, "GS_TYPE_SMALLINT"},
    {"short", GStorDataType::GS_TYPE_SMALLINT, 4, "GS_TYPE_SMALLINT"},
    {"int16", GStorDataType::GS_TYPE_SMALLINT, 4, "GS_TYPE_SMALLINT"},
    {"timestamp", GStorDataType::GS_TYPE_TIMESTAMP, 8, "GS_TYPE_TIMESTAMP"},
    {"datetime", GStorDataType::GS_TYPE_TIMESTAMP, 8, "GS_TYPE_TIMESTAMP"},
    // {"timestamp_us", GStorDataType::GS_TYPE_TIMESTAMP, "GS_TYPE_TIMESTAMP"},
    {"bool", GStorDataType::GS_TYPE_BOOLEAN, 4, "GS_TYPE_BOOLEAN"},
    {"boolean", GStorDataType::GS_TYPE_BOOLEAN, 4, "GS_TYPE_BOOLEAN"},
    {"logical", GStorDataType::GS_TYPE_BOOLEAN, 4, "GS_TYPE_BOOLEAN"},
    {"decimal", GStorDataType::GS_TYPE_DECIMAL, 8, "GS_TYPE_DECIMAL"},
    {"dec", GStorDataType::GS_TYPE_DECIMAL, 8, "GS_TYPE_DECIMAL"},
    {"numeric", GStorDataType::GS_TYPE_DECIMAL, 8, "GS_TYPE_DECIMAL"},
    {"number", GStorDataType::GS_TYPE_DECIMAL, 8, "GS_TYPE_NUMBER"},
    {"float4", GStorDataType::GS_TYPE_FLOAT, 8, "GS_TYPE_FLOAT"},  // 暂时不支持float，先映射成real
    {"float", GStorDataType::GS_TYPE_FLOAT, 8,
     "GS_TYPE_FLOAT"},  // 暂时不支持float，先映射成real(sql语句的REAL类型会映射成float)
    {"float32", GStorDataType::GS_TYPE_FLOAT, 8, "GS_TYPE_FLOAT"}, 
    {"real", GStorDataType::GS_TYPE_REAL, 8, "GS_TYPE_REAL"},
    {"double", GStorDataType::GS_TYPE_REAL, 8, "GS_TYPE_REAL"},
    {"float8", GStorDataType::GS_TYPE_REAL, 8, "GS_TYPE_REAL"},
    {"float64", GStorDataType::GS_TYPE_REAL, 8, "GS_TYPE_REAL"},
    {"tinyint", GStorDataType::GS_TYPE_TINYINT, 4, "GS_TYPE_TINYINT"},
    {"int1", GStorDataType::GS_TYPE_TINYINT, 4, "GS_TYPE_TINYINT"},
    {"date", GStorDataType::GS_TYPE_DATE, 8, "GS_TYPE_DATE"},
    // {"interval", GStorDataType::GS_TYPE_INTERVAL, "GS_TYPE_INTERVAL"},
    {"utinyint", GStorDataType::GS_TYPE_UTINYINT, 4, "GS_TYPE_UTINYINT"},
    {"uint8", GStorDataType::GS_TYPE_UTINYINT, 4, "GS_TYPE_UTINYINT"},
    {"usmallint", GStorDataType::GS_TYPE_USMALLINT, 4, "GS_TYPE_USMALLINT"},
    {"uint16", GStorDataType::GS_TYPE_USMALLINT, 4, "GS_TYPE_USMALLINT"},
    {"uinteger", GStorDataType::GS_TYPE_UINT32, 4, "GS_TYPE_UINT32"},
    {"uint32", GStorDataType::GS_TYPE_UINT32, 4, "GS_TYPE_UINT32"},
    {"ubigint", GStorDataType::GS_TYPE_UINT64, 8, "GS_TYPE_UINT64"},
    {"uint64", GStorDataType::GS_TYPE_UINT64, 8, "GS_TYPE_UINT64"},
    // {"timestamptz", GStorDataType::GS_TYPE_TIMESTAMP_TZ, "GS_TYPE_TIMESTAMP_TZ"},
    // {"hugeint", GStorDataType::GS_TYPE_HUGEINT, "GS_TYPE_HUGEINT"},
    {nullptr, GStorDataType::GS_TYPE_UNKNOWN, 0, "GS_TYPE_UNKNOWN"}};

LogicalType GetDefaultType(std::string_view name);
LogicalType TransformTypeName(duckdb_libpgquery::PGTypeName& type_name);
