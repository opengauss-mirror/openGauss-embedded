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
 * default_value.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/common/default_value.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <cstdint>
#include <memory>

#include "binder/bound_expression.h"
#include "datasource/table_datasource.h"
#include "storage/gstor/gstor_executor.h"
#include "type/type_id.h"
#include "type/value.h"

using DefaultValueType = gs_default_value_type_t;

using DefaultValue = internal_default_value_t;

std::unique_ptr<DefaultValue> CreateDefaultValue(DefaultValueType type, size_t size, const char* buff);

// 把默认值以String的方式进行展示, 都是以字符串的形式返回
Value ShowValue(DefaultValue& default_value, LogicalType type);

// 获取默认值的真实值 , e.g: 默认值为18, now() 等，都会获得对应的值
Value ToValue(DefaultValue& default_value, LogicalType type, TableDataSource& source);

size_t DefaultValueSize(DefaultValue* default_value);
