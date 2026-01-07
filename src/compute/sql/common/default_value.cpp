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
 * default_value.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/common/default_value.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "common/default_value.h"

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <optional>
#include <regex>

#include "binder/expressions/bound_constant.h"
#include "function/sql_function.h"
#include "type/type_system.h"

std::unique_ptr<DefaultValue> CreateDefaultValue(DefaultValueType type, size_t size, const char* buff) {
    size_t alloc_size = sizeof(DefaultValue) + size;
    if (alloc_size == 0) {
        throw intarkdb::Exception(ExceptionType::EXECUTOR, "malloc zero size!");
    }
    auto ptr = std::unique_ptr<DefaultValue>(static_cast<DefaultValue*>(malloc(alloc_size)));
    if (ptr == nullptr) {
        throw intarkdb::Exception(ExceptionType::EXECUTOR, "malloc failed!");
    }
    ptr->type = type;
    ptr->size = size;
    memcpy(ptr->data, buff, size);
    return ptr;
}

//
Value ShowValue(DefaultValue& default_value, LogicalType type) {
    col_text_t default_text = {default_value.data, default_value.size};
    if (default_value.type == DefaultValueType::DEFAULT_VALUE_TYPE_FUNC) {
        return Value(GS_TYPE_VARCHAR,default_text,0,0);
    } else if (default_value.type == DefaultValueType::DEFAULT_VALUE_TYPE_LITERAL) {
        return Value(type,default_text);
    } else {
        throw intarkdb::Exception(ExceptionType::EXECUTOR, "default value type not support!");
    }
}

static Value MatchFunc(TableDataSource& source, const std::string& func_name) {
    static std::regex pattern("nextval\\(([\\w\\d]+)\\)");  // 正则表达式模式，使用双斜杠转义特殊字符
    std::smatch match;
    Value val = ValueFactory::ValueNull();
    if (std::regex_search(func_name, match, pattern)) {
        const std::string& seq_name = match[1];  // 提取匹配的内容
        val = ValueFactory::ValueBigInt(source.SeqNextValue(seq_name));
    } else {                             // 无参数函数
        std::string raw_func_name = "";  // 只支持下面的函数
        if (func_name == "now()") {
            raw_func_name = "now";
        } else if (func_name == "current_date()") {
            raw_func_name = "current_date";
        } else if (func_name == "random()") {
            raw_func_name = "random";
        } else {
            throw intarkdb::Exception(ExceptionType::EXECUTOR,
                                      fmt::format("function {} not support as default value", func_name));
        }
        auto func_iter = SQLFunction::FUNC_MAP.find(raw_func_name);
        if (func_iter != SQLFunction::FUNC_MAP.end()) {
            val = func_iter->second.func({});
        } else {
            throw intarkdb::Exception(ExceptionType::EXECUTOR,
                                      fmt::format("function {} not support as default value", func_name));
        }
    }
    return val;
}

Value ToValue(DefaultValue& default_value, LogicalType type, TableDataSource& source) {
    Value val = ValueFactory::ValueNull();
    if (default_value.type == DefaultValueType::DEFAULT_VALUE_TYPE_LITERAL) {  // 默认值为字面量
        col_text_t default_constant = {default_value.data, default_value.size};
        val = Value(type,default_constant);
    } else if (default_value.type == DefaultValueType::DEFAULT_VALUE_TYPE_FUNC) {
        std::string func_name = std::string(default_value.data, default_value.data + default_value.size);
        transform(func_name.begin(), func_name.end(), func_name.begin(), ::tolower);
        val = MatchFunc(source, func_name);
        if (val.GetType() != type.TypeId()) {  // 函数的返回类型 与 列的类型不一致
            val = DataType::GetTypeInstance(type.TypeId())->CastValue(val);
        }
    } else {
        throw intarkdb::Exception(ExceptionType::EXECUTOR, "default value type not support!");
    }
    return val;
}

size_t DefaultValueSize(DefaultValue* default_value) {
    return sizeof(DefaultValue) + default_value->size;
}
