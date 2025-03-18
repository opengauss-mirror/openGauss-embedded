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
 * function.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/function/function.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <functional>
#include <optional>
#include <vector>

#include "type/type_id.h"
#include "type/value.h"

namespace intarkdb {

using FunctionBody = std::function<Value(const std::vector<Value>&)>;
using FunctionCalReturnType = std::function<LogicalType(const std::vector<LogicalType>&)>;

struct FunctionSignature {
    std::vector<LogicalType> args;  // 函数签名
    LogicalType return_type;        // 返回值类型
    FunctionCalReturnType return_type_func;
};

struct Function {
    FunctionSignature sig;  // 函数签名
    FunctionBody func;      // 函数体
};

class FunctionGroup {
   public:
    // 获取函数
    auto GetFunction(const std::vector<LogicalType>& args) const -> std::optional<Function>;

    auto AddFunction(Function func) -> void;

   private:
    std::vector<Function> functions;
};

// 用于函数注册
class FunctionContext {
   public:
    FunctionContext() = default;
    ~FunctionContext() = default;

    static void Init();
    // 注册函数
    auto RegisterFunction(const std::string& name, const FunctionSignature& sig, FunctionBody func) -> void;

    // 获取函数
    static auto GetFunction(const std::string& name, const std::vector<LogicalType>& args) -> std::optional<Function>;

    // 获取比较函数
    static auto GetCompareFunction(const std::string& name, const std::vector<LogicalType>& args) -> std::optional<Function>;

   private:
    std::unordered_map<std::string, FunctionGroup> functions_;
};


// 全局函数上下文
extern FunctionContext g_function_context;

}  // namespace intarkdb
