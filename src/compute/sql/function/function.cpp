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
 * function.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/function/function.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "function/function.h"

#include "common/compare_type.h"
#include "function/cast_rules.h"
#include "function/compare/compare.h"
#include "function/math/add.h"
#include "function/math/divided.h"
#include "function/math/minus.h"
#include "function/math/mod.h"
#include "function/math/multiply.h"
#include "function/string/sql_string.h"

namespace intarkdb {

FunctionContext g_function_context;

auto FunctionGroup::GetFunction(const std::vector<LogicalType>& args) const -> std::optional<Function> {
    // match function
    int best_match = -1;
    int64_t lowest_cost = std::numeric_limits<int64_t>::max();
    for (size_t idx = 0; idx < functions.size(); idx++) {
        const auto& func = functions[idx];
        // args 数量不可小于函数参数数量，存在可变参数的情况, args 数量大于函数参数数量
        if (func.sig.args.size() > args.size()) {
            continue;
        }
        bool found = true;
        int64_t curr_cost = 0;
        for (size_t i = 0; i < args.size(); i++) {
            if (args[i].TypeId() == GS_TYPE_PARAM) {
                continue;
            }
            // 计算隐式转换的代价
            auto cost = CastRules::ImplicitCastable(args[i], func.sig.args[i]);
            if (cost < 0) {
                found = false;
                break;
            }
            curr_cost += cost;
        }
        if (found == true && curr_cost < lowest_cost) {
            best_match = idx;
            lowest_cost = curr_cost;
            if (curr_cost == 0) {  // perfect match
                break;
            }
        }
    }

    auto func = best_match >= 0 ? std::optional<Function>{functions[best_match]} : std::nullopt;
    if (func.has_value() && func->sig.return_type_func) {
        func->sig.return_type = func->sig.return_type_func(args);
    }
    return func;
}

auto FunctionGroup::AddFunction(Function func) -> void { functions.emplace_back(std::move(func)); }

// 注册函数
auto FunctionContext::RegisterFunction(const std::string& name, const FunctionSignature& sig, FunctionBody func)
    -> void {
    functions_[name].AddFunction(Function{std::move(sig), std::move(func)});
}
// 获取函数
auto FunctionContext::GetFunction(const std::string& name, const std::vector<LogicalType>& args)
    -> std::optional<Function> {
    auto iter = g_function_context.functions_.find(name);
    if (iter != g_function_context.functions_.end()) {
        return iter->second.GetFunction(args);
    }
    return std::nullopt;
}

static auto GetRevertFunction(const std::optional<Function>& func) -> std::optional<Function> {
    if (func.has_value()) {
        return Function{func->sig, [func](const std::vector<Value>& values) -> Value {
                            return ValueFactory::ValueBool(!func->func(values).GetCastAs<bool>());
                        }};
    }
    return std::nullopt;
}

auto FunctionContext::GetCompareFunction(const std::string& name, const std::vector<LogicalType>& args)
    -> std::optional<Function> {
    auto compare_type = ToComparisonType(name);
    switch (compare_type) {
        case ComparisonType::Equal:
            return GetFunction("=", args);
        case ComparisonType::NotEqual:
            return GetRevertFunction(GetFunction("=", args));
        case ComparisonType::LessThan:
            return GetFunction("<", args);
        case ComparisonType::LessThanOrEqual:
            return GetFunction("<=", args);
        case ComparisonType::GreaterThan:
            return GetRevertFunction(GetFunction("<=", args));
        case ComparisonType::GreaterThanOrEqual:
            return GetRevertFunction(GetFunction("<", args));
        default:
            break;
    }
    throw intarkdb::Exception(ExceptionType::EXECUTOR, name + " is unsupported compare function");
}

// 函数加载列表
auto FunctionContext::Init() -> void {
    // 注册函数
    static bool init = false;
    if (init == false) {
        init = true;
        AddSet::Register(g_function_context);
        MinusSet::Register(g_function_context);
        MultiplySet::Register(g_function_context);
        DividedSet::Register(g_function_context);
        ModSet::Register(g_function_context);
        EqualSet::Register(g_function_context);
        LessThanSet::Register(g_function_context);
        LessThanOrEqualSet::Register(g_function_context);
        StringSet::Register(g_function_context);
    }
}

}  // namespace intarkdb
