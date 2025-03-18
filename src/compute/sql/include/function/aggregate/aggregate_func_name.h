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
 * aggregate_func_name.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/function/aggregate/aggregate_func_name.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <string>

#include "common/string_util.h"

enum class AggReturnType {
    INVALID,
    ARG,  // 依赖参数类型
    BIGINT,
    DOUBLE,
};

enum class InputType {
    UNLIMITED,  // 无限制
    NUMERIC,    // 数值类型
};

struct AggFuncInfo {
    InputType input_type;
    AggReturnType return_type;

    bool NeedNumericInput() const { return input_type == InputType::NUMERIC; }

    bool IsValid() const { return return_type != AggReturnType::INVALID; }
};

inline const AggFuncInfo& FindAggInfo(const std::string& name) {
    static AggFuncInfo invalid_info = {InputType::UNLIMITED, AggReturnType::INVALID};

    static AggFuncInfo agg_func_infos[] = {
        {InputType::UNLIMITED, AggReturnType::BIGINT},
        {InputType::NUMERIC, AggReturnType::ARG},
        {InputType::NUMERIC, AggReturnType::DOUBLE},
        {InputType::UNLIMITED, AggReturnType::ARG},
    };

    static intarkdb::CaseInsensitiveMap<std::reference_wrapper<const AggFuncInfo>> valid_agg_func_name = {
        {"count", std::cref(agg_func_infos[0])},  {"count_star", std::cref(agg_func_infos[0])},
        {"sum", std::cref(agg_func_infos[1])},    {"avg", std::cref(agg_func_infos[2])},
        {"max", std::cref(agg_func_infos[3])},    {"min", std::cref(agg_func_infos[3])},
        {"mode", std::cref(agg_func_infos[3])},   {"top", std::cref(agg_func_infos[3])},
        {"bottom", std::cref(agg_func_infos[3])}, {"variance", std::cref(agg_func_infos[2])},
    };

    auto iter = valid_agg_func_name.find(name);
    if (iter != valid_agg_func_name.end()) {
        return iter->second;
    }
    return invalid_info;
}

inline bool IsValidAggregateFuncName(const std::string& name) { return FindAggInfo(name).IsValid(); }
