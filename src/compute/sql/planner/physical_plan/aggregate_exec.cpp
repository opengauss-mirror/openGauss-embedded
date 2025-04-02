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
 * aggregate_exec.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/planner/physical_plan/aggregate_exec.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "planner/physical_plan/aggregate_exec.h"

#include "common/distinct_keys.h"
#include "common/exception.h"
#include "function/aggregate/aggregate_func.h"
#include "function/function.h"
#include "type/type_id.h"

AggregateExec::AggregateExec(std::vector<std::unique_ptr<Expression>> groupby, std::vector<std::string> ops,
                             std::vector<std::unique_ptr<Expression>> be_group, const std::vector<bool>& distincts,
                             Schema schema, PhysicalPlanPtr child)
    : schema_(schema),
      child_(child),
      groups_(std::move(groupby)),
      ops_(std::move(ops)),
      be_groups_(std::move(be_group)),
      distincts(distincts) {}

Schema AggregateExec::GetSchema() const {
    // 可以查询的字段包括 聚合字段 + groupb 字段
    return schema_;
}

static auto AggFuncFactory(const std::string& name, bool is_disinct) -> std::unique_ptr<AggFunc> {
    if (name == "count_star") {
        return std::make_unique<CountAggFunc>(true, is_disinct);
    }
    if (name == "count") {
        return std::make_unique<CountAggFunc>(false, is_disinct);
    }
    if (name == "sum") {
        return std::make_unique<SumAggFunc>(is_disinct);
    }
    if (name == "avg") {
        return std::make_unique<AVGAggFunc>(is_disinct);
    }
    if (name == "max") {
        return std::make_unique<MaxAggFunc>(is_disinct);
    }
    if (name == "min") {
        return std::make_unique<MinAggFunc>(is_disinct);
    }
    if (name == "mode") {
        return std::make_unique<ModeAggFunc>(is_disinct);
    }
    if (name == "top") {
        return std::make_unique<TopAggFunc>(is_disinct);
    }
    if (name == "bottom") {
        return std::make_unique<BottomAggFunc>(is_disinct);
    }
    if(name == "variance") {
        return std::make_unique<VarianceAggFunc>(is_disinct);
    }
    throw intarkdb::Exception(ExceptionType::NOT_IMPLEMENTED, "not supported aggregate func:" + name);
}

void AggregateExec::Init() {
    DistinctKeySortedMap<std::vector<AggContext>> groups_map;

    results_.clear();
    std::vector<std::unique_ptr<AggFunc>> agg_funcs;
    for (size_t i = 0; i < ops_.size(); ++i) {
        agg_funcs.push_back(AggFuncFactory(ops_[i], distincts[i]));
    }
    // NOTE: agg_funcs 与 be_groups_ 一对应多

    while (true) {
        auto&& [record, _, eof] = child_->Next();
        if (eof) {
            break;
        }

        std::vector<Value> values;
        values.reserve(groups_.size());
        for (size_t i = 0; i < groups_.size(); ++i) {
            values.push_back(groups_[i]->Evaluate(record));
        }
        if (values.size() == 0) {
            values.push_back(ValueFactory::ValueInt(1));
        }
        DistinctKey key{values};

        auto iter = groups_map.find(key);
        if (iter != groups_map.end()) {
            auto& ctxs = iter->second;
            auto func_iter = agg_funcs.begin();
            auto ctx_iter = ctxs.begin();
            auto be_groups_iter = be_groups_.begin();

            while (func_iter != agg_funcs.end() && ctx_iter != ctxs.end() && be_groups_iter != be_groups_.end()) {
                auto& func = *func_iter;
                auto& ctx = *ctx_iter;
                // 获取当前迭代器位置的元素
                Value new_value = (*be_groups_iter != nullptr) ? (*be_groups_iter)->Evaluate(record) : ValueFactory::ValueInt(1);

                // 获取索引位置
                size_t index = std::distance(agg_funcs.begin(), func_iter);
                if(ops_[index] == "top" || ops_[index] == "bottom") {
                    ++be_groups_iter;
                    if(*be_groups_iter != nullptr && !(*be_groups_iter)->Evaluate(record).IsInteger()){
                        throw intarkdb::Exception(ExceptionType::MISMATCH_TYPE, "arg 2 must be integer");
                    }

                    ctx.count = (*be_groups_iter != nullptr) ? (*be_groups_iter)->Evaluate(record).GetCastAs<int>() : 1;
                    if(ctx.count < 0) {
                        throw intarkdb::Exception(ExceptionType::OUT_OF_RANGE, "arg 2 must be greater than 0");
                    }
                }

                // 调用 Accumulate 函数
                func->Accumulate(new_value, ctx);
                // 移动到下一个 func, ctx 和 be_group
                ++func_iter;
                ++ctx_iter;
                ++be_groups_iter;
            }            
        } else {
            std::vector<AggContext> ctxs;
            ctxs.reserve(agg_funcs.size());
            auto be_groups_iter = be_groups_.begin();

            for (auto it = agg_funcs.begin(); it != agg_funcs.end(); ++it) {
                // 获取当前迭代器位置的元素
                auto& func = *it;
                // 创建 AggContext 对象
                AggContext ctx;
                // in count_star , be_groups[i] is null
                Value new_value = (*be_groups_iter != nullptr) ? (*be_groups_iter)->Evaluate(record) : ValueFactory::ValueInt(1);

                // 获取索引位置
                size_t index = std::distance(agg_funcs.begin(), it);
                if(ops_[index] == "top" || ops_[index] == "bottom") {
                    ++be_groups_iter;
                    if(*be_groups_iter != nullptr && !(*be_groups_iter)->Evaluate(record).IsInteger()){
                        throw intarkdb::Exception(ExceptionType::MISMATCH_TYPE, "arg 2 must be integer");
                    }

                    ctx.count = (*be_groups_iter != nullptr) ? (*be_groups_iter)->Evaluate(record).GetCastAs<int>() : 1;
                    if(ctx.count < 0) {
                        throw intarkdb::Exception(ExceptionType::OUT_OF_RANGE, "arg 2 must be greater than 0");
                    }
                }

                // 调用 First 函数
                func->First(new_value, ctx);
                // 将 ctx 移动构造到 ctxs 中
                ctxs.emplace_back(std::move(ctx));
                
                ++be_groups_iter;
            }
            groups_map.emplace(std::move(key), std::move(ctxs));
        }
    }

    // 构建结果
    for (auto& [key, ctxs] : groups_map) {
        std::vector<std::variant<Value, std::vector<Value>>> values;
        const auto& key_items = key.Keys();
        values.reserve(key_items.size() + ctxs.size());
        if (groups_.size() > 0) {
            std::copy(key_items.begin(), key_items.end(), std::back_inserter(values));
        }
        for (size_t i = 0; i < ctxs.size(); ++i) {
            values.push_back(agg_funcs[i]->Final(ctxs[i]));
        }
        
        std::vector<Value> flattened_values;
        bool agg_flag = false;
        for (const auto& val : values) {
            if (std::holds_alternative<Value>(val)) {
                flattened_values.push_back(std::get<Value>(val));
                agg_flag = true;
            } 
            if (std::holds_alternative<std::vector<Value>>(val)) {
                const auto& vec_val = std::get<std::vector<Value>>(val);
                for (const auto& v : vec_val) {
                    flattened_values.clear();
                    flattened_values.push_back(v);
                    Record record(std::move(flattened_values));
                    results_.push_back(std::move(record));
                }
            }
        }
        if(agg_flag){
            Record record(std::move(flattened_values));
            results_.push_back(std::move(record));
        }
    }
    // 空表处理
    if (results_.size() == 0 && groups_.size() == 0) {
        std::vector<Value> values;
        for (size_t i = 0; i < agg_funcs.size(); ++i) {
            values.push_back(agg_funcs[i]->Default());
        }
        results_.emplace_back(std::move(values));
    }
    idx_ = 0;
    init_ = true;
}

auto AggregateExec::Next() -> std::tuple<Record, knl_cursor_t*, bool> {
    if (!init_) {
        Init();
    }
    if (idx_ < results_.size()) {
        std::tuple<Record, knl_cursor_t*, bool> r = {std::move(results_[idx_]), nullptr, false};
        idx_++;
        return r;
    }
    init_ = false;
    return {Record{}, nullptr, true};
}
