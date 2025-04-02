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
 * aggregate_func.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/function/aggregate/aggregate_func.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "function/aggregate/aggregate_func.h"

#include "type/type_str.h"

void CountAggFunc::Accumulate(const Value& new_value, AggContext& ctx) {
    if (is_count_star_) {
        ctx.count += 1;
        return;
    }
    if (new_value.IsNull()) {
        return;
    }
    if (is_distinct_) {
        DistinctKey k(std::vector<Value>{new_value});
        auto distinct_key = ctx.distinct_set.find(k);
        if (distinct_key != ctx.distinct_set.end()) {
            return;
        }
        ctx.distinct_set.insert(std::move(k));
    }
    ctx.count += 1;
}

void CountAggFunc::First(const Value& value, AggContext& ctx) {
    if (is_count_star_) {
        ctx.count = 1;
    } else {
        if (value.IsNull()) {
            ctx.count = 0;
        } else {
            if (is_distinct_) {
                DistinctKey k{std::vector<Value>{value}};
                ctx.distinct_set.insert(std::move(k));
            }
            ctx.count = 1;
        }
    }
}

std::variant<Value, std::vector<Value>> CountAggFunc::Final(const AggContext& ctx) {
    return ValueFactory::ValueBigInt(ctx.count);
}

Value CountAggFunc::Default() { return ValueFactory::ValueBigInt(0); }

void SumAggFunc::Accumulate(const Value& new_value, AggContext& ctx) {
    if (new_value.IsNull()) {
        return;
    }
    if (is_distinct_) {
        DistinctKey k{std::vector<Value>{new_value}};
        auto distinct_key = ctx.distinct_set.find(k);
        if (distinct_key != ctx.distinct_set.end()) {
            return;
        }
        ctx.distinct_set.insert(std::move(k));
    }
    if (ctx.acc.IsNull()) {
        ctx.acc = new_value;
        return;
    }
    ctx.acc = func_info_.value().func({ctx.acc, new_value});
}

void SumAggFunc::First(const Value& value, AggContext& ctx) {
    if (!value.IsNumeric()) {
        throw intarkdb::Exception(ExceptionType::NOT_IMPLEMENTED,
                                  fmt::format("not supported aggregate sum with {}", value.GetLogicalType()));
    }
    if (value.IsDecimal()) {
        func_info_ = intarkdb::FunctionContext::GetFunction("+", {value.GetLogicalType(), value.GetLogicalType()});
    } else if (value.IsFloat()) {
        func_info_ = intarkdb::FunctionContext::GetFunction("+", {LogicalType::Double(), LogicalType::Double()});
    } else if (value.IsUnSigned()) {
        func_info_ = intarkdb::FunctionContext::GetFunction("+", {LogicalType::UINT64(), LogicalType::UINT64()});
    } else {
        func_info_ = intarkdb::FunctionContext::GetFunction("+", {LogicalType::Bigint(), LogicalType::Bigint()});
    }
    if (!func_info_.has_value()) {
        throw intarkdb::Exception(ExceptionType::NOT_IMPLEMENTED,
                                  fmt::format("not supported aggregate sum with {}", value.GetLogicalType()));
    }
    if (!value.IsNull() && is_distinct_) {
        DistinctKey k{std::vector<Value>{value}};
        ctx.distinct_set.insert(std::move(k));
    }
    ctx.acc = value;
}

std::variant<Value, std::vector<Value>> SumAggFunc::Final(const AggContext& ctx) { return ctx.acc; }

void AVGAggFunc::Accumulate(const Value& new_value, AggContext& ctx) {
    if (new_value.IsNull()) {
        return;
    }
    if (is_distinct_) {
        DistinctKey k{std::vector<Value>{new_value}};
        auto distinct_key = ctx.distinct_set.find(k);
        if (distinct_key != ctx.distinct_set.end()) {
            return;
        }
        ctx.distinct_set.insert(std::move(k));
    }
    if (ctx.acc.IsNull()) {
        ctx.acc = new_value;
        ctx.count++;
        return;
    }
    ctx.acc = func_info_.value().func({ctx.acc, new_value});
    ctx.count++;
}

void AVGAggFunc::First(const Value& value, AggContext& ctx) {
    if (!value.IsNumeric()) {
        throw intarkdb::Exception(ExceptionType::NOT_IMPLEMENTED,
                                  fmt::format("not supported aggregate sum with {}", value.GetLogicalType()));
    }
    if (value.IsFloat()) {
        func_info_ = intarkdb::FunctionContext::GetFunction("+", {LogicalType::Double(), LogicalType::Double()});
    } else if (value.IsUnSigned()) {
        func_info_ = intarkdb::FunctionContext::GetFunction("+", {LogicalType::UINT64(), LogicalType::UINT64()});
    } else {
        func_info_ = intarkdb::FunctionContext::GetFunction("+", {LogicalType::Bigint(), LogicalType::Bigint()});
    }
    if (!func_info_.has_value()) {
        throw intarkdb::Exception(ExceptionType::NOT_IMPLEMENTED,
                                  fmt::format("not supported aggregate sum with {}", value.GetLogicalType()));
    }
    if (!value.IsNull() && is_distinct_) {
        DistinctKey k{std::vector<Value>{value}};
        ctx.distinct_set.insert(std::move(k));
    }
    ctx.acc = value;
    ctx.count = ctx.acc.IsNull() ? 0 : 1;
}

std::variant<Value, std::vector<Value>> AVGAggFunc::Final(const AggContext& ctx) {
    if (ctx.acc.IsNull()) {
        return ctx.acc;
    }
    if (ctx.count == 0) {
        return ValueFactory::ValueNull();
    }
    double result = ctx.acc.GetCastAs<double>() / ctx.count;
    return ValueFactory::ValueDouble(result);
}

void MaxAggFunc::Accumulate(const Value& new_value, AggContext& ctx) {
    if (new_value.IsNull()) {
        return;
    }
    if (is_distinct_) {
        DistinctKey k{std::vector<Value>{new_value}};
        auto distinct_key = ctx.distinct_set.find(k);
        if (distinct_key != ctx.distinct_set.end()) {
            return;
        }
        ctx.distinct_set.insert(std::move(k));
    }
    if (ctx.acc.IsNull()) {
        ctx.acc = new_value;
        return;
    }
    auto result = less_.value().func({ctx.acc, new_value});
    if (result.GetCastAs<bool>()) {
        ctx.acc = new_value;
    }
}

void MaxAggFunc::First(const Value& value, AggContext& ctx) {
    less_ = intarkdb::FunctionContext::GetCompareFunction("<", {value.GetLogicalType(), value.GetLogicalType()});
    if (!less_.has_value()) {
        throw intarkdb::Exception(ExceptionType::NOT_IMPLEMENTED,
                                  fmt::format("not supported aggregate max with {}", value.GetLogicalType()));
    }
    if (!value.IsNull() && is_distinct_) {
        DistinctKey k{std::vector<Value>{value}};
        ctx.distinct_set.insert(std::move(k));
    }
    ctx.acc = value;
}

std::variant<Value, std::vector<Value>> MaxAggFunc::Final(const AggContext& ctx) { return ctx.acc; }

void MinAggFunc::Accumulate(const Value& new_value, AggContext& ctx) {
    if (new_value.IsNull()) {
        return;
    }
    if (is_distinct_) {
        DistinctKey k{std::vector<Value>{new_value}};
        auto distinct_key = ctx.distinct_set.find(k);
        if (distinct_key != ctx.distinct_set.end()) {
            return;
        }
        ctx.distinct_set.insert(std::move(k));
    }
    if (ctx.acc.IsNull()) {
        ctx.acc = new_value;
        return;
    }
    auto result = less_.value().func({ctx.acc, new_value});
    if (!result.GetCastAs<bool>()) {
        ctx.acc = new_value;
    }
}

void MinAggFunc::First(const Value& value, AggContext& ctx) {
    less_ = intarkdb::FunctionContext::GetCompareFunction("<", {value.GetLogicalType(), value.GetLogicalType()});
    if (!less_.has_value()) {
        throw intarkdb::Exception(ExceptionType::NOT_IMPLEMENTED,
                                  fmt::format("not supported aggregate min with {}", value.GetLogicalType()));
    }
    if (!value.IsNull() && is_distinct_) {
        DistinctKey k{std::vector<Value>{value}};
        ctx.distinct_set.insert(std::move(k));
    }
    ctx.acc = value;
}

std::variant<Value, std::vector<Value>> MinAggFunc::Final(const AggContext& ctx) { return ctx.acc; }

void ModeAggFunc::Accumulate(const Value& new_value, AggContext& ctx) {
    if (new_value.IsNull()) {
        return;
    }
    if (is_distinct_) {
        throw intarkdb::Exception(ExceptionType::NOT_IMPLEMENTED, "mode not support distinct");
    }
    DistinctKey distinctKey{std::vector<Value>{new_value}};
    auto iter = ctx.count_map.find(distinctKey);
    if (iter == ctx.count_map.end()) {
        ctx.count_map.emplace(std::move(distinctKey), 1);
    } else {
        iter->second++;
    }
}

void ModeAggFunc::First(const Value& value, AggContext& ctx) {
    if (!value.IsNull()) {
        return;
    }
    if (is_distinct_) {
        throw intarkdb::Exception(ExceptionType::NOT_IMPLEMENTED, "mode not support distinct");
    }
    DistinctKey distinctKey{std::vector<Value>{value}};
    ctx.count_map.emplace(std::move(distinctKey), 1);
}

std::variant<Value, std::vector<Value>> ModeAggFunc::Final(const AggContext& ctx) {
    auto iter = ctx.count_map.begin();
    int max_count = 0;
    for (auto i = ctx.count_map.begin(); i != ctx.count_map.end(); i++) {
        if (i->second > max_count) {
            max_count = i->second;
            iter = i;
        }
    }
    if (iter == ctx.count_map.end()) {
        return ValueFactory::ValueNull();
    }
    return iter->first.Keys()[0];
}

void TopAggFunc::Accumulate(const Value& new_value, AggContext& ctx) {
    if (new_value.IsNull()) {
        return;
    }
    if (is_distinct_) {
        DistinctKey k{std::vector<Value>{new_value}};
        auto distinct_key = ctx.distinct_set.find(k);
        if (distinct_key != ctx.distinct_set.end()) {
            return;
        }
        ctx.distinct_set.insert(std::move(k));
    }
    ctx.top_queue.push(new_value);
}

void TopAggFunc::First(const Value& value, AggContext& ctx) {
    if (!value.IsNull() && is_distinct_) {
        DistinctKey k{std::vector<Value>{value}};
        ctx.distinct_set.insert(std::move(k));
    }
    ctx.top_queue.push(value);
}

std::variant<Value, std::vector<Value>> TopAggFunc::Final(const AggContext& ctx) {
    if (ctx.top_queue.empty() || ctx.count == 0) {
        return ValueFactory::ValueNull();
    }

    auto& const_top_queue = ctx.top_queue;
    std::vector<Value> result;
    size_t size = ctx.count > const_top_queue.size() ? const_top_queue.size() : ctx.count;
    for (size_t i = 0; i < size; i++) {
        result.push_back(const_top_queue.top());
        const_top_queue.pop();
    }
    return result;
}

void BottomAggFunc::Accumulate(const Value& new_value, AggContext& ctx) {
    if (new_value.IsNull()) {
        return;
    }
    if (is_distinct_) {
        DistinctKey k{std::vector<Value>{new_value}};
        auto distinct_key = ctx.distinct_set.find(k);
        if (distinct_key != ctx.distinct_set.end()) {
            return;
        }
        ctx.distinct_set.insert(std::move(k));
    }
    ctx.bottom_queue.push(new_value);
}

void BottomAggFunc::First(const Value& value, AggContext& ctx) {
    if (!value.IsNull() && is_distinct_) {
        DistinctKey k{std::vector<Value>{value}};
        ctx.distinct_set.insert(std::move(k));
    }
    ctx.bottom_queue.push(value);
}

std::variant<Value, std::vector<Value>> BottomAggFunc::Final(const AggContext& ctx) {
    if (ctx.bottom_queue.empty() || ctx.count == 0) {
        return ValueFactory::ValueNull();
    }

    auto& const_bottom_queue = ctx.bottom_queue;
    std::vector<Value> result;
    size_t size = ctx.count > const_bottom_queue.size() ? const_bottom_queue.size() : ctx.count;
    for (size_t i = 0; i < size; i++) {
        result.push_back(const_bottom_queue.top());
        const_bottom_queue.pop();
    }
    return result;
}

static void UpdateVariance(AggContext& ctx, const Value& new_value) {
    if (new_value.IsNull()) {
        return;
    }
    double val = new_value.GetCastAs<double>();
    ctx.count++;
    double delta = val - ctx.mean;
    ctx.mean += delta / ctx.count;
    double delta2 = val - ctx.mean;
    ctx.M2 += delta * delta2;
}

void VarianceAggFunc::Accumulate(const Value& new_value, AggContext& ctx) {
    if (new_value.IsNull()) {
        return;
    }
    if (is_distinct_) {
        DistinctKey k{std::vector<Value>{new_value}};
        auto distinct_key = ctx.distinct_set.find(k);
        if (distinct_key != ctx.distinct_set.end()) {
            return;
        }
        ctx.distinct_set.insert(std::move(k));
    }
    UpdateVariance(ctx, new_value);
}

void VarianceAggFunc::First(const Value& value, AggContext& ctx) {
    if (value.IsNull()) {
        return;
    }
    if (is_distinct_) {
        DistinctKey k{std::vector<Value>{value}};
        auto distinct_key = ctx.distinct_set.find(k);
        if (distinct_key != ctx.distinct_set.end()) {
            return;
        }
        ctx.distinct_set.insert(std::move(k));
    }
    UpdateVariance(ctx, value);
}

std::variant<Value, std::vector<Value>> VarianceAggFunc::Final(const AggContext& ctx) {
    if (ctx.count < 1) {
        // variance is not defined for less than 1 elements
        return ValueFactory::ValueDouble(0);
    }
    return ValueFactory::ValueDouble(ctx.M2 / ctx.count);
}
