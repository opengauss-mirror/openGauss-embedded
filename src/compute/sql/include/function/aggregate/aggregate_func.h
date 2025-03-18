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
 * aggregate_func.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/function/aggregate/aggregate_func.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <vector>

#include "common/distinct_keys.h"
#include "function/function.h"
#include "type/value.h"

struct AggContext {
    Value acc;
    int count{0};
    DistinctKeySet distinct_set;
    DistinctKeyMap<int> count_map;
    mutable TopPriorityQueue top_queue;
    mutable BottomPriorityQueue bottom_queue;
    // for variance
    double mean{0};
    double M2{0};
};

class AggFunc {
   public:
    AggFunc(bool is_distinct) : is_distinct_(is_distinct) {}
    virtual ~AggFunc() {}

    virtual void Accumulate(const Value& new_value, AggContext&) = 0;

    virtual void First(const Value&, AggContext&) = 0;

    virtual std::variant<Value, std::vector<Value>> Final(const AggContext&) = 0;

    virtual Value Default() { return ValueFactory::ValueNull(); }

   protected:
    bool is_distinct_{false};
};

class CountAggFunc : public AggFunc {
   public:
    CountAggFunc(bool is_count_star, bool is_distinct) : AggFunc(is_distinct), is_count_star_(is_count_star) {}
    virtual ~CountAggFunc() override = default;

    virtual void Accumulate(const Value& new_value, AggContext& ctx) override;

    virtual void First(const Value& value, AggContext& ctx) override;

    virtual std::variant<Value, std::vector<Value>> Final(const AggContext& ctx) override;

    virtual Value Default() override;

   private:
    bool is_count_star_;
};

class SumAggFunc : public AggFunc {
   public:
    SumAggFunc(bool is_distinct) : AggFunc(is_distinct) {}
    virtual ~SumAggFunc() override = default;

    virtual void Accumulate(const Value& new_value, AggContext& ctx) override;

    virtual void First(const Value& value, AggContext& ctx) override;

    virtual std::variant<Value, std::vector<Value>> Final(const AggContext& ctx) override;

   private:
    std::optional<intarkdb::Function> func_info_;
};

class AVGAggFunc : public AggFunc {
   public:
    AVGAggFunc(bool is_distinct) : AggFunc(is_distinct) {}
    virtual ~AVGAggFunc() override = default;

    virtual void Accumulate(const Value& new_value, AggContext& ctx) override;

    virtual void First(const Value& value, AggContext& ctx) override;

    virtual std::variant<Value, std::vector<Value>> Final(const AggContext& ctx) override;

   private:
    std::optional<intarkdb::Function> func_info_;
};

class MaxAggFunc : public AggFunc {
   public:
    MaxAggFunc(bool is_distinct) : AggFunc(is_distinct) {}
    virtual ~MaxAggFunc() override = default;

    virtual void Accumulate(const Value& new_value, AggContext& ctx) override;

    virtual void First(const Value& value, AggContext& ctx) override;

    virtual std::variant<Value, std::vector<Value>> Final(const AggContext& ctx) override;

   private:
    std::optional<intarkdb::Function> less_;
};

class MinAggFunc : public AggFunc {
   public:
    MinAggFunc(bool is_distinct) : AggFunc(is_distinct) {}
    virtual ~MinAggFunc() override = default;

    virtual void Accumulate(const Value& new_value, AggContext& ctx) override;

    virtual void First(const Value& value, AggContext& ctx) override;

    virtual std::variant<Value, std::vector<Value>> Final(const AggContext& ctx) override;

   private:
    std::optional<intarkdb::Function> less_;
};

class ModeAggFunc : public AggFunc {
   public:
    ModeAggFunc(bool is_distinct) : AggFunc(is_distinct) {}
    virtual ~ModeAggFunc() override = default;

    virtual void Accumulate(const Value& new_value, AggContext& ctx) override;

    virtual void First(const Value& value, AggContext& ctx) override;

    virtual std::variant<Value, std::vector<Value>> Final(const AggContext& ctx) override;
};

class TopAggFunc : public AggFunc {
   public:
    TopAggFunc(bool is_distinct) : AggFunc(is_distinct) {}
    virtual ~TopAggFunc() override = default;

    virtual void Accumulate(const Value& new_value, AggContext& ctx) override;

    virtual void First(const Value& value, AggContext& ctx) override;

    virtual std::variant<Value, std::vector<Value>> Final(const AggContext& ctx) override;
};

class BottomAggFunc : public AggFunc {
   public:
    BottomAggFunc(bool is_distinct) : AggFunc(is_distinct) {}
    virtual ~BottomAggFunc() override = default;

    virtual void Accumulate(const Value& new_value, AggContext& ctx) override;

    virtual void First(const Value& value, AggContext& ctx) override;

    virtual std::variant<Value, std::vector<Value>> Final(const AggContext& ctx) override;
};

class VarianceAggFunc : public AggFunc {
   public:
    VarianceAggFunc(bool is_distinct) : AggFunc(is_distinct) {}
    virtual ~VarianceAggFunc() override = default;

    virtual void Accumulate(const Value& new_value, AggContext& ctx) override;

    virtual void First(const Value& value, AggContext& ctx) override;

    virtual std::variant<Value, std::vector<Value>> Final(const AggContext& ctx) override;
};
