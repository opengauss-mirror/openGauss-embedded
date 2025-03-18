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
* physical_plan.h
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/include/planner/physical_plan/physical_plan.h
*
* -------------------------------------------------------------------------
*/
#pragma once

#include <fmt/format.h>

#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "catalog/schema.h"
#include "common/record_batch.h"

class PhysicalPlan;
using PhysicalPlanPtr = std::shared_ptr<PhysicalPlan>;

class PhysicalPlan {
   public:
    PhysicalPlan() {}

    virtual Schema GetSchema() const = 0;

    virtual auto Execute() const -> RecordBatch = 0;
    virtual void Execute(RecordBatch &rb) {};

    virtual std::vector<PhysicalPlanPtr> Children() const = 0;

    virtual std::string ToString() const { return "BasePhysicalPlan"; }

    virtual ~PhysicalPlan() {}

    virtual auto Next() -> std::tuple<Record, knl_cursor_t *, bool> = 0;

    friend std::string format(const PhysicalPlan &, int);
    std::string Print() { return format(*this, 0); }

   public:
    virtual void ResetNext() {}

    virtual void SetNeedResultSetEx(bool need) {}

    virtual bool NeedResultSetEx() { return false; }

    virtual void SetAutoCommit(bool auto_commit) {}
    virtual bool IsAutoCommit() { return false; }
};

template <typename T>
struct fmt::formatter<T, std::enable_if_t<std::is_base_of<PhysicalPlan, T>::value, char>>
    : fmt::formatter<std::string> {
    template <typename FormatCtx>
    auto format(const T &x, FormatCtx &ctx) const {
        return fmt::formatter<std::string>::format(x.ToString(), ctx);
    }
};

template <typename T>
struct fmt::formatter<std::unique_ptr<T>, std::enable_if_t<std::is_base_of<PhysicalPlan, T>::value, char>>
    : fmt::formatter<std::string> {
    template <typename FormatCtx>
    auto format(const std::unique_ptr<T> &x, FormatCtx &ctx) const {
        return fmt::formatter<std::string>::format(x->ToString(), ctx);
    }
};

template <typename T>
struct fmt::formatter<std::shared_ptr<T>, std::enable_if_t<std::is_base_of<PhysicalPlan, T>::value, char>>
    : fmt::formatter<std::string> {
    template <typename FormatCtx>
    auto format(const std::unique_ptr<T> &x, FormatCtx &ctx) const {
        return fmt::formatter<std::string>::format(x->ToString(), ctx);
    }
};
