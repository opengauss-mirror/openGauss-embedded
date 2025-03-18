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
* window_exec.h
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/include/planner/physical_plan/window_exec.h
*
* -------------------------------------------------------------------------
*/
#pragma once

#include <algorithm>

#include "common/row_container.h"
#include "function/function.h"
#include "planner/physical_plan/physical_plan.h"

namespace intarkdb {
enum class WindowFrameType {
    ROWS,
    // RANGE,
    // GROUPS
};

struct WindowSortItem {
    size_t idx;
    bool is_asc;
    bool is_null_first;
};

struct WindowSpec {
    WindowFrameType frame_type;
    std::vector<size_t> partition_by;
    std::vector<WindowSortItem> order_by;
    // int64_t start;
    // int64_t end;
};

static bool isSamePartition(const Record& r1, const Record& r2, const std::vector<size_t>& partition_by) {
    for (size_t col : partition_by) {
        // FIXME: use compare function to compare
        const Value& v1 = r1.Field(col);
        const Value& v2 = r2.Field(col);
        if (v1.Equal(v2) != Trivalent::TRI_TRUE) {
            return false;
        }
    }
    return true;
}

class RecordCmp {
   public:
    RecordCmp(const WindowSpec& window_spec, const std::vector<LogicalType>& types) : window_spec_(window_spec) {
        for(auto idx : window_spec_.partition_by) {
            const auto& func = intarkdb::FunctionContext::GetCompareFunction("=", {types[idx], types[idx]});
            partition_equal_funcs_.push_back(func);
        }
        for (auto idx : window_spec_.partition_by) {
            const auto& func = intarkdb::FunctionContext::GetCompareFunction("<", {types[idx], types[idx]});
            partition_cmp_funcs_.push_back(func);
        }

        for (auto& item : window_spec_.order_by) {
            size_t idx = item.idx;
            const auto& func = intarkdb::FunctionContext::GetCompareFunction("<", {types[idx], types[idx]});
            order_cmp_funcs_.push_back(func);
        }
    }

    bool operator()(const Record& lhs, const Record& rhs) {
        bool is_same_partition = true;
        for (size_t i = 0; i < window_spec_.partition_by.size(); i++) {
            // FIXME: compare function not found , just ignore it, need to handle it later
            if (partition_cmp_funcs_[i].has_value()) {
                const Value& left = lhs.Field(window_spec_.partition_by[i]);
                const Value& right = rhs.Field(window_spec_.partition_by[i]);
                if (left.IsNull() || right.IsNull()) {
                    continue;
                } else if (left.IsNull()) {
                    return true;
                } else if (right.IsNull()) {
                    return false;
                }
                if (partition_cmp_funcs_[i].value().func({left, right}).GetCastAs<int32_t>() != 0) {
                    return true;
                }
                if(partition_equal_funcs_[i].value().func({left, right}).GetCastAs<bool>() == false) {
                    is_same_partition = false;
                }
            }
        }
        if(!is_same_partition) {
            return false;
        }
        // sort in same partition
        for (size_t i = 0; i < window_spec_.order_by.size(); i++) {
            if (order_cmp_funcs_[i].has_value()) {
                const Value& left = lhs.Field(window_spec_.order_by[i].idx);
                const Value& right = rhs.Field(window_spec_.order_by[i].idx);
                if (left.IsNull() || right.IsNull()) {
                    continue;
                } else if (left.IsNull()) {
                    return window_spec_.order_by[i].is_null_first ? true : false;
                } else if (right.IsNull()) {
                    return window_spec_.order_by[i].is_null_first ? false : true;
                }
                auto r = order_cmp_funcs_[i].value().func({left, right}).GetCastAs<bool>();
                return window_spec_.order_by[i].is_asc ? r : !r;
            }
        }

        return false;
    }

   private:
    const WindowSpec& window_spec_;
    std::vector<std::optional<intarkdb::Function>> partition_equal_funcs_;
    std::vector<std::optional<intarkdb::Function>> partition_cmp_funcs_;
    std::vector<std::optional<intarkdb::Function>> order_cmp_funcs_;
};

class WindowFuncExpr {
   public:
    WindowFuncExpr() = default;
    virtual ~WindowFuncExpr() = default;

    virtual auto Evaluate(const Record& record) -> Value { return ValueFactory::ValueBigInt(idx_++); }

    virtual void ResetFrame() { idx_ = 1; }

   private:
    int64_t idx_{1};
};

class WindowExec : public PhysicalPlan {
   public:
    WindowExec(int64_t func_idx, const std::vector<size_t>& partition_by, const std::vector<WindowSortItem>& order_by,
               PhysicalPlanPtr child)
        : child_(child), func_idx_(func_idx) {
        window_spec_.frame_type = WindowFrameType::ROWS;
        window_spec_.partition_by = partition_by;
        window_spec_.order_by = order_by;
        window_fun_ = std::make_unique<WindowFuncExpr>();
        schema_ = child_->GetSchema();
    }
    virtual ~WindowExec() = default;

    virtual Schema GetSchema() const override { return schema_; };

    virtual auto Execute() const -> RecordBatch override { return RecordBatch(schema_); }

    virtual void Execute(RecordBatch& rb) override{};

    virtual std::vector<PhysicalPlanPtr> Children() const override { return {child_}; }

    virtual void ResetNext() override {
        child_->ResetNext();
        idx_ = 0;
        records_.clear();
        first_ = true;
        is_new_frame_ = true;
    }

    virtual std::string ToString() const override { return "WindowExec"; }

    virtual auto Next() -> std::tuple<Record, knl_cursor_t*, bool> override {
        if (first_) {
            init();
        }
        if (idx_ >= (int64_t)records_.size()) {
            ResetNext();
            return {Record(), nullptr, true};
        }
        if (is_new_frame_) {
            is_new_frame_ = false;
            window_fun_->ResetFrame();
        }
        const auto& r = records_[idx_++];
        std::vector<Value> values;
        int64_t column_count = r.ColumnCount();
        for (int64_t i = 0; i < column_count; i++) {
            if (i == func_idx_) {
                values.push_back(window_fun_->Evaluate(r));
            } else {
                values.push_back(r.Field(i));
            }
        }
        if (idx_ < (int64_t)records_.size()) {
            is_new_frame_ = !inSamePartition(r, records_[idx_]);
        }
        return {Record(std::move(values)), nullptr, false};
    }

    void init() {
        first_ = false;
        while (true) {
            auto&& [record, cursor, eof] = child_->Next();
            if (eof) {
                break;
            }
            records_.push_back(std::move(record));
        }

        // 按分区和排序键排序
        if (!window_spec_.partition_by.empty() || !window_spec_.order_by.empty()) {
            std::vector<LogicalType> types;
            const auto& columns = schema_.GetColumns();
            for (size_t i = 0; i < columns.size(); i++) {
                types.push_back(columns[i].GetLogicalType());
            }
            std::stable_sort(records_.begin(), records_.end(), RecordCmp(window_spec_, types));
        }
    }

   private:
    bool inSamePartition(const Record& r1, const Record& r2) const {
        return isSamePartition(r1, r2, window_spec_.partition_by);
    }

   private:
    bool first_{true};
    PhysicalPlanPtr child_;
    Schema schema_;
    std::vector<Record> records_;
    WindowSpec window_spec_;
    int64_t idx_{0};
    std::unique_ptr<WindowFuncExpr> window_fun_;
    int64_t func_idx_;
    bool is_new_frame_{true};
};
}  // namespace intarkdb
