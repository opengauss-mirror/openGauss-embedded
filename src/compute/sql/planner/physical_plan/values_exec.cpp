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
* values_exec.cpp
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/planner/physical_plan/values_exec.cpp
*
* -------------------------------------------------------------------------
*/
#include "planner/physical_plan/values_exec.h"

#include "binder/expressions/bound_constant.h"

auto ValuesExec::GetSchema() const -> Schema { return schema_; }

auto ValuesExec::Execute() const -> RecordBatch {
    RecordBatch rb(schema_);
    Record r;  // 只是用于占位
    for (auto &row : insert_values_) {
        std::vector<Value> val;
        for (size_t i = 0; i < row.size(); i++) {
            auto &col = row[i];
            val.push_back(col->Evaluate(r));
        }
        rb.AddRecord(Record(std::move(val)));
    }
    return rb;
}

void ValuesExec::Execute(RecordBatch &rb) {
    rb = RecordBatch(schema_);
    Record r;
    for (auto &row : insert_values_) {
        std::vector<Value> val;
        for (size_t i = 0; i < row.size(); i++) {
            auto &col = row[i];
            val.push_back(col->Evaluate(r));
        }
        rb.AddRecord(Record(std::move(val)));
    }
}

auto ValuesExec::Next() -> std::tuple<Record, knl_cursor_t *, bool> {
    Record rd;
    bool eof = true;
    if (idx_ < insert_values_.size()) {
        eof = false;
        std::vector<Value> val;
        auto &row = insert_values_[idx_++];
        val.reserve(row.size());
        for (size_t i = 0; i < row.size(); ++i) {
            auto &col = row[i];
            val.push_back(std::move(col->Evaluate(rd)));
        }
        rd = Record(std::move(val));
    }
    return {std::move(rd), nullptr, eof};
}

auto ValuesExec::Children() const -> std::vector<PhysicalPlanPtr> { return {child_}; }

auto ValuesExec::ToString() const -> std::string {
    // TODO
    return "ValuesExec";
}
