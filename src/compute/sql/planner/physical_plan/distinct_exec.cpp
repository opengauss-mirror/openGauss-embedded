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
 * distinct_exec.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/planner/physical_plan/distinct_exec.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "planner/physical_plan/distinct_exec.h"

Schema DistinctExec::GetSchema() const { return schema_; }

auto DistinctExec::Next() -> std::tuple<Record, knl_cursor_t*, bool> {
    while (true) {
        // clear , ready for next time, avoid allocate memory again
        auto&& [record, cursor, eof] = child_->Next();
        if (eof) {
            set_.clear();
            return {Record{}, nullptr, true};
        }
        std::vector<Value> values;
        if (exprs_.size() > 0) {
            values.reserve(exprs_.size());
            for (size_t i = 0; i < exprs_.size(); ++i) {
                values.push_back(exprs_[i]->Evaluate(record));
            }
        } else {
            const auto& cols = schema_.GetColumnInfos();
            values.reserve(cols.size());
            for (size_t i = 0; i < cols.size(); ++i) {
                values.push_back(record.Field(cols[i].slot));
            }
        }

        DistinctKey key(values);

        if (set_.find(key) == set_.end()) {
            set_.insert(std::move(key));
            return {std::move(record), cursor, false};
        }
    }
    return {Record{}, nullptr, false};
}
