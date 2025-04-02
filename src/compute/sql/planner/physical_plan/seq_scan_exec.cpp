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
 * seq_scan_exec.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/planner/physical_plan/seq_scan_exec.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "planner/physical_plan/seq_scan_exec.h"

#include "planner/logical_plan/logical_plan.h"

Schema SeqScanExec::GetSchema() const { return source_->GetSchema(); }

std::string SeqScanExec::ToString() const {
    if (projection_.empty()) {
        return fmt::format("SeqScan: table={} projection=None", source_->GetTableName());
    }
    return fmt::format("SeqScan: table={} projection={}", source_->GetTableName(), projection_);
}

void SeqScanExec::init() { source_->Init(); }

auto SeqScanExec::Next() -> std::tuple<Record, knl_cursor_t*, bool> {
    if (!init_) {
        init();
        init_ = true;
    }
    while (true) {
        auto&& [row, _, fin] = source_->Next();
        if (fin) {
            init_ = false;
            return std::make_tuple(Record(), nullptr, true);
        }
        if(row == nullptr) {
            row = std::make_unique<intarkdb::RowContainer>();
        }
        Record r(std::move(row));
        bool pass = true;
        for (auto& p : predicates_) {
            Value expr_value = p->Evaluate(r);
            if (expr_value.IsNull()) {
                pass = false;
                break;
            }
            if (static_cast<Trivalent>(expr_value.GetCastAs<uint32_t>()) != Trivalent::TRI_TRUE) {
                pass = false;
                break;
            }
        }
        if (!pass) {
            continue;
        }
        return std::make_tuple(std::move(r), nullptr, false);
    }
}
