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
* delete_exec.cpp
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/planner/physical_plan/delete_exec.cpp
*
* -------------------------------------------------------------------------
*/
#include "planner/physical_plan/delete_exec.h"

auto DeleteExec::GetSchema() const -> Schema { return Schema{}; }

auto DeleteExec::Children() const -> std::vector<PhysicalPlanPtr> { return {child_}; }

auto DeleteExec::ToString() const -> std::string {return "Delete";}

std::tuple<Record, knl_cursor_t *, bool> DeleteExec::Next(){
  while (true) {
        auto [r, cur, eof] = child_->Next();
        if (eof) {
            break;
        }
        source_.Delete();  
    }
    return {{}, nullptr, true};
}

auto DeleteExec::Execute() const -> RecordBatch {
    auto result = RecordBatch(Schema());
    int64_t effect_row = 0;
    while (true) {
        auto [r, cur, eof] = child_->Next();
        if (eof) {
            break;
        }
        source_.Delete();
        effect_row++;
    }
    result.SetEffectRow(effect_row);
    return result;
}