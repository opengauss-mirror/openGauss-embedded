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
* update_exec.cpp
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/planner/physical_plan/update_exec.cpp
*
* -------------------------------------------------------------------------
*/
#include "planner/physical_plan/update_exec.h"
#include <iostream>

auto UpdateExec::GetSchema() const -> Schema { return Schema{}; }

auto UpdateExec::Children() const -> std::vector<PhysicalPlanPtr> { return {children_}; }

auto UpdateExec::ToString() const -> std::string {return "Update";}

std::tuple<Record, knl_cursor_t *, bool> UpdateExec::Next(){
  while (true) {
        auto [r, cur, eof] = children_[0]->Next();
        if (eof) {
            break;
        }

        int column_count = target_expr_.size();
#ifdef _MSC_VER
        std::vector<exp_column_def_t> column_list(column_count);  // 使用 std::vector
        std::vector<Value> values(column_count);                  // 使用 std::vector
#else
        exp_column_def_t column_list[column_count];
        std::vector<Value> values;
        values.resize(column_count);
#endif
        int index = 0;
        for (auto &[col, target_expr] : target_expr_) {
            exp_column_def_t col_def = col.GetRaw();  
            auto val = target_expr->Evaluate(r);
            if(!val.IsNull() && col_def.col_type != val.GetType()) {
                try {
                    val = DataType::GetTypeInstance(col_def.col_type)->CastValue(val);
                } catch (intarkdb::Exception& e) {
                    if (e.type == ExceptionType::OUT_OF_RANGE) {
                        throw intarkdb::Exception(ExceptionType::OUT_OF_RANGE,
                            fmt::format("column ({}), value ({}) out of range!", col_def.name.str, val.ToString()));
                    }
                }
            }
            CheckValueWithDef(val,col_def);
            values[index] = val;
            col_def.crud_value.str = (char*)values[index].GetRawBuff();
            col_def.crud_value.len = values[index].Size();
            column_list[index++] = col_def;
        }
#ifdef _MSC_VER
        source_.Update(column_count, column_list.data());
#else
        source_.Update(column_count, column_list);
#endif      
    }
    return {{}, nullptr, true};
}

auto UpdateExec::Execute() const -> RecordBatch {
    auto result = RecordBatch(Schema());
    int64_t effect_row = 0;
    while (true) {
        auto [r, cur, eof] = children_[0]->Next();
        if (eof) {
            break;
        }

        int column_count = target_expr_.size();
#ifdef _MSC_VER
        std::vector<exp_column_def_t> column_list(column_count);  // 使用 std::vector
        std::vector<Value> values(column_count);                  // 使用 std::vector
#else
        exp_column_def_t column_list[column_count];
        std::vector<Value> values;
        values.resize(column_count);
#endif
        int index = 0;
        for (auto &[col, target_expr] : target_expr_) {
            exp_column_def_t col_def = col.GetRaw();
            auto val = target_expr->Evaluate(r);
            if(!val.IsNull() && col_def.col_type != val.GetType()) {
                try {
                    val = DataType::GetTypeInstance(col_def.col_type)->CastValue(val);
                } catch (intarkdb::Exception& e) {
                    if (e.type == ExceptionType::OUT_OF_RANGE) {
                        throw intarkdb::Exception(ExceptionType::OUT_OF_RANGE,
                            fmt::format("column ({}), value ({}) out of range!", col_def.name.str, val.ToString()));
                    }
                }
            }
            CheckValueWithDef(val,col_def);
            values[index] = val;
            col_def.crud_value.str = (char*)values[index].GetRawBuff();
            col_def.crud_value.len = values[index].Size();
            column_list[index++] = col_def;
        }
#ifdef _MSC_VER
        source_.Update(column_count, column_list.data());
#else
        source_.Update(column_count, column_list);
#endif
        effect_row++;
    }
    result.SetEffectRow(effect_row);
    return result;
}