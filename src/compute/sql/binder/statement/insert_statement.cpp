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
* insert_statement.cpp
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/binder/statement/insert_statement.cpp
*
* -------------------------------------------------------------------------
*/

#include "binder/statement/insert_statement.h"

#include <fmt/core.h>
#include <fmt/ranges.h>

#include <string>

#include "binder/table_ref/bound_base_table.h"
#include "binder/statement/select_statement.h"

auto InsertStatement::ToString() const -> std::string {
    return fmt::format("BoundInsert {{\n table={},select_list={}} \n}}", table_->ToString(), select_->ToString());
}
