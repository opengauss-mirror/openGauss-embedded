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
* physical_plan.cpp
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/planner/physical_plan/physical_plan.cpp
*
* -------------------------------------------------------------------------
*/
#include "planner/physical_plan/physical_plan.h"

// 递归打印美化输出
std::string format(const PhysicalPlan &plan, int indent) {
    std::string indent_str(indent, '\t');
    std::vector<char> out;
    fmt::format_to(std::back_inserter(out), "{}{}\n", indent_str, plan.ToString());
    for (const auto &child : plan.Children()) {
        fmt::format_to(std::back_inserter(out), "{}", format(*child, indent + 1));
    }
    return std::string(out.cbegin(), out.cend());
}