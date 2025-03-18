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
* compare_type.cpp
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/common/compare_type.cpp
*
* -------------------------------------------------------------------------
*/
#include "common/compare_type.h"

namespace intarkdb {

ComparisonType ToComparisonType(const std::string& name) {
    if (name == "=" || name == "==") {
        return ComparisonType::Equal;
    } else if (name == "<>" || name == "!=") {
        return ComparisonType::NotEqual;
    } else if (name == "<") {
        return ComparisonType::LessThan;
    } else if (name == ">") {
        return ComparisonType::GreaterThan;
    } else if (name == "<=") {
        return ComparisonType::LessThanOrEqual;
    } else if (name == ">=") {
        return ComparisonType::GreaterThanOrEqual;
    }
    return ComparisonType::InValid;
}

std::string CompareTypeToString(ComparisonType type) {
    switch (type) {
        case ComparisonType::Equal:
            return "=";
        case ComparisonType::NotEqual:
            return "<>";
        case ComparisonType::LessThan:
            return "<";
        case ComparisonType::GreaterThan:
            return ">";
        case ComparisonType::LessThanOrEqual:
            return "<=";
        case ComparisonType::GreaterThanOrEqual:
            return ">=";
        default:
            return "invalid";
    }
    return "";
}

ComparisonType RevertComparisonType(ComparisonType type) {
    switch (type) {
        case ComparisonType::Equal:
            return ComparisonType::NotEqual;
        case ComparisonType::NotEqual:
            return ComparisonType::Equal;
        case ComparisonType::LessThan:
            return ComparisonType::GreaterThanOrEqual;
        case ComparisonType::LessThanOrEqual:
            return ComparisonType::GreaterThan;
        case ComparisonType::GreaterThan:
            return ComparisonType::LessThanOrEqual;
        case ComparisonType::GreaterThanOrEqual:
            return ComparisonType::LessThan;
        default:
            return ComparisonType::InValid;
    }
}

ComparisonType FilpComparisonType(ComparisonType type) {
    ComparisonType new_type = type;
    switch (type) {
        case ComparisonType::Equal:
        case ComparisonType::NotEqual:
            break;
        case ComparisonType::LessThan:
            new_type = ComparisonType::GreaterThan;
            break;
        case ComparisonType::LessThanOrEqual:
            new_type = ComparisonType::GreaterThanOrEqual;
            break;
        case ComparisonType::GreaterThan:
            new_type = ComparisonType::LessThan;
            break;
        case ComparisonType::GreaterThanOrEqual:
            new_type = ComparisonType::LessThanOrEqual;
            break;
        default:
            break;
    }
    return new_type;
}

std::string RevertComparisonOpName(const std::string& name) {
    return CompareTypeToString(RevertComparisonType(ToComparisonType(name)));
}
// 比较操作符是否合法
bool IsCompareOp(const std::string& name) {
    auto r = ToComparisonType(name);
    return r != ComparisonType::InValid;
}

}  // namespace intarkdb
