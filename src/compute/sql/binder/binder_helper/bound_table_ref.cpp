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
 * bound_table_ref.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/binder/bound_table_ref.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "binder/bound_table_ref.h"

auto DataSourceTypeToString(DataSourceType type) -> std::string_view {
    switch (type) {
        case DataSourceType::INVALID:
            return "Invalid";
        case DataSourceType::BASE_TABLE:
            return "BaseTable";
        case DataSourceType::JOIN_RESULT:
            return "Join";
        case DataSourceType::DUAL:
            return "Empty";
        case DataSourceType::SUBQUERY_RESULT:
            return "Subquery";
        case DataSourceType::VALUE_CLAUSE:
            return "ValueClause";
        default:
            return "Unknown";
    }
}
