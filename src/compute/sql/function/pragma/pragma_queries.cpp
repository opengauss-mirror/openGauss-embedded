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
* pragma_queries.cpp
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/function/pragma/pragma_queries.cpp
*
* -------------------------------------------------------------------------
*/
#include "function/pragma/pragma_queries.h"

#include <fmt/core.h>
#include <fmt/format.h>

#include <algorithm>
#include <iomanip>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>

#include "catalog/catalog.h"
#include "catalog/table_info.h"
#include "common/string_util.h"
#include "common/util.h"
#include "type/type_system.h"

std::string PragmaShowTables(uint32 user_id, uint32 space_id) {
    std::string ts_tables = "''";
    return fmt::format("SELECT \"NAME\" FROM \"SYS_TABLES\" WHERE \"USER#\" = {} and \"SPACE#\" = {} "
                      "and NAME not in ({})", user_id, space_id, ts_tables);
}


std::string PragmaDescribeTable(uint32 user_id, uint32 table_id) {
    std::string sql = R"(SELECT 
                            _to_null_string("SYS_COLUMNS"."NAME") AS 'col_name', 
                            _to_type_string("DATATYPE") AS {}, 
                            _to_null_string("BYTES") AS 'col_size',
                            _to_empty_string("PRECISION") AS {}, 
                            _to_empty_string("SCALE") AS {}, 
                            _boolean_to_string("NULLABLE") AS {}, 
                            _default_value_to_string("DEFAULT_TEXT","DATATYPE") AS {} ,
                            _format_key_to_string("IS_PRIMARY","IS_UNIQUE") as 'key',
                            _is_auto_increment("SYS_COLUMNS"."FLAGS") AS extra,
                            "COMMENT#" AS 'comment',
                            FROM 
                              ( 
                                SELECT * FROM "SYS_COLUMNS" 
                                WHERE "TABLE#" = {} AND "SYS_COLUMNS"."USER#" = {} AND _is_valid_col("SYS_COLUMNS"."FLAGS") ORDER BY "ID"
                              )  AS "SYS_COLUMNS"
                            LEFT JOIN "SYS_INDEXES" ON "SYS_COLUMNS"."TABLE#" = "SYS_INDEXES"."TABLE#" AND "SYS_COLUMNS"."ID" = "SYS_INDEXES"."COL_LIST"
                            LEFT JOIN "SYS_COMMENT" ON "SYS_COLUMNS"."TABLE#" = "SYS_COMMENT"."TABLE#" AND "SYS_COLUMNS"."ID" = "SYS_COMMENT"."COLUMN#"
                        )";
    return fmt::format(sql, COLUMN_ATTR_DATATYPE, COLUMN_ATTR_PRECISION, COLUMN_ATTR_SCALE, COLUMN_ATTR_NULLABLE,
                       COLUMN_ATTR_DEFAULT, table_id, user_id);
}

