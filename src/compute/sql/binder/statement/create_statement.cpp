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
* create_statement.cpp
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/binder/statement/create_statement.cpp
*
* -------------------------------------------------------------------------
*/
#include "binder/statement/create_statement.h"

CreateStatement::CreateStatement(std::string table, std::vector<Column> columns)
    : BoundStatement(StatementType::CREATE_STATEMENT), tablename_(std::move(table)), columns_(std::move(columns)) {
    memset_s(&part_def_, sizeof(exp_part_obj_def_t), 0, sizeof(exp_part_obj_def_t));
}

CreateStatement::CreateStatement(std::string table)
    : BoundStatement(StatementType::CREATE_STATEMENT), tablename_(std::move(table)) {
    memset_s(&part_def_, sizeof(exp_part_obj_def_t), 0, sizeof(exp_part_obj_def_t));
}

CreateStatement::CreateStatement(std::string_view table)
    : BoundStatement(StatementType::CREATE_STATEMENT), tablename_(table) {
    memset_s(&part_def_, sizeof(exp_part_obj_def_t), 0, sizeof(exp_part_obj_def_t));
}
