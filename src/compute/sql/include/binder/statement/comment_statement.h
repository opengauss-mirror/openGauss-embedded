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
* comment_statement.h
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/include/binder/statement/comment_statement.h
*
* -------------------------------------------------------------------------
*/
#pragma once

#include <string>

#include "binder/bound_statement.h"
#include "type/type_system.h"

const uint8_t OBJ_LIST_1ARG = 1;
const uint8_t OBJ_LIST_2ARG = 2;
const uint8_t OBJ_LIST_3ARG = 3;

class CommentStatement : public BoundStatement {
  public:
    explicit CommentStatement() : BoundStatement(StatementType::COMMENT_STATEMENT){};

  public:
    std::string user_name;
    std::string table_name;
    std::string column_name;
    std::string comment;
    
    ObjectType object_type;
};