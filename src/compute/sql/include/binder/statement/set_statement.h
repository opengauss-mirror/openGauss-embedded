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
* set_statement.h
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/include/binder/statement/set_statement.h
*
* -------------------------------------------------------------------------
*/
#pragma once

#include "binder/bound_statement.h"

enum class SetKind : uint8_t { SET = 0, RESET = 1 };

enum class SetName : int8_t {
    SET_NAME_INVALID = 0,
    SET_NAME_AUTOCOMMIT = 1,
    SET_NAME_MAXCONNECTIONS = 2,
    SET_NAME_SYNCHRONOUS_COMMIT = 3,
};

class SetStatement : public BoundStatement {
   public:
    SetStatement(SetKind kind_p, SetName set_name_p, Value value_p)
        : BoundStatement(StatementType::SET_STATEMENT),
        set_kind(kind_p),
        set_name(set_name_p),
        set_value(value_p) {}

   public:
    SetKind set_kind;
    SetName set_name;
    Value set_value;
};
