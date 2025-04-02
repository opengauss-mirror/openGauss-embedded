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
* drop_statement.h
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/include/binder/statement/drop_statement.h
*
* -------------------------------------------------------------------------
*/
#pragma once

#include "binder/bound_statement.h"
#include "type/type_system.h"

class DropStatement : public BoundStatement {
   public:
    explicit DropStatement(const std::string& name, bool if_exists, ObjectType type)
        : BoundStatement(StatementType::DROP_STATEMENT), name(name), if_exists(if_exists), type(type) {}

    auto ToString() const -> std::string override { return "DropStatement"; }

    std::string name;
    bool if_exists = false;
    ObjectType type;
};