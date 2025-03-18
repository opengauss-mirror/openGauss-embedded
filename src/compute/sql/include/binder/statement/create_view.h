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
* create_view.h
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/include/binder/statement/create_view.h
*
* -------------------------------------------------------------------------
*/
#pragma once

#include "binder/bound_statement.h"
#include "binder/statement/select_statement.h"
#include "catalog/column.h"

class CreateViewStatement : public BoundStatement {
   public:
    explicit CreateViewStatement(std::string viewName, std::unique_ptr<BoundStatement> stmt);
    ~CreateViewStatement(){};

    const std::string& getViewName() { return viewName_; }
    std::unique_ptr<BoundStatement> getBoundSTMT() { return std::move(stmt_); }

    bool ignore_conflict{false};

   private:
    std::string viewName_;
    std::unique_ptr<BoundStatement> stmt_;
};
