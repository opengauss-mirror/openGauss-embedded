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
* create_view.cpp
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/binder/statement/create_view.cpp
*
* -------------------------------------------------------------------------
*/
#include "binder/statement/create_view.h"

CreateViewStatement::CreateViewStatement(std::string viewName, std::unique_ptr<BoundStatement> stmt)
   : BoundStatement(StatementType::CREATE_VIEW_STATEMENT), viewName_(std::move(viewName))
   , stmt_(std::move(stmt)) { }