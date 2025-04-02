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
 * bound_subquery.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/binder/table_ref/bound_subquery.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include "binder/bound_table_ref.h"
#include "binder/statement/select_statement.h"

class BoundSubquery : public BoundTableRef {
   public:
    explicit BoundSubquery(std::unique_ptr<SelectStatement> subquery, std::string alias)
        : BoundTableRef(DataSourceType::SUBQUERY_RESULT), subquery(std::move(subquery)), alias(std::move(alias)) {}

    auto ToString() const -> std::string override { return "BoundSubquery"; }

    std::unique_ptr<SelectStatement> subquery;

    std::string alias;
};
