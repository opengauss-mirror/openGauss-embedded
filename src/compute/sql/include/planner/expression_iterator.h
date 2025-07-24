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
 * expression_iterator.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/planner/expression_iterator.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <functional>
#include <memory>

#include "binder/bound_expression.h"
#include "binder/bound_table_ref.h"
#include "binder/statement/select_statement.h"
#include "binder/table_ref/bound_base_table.h"

class ExpressionIterator {
   public:
    static void EnumerateChildren(BoundExpression &expression,
                                  const std::function<void(std::unique_ptr<BoundExpression> &child)> &callback);
    static void EnumerateChildren(BoundExpression &expression,
                                  const std::function<void(BoundExpression &child)> &callback);
    static void EnumerateExpression(BoundExpression &expr, const std::function<void(BoundExpression &child)> &callback);

    static void EnumerateExpression(std::unique_ptr<BoundExpression> &expr,
                                    const std::function<void(std::unique_ptr<BoundExpression> &child)> &callback);

    static void EnumerateTableRefChildren(BoundTableRef &ref,
                                          const std::function<void(BoundExpression &child)> &callback);

    static void EnumerateSelectStatement(SelectStatement &stmt,
                                         const std::function<void(BoundExpression &child)> &callback);
};

class TableRefIterator {
   public:
    static void EnumerateTableRef(BoundTableRef &ref, const std::function<void(BoundBaseTable &tbl)> &callback);
    static void EnumerateTableRef(SelectStatement &stmt, const std::function<void(BoundBaseTable &tbl)> &callback);
};
