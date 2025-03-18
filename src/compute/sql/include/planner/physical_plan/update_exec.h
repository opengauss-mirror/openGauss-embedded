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
 * update_exec.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/planner/physical_plan/update_exec.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include <memory>

#include "datasource/table_datasource.h"
#include "planner/expressions/expression.h"
#include "planner/logical_plan/delete_plan.h"
#include "planner/physical_plan/physical_plan.h"

class UpdateExec : public PhysicalPlan {
   public:
    explicit UpdateExec(TableDataSource& source, std::vector<PhysicalPlanPtr> children,
                        std::vector<std::pair<Column, std::unique_ptr<Expression>>>& target_expr)
        : source_(source), children_(std::move(children)), target_expr_(std::move(target_expr)) {}

    ~UpdateExec() {}

    virtual Schema GetSchema() const override;
    virtual std::vector<PhysicalPlanPtr> Children() const override;
    virtual std::string ToString() const override;
    virtual auto Next() -> std::tuple<Record, knl_cursor_t*, bool> override;

    virtual auto Execute() const -> RecordBatch override;

   private:
    TableDataSource& source_;
    const std::vector<PhysicalPlanPtr> children_;
    std::vector<std::pair<Column, std::unique_ptr<Expression>>> target_expr_;
};
