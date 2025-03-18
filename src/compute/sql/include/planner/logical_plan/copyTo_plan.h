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
* copyTo_plan.h
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/include/planner/logical_plan/copyTo_plan.h
*
* -------------------------------------------------------------------------
*/
#pragma once

#include "binder/bound_statement.h"
#include "datasource/table_datasource.h"
#include "planner/logical_plan/logical_plan.h"

class CopyToLogicalPlan : public LogicalPlan {
   public:
    CopyToLogicalPlan(std::string filePath, LogicalPlanPtr child, std::unordered_map<std::string,std::vector<Value>>&& options)
        : LogicalPlan(LogicalPlanType::Copy_To), file_path_(std::move(filePath)), child_(std::move(child)), options_(std::move(options)) {}

    virtual const Schema& GetSchema() const override;
    virtual std::vector<LogicalPlanPtr> Children() const override;
    virtual void SetChildren(const std::vector<LogicalPlanPtr>& children) override { child_ = children[0]; }
    virtual std::string ToString() const override;

    std::string file_path_;
    LogicalPlanPtr child_;
    std::unordered_map<std::string,std::vector<Value>> options_;
};
