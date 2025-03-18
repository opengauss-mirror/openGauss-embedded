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
 * prepare_statement.h
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/include/main/prepare_statement.h
 *
 * -------------------------------------------------------------------------
 */
#pragma once

#include "binder/bound_statement.h"
#include "planner/expressions/column_param_expression.h"
#include "planner/physical_plan/physical_plan.h"

class Connection;

class PreparedStatement {
   public:
    EXPORT_API PreparedStatement(const std::string& query, uint32_t n_param, Connection* conn,
                                 std::unique_ptr<BoundStatement> unbound_statement,
                                 std::shared_ptr<PhysicalPlan> physical_plan)
        : sql_(query),
          n_param_(n_param),
          conn_(conn),
          unbound_statement_(std::move(unbound_statement)),
          physical_plan_(std::move(physical_plan)) {}

    EXPORT_API explicit PreparedStatement(bool has_error, std::string error_msg)
        : sql_(std::string()),
          n_param_(0),
          conn_(nullptr),
          unbound_statement_(nullptr),
          physical_plan_(nullptr),
          has_error_(has_error),
          error_msg_(error_msg) {}

    EXPORT_API ~PreparedStatement(){};

    EXPORT_API bool HasError() { return has_error_; }
    EXPORT_API std::string& ErrorMsg() { return error_msg_; }
    EXPORT_API uint32_t ParamCount() { return n_param_; }
    EXPORT_API std::string& Original_sql() { return sql_; }
    EXPORT_API std::string ExpandedSQL();

    EXPORT_API void SetNeedResultSetEx(bool need) { is_need_result_ex = need; }

    void SetLimitRowsEx(uint64_t limit) { limit_rows_ex = limit; }

    // !Execute
    EXPORT_API std::unique_ptr<RecordBatch> Execute(const std::vector<Value>& values);

    std::vector<const ColumnParamExpression*> params;

   public:
    void SetIsRecordBatchSelect(bool is_select) { is_recordbatch_select = is_select; }
    bool IsRecordBatchSelect() { return is_recordbatch_select; }
    Connection* GetConnection() { return conn_; }

   private:
    void ResetNext(PhysicalPlanPtr plan);

   private:
    std::string sql_;
    uint32_t n_param_;
    Connection* conn_;
    std::unique_ptr<BoundStatement> unbound_statement_;
    std::shared_ptr<PhysicalPlan> physical_plan_;

    bool has_error_ = false;
    std::string error_msg_;

    bool is_need_result_ex = false;
    // if need limit rows
    uint64_t limit_rows_ex = 0;

    bool is_recordbatch_select = false;
};
