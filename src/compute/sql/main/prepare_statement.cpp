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
 * prepare_statement.cpp
 *
 * IDENTIFICATION
 * openGauss-embedded/src/compute/sql/main/prepare_statement.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "main/prepare_statement.h"
#include "main/connection.h"
#include "storage/gstor/zekernel/common/cm_log.h"

std::unique_ptr<RecordBatch> PreparedStatement::Execute(const std::vector<Value>& values) {
    GS_LOG_RUN_INF("[DB:%s][Execute SQL]:%s", conn_->GetStorageInstance().lock()->GetDbPath().c_str(), sql_.c_str());
    if (LOG_RUN_INF_ON) {
        for (auto& s : values) {
            GS_LOG_RUN_INF("[Bind VALUE]:%s", s.ToString().c_str());
        }
    }
    // statistical time
    struct timeval tv_begin;
    cm_gettimeofday(&tv_begin);
    // end statistical time

    if (!physical_plan_) {
        return conn_->Query(sql_.c_str());
    }

    ResetNext(physical_plan_);

    std::unique_ptr<RecordBatch> result = std::make_unique<RecordBatch>(physical_plan_->GetSchema());

    try {
        if (values.size() != n_param_) {
            throw std::invalid_argument("Parameter count mismatch for prepared statement.");
        }

        // bind param
        for (auto& param : params) {
            param->InitParam(values);
        }

        switch (unbound_statement_->Type()) {
            case StatementType::SELECT_STATEMENT: {
                result->SetRecordBatchType(RecordBatchType::Select);
                uint64_t row_count = 0;
                while (true) {
                    if (limit_rows_ex > 0 && row_count >= limit_rows_ex) {
                        break;
                    }
                    auto&& [r, _, eof] = physical_plan_->Next();
                    if (eof) {
                        break;
                    }
                    result->AddRecord(std::move(r));
                    row_count++;
                }
                break;
            }
            case StatementType::INSERT_STATEMENT: {
                physical_plan_->SetAutoCommit(conn_->IsAutoCommit());
                physical_plan_->SetNeedResultSetEx(is_need_result_ex);
                physical_plan_->Execute(*result);
                result->SetRecordBatchType(RecordBatchType::Insert);
                if (conn_->IsAutoCommit()) {
                    gstor_commit(((db_handle_t*)conn_->GetStorageHandle())->handle);
                }
                break;
            }
            case StatementType::DELETE_STATEMENT:
            case StatementType::UPDATE_STATEMENT: {
                *result = physical_plan_->Execute();
                if (conn_->IsAutoCommit()) {
                    gstor_commit(((db_handle_t*)conn_->GetStorageHandle())->handle);
                }
                break;
            }
            case StatementType::CREATE_STATEMENT:
            case StatementType::INDEX_STATEMENT:
            case StatementType::SEQUENCE_STATEMENT:
            case StatementType::TRANSACTION_STATEMENT:
            case StatementType::SET_STATEMENT:
            case StatementType::ALTER_STATEMENT:
            case StatementType::SHOW_STATEMENT:
            case StatementType::DROP_STATEMENT:
            case StatementType::CTAS_STATEMENT:
            case StatementType::CREATE_VIEW_STATEMENT:
            case StatementType::COPY_STATEMENT:
            case StatementType::CHECKPOINT_STATEMENT:
            case StatementType::EXPLAIN_STATEMENT:
            case StatementType::COMMENT_STATEMENT:
                return conn_->Query(sql_.c_str());
            default:
                std::invalid_argument("unspported statement type");
        }
    } catch (const std::exception& e) {
        if (conn_->IsAutoCommit()) {
            conn_->Rollback();
        }
        result->SetRetCode(-1);
        result->SetRetMsg(e.what());
    }

    // statistical time
    struct timeval tv_end;
    cm_gettimeofday(&tv_end);
    auto nsec = tv_end.tv_sec - tv_begin.tv_sec;
    if (nsec > LONG_SQL_TIME) {
        auto nusec = tv_end.tv_usec - tv_begin.tv_usec;
        GS_LOG_RUN_WAR("[SQL_TIME:(%ld秒)(%ld微秒)][Query SQL]:%s", nsec, nusec, sql_.c_str());
        for (auto& s : values) {
            GS_LOG_RUN_WAR("[Bind VALUE]:%s", s.ToString().c_str());
        }
    }
    // end statistical time

#ifdef _MSC_VER
    GS_LOG_RUN_INF("[SessionSQL]PID:%lu TableType:%s SID:%d", GetCurrentThreadId(),result->stmt_props.GetStrTableTypes().c_str(), conn_->GetUser().GetSID());
#else
    GS_LOG_RUN_INF("[SessionSQL]PID:%lu TableType:%s SID:%d", pthread_self(), result->stmt_props.GetStrTableTypes().c_str(),
                   conn_->GetUser().GetSID());
#endif
    return result;
}

void PreparedStatement::ResetNext(PhysicalPlanPtr plan) {
    plan->ResetNext();
    auto childs = plan->Children();
    for (auto child : childs) {
        if (child) {
            ResetNext(child);
        }
    }
}

std::string PreparedStatement::ExpandedSQL() {
    // replace placeholder with actual value
    std::ostringstream oss;
    std::string_view tmp = sql_;
    for (size_t i = 0; i < params.size(); ++i) {
        oss << tmp.substr(0, tmp.find('?')) << params[i]->val_.ToSQLString();
        // 移动到下一个占位符
        tmp = tmp.substr(tmp.find('?') + 1);
    }
    oss << tmp;
    return oss.str();
}
