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
* transaction_exec.cpp
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/planner/physical_plan/transaction_exec.cpp
*
* -------------------------------------------------------------------------
*/
#include "planner/physical_plan/transaction_exec.h"

#include "binder/statement/transaction_statement.h"
#include "storage/db_handle.h"

auto TransactionExec::Execute() const -> RecordBatch {
    RecordBatch rb(schema_);
    int status = -1;
    switch (type) {
        case TransactionType::BEGIN_TRANSACTION:
            status = gstor_commit(((db_handle_t*)handle)->handle);
            status = gstor_begin(((db_handle_t*)handle)->handle);
            break;
        case TransactionType::COMMIT:
            status = gstor_commit(((db_handle_t*)handle)->handle);
            break;
        case TransactionType::ROLLBACK:
            status = gstor_rollback(((db_handle_t*)handle)->handle);
            break;
        default:
            throw std::invalid_argument(fmt::format("physical plan Transaction type {} not implemented yet",
                                                    static_cast<std::underlying_type_t<TransactionType>>(type)));
    }
    if(status != GS_SUCCESS) {
        int32_t err_code;
        const char *message = nullptr;
        cm_get_error(&err_code, &message, NULL);
        std::string err_msg = fmt::format("Transaction fail!! errno = {}, message = {}",
                                    err_code, message);
        cm_reset_error();
        GS_LOG_RUN_INF("%s", err_msg.c_str());
        rb.SetRetMsg(err_msg);
    }
    rb.SetRetCode(status);
    return rb;
}