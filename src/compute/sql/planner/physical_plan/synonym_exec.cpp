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
* synonym_exec.cpp
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/planner/physical_plan/synonym_exec.cpp
*
* -------------------------------------------------------------------------
*/
#include "planner/physical_plan/synonym_exec.h"

#include "storage/gstor/gstor_executor.h"
#include "storage/gstor/zekernel/kernel/common/knl_session.h"

auto SynonymExec::Execute() const -> RecordBatch {
    knl_session_t *session = EC_SESSION(catalog_.GetStorageHandle()->handle);

    knl_synonym_def_t synonym_def;
    text_t owner = { (char *)synonym_statement.synonym_schema.c_str(), (uint32_t)synonym_statement.synonym_schema.length() };
    text_t name = { (char *)synonym_statement.synonym_name.c_str(), (uint32_t)synonym_statement.synonym_name.length() };
    text_t table_owner = { (char *)synonym_statement.table_schema.c_str(), (uint32_t)synonym_statement.table_schema.length() };
    text_t table_name = { (char *)synonym_statement.table_name.c_str(), (uint32_t)synonym_statement.table_name.length() };
    synonym_def.owner = owner;
    synonym_def.name = name;
    synonym_def.table_owner = table_owner;
    synonym_def.table_name = table_name;
    synonym_def.ref_dc_type = DICT_TYPE_TABLE;

    // storage
    if (knl_create_synonym(session, &synonym_def) != GS_SUCCESS) {
        int32_t err_code;
        const char *message = nullptr;
        cm_get_error(&err_code, &message, NULL);
        std::string err_msg = fmt::format("create synonym fail!! errno = {}, message = {}", err_code, message);
        cm_reset_error();
        GS_LOG_RUN_INF("%s", err_msg.c_str());
        throw std::runtime_error(err_msg);
    }

    return RecordBatch(Schema());
}
