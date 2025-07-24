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
* drop_exec.cpp
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/planner/physical_plan/drop_exec.cpp
*
* -------------------------------------------------------------------------
*/
#include "planner/physical_plan/drop_exec.h"

#include "storage/gstor/gstor_executor.h"
#include "storage/gstor/zekernel/kernel/common/knl_session.h"

auto DropExec::Execute() const -> RecordBatch {
    RecordBatch rb(schema_);
    drop_def_t drop_info = {0};
    drop_info.name = (char*)name.c_str();
    drop_info.if_exists = (int)if_exists;
    switch (type) {
        case ObjectType::TABLE_ENTRY:
            drop_info.type = DROP_TYPE_TABLE;
            break;
        case ObjectType::INDEX_ENTRY:
            drop_info.type = DROP_TYPE_INDEX;
            break;
        case ObjectType::SEQUENCE_ENTRY:
            drop_info.type = DROP_TYPE_SEQUENCE;
            break;
        case ObjectType::VIEW_ENTRY:
            drop_info.type = DROP_TYPE_VIEW;
            break;
        case ObjectType::SYNONYM_ENTRY:
            return DropSynonym();
        default:
            throw std::invalid_argument(fmt::format("physical plan drop type not implemented yet"));
    }

    auto storage = catalog_.GetStorageHandle();
    auto user = catalog_.GetUser();
    auto status = gstor_drop(storage->handle, (char *)user.c_str(), &drop_info);
    if (status != GS_SUCCESS) {
        int32_t err_code;
        const char *message = nullptr;
        cm_get_error(&err_code, &message, NULL);
        std::string err_msg =  message;
        cm_reset_error();
        GS_LOG_RUN_INF("%s", err_msg.c_str());
        rb.SetRetMsg(err_msg);
    }
    rb.SetRetCode(status);
    return rb;
}

auto DropExec::DropSynonym() const -> RecordBatch {
    RecordBatch rb(schema_);
    knl_session_t *session = EC_SESSION(catalog_.GetStorageHandle()->handle);

    knl_drop_def_t def;
    auto synonym_schema = catalog_.GetUser();
    text_t synonym_owner = { (char *)synonym_schema.c_str(), (uint32_t)synonym_schema.length() };
    text_t synonym_name = { (char *)name.c_str(), (uint32_t)name.length() };
    def.owner = synonym_owner;
    def.name = synonym_name;

    def.purge = GS_TRUE;
    def.temp = GS_FALSE;
    if (if_exists) {
        def.options = DROP_IF_EXISTS;
    } else {
        def.options = DROP_DIRECTLY;
    }
    def.ex_name.str = nullptr;
    def.ex_name.len = 0;

    if (knl_drop_synonym(session, &def) != GS_SUCCESS) {
        int32_t err_code;
        const char *message = nullptr;
        cm_get_error(&err_code, &message, NULL);
        std::string err_msg = fmt::format("create synonym fail!! errno = {}, message = {}", err_code, message);
        cm_reset_error();
        GS_LOG_RUN_INF("%s", err_msg.c_str());
        throw std::runtime_error(err_msg);
    }
    rb.SetRetCode(GS_SUCCESS);
    return rb;
}
