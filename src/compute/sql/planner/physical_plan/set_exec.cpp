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
* set_exec.cpp
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/planner/physical_plan/set_exec.cpp
*
* -------------------------------------------------------------------------
*/
#include "planner/physical_plan/set_exec.h"

#include <fmt/core.h>
#include <fmt/ranges.h>

#include "storage/gstor/gstor_instance.h"
#include "storage/gstor/zekernel/kernel/common/knl_context.h"
#include "storage/gstor/zekernel/kernel/common/knl_session.h"
#include "type/type_str.h"

auto SetExec::Execute() const -> RecordBatch {
    switch (set_name_) {
        case SetName::SET_NAME_MAXCONNECTIONS: {
            SetMaxConnections();
            break;
        }
        case SetName::SET_NAME_SYNCHRONOUS_COMMIT: {
            SetSynchronousCommit();
            break;
        }
        default: {
            throw intarkdb::Exception(ExceptionType::EXECUTOR, fmt::format("the variable is not supported"));
        }
    }
    return RecordBatch(Schema());
}

void SetExec::SetMaxConnections() const {
    auto storage = catalog_.GetStorageHandle();

    auto max_conn = set_value_.GetCastAs<uint32_t>();

    gstor_set_max_connections(storage->handle, max_conn);
}

void SetExec::SetSynchronousCommit() const {
    knl_session_t *session = EC_SESSION(catalog_.GetStorageHandle()->handle);
    instance_t *instance = (instance_t *)session->kernel->server;

    auto sync_commit = set_value_.GetCastAs<std::string>();
    if (sync_commit == "off") {
        session->kernel->attr.synchronous_commit = GS_FALSE;
    } else if (sync_commit == "on") {
        session->kernel->attr.synchronous_commit = GS_TRUE;
    } else {
        throw intarkdb::Exception(ExceptionType::EXECUTOR, fmt::format("bad set value, must be off or on"));
    }

    cm_alter_config(instance->cc_config, "SYNCHRONOUS_COMMIT", sync_commit.c_str(), CONFIG_SCOPE_BOTH, GS_TRUE);
}
