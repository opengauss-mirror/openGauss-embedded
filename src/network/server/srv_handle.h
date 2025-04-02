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
* srv_handle.h
*
* IDENTIFICATION
* openGauss-embedded/src/network/server/srv_handle.h
*
* -------------------------------------------------------------------------
*/

#pragma once
#include "srv_session.h"

#ifdef __cplusplus
extern "C" {
#endif

status_t srv_process_command(session_t *session);
void set_prepare_stmt(session_t *session);
void delete_prepare_stmt(session_t *session);

#ifdef __cplusplus
}
#endif
