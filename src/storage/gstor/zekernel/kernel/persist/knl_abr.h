/*
 * Copyright (c) 2022 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * knl_abr.h
 *    implement of auto block recover for repairing disk page 
 *
 * IDENTIFICATION
 *    src/storage/gstor/zekernel/kernel/persist/knl_abr.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef __KNL_ABR_H__
#define __KNL_ABR_H__

#include "cm_defs.h"
#include "knl_page.h"
#include "knl_buffer.h"
#include "bak_common.h"
#ifdef _REPLICATION
#include "repl_log_send.h"
#endif
#ifdef __cplusplus
extern "C" {
#endif

// Interfaces for automatic block repair
bool32 abr_repair_page_from_standy(knl_session_t *session, buf_ctrl_t *ctrl);
#ifdef _REPLICATION
bool32 abr_notify_task(knl_session_t *session, buf_ctrl_t *ctrl, lsnd_abr_task_t **task_handle);
bool32 abr_wait_task_done(knl_session_t *session, lsnd_abr_task_t *task_handle);
void abr_finish_task(lsnd_abr_task_t *task, bool32 succeeded, const char *buf, uint32 buf_size);
#endif
void abr_try_save_page(knl_session_t *session, page_head_t *page);
#ifdef _REPLICATION
status_t abr_send_page_fetch_req(lsnd_t *lsnd, lsnd_abr_task_t *task);
#endif
status_t abr_repair_page_from_backup(knl_session_t *session, bak_page_search_t *repair_ctx, const char *path);
void abr_clear_page(knl_session_t *session, uint32 file_id);
bool32 abr_verify_pageid(knl_session_t *session, page_id_t page_id);
bool32 abr_precheck_corrupted_page(knl_session_t *session, page_id_t page_id);
status_t abr_restore_block_recover(knl_session_t *session, knl_restore_t *param);
status_t abr_restore_file_recover(knl_session_t *session, knl_restore_t *param);
status_t abr_wait_paral_rcy_compelte(knl_session_t *session);

#ifdef __cplusplus
}
#endif

#endif
