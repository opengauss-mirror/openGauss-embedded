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
 * kv_executor.h
 *    kv_executor header file
 *
 * IDENTIFICATION
 *    src/storage/kv_executor/kv_executor.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef __KV_EXECUTOR_H__
#define __KV_EXECUTOR_H__

#include "gstor_adpt.h"

#include "storage/gstor/zekernel/common/cm_text.h"

#ifdef __cplusplus
extern "C" {
#endif


EXPORT_API status_t kv_open_table(void *handle, const char *table_name);

EXPORT_API status_t kv_del(void *handle, text_t *key, bool32 prefix, uint32 *count);

EXPORT_API status_t kv_put(void *handle, text_t *key, text_t *val);

EXPORT_API status_t kv_get(void *handle, text_t *key, text_t *val, bool32 *eof);

EXPORT_API status_t kv_begin(void *handle);

EXPORT_API status_t kv_commit(void *handle);

EXPORT_API status_t kv_rollback(void *handle);

#ifdef __cplusplus
}
#endif

#endif
