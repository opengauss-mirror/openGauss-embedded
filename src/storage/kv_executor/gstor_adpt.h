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
 * gstor_adpt.h
 *    gstor adapter
 *
 * IDENTIFICATION
 *    src/storage/gstor_adpt.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef __V3_ADAPTER_H__
#define __V3_ADAPTER_H__

#include "storage/gstor/zekernel/common/cm_error.h"
#include "storage/gstor/gstor_executor.h"

#define EXC_DCC_KV_TABLE            ((char *)"SYS_KV")

typedef enum en_dbtype {
    DB_TYPE_GSTOR = 0,
    DB_TYPE_CEIL  = 1,
} dbtype_t;

typedef enum en_handle_type {
    HANDLE_TYPE_KV  = 0,
    HANDLE_TYPE_SQL = 1,
    HANDLE_TYPE_KVSQL = 2,
} handle_type_t;

#endif