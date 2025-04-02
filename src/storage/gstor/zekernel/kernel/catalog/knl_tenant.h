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
 * knl_tenant.h
 *    implement of tenant
 *
 * IDENTIFICATION
 *    src/storage/gstor/zekernel/kernel/catalog/knl_tenant.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef KNL_TENANT_H
#define KNL_TENANT_H

#include "cm_defs.h"
#include "cm_memory.h"
#include "knl_interface.h"
#include "knl_log.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct st_rd_tenant {
    logic_op_t op_type;
    uint32 tid;
    char name[GS_TENANT_BUFFER_SIZE];

    CM_MAGIC_DECLARE
} rd_tenant_t;
#define rd_tenant_t_MAGIC    481511651

status_t tenant_create(knl_session_t *session, knl_tenant_def_t *def);
status_t tenant_alter(knl_session_t *session, knl_tenant_def_t *def);
status_t tenant_drop(knl_session_t *session, knl_drop_tenant_t *def);

#ifdef __cplusplus
}
#endif

#endif