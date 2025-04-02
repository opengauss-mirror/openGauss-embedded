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
 * srv_lsnr.h
 *    listener interface
 *
 * IDENTIFICATION
 *    src/server/srv_lsnr.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef __SRV_LSNR_H__
#define __SRV_LSNR_H__

#include "cm_defs.h"
#include "cs_pipe.h"
#include "cs_listener.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct st_lsnr {
    tcp_lsnr_t tcp_service;
} lsnr_t;

status_t srv_start_lsnr(void);
void srv_stop_lsnr(lsnr_type_t type);
void srv_pause_lsnr(lsnr_type_t type);

#ifdef __cplusplus
}
#endif

#endif
