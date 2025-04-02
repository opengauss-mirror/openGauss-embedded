/*
 * Copyright (c) 2022 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * gstor_sga.h
 * sga interface
 *
 * IDENTIFICATION
 * src/storage/gstor/gstor_sga.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef __SRV_SGA_H__
#define __SRV_SGA_H__

#include "cm_defs.h"
#include "cm_vma.h"
#include "cm_buddy.h"

#ifdef __cplusplus
extern "C" {
#endif

struct st_instance;

typedef enum en_large_pages_mode {
    LARGE_PAGES_TRUE = 1,  // try to use large_pages first
    LARGE_PAGES_FALSE = 2, // do not use large_pages
    LARGE_PAGES_ONLY = 3   // only use large_pages
} large_pages_mode_t;

typedef struct st_sga {
    uint64 size;
    char *buf;

    char *data_buf;
    char *cr_buf;
    char *log_buf;
    char *shared_buf;
    char *vma_buf;
    char *vma_large_buf;
    char *large_buf;
    char *temp_buf;
    char *dbwr_buf;
    char *lgwr_buf;
    char *lgwr_cipher_buf;
    char *lgwr_async_buf;
    char *lgwr_head_buf;
    char *tran_buf;
    char *index_buf;
    char *buf_iocbs;

    memory_area_t shared_area;
    vma_t vma;
    memory_pool_t large_pool;
    mem_pool_t buddy_pool;
} sga_t;

status_t knl_create_sga(struct st_instance *cc_instance);
void knl_destroy_sga(struct st_instance *cc_instance);

#ifdef __cplusplus
}
#endif

#endif
