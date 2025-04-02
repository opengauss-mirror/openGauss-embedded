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
 * cs_packet.c
 *    Implement of uds management
 *
 * IDENTIFICATION
 *    src/network/network/cs_packet.c
 *
 * -------------------------------------------------------------------------
 */
#include "cs_packet.h"

#ifdef __cplusplus
extern "C" {
#endif

status_t cs_try_realloc_send_pack(cs_packet_t *pack, uint32 expect_size)
{
    errno_t errcode = 0;
    if (!CS_HAS_REMAIN(pack, expect_size)) {
        if (GS_MAX_UINT32 - pack->head->size < (uint32)CM_ALIGN_8K(expect_size)) {
            GS_THROW_ERROR(ERR_NUM_OVERFLOW);
            return GS_ERROR;
        }
        // extend memory align 8K
        if (GS_MAX_UINT32 - pack->head->size < CM_ALIGN_8K(expect_size)) {
            GS_THROW_ERROR(ERR_NUM_OVERFLOW);
            return GS_ERROR;
        }

        if (pack->head->size + expect_size > pack->max_buf_size) {
            GS_THROW_ERROR(ERR_FULL_PACKET, "send", pack->head->size + expect_size, pack->max_buf_size);
            return GS_ERROR;
        }
        uint32 new_buf_size = MIN(CM_REALLOC_SEND_PACK_SIZE(pack, expect_size), pack->max_buf_size);
        
        if(new_buf_size <= 0)
            return GS_ERROR;

        char *new_buf = (char *)malloc(new_buf_size);
        if (new_buf == NULL) {
            GS_THROW_ERROR(ERR_ALLOC_MEMORY, (uint64)new_buf_size, "large packet buffer");
            return GS_ERROR;
        }
        errcode = memcpy_s(new_buf, new_buf_size, pack->buf, pack->head->size);
        if (SECUREC_UNLIKELY(errcode != EOK)) {
            GS_THROW_ERROR(ERR_SYSTEM_CALL, errcode);
            CM_FREE_PTR(new_buf);
            return GS_ERROR;
        }
        if (pack->buf != pack->init_buf) {
            errcode = memset_s(pack->buf, pack->buf_size, 0, pack->head->size);
            if (SECUREC_UNLIKELY(errcode != EOK)) {
                GS_THROW_ERROR(ERR_SYSTEM_CALL, errcode);
                CM_FREE_PTR(new_buf);
                return GS_ERROR;
            }
            CM_FREE_PTR(pack->buf);
        }

        pack->buf_size = new_buf_size;
        pack->buf = new_buf;
        pack->head = (cs_packet_head_t *)pack->buf;
    }

    return GS_SUCCESS;
}

#ifdef __cplusplus
}
#endif
