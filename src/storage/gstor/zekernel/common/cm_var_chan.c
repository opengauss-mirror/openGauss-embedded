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
 * cm_var_chan.c
 * Implement of var channel which support store variable data
 *
 * IDENTIFICATION
 * src/storage/gstor/zekernel/common/cm_var_chan.c
 *
 * -------------------------------------------------------------------------
 */

#include "cm_var_chan.h"
#include "cm_error.h"
#include "cm_defs.h"

static bool32 cm_alloc_buf_valid(var_chan_t *chan)
{
    uint32 i;
    for (i = 0; i < chan->buf_ctrl.buf_count; i++) {
        if (chan->buf_ctrl.bufs[i] == NULL) {
            return GS_FALSE;
        }
    }
    return GS_TRUE;
}

static bool32 cm_var_chan_can_send(var_chan_t *chan, uint32 len)
{
    uint32 cur_buf_remain;
    // [end,begin] is remain area
    if (chan->buf_ctrl.end_buf_id == chan->buf_ctrl.beg_buf_id && chan->ori_chan.end <= chan->ori_chan.begin &&
        chan->ori_chan.count > 0) {
        cur_buf_remain = (uint32)(chan->ori_chan.begin - chan->ori_chan.end);
        if (cur_buf_remain < (len + sizeof(uint32))) {
            return GS_FALSE;
        }
        return GS_TRUE;
    }
    cur_buf_remain = (uint32)(chan->buf_ctrl.bufs_end[chan->buf_ctrl.end_buf_id] - chan->ori_chan.end);
    uint32 next_buf_id = (chan->buf_ctrl.end_buf_id + 1) % chan->buf_ctrl.buf_count;
    if (cur_buf_remain < (len + sizeof(uint32))) {
        chan->buf_ctrl.available -= cur_buf_remain;
        chan->buf_ctrl.data_end[chan->buf_ctrl.end_buf_id] = chan->ori_chan.end;
        chan->ori_chan.end = chan->buf_ctrl.bufs[next_buf_id];
        chan->buf_ctrl.end_buf_id = next_buf_id;
    }

    if (chan->buf_ctrl.available < (len + sizeof(uint32))) {
        return GS_FALSE;
    }
    return GS_TRUE;
}

var_chan_t *cm_var_chan_new(uint32 total, void *owner, ga_alloc_func_t alloc_func)
{
    var_chan_t *chan = NULL;
    uint32 i;
    errno_t rc_memzero;
    if (total > MAX_BUF_COUNT * GS_VMEM_PAGE_SIZE || total == 0) {
        return NULL;
    }

    if (owner == NULL || alloc_func == NULL) {
        return NULL;
    }
    if (alloc_func(owner, sizeof(var_chan_t), (void **)&chan) != GS_SUCCESS) {
        return NULL;
    }

    rc_memzero = memset_sp(chan, sizeof(var_chan_t), 0, sizeof(var_chan_t));
    if (rc_memzero != EOK) {
        return NULL;
    }

    // alloc mem which is multiple of GS_VMEM_PAGE_SIZE
    uint32 page_count = total / GS_VMEM_PAGE_SIZE;
    uint32 remain = total % GS_VMEM_PAGE_SIZE;
    chan->buf_ctrl.buf_count = (remain == 0 ? page_count : page_count + 1);

    for (i = 0; i < chan->buf_ctrl.buf_count; i++) {
        if (alloc_func(owner, GS_VMEM_PAGE_SIZE, (void **)&chan->buf_ctrl.bufs[i]) != GS_SUCCESS) {
            return NULL;
        }
        rc_memzero = memset_sp(chan->buf_ctrl.bufs[i], GS_VMEM_PAGE_SIZE, 0, GS_VMEM_PAGE_SIZE);
        if (rc_memzero != EOK) {
            return NULL;
        }

        chan->buf_ctrl.bufs_end[i] = chan->buf_ctrl.bufs[i] + GS_VMEM_PAGE_SIZE;
        chan->buf_ctrl.data_end[i] = chan->buf_ctrl.bufs[i] + GS_VMEM_PAGE_SIZE;
    }

    chan->ori_chan.begin = chan->buf_ctrl.bufs[0];
    chan->ori_chan.end = chan->buf_ctrl.bufs[0];
    chan->buf_ctrl.beg_buf_id = 0;
    chan->buf_ctrl.end_buf_id = 0;
    chan->ori_chan.count = 0;
    chan->buf_ctrl.total = chan->buf_ctrl.buf_count * GS_VMEM_PAGE_SIZE;
    chan->buf_ctrl.available = chan->buf_ctrl.total;

    chan->ori_chan.lock = 0;
    (void)cm_event_init(&chan->ori_chan.event_send);
    (void)cm_event_init(&chan->ori_chan.event_recv);
    chan->ori_chan.waittime_ms = 100;

    chan->ori_chan.is_closed = GS_FALSE;
    chan->ori_chan.ref_count = 0;

    return chan;
}

status_t cm_var_chan_send_timeout(var_chan_t *chan, const void *elem, uint32 len, uint32 timeout_ms)
{
    errno_t errcode;
    if (chan == NULL || elem == NULL) {
        return GS_ERROR;
    }

    cm_spin_lock(&chan->ori_chan.lock, NULL);
    {
        if (!cm_alloc_buf_valid(chan) || chan->ori_chan.is_closed) {
            cm_spin_unlock(&chan->ori_chan.lock);
            return GS_ERROR;
        }

        // chan is full
        while (!cm_var_chan_can_send(chan, len)) {
            cm_spin_unlock(&chan->ori_chan.lock);

            // wait for the recv signal
            if (GS_TIMEDOUT == cm_event_timedwait(&chan->ori_chan.event_recv, timeout_ms)) {
                return GS_TIMEDOUT;
            }

            cm_spin_lock(&chan->ori_chan.lock, NULL);

            if (cm_var_chan_can_send(chan, len)) {
                break;
            }
        }

        // send
        *(uint32 *)chan->ori_chan.end = len;
        chan->ori_chan.end += sizeof(uint32);
        errcode = memcpy_sp(chan->ori_chan.end, len, elem, len);
        if (errcode != EOK) {
            cm_spin_unlock(&chan->ori_chan.lock);
            GS_THROW_ERROR(ERR_SYSTEM_CALL, errcode);
            return GS_ERROR;
        }
        chan->ori_chan.end += len;
        chan->ori_chan.count++;
        chan->buf_ctrl.available -= (len + sizeof(uint32));
    }
    cm_spin_unlock(&chan->ori_chan.lock);

    cm_event_notify(&chan->ori_chan.event_send);

    return GS_SUCCESS;
}

// send an element, will block until there are space to store
status_t cm_var_chan_send(var_chan_t *chan, const void *elem, uint32 len)
{
    return cm_var_chan_send_timeout(chan, elem, len, 0xFFFFFFFF);
}

// recv an element, will block until there are elems in the chan
status_t cm_var_chan_recv_timeout(var_chan_t *chan, void *elem, uint32 *len, uint32 timeout_ms)
{
    errno_t errcode;
    if (chan == NULL || elem == NULL) {
        return GS_ERROR;
    }

    cm_spin_lock(&chan->ori_chan.lock, NULL);
    {
        if (!cm_alloc_buf_valid(chan)) {
            cm_spin_unlock(&chan->ori_chan.lock);
            return GS_ERROR;
        }

        // chan is empty
        while (chan->ori_chan.count == 0) {
            if (chan->ori_chan.is_closed) {
                cm_spin_unlock(&chan->ori_chan.lock);
                return GS_ERROR;
            }

            cm_spin_unlock(&chan->ori_chan.lock);

            // wait for the send signal
            if (GS_TIMEDOUT == cm_event_timedwait(&chan->ori_chan.event_send, timeout_ms)) {
                return GS_TIMEDOUT;
            }

            cm_spin_lock(&chan->ori_chan.lock, NULL);

            if (chan->ori_chan.count > 0) {
                break;
            }
        }

        // ring
        uint32 cur_buf_id = chan->buf_ctrl.beg_buf_id;
        uint32 next_buf_id = (cur_buf_id + 1) % chan->buf_ctrl.buf_count;
        if (chan->ori_chan.begin >= chan->buf_ctrl.data_end[cur_buf_id]) {
            chan->buf_ctrl.available += (uint32)(chan->buf_ctrl.bufs_end[cur_buf_id] - chan->ori_chan.begin);
            chan->buf_ctrl.data_end[cur_buf_id] = chan->buf_ctrl.bufs_end[cur_buf_id];
            chan->ori_chan.begin = chan->buf_ctrl.bufs[next_buf_id];
            chan->buf_ctrl.beg_buf_id = next_buf_id;
        }

        // recv
        *len = *(uint32 *)chan->ori_chan.begin;
        chan->ori_chan.begin += sizeof(uint32);
        errcode = memcpy_sp(elem, *len, chan->ori_chan.begin, *len);
        if (errcode != EOK) {
            cm_spin_unlock(&chan->ori_chan.lock);
            GS_THROW_ERROR(ERR_SYSTEM_CALL, errcode);
            return GS_ERROR;
        }
        chan->ori_chan.begin += *len;
        chan->ori_chan.count--;
        chan->buf_ctrl.available += (sizeof(uint32) + *len);
    }
    cm_spin_unlock(&chan->ori_chan.lock);

    cm_event_notify(&chan->ori_chan.event_recv);

    return GS_SUCCESS;
}

// send an element, will block until there are space to store
status_t cm_var_chan_recv(var_chan_t *chan, void *elem, uint32 *len)
{
    return cm_var_chan_recv_timeout(chan, elem, len, 0xFFFFFFFF);
}

void cm_var_chan_close(var_chan_t *chan)
{
    cm_chan_close(&chan->ori_chan);
}

void cm_var_chan_free(var_chan_t *chan)
{
    uint32 i;
    if (chan == NULL) {
        return;
    }

    cm_event_destory(&chan->ori_chan.event_recv);
    cm_event_destory(&chan->ori_chan.event_send);

    for (i = 0; i < MAX_BUF_COUNT; i++) {
        chan->buf_ctrl.bufs[i] = NULL;
        chan->buf_ctrl.bufs_end[i] = NULL;
        chan->buf_ctrl.data_end[i] = NULL;
    }
    chan->buf_ctrl.buf_count = 0;
    chan->buf_ctrl.available = 0;
    chan->buf_ctrl.beg_buf_id = 0;
    chan->buf_ctrl.end_buf_id = 0;
    chan->buf_ctrl.total = 0;
    chan->ori_chan.begin = NULL;
    chan->ori_chan.end = NULL;
    chan->ori_chan.count = 0;

    chan->ori_chan.is_closed = GS_TRUE;
    chan->ori_chan.ref_count = 0;
}

bool32 cm_var_chan_empty(var_chan_t *chan)
{
    return cm_chan_empty(&chan->ori_chan);
}
