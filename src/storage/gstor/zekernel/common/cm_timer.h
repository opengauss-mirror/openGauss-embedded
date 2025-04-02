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
 * cm_timer.h
 *    update system timer timely
 *
 * IDENTIFICATION
 *    src/storage/gstor/zekernel/common/cm_timer.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef __CM_TIMER_H__
#define __CM_TIMER_H__

#include "cm_defs.h"
#include "cm_thread.h"
#include "cm_date.h"

#define CM_HOST_TIMEZONE (g_timer()->host_tz_offset)

typedef enum en_timer_status {
    TIMER_STATUS_RUNNING,
    TIMER_STATUS_PAUSING,
    TIMER_STATUS_PAUSED,
} timer_status_t;

typedef struct st_gs_timer {
    volatile date_detail_t detail;  // detail of date, yyyy-mm-dd hh24:mi:ss
    volatile date_t now;
    volatile date_t monotonic_now;  // not affected by user change
    volatile date_t today;          // the day with time 00:00:00
    volatile uint32 systime;        // seconds between timer started and now
    volatile int32 tz;              // time zone (min)
    volatile int64 host_tz_offset;  // host timezone offset (us)
    thread_t thread;
    timer_status_t status;
} gs_timer_t;

EXPORT_API status_t cm_start_timer(gs_timer_t *timer);
EXPORT_API void cm_close_timer(gs_timer_t *timer);
EXPORT_API gs_timer_t *g_timer();
EXPORT_API date_t cm_get_sync_time();
EXPORT_API void cm_set_sync_time(date_t time);
EXPORT_API void cm_pause_timer(gs_timer_t *input_timer);
EXPORT_API void cm_resume_timer(gs_timer_t *input_timer);

#endif
