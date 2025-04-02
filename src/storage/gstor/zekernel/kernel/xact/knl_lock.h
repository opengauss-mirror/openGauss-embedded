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
 * knl_lock.h
 *    kernel lock manage
 *
 * IDENTIFICATION
 *    src/storage/gstor/zekernel/kernel/xact/knl_lock.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef __KNL_LOCK_H__
#define __KNL_LOCK_H__

#include "knl_interface.h"
#include "knl_session.h"

typedef enum en_lock_mode {
    LOCK_MODE_IDLE = 0,  // idle
    LOCK_MODE_S = 1,     // shared
    LOCK_MODE_IX = 2,    // intent exclusive
    LOCK_MODE_X = 3,     // exclusive
} lock_mode_t;

#define SCHEMA_LOCK_ALIGN_SIZE 4

typedef struct st_schema_lock {
    void *next;  // !!! must be the first member of structure
    lock_mode_t mode;
    uint32 shared_count;
    uint8 map[SCHEMA_LOCK_ALIGN_SIZE];
} schema_lock_t;

#define EXPLICT_LOCKED                     2
#define SCHEMA_LOCK_SIZE \
    CM_ALIGN8(session->kernel->attr.max_rms + OFFSET_OF(schema_lock_t, map))
#define SCH_LOCK_SET(session, lock)        ((lock)->map[(session)->rmid] = 1)
#define SCH_LOCK_CLEAN(session, lock)      ((lock)->map[(session)->rmid] = 0)
#define SCH_LOCK_EXPLICIT(session, lock)   ((lock)->map[(session)->rmid] = EXPLICT_LOCKED)
#define SCH_LOCKED_BY_RMID(rmid, lock)     ((lock)->map[rmid])
#define SCH_LOCKED_EXCLUSIVE(dc_entity)    (((dc_entity_t *)(dc_entity))->entry->sch_lock->mode == LOCK_MODE_X)

typedef enum en_lock_type {
    LOCK_TYPE_FREE     = 0,  // unused
    LOCK_TYPE_TS       = 1,  // table shared
    LOCK_TYPE_TX       = 2,  // table exclusive
    LOCK_TYPE_RCR_RX   = 3,  // row(RCR itl lock) exclusive
    LOCK_TYPE_RCR_KX   = 4,  // key(RCR itl lock) exclusive
    LOCK_TYPE_PCR_RX   = 5,  // row(PCR itl lock) exclusive
    LOCK_TYPE_PCR_KX   = 6,  // key(PCR itl lock) exclusive
    LOCK_TYPE_ALCK_TS  = 7,  // transaction level shared advisory lock
    LOCK_TYPE_ALCK_TX  = 8,  // transaction level exclusive advisory lock
    LOCK_TYPE_ALCK_SS  = 9,  // session level shared advisory lock
    LOCK_TYPE_ALCK_SX  = 10, // session level exclusive advisory lock
    LOCK_TYPE_PL_S  = 11,    // plsql level shared advisory lock
    LOCK_TYPE_PL_X  = 12,    // plsql level exclusive advisory lock
} lock_type_t;

typedef struct st_lock_item {
    union {
        struct {
            uint32 page;
            uint16 file;
            uint8 itl;  // for row lock, slot of itl
            uint8 unused;
        };

        struct st_dc_entry *dc_entry;  // for sch lock
        struct {
            uint32 prev;
            uint32 alck_id; // for advisory lock
            uint8  alck_times;
            uint8  alck_x_times;
            uint16 un_used;
        };
    };

    uint32 type : 8;
    uint32 part_no : 24;
    uint32 subpart_no;
    uint32 next;
    pagid_data_t next_pagid;  // next page id of btree page
    uint16 rmid;              // rm id holding this lock
} lock_item_t;

#define LOCK_PAGE_CAPACITY (GS_SHARED_PAGE_SIZE / sizeof(lock_item_t))
#define MAX_LOCKS          (GS_MAX_LOCK_PAGES * LOCK_PAGE_CAPACITY)
#define UNLOCK_IX(lock)    ((lock)->shared_count > 0 ? LOCK_MODE_S : LOCK_MODE_IDLE);
#define LOCK_NEXT(lock_id) (lock_addr(area, (lock_id))->next)

#define IS_SESSION_OR_PL_LOCK(type) \
    ((type) == LOCK_TYPE_ALCK_SS || (type) == LOCK_TYPE_ALCK_SX || (type) == LOCK_TYPE_PL_S ||      \
     (type) == LOCK_TYPE_PL_X)

typedef struct st_lock_area {
    spinlock_t lock;
    memory_pool_t pool;
    uint32 page_count;
    char *pages[GS_MAX_LOCK_PAGES];
    id_list_t free_items;
    uint32 hwm;
    uint32 capacity;
    spinlock_t upgrade_lock;
} lock_area_t;

#define LOCKS_THRESHOLD(session) \
    ((session)->kernel->buf_ctx.buf_set[0].capacity * (session)->kernel->buf_ctx.buf_set_count / 10)
#define LOCK_TIMEOUT(time)       ((time) >= GS_INVALID_ID32 ? GS_INVALID_ID32 : (time))

status_t lock_area_init(knl_session_t *session);
void lock_init(knl_rm_t *rm);
void lock_free_sch_group(knl_session_t *session);
void lock_reset(knl_rm_t *rm);
void lock_free(knl_session_t *session, knl_rm_t *rm);
status_t lock_itl(knl_session_t *session, page_id_t page_id, uint8 itl_id, knl_part_locate_t part_loc, 
    page_id_t next_pagid, lock_type_t type);
status_t lock_table_shared(knl_session_t *session, knl_handle_t dc_entity,
                           uint32 timeout_s);     // timeout_s timeout seconds
status_t lock_table_exclusive(knl_session_t *session, knl_handle_t dc_entity,
                              uint32 timeout_s);  // timeout_s timeout seconds
status_t lock_table_directly(knl_session_t *session, knl_handle_t dc_entity, uint32 timeout);
status_t lock_table_shared_directly(knl_session_t *session, knl_handle_t dc_entity);
status_t lock_table_ux(knl_session_t *session, knl_handle_t dc_entry);
status_t lock_table_in_exclusive_mode(knl_session_t *session, knl_handle_t dc_entity, knl_handle_t dc_entry,
                                      uint32 timeout_s);
void unlock_tables_directly(knl_session_t *session);
void unlock_table(knl_session_t *session, lock_item_t *item);
status_t lock_upgrade_table_lock(knl_session_t *session, knl_handle_t dc_entity);
void lock_degrade_table_lock(knl_session_t *session, knl_handle_t dc_entity);
bool32 lock_table_without_xact(knl_session_t *session, knl_handle_t dc_entity, bool32 *inuse);
void unlock_table_without_xact(knl_session_t *session, knl_handle_t dc_entity, bool32 inuse);
void lock_free_to_svpt(knl_session_t *session, knl_savepoint_t *savepoint);
void lock_reset_to_svpt(knl_session_t *session, knl_savepoint_t *savepoint);
status_t lock_parent_table_directly(knl_session_t *session, knl_handle_t entity, bool32 is_default);
status_t lock_child_table_directly(knl_session_t *session, knl_handle_t entity, bool32 is_default);
char *lock_mode_string(knl_handle_t entry);
status_t lock_alloc(knl_session_t *session, lock_type_t type, lock_item_t **lock);
void lock_add_alck_times(knl_session_t *session, uint32 alck_id, int32 lock_set);
void lock_del_alck_times(knl_session_t *session, uint32 alck_id, int32 lock_set);
void lock_destroy_se_alcks(knl_session_t *session);

extern char *g_lock_type_str[];
extern char *g_lock_mode_str[];

static inline char *lock_type_string(lock_type_t lock_type)
{
    return g_lock_type_str[lock_type - LOCK_TYPE_FREE];
}

static inline lock_item_t *lock_addr(lock_area_t *area, uint32 id)
{
    uint32 page_id = id / LOCK_PAGE_CAPACITY;
    uint32 lock_id = id % LOCK_PAGE_CAPACITY;
    return (lock_item_t *)(area->pages[page_id] + lock_id * sizeof(lock_item_t));
}

static inline void lock_init_group(lock_group_t *group)
{
    group->plock_id = GS_INVALID_ID32;
    cm_reset_id_list(&group->plocks);
    cm_reset_id_list(&group->glocks);
}

#endif

