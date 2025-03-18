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
 * knl_log.h
 * Functions for constructing redo logs
 *
 * IDENTIFICATION
 * src/storage/gstor/zekernel/kernel/persist/knl_log.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef __KNL_LOG_H__
#define __KNL_LOG_H__
#include "knl_log_type.h"
#include "cm_utils.h"
#include "cm_defs.h"
#include "cm_text.h"
#include "cm_thread.h"
#include "cm_device.h"
#include "knl_session.h"
#include "knl_page.h"
#include "knl_common.h"

#ifdef __cplusplus
extern "C" {
#endif

#define LOG_FLUSH_INTERVAL 10000
#define LOG_KEEP_SIZE(kernel) \
    (GS_PLOG_PAGES * (uint64)(kernel)->assigned_sessions * DEFAULT_PAGE_SIZE + (kernel)->attr.log_buf_size)

// GS_PLOG_PAGES is 7, GS_MAX_AGENTS is 1024, and DEFAULT_PAGE_SIZE is 8k, so their product is less than 2^26
// log_buf_size is less than 2^16, lgwr_async_buf_size equals 2^14, so sum total value is smaller than max uint32 value
#define LOG_MIN_SIZE(kernel)                                                           \
    (GS_PLOG_PAGES * GS_MAX_AGENTS * DEFAULT_PAGE_SIZE + (kernel)->attr.log_buf_size + \
        (kernel)->attr.lgwr_async_buf_size)
#ifdef _REPLICATION
#define LOG_SKIP_CHECK_ASN(kernel, force_ignorlog) \
    (DB_IS_RAFT_ENABLED(kernel) || !DB_IS_PRIMARY(&(kernel)->db) || (force_ignorlog))
#else
#define LOG_SKIP_CHECK_ASN(kernel, force_ignorlog) (!DB_IS_PRIMARY(&(kernel)->db) || (force_ignorlog))
#endif
#define LOG_MAGIC_NUMBER (uint64)0xfedcba98654321fe
#define LOG_ENTRY_SIZE (OFFSET_OF(log_entry_t, data))
#define LOG_FLUSH_THRESHOLD (uint32)1048576
#define LOG_BUF_SHIFT_FACTOR (uint32)3
#define LOG_HAS_LOGIC_DATA(s) ((s)->rm->logic_log_size != 0 || (s)->rm->large_page_id != GS_INVALID_ID32)
#define LOG_BUF_SLOT_FULL 0x0101010101010101
#define LOG_FLAG_DROPPED 0x01
#define LOG_FLAG_ALARMED 0x02
#define GS_LOG_AREA_COUNT 2
#define LOG_BUF_SLOT_COUNT 8

#define LOG_IS_DROPPED(flag) ((flag)&LOG_FLAG_DROPPED)
#define LOG_SET_DROPPED(flag) CM_SET_FLAG(flag, LOG_FLAG_DROPPED)
#define LOG_UNSET_DROPPED(flag) CM_CLEAN_FLAG(flag, LOG_FLAG_DROPPED)

#define LOG_IS_ALARMED(flag) ((flag)&LOG_FLAG_ALARMED)
#define LOG_SET_ALARMED(flag) CM_SET_FLAG((flag), LOG_FLAG_ALARMED)
#define LOG_UNSET_ALARMED(flag) CM_CLEAN_FLAG((flag), LOG_FLAG_ALARMED)

typedef enum en_logfile_status {
    LOG_FILE_INACTIVE = 1,
    LOG_FILE_ACTIVE = 2,
    LOG_FILE_CURRENT = 3,
    LOG_FILE_UNUSED = 4,
} logfile_status_t;

typedef struct st_log_file_ctrl {
    char name[GS_FILE_NAME_BUFFER_SIZE];
    int64 size;
    int64 hwm;
    int32 file_id;
    uint32 seq; // The header write sequence number
    uint16 block_size;
    uint16 flg;
    device_type_t type;
    logfile_status_t status;
    uint16 forward;
    uint16 backward;
    bool8 archived;
    uint8 reserved[31];
} log_file_ctrl_t;

typedef struct st_reset_log {
    uint32 rst_id;
    uint32 last_asn;
    uint64 last_lfn;
} reset_log_t;

typedef struct st_log_file_head {
    knl_scn_t first;
    knl_scn_t last;
    volatile uint64 write_pos;
    uint32 asn;
    int32 block_size : 16;
    int32 cmp_algorithm : 4;
    int32 reserve : 12;
    uint32 rst_id;
    uint32 checksum;
} log_file_head_t;

typedef struct st_logfile {
    int32 handle;
    latch_t latch;
    uint64 arch_pos;
    log_file_ctrl_t *ctrl;
    log_file_head_t head;
    int32 wd; // watch decriptor
} log_file_t;

typedef struct st_log_point {
    uint32 asn; // log file id
    uint32 block_id;
    uint64 rst_id : 18;
    uint64 lfn : 46;
} log_point_t;

typedef struct st_log_queue {
    spinlock_t lock;
    knl_session_t *first;
    knl_session_t *last;
} log_queue_t;

typedef struct st_log_group {
    uint64 lsn;
    uint16 rmid;
    uint16 size;
    uint16 opr_uid; // operator user id
    uint16 reserved;
} log_group_t;

typedef struct st_log_part {
    uint32 size;
} log_part_t;

typedef struct st_log_batch_id {
    uint64 magic_num;  // for checking batch is completed or not
    log_point_t point; // log address for batch
} log_batch_id_t;

typedef struct st_log_batch {
    log_batch_id_t head;
    knl_scn_t scn;
    uint32 padding;
    uint32 size; // batch length, include log_batch_t head size

    uint32 space_size; // The actual space occupied by the batch
    uint8 part_count;  // a batch contains multiple buffers,less or queue to buffer count
    uint8 version;
    uint16 batch_session_cnt;

    uint64 raft_index;
    uint16 checksum;
    bool8 encrypted : 1;
    uint8 reserve : 7;
    uint8 unused[5];
} log_batch_t;

typedef log_batch_id_t log_batch_tail_t;

/* every option use one bit of flags in log_entry_t */
#define LOG_ENTRY_FLAG_NONE 0x0000
#define LOG_ENTRY_FLAG_WITH_LOGIC 0x0001     // only for compability
#define LOG_ENTRY_FLAG_WITH_LOGIC_OID 0x0010 // new version, oid included in logic data

#pragma pack(4)
typedef struct st_log_entry {
    uint16 size;
    uint8 type;
    uint8 flag;
    char data[4];
} log_entry_t;
#pragma pack()

typedef void (*log_replay_proc)(knl_session_t *session, log_entry_t *log);
typedef void (*log_desc_proc)(log_entry_t *log);
typedef void (*log_analysis_proc)(knl_session_t *session, log_entry_t *log, uint64 lsn);
typedef status_t (*callback_keep_hb_entry)(knl_session_t *session);
typedef bool32 (*log_check_punch_proc)(knl_session_t *session, log_entry_t *log);

typedef struct log_manager {
    log_type_t type;
    const char *name;
    log_replay_proc replay_proc;
    log_desc_proc desc_proc;
    log_analysis_proc analysis_proc;
    log_check_punch_proc check_punch_proc;
} log_manager_t;

typedef struct logic_log_manager {
    logic_op_t type;
    const char *name;
    log_replay_proc replay_proc;
    log_desc_proc desc_proc;
} logic_log_manager_t;

#ifdef _WIN32
typedef struct st_log_buffer {
#else
typedef struct __attribute__((aligned(128))) st_log_buffer {
#endif
    spinlock_t lock; // buf lock for switch and write
    bool32 log_encrypt;
    uint32 lock_align[GS_RESERVED_BYTES_14];

    union {
        volatile uint8 slots[LOG_BUF_SLOT_COUNT];
        volatile uint64 value;
    };

    uint32 size;
    volatile uint32 write_pos;
    char *addr;
} log_buffer_t;

typedef struct st_log_dual_buffer {
    log_buffer_t members[GS_LOG_AREA_COUNT];
} log_dual_buffer_t;

typedef struct st_log_stat {
    struct timeval flush_begin;
    uint64 flush_times;
    uint64 flush_bytes;
    uint64 flush_elapsed;
    uint64 times_4k;
    uint64 times_8k;
    uint64 times_16k;
    uint64 times_32k;
    uint64 times_64k;
    uint64 times_128k;
    uint64 times_256k;
    uint64 times_512k;
    uint64 times_1m;
    uint64 times_inf;
    uint64 space_requests;
    uint64 switch_count;
} log_stat_t;

typedef struct st_replay_stat {
    struct timeval analyze_begin;
    struct timeval analyze_end;
    struct timeval replay_begin;
    struct timeval replay_end;
    uint64 analyze_elapsed; /* us */
    uint64 analyze_pages;
    uint64 analyze_resident_pages;
    uint64 analyze_new_pages;
    uint64 replay_elapsed; /* us */
} replay_stat_t;

/* log analyze item(page/lsn/lfn) */
typedef struct st_gbp_analyse_item {
    page_id_t page_id;
    volatile uint64 lsn;             // expect lsn after failover done
    volatile uint64 unused : 8;      // reseved
    volatile uint64 is_verified : 8; // whether gbp page is verifyed
    volatile uint64 lfn : 48;        // the lfn of batch contain this page
    struct st_gbp_analyse_item *next;
} gbp_analyse_item_t;

typedef struct st_gbp_analyse_bucket {
    uint32 count;
    gbp_analyse_item_t *first;
} gbp_analyse_bucket_t;

typedef struct st_gbp_analyse_result {
    volatile bool32 gbp_unsafe;
    log_type_t unsafe_type;
    uint64 unsafe_max_lsn;
} gbp_analyse_result_t;

typedef struct st_log_context {
    spinlock_t commit_lock; // lock for commit
    uint32 lock_align1[15];
    spinlock_t flush_lock; // buf lock for flush
    uint32 lock_align2[15];
    spinlock_t alert_lock; // for checkpoint not completed
    uint32 lock_align3[15];
    volatile uint64 flushed_lfn; // latest global flushed batch lfn
    volatile uint64 quorum_lfn;  // latest lfn which meets quorum agreement

    uint32 buf_size;
    uint32 buf_count;
    volatile uint16 wid;
    volatile uint16 fid;
    volatile bool32 alerted; // for checkpoint not completed
    uint64 lfn;
    volatile uint64 analysis_lfn; // latest lfn which is doing analysis

    uint64 buf_lfn[GS_LOG_AREA_COUNT];
    log_dual_buffer_t bufs[GS_MAX_LOG_BUFFERS];
    log_queue_t tx_queue;

    char *logwr_head_buf;
    char *logwr_buf; // for log flush
    char *logwr_cipher_buf;
    uint32 logwr_buf_pos;
    uint32 logwr_buf_size;
    uint32 logwr_cipher_buf_size;
    bool32 log_encrypt;

    log_point_t curr_point;
    log_point_t curr_analysis_point;
    log_point_t curr_replay_point;
    knl_scn_t curr_scn;
    log_stat_t stat;
    uint32 batch_session_cnt;
    uint32 batch_sids[GS_MAX_SESSIONS];
    log_replay_proc replay_procs[RD_TYPE_END];
    uint8 cache_align[CACHE_LINESIZE];

    uint16 curr_file;   // current used file
    uint16 active_file; // first active file
    uint32 logfile_hwm; // max logfile placeholder, may be some holes included(logfile has been dropped)
    log_file_t *files;  // point to db logfiles
    uint64 free_size;

    thread_t thread;
    thread_t async_thread;

    /* for redo log analyze */
    replay_stat_t replay_stat;
    log_analysis_proc analysis_procs[RD_TYPE_END];
    log_check_punch_proc check_punch_proc[RD_TYPE_END];
    log_point_t redo_end_point;
    aligned_buf_t gbp_aly_mem;
    gbp_analyse_item_t *gbp_aly_items;
    gbp_analyse_bucket_t *gbp_aly_buckets;
    gbp_analyse_bucket_t gbp_aly_free_list;
    gbp_analyse_result_t gbp_aly_result;
    volatile uint64 gbp_aly_lsn;
    volatile bool32 rcy_with_gbp;
    bool32 last_rcy_with_gbp;
    log_point_t gbp_skip_point;
    log_point_t gbp_begin_point;
    log_point_t gbp_rcy_point;
    log_point_t gbp_lrp_point;
    uint64 gbp_rcy_lfn;

    date_t promote_begin_time;
    date_t promote_temp_time;
    date_t promote_end_time;
} log_context_t;

typedef struct st_callback {
    callback_keep_hb_entry keep_hb_entry; // used to send heart beat message to primary for log receiver thread
    knl_session_t *keep_hb_param;
} callback_t;

typedef struct st_raft_point {
    knl_scn_t scn;
    uint64 lfn;
    uint64 raft_index;
} raft_point_t;

typedef struct st_drop_table_def {
    char name[GS_NAME_BUFFER_SIZE];
    bool32 purge;
    uint32 options;
    bool32 is_referenced;
} drop_table_def_t;

typedef struct st_log_cursor {
    uint32 part_count;
    log_part_t *parts[GS_MAX_LOG_BUFFERS];
    uint32 offsets[GS_MAX_LOG_BUFFERS];
} log_cursor_t;

static inline int32 log_cmp_point(log_point_t *l, log_point_t *r)
{
    int32 result;

    result = l->rst_id > r->rst_id ? 1 : (l->rst_id < r->rst_id ? (-1) : 0);
    if (result != 0) {
        return result;
    }

    result = l->asn > r->asn ? 1 : (l->asn < r->asn ? (-1) : 0);
    if (result != 0) {
        return result;
    }

    result = l->block_id > r->block_id ? 1 : (l->block_id < r->block_id ? (-1) : 0);
    return result;
}

#define CURR_GROUP(cursor, id)                             \
    ((cursor)->offsets[id] >= (cursor)->parts[id]->size) ? \
        NULL :                                             \
        (log_group_t *)((char *)(cursor)->parts[id] + (cursor)->offsets[id])

// fetch a valid group which has the smallest scn in all log cursor
// the algorithm is simple but efficient and can keep scn consistency
log_group_t *log_fetch_group(log_context_t *ctx, log_cursor_t *cursor);

#define LOG_POINT_FILE_LT(l_pt, r_pt) \
    ((l_pt).rst_id < (r_pt).rst_id || ((l_pt).rst_id == (r_pt).rst_id && (l_pt).asn < (r_pt).asn))
#define LOG_POINT_FILE_EQUAL(l_pt, r_pt) ((l_pt).rst_id == (r_pt).rst_id && (l_pt).asn == (r_pt).asn)
#define LOG_LFN_EQUAL(l, r) ((l).lfn == (r).lfn)
#define LOG_LFN_GT(l_pt, r_pt) ((l_pt).lfn > (r_pt).lfn)
#define LOG_LFN_GE(l_pt, r_pt) ((l_pt).lfn >= (r_pt).lfn)
#define LOG_LFN_LT(l_pt, r_pt) ((l_pt).lfn < (r_pt).lfn)
#define LOG_LFN_LE(l_pt, r_pt) ((l_pt).lfn <= (r_pt).lfn)
#define LOG_POINT_LFN_EQUAL(l_pt, r_pt) ((l_pt)->lfn == (r_pt)->lfn)
#define LOG_LGWR_BUF_SIZE(session) ((session)->kernel->attr.lgwr_buf_size)
#define LFN_IS_CONTINUOUS(l_lfn, r_lfn) ((l_lfn) == (r_lfn) + 1)

status_t log_init(knl_session_t *session);
status_t log_load(knl_session_t *session);
void log_close(knl_session_t *session);
void log_proc(thread_t *thread);

// atomic operation
void log_atomic_op_begin(knl_session_t *session);
void log_atomic_op_end(knl_session_t *session);
void log_put(knl_session_t *session, log_type_t type, const void *data, uint32 size, uint8 flag);
void log_append_data(knl_session_t *session, const void *data, uint32 size);
void log_copy_logic_data(knl_session_t *session, log_buffer_t *buf, uint32 start_pos);
void log_commit(knl_session_t *session);

bool32 log_need_flush(log_context_t *ctx);
status_t log_flush(knl_session_t *session, log_point_t *point, knl_scn_t *scn);
void log_recycle_file(knl_session_t *session, log_point_t *point);

void log_set_page_lsn(knl_session_t *session, uint64 lsn, uint64 lfn);
void log_reset_point(knl_session_t *session, log_point_t *point);
void log_reset_analysis_point(knl_session_t *session, log_point_t *point);
void log_reset_file(knl_session_t *session, log_point_t *point);
status_t log_switch_file(knl_session_t *session);
bool32 log_switch_need_wait(knl_session_t *session, uint16 spec_file_id, uint32 spec_asn);
status_t log_switch_logfile(knl_session_t *session, uint16 spec_file_id, uint32 spec_asn, callback_t *callback);
void log_get_next_file(knl_session_t *session, uint32 *next, bool32 use_curr);
uint32 log_get_free_count(knl_session_t *session);
void log_add_freesize(knl_session_t *session, uint32 inx);
void log_decrease_freesize(log_context_t *ctx, log_file_t *logfile);
bool32 log_file_can_drop(log_context_t *ctx, uint32 file);
void log_flush_head(knl_session_t *session, log_file_t *file);
uint32 log_get_id_by_asn(knl_session_t *session, uint32 rst_id, uint32 asn, bool32 *is_curr_file);
status_t log_check_blocksize(knl_session_t *session);
status_t log_check_minsize(knl_session_t *session);
status_t log_check_asn(knl_session_t *session, bool32 force_ignorlog);
uint32 log_get_count(knl_session_t *session);
bool32 log_point_equal(log_point_t *point, log_context_t *redo_ctx);
void log_flush_init(knl_session_t *session, uint32 batch_size);
void log_stat_prepare(log_context_t *ctx);
status_t log_flush_to_disk(knl_session_t *session, log_context_t *ctx, log_batch_t *batch);
uint64 log_file_freesize(log_file_t *file);
void log_get_curr_rstid_asn(knl_session_t *session, uint32 *rst_id, uint32 *asn);
void log_unlatch_file(knl_session_t *session, uint32 file_id);

bool32 log_try_lock_logfile(knl_session_t *session);
void log_lock_logfile(knl_session_t *session);
void log_unlock_logfile(knl_session_t *session);
status_t log_set_file_asn(knl_session_t *session, uint32 asn, uint32 file_id);
status_t log_reset_logfile(knl_session_t *session, uint32 asn, uint32 log_first);
bool32 log_need_realloc_buf(log_batch_t *batch, aligned_buf_t *buf, const char *name, int64 new_size);
status_t log_get_file_offset(knl_session_t *session, const char *file_name, aligned_buf_t *buf, uint64 *offset,
    uint64 *latest_lfn, uint64 *last_scn);
status_t log_repair_file_offset(knl_session_t *session, log_file_t *file);
status_t log_verify_head_checksum(knl_session_t *session, log_file_head_t *head, char *name);
void log_calc_head_checksum(knl_session_t *session, log_file_head_t *head);
status_t log_init_file_head(knl_session_t *session, log_file_t *file);
status_t log_prepare_for_pitr(knl_session_t *se);
status_t log_size_between_two_point(knl_session_t *session, log_point_t begin, log_point_t end, uint64 *file_size);
status_t log_decrypt(knl_session_t *session, log_batch_t *batch, char *plain_buf, uint32 plain_len);
void log_append_lrep_info(knl_session_t *session, uint32 op_type, bool32 has_logic);
void log_append_lrep_addcol(knl_session_t *session, uint32 op_type, bool32 has_logic, uint32 *action);

void log_append_lrep_colname(knl_session_t *session, uint32 op_type, bool32 has_logic, uint32 *action, text_t *name);
void log_append_lrep_altindex(knl_session_t *session, uint32 op_type, bool32 has_logic, uint32 *type, const char *name);
void log_append_lrep_table(knl_session_t *session, uint32 op_type, bool32 has_logic, drop_table_def_t *def);
void log_append_lrep_index(knl_session_t *session, uint32 op_type, bool32 has_logic, const char *name);
void log_append_lrep_seq(knl_session_t *session, uint32 op_type, bool32 has_logic, text_t name);
void log_calc_batch_checksum(knl_session_t *session, log_batch_t *batch);
status_t log_load_batch(knl_session_t *session, log_point_t *point, uint32 *data_size, aligned_buf_t *buf);
status_t log_get_file_head(const char *file_name, log_file_head_t *head);
bool32 log_validate_ctrl(log_file_t *logfile);
void log_set_logfile_writepos(knl_session_t *session, log_file_t *file, uint64 offset);

static inline bool32 log_is_empty(log_file_head_t *head)
{
    return (bool32)(head->write_pos <= (uint32)CM_CALC_ALIGN(sizeof(log_file_head_t), (uint32)head->block_size));
}

static inline bool32 log_point_is_invalid(log_point_t *point)
{
    return (bool32)(point->asn == GS_INVALID_ASN || point->lfn == 0);
}

static inline void log_encrypt_prepare(knl_session_t *session, uint8 page_type, bool32 need_encrypt)
{
    if (SECUREC_UNLIKELY(need_encrypt)) {
        session->log_encrypt = GS_TRUE;
#ifdef LOG_DIAG
        if (page_type != GS_INVALID_ID8) {
            knl_panic(page_type_suport_encrypt(page_type));
        }
#endif
    }
}

#ifdef __cplusplus
}
#endif

#endif
