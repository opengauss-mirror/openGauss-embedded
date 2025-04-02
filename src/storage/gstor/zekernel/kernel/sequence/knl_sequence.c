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
 * knl_sequence.c
 * implement of sequence
 *
 * IDENTIFICATION
 * src/storage/gstor/zekernel/kernel/sequence/knl_sequence.c
 *
 * -------------------------------------------------------------------------
 */
#include "knl_sequence.h"
#include "dc_seq.h"
#include "dc_log.h"
#include "knl_context.h"

#define MINVAL_COLUMN_ID 3
#define MAXVAL_COLUMN_ID 4
#define STEP_COLUMN_ID 5
#define CACHESIZE_COLUMN_ID 6
#define CYCLE_FLAG_COLUMN_ID 7
#define ORDER_FALG_COLUMN_ID 8
#define ORG_SCN_COLUMN_ID 9
#define CHG_SCN_COLUMN_ID 10
#define LASTVAL_COLUMN_ID 11

/*
 * create sequence
 * @param[in]  session - knl_session_t
 * @param[in]  def - pointer to the definition of sequence
 * @return
 * - GS_SUCCESS
 * - GS_ERROR
 * @note
 * @par history:  2017/4/26, wangjincheng 343637, create
 *
 */
status_t db_create_sequence(knl_session_t *session, knl_sequence_def_t *def)
{
    uint32 max_size;
    row_assist_t ra;
    knl_cursor_t *cursor = NULL;
    text_t txt_seq;
    sequence_desc_t desc;
    dc_user_t *user = NULL;
    rd_seq_t redo;
    errno_t ret;

    bool32 has_logic = session->kernel->db.ctrl.core.lrep_mode == LOG_REPLICATION_ON;
    uint32 op_type = RD_CREATE_SEQUENCE;

    if (!dc_get_user_id(session, &def->user, &desc.uid)) {
        GS_THROW_ERROR(ERR_USER_NOT_EXIST, T2S(&def->user));
        return GS_ERROR;
    }

    txt_seq = def->name;

    (void)cm_text2str(&txt_seq, desc.name, GS_MAX_NAME_LEN + 1);
    desc.is_cache = def->nocache ? 0 : 1;
    desc.is_cyclable = def->is_cycle;
    desc.is_order = def->is_order;
    desc.step = def->step;
    desc.org_scn = db_inc_scn(session);
    desc.chg_scn = desc.org_scn;
    desc.minval = def->min_value;
    desc.maxval = def->max_value;
    desc.cache = (uint64)(desc.is_cache ? def->cache : 0);
#ifdef Z_SHARDING
    desc.dist_data = def->dist_data;
#endif

    if (dc_alloc_seq_entry(session, &desc) != GS_SUCCESS) {
        return GS_ERROR;
    }

    CM_SAVE_STACK(session->stack);

    log_append_lrep_info(session, op_type, has_logic);

    cursor = knl_push_cursor(session);

    max_size = session->kernel->attr.max_row_size;
    row_init(&ra, cursor->buf, max_size, SYS_SEQUENCE_COLUMN_COUNT);
    (void)row_put_int32(&ra, desc.uid);
    (void)row_put_int32(&ra, desc.id);
    (void)row_put_str(&ra, desc.name);
    (void)row_put_int64(&ra, desc.minval);
    (void)row_put_int64(&ra, desc.maxval);
    (void)row_put_int64(&ra, desc.step);
    (void)row_put_int64(&ra, desc.cache);
    (void)row_put_int32(&ra, desc.is_cyclable);
    (void)row_put_int32(&ra, desc.is_order);
    (void)row_put_int64(&ra, desc.org_scn);
    (void)row_put_int64(&ra, desc.chg_scn);
    (void)row_put_int64(&ra, def->start);
#ifdef Z_SHARDING
    (void)row_put_bin(&ra, &desc.dist_data);
#endif

    knl_open_sys_cursor(session, cursor, CURSOR_ACTION_INSERT, SYS_SEQ_ID, SYS_SEQ001_ID);

    if (knl_internal_insert(session, cursor) != GS_SUCCESS) {
        session->rm->is_ddl_op = GS_FALSE;
        if (dc_open_user_by_id(session, desc.uid, &user) != GS_SUCCESS) {
            knl_panic_log(0, "[SEQUENCE] ABORT INFO: open user failed while drop sequence");
        }
        sequence_entry_t *entry = DC_GET_SEQ_ENTRY(user, desc.id);
        dc_sequence_drop(session, entry);
        CM_RESTORE_STACK(session->stack);
        return GS_ERROR;
    }
    knl_commit(session);
    redo.op_type = RD_CREATE_SEQUENCE;
    redo.id = desc.id;
    redo.uid = desc.uid;
    ret = strcpy_sp(redo.seq_name, GS_NAME_BUFFER_SIZE, desc.name);
    knl_securec_check(ret);
    log_put(session, RD_LOGIC_OPERATION, &redo, sizeof(rd_seq_t),
        has_logic ? LOG_ENTRY_FLAG_WITH_LOGIC_OID : LOG_ENTRY_FLAG_NONE);

    if (has_logic) {
        session->rm->is_ddl_op = GS_FALSE;
    }

    CM_RESTORE_STACK(session->stack);
    return GS_SUCCESS;
}

status_t rd_dc_create_sequence(knl_session_t *session, dc_user_t *user, text_t *seq_name)
{
    knl_cursor_t *cursor = NULL;
    sequence_desc_t desc;
    sequence_entry_t *entry = NULL;
    errno_t err;

    CM_SAVE_STACK(session->stack);

    cursor = knl_push_cursor(session);

    knl_open_sys_cursor(session, cursor, CURSOR_ACTION_SELECT, SYS_SEQ_ID, SYS_SEQ001_ID);

    knl_init_index_scan(cursor, GS_TRUE);
    knl_set_scan_key(INDEX_DESC(cursor->index), &cursor->scan_range.l_key, GS_TYPE_INTEGER, (void *)&user->desc.id,
        sizeof(uint32), IX_COL_SYS_SEQ001_UID);
    knl_set_scan_key(INDEX_DESC(cursor->index), &cursor->scan_range.l_key, GS_TYPE_STRING, (void *)seq_name->str,
        seq_name->len, IX_COL_SYS_SEQ001_NAME);

    if (knl_fetch(session, cursor) != GS_SUCCESS) {
        CM_RESTORE_STACK(session->stack);
        return GS_ERROR;
    }

    if (cursor->eof) {
        CM_RESTORE_STACK(session->stack);
        return GS_ERROR;
    }

    dc_convert_seq_desc(cursor, &desc);
    cm_spin_lock(&user->lock, NULL);
    if (dc_create_sequence_entry(session, user, desc.id, &entry) != GS_SUCCESS) {
        cm_spin_unlock(&user->lock);
        CM_RESTORE_STACK(session->stack);
        return GS_ERROR;
    }

    err = memcpy_sp(entry->name, GS_NAME_BUFFER_SIZE, desc.name, GS_MAX_NAME_LEN + 1);
    knl_securec_check(err);
    dc_insert_into_seqindex(user, entry);

    if (desc.id >= user->sequence_set.sequence_hwm) {
        user->sequence_set.sequence_hwm = desc.id + 1;
    }
    cm_spin_unlock(&user->lock);
    CM_RESTORE_STACK(session->stack);

    return GS_SUCCESS;
}

void rd_create_sequence(knl_session_t *session, log_entry_t *log)
{
    rd_seq_t *rd = (rd_seq_t *)log->data;
    dc_user_t *user = NULL;
    knl_dictionary_t dc;
    text_t seq_name;

    if (dc_open_user_by_id(session, rd->uid, &user) != GS_SUCCESS) {
        GS_LOG_RUN_ERR("[SEQ] failed to replay create sequence,user id %u doesn't exists", rd->uid);
        rd_check_dc_replay_err(session);
        return;
    }

    cm_str2text(rd->seq_name, &seq_name);

    if (dc_init_sequence_set(session, user) != GS_SUCCESS) {
        GS_LOG_RUN_ERR("[SEQ] failed to replay create sequence");
        rd_check_dc_replay_err(session);
        return;
    }

    if (dc_seq_find(session, user, &seq_name, &dc)) {
        GS_LOG_RUN_INF("[SEQ] no need to replay create sequence,sequence already exists");
        return;
    }

    if (rd_dc_create_sequence(session, user, &seq_name) != GS_SUCCESS) {
        GS_LOG_RUN_ERR("[SEQ] failed to create sequence");
        rd_check_dc_replay_err(session);
    }
}

void print_create_sequence(log_entry_t *log)
{
    rd_seq_t *rd = (rd_seq_t *)log->data;
    printf("create sequence uid:%d,seq_name:%s\n", rd->uid, rd->seq_name);
}

status_t db_get_seq_dist_data(knl_session_t *session, text_t *user, text_t *name, binary_t **dist_data)
{
    knl_dictionary_t dc;
    dc_sequence_t *sequence = NULL;

    if (GS_SUCCESS != dc_seq_open(session, user, name, &dc)) {
        return GS_ERROR;
    }

    sequence = (dc_sequence_t *)dc.handle;
    *dist_data = &sequence->dist_data;
    dc_seq_close(&dc);
    return GS_SUCCESS;
}

status_t db_get_sequence_id(knl_session_t *session, text_t *user, text_t *name, uint32 *id)
{
    knl_dictionary_t dc;
    dc_sequence_t *sequence = NULL;

    if (GS_SUCCESS != dc_seq_open(session, user, name, &dc)) {
        return GS_ERROR;
    }

    sequence = (dc_sequence_t *)dc.handle;
    *id = sequence->id;
    dc_seq_close(&dc);
    return GS_SUCCESS;
}

status_t db_set_cn_seq_currval(knl_session_t *session, text_t *user, text_t *name, int64 nextval)
{
    dc_sequence_t *sequence = NULL;
    sequence_entry_t *entry = NULL;
    dc_currval_t *currval = NULL;
    knl_dictionary_t dc_seq;

    if (GS_SUCCESS != dc_seq_open(session, user, name, &dc_seq)) {
        return GS_ERROR;
    }

    sequence = (dc_sequence_t *)dc_seq.handle;
    entry = sequence->entry;
    cm_spin_lock(&entry->lock, NULL);

    if (!sequence->valid || entry->org_scn > DB_CURR_SCN(session)) {
        dc_seq_close(&dc_seq);
        cm_spin_unlock(&entry->lock);
        GS_THROW_ERROR(ERR_SEQ_NOT_EXIST, T2S(user), T2S_EX(name));
        return GS_ERROR;
    }

    /* Initialize the currval in current session */
    if (sequence->currvals[session->id / DC_GROUP_CURRVAL_COUNT] == NULL) {
        if (dc_alloc_mem(&session->kernel->dc_ctx, entry->entity->memory, GS_SHARED_PAGE_SIZE,
            (void **)&sequence->currvals[session->id / DC_GROUP_CURRVAL_COUNT]) != GS_SUCCESS) {
            dc_seq_close(&dc_seq);
            cm_spin_unlock(&entry->lock);
            return GS_ERROR;
        }
    }

    /* save the currval on the that user session's sequence slot */
    currval = DC_GET_SEQ_CURRVAL(sequence, session->id);
    currval->data = nextval;
    currval->serial_id = session->serial_id;
    sequence->lastval = nextval;
    sequence->rsv_nextval = nextval;

    dc_seq_close(&dc_seq);
    cm_spin_unlock(&entry->lock);

    return GS_SUCCESS;
}

status_t db_current_seq_value(knl_session_t *session, text_t *user, text_t *name, int64 *currval)
{
    knl_dictionary_t dc;
    dc_sequence_t *sequence = NULL;
    dc_currval_t *dc_currval = NULL;

    if (GS_SUCCESS != dc_seq_open(session, user, name, &dc)) {
        return GS_ERROR;
    }

    sequence = (dc_sequence_t *)dc.handle;
    cm_spin_lock(&sequence->entry->lock, NULL);

    if (sequence->currvals[session->id / DC_GROUP_CURRVAL_COUNT] == NULL) {
        GS_THROW_ERROR(ERR_SEQ_INVALID, "sequence CURRVAL is not defined in this session");
        cm_spin_unlock(&sequence->entry->lock);
        dc_seq_close(&dc);
        return GS_ERROR;
    }

    dc_currval = DC_GET_SEQ_CURRVAL(sequence, session->id);
    if (dc_currval->serial_id != session->serial_id) {
        GS_THROW_ERROR(ERR_SEQ_INVALID, "sequence CURRVAL is not defined in this session");
        cm_spin_unlock(&sequence->entry->lock);
        dc_seq_close(&dc);
        return GS_ERROR;
    }
    /* max_value of sequence_def is DDL_ASC_SEQUENCE_DEFAULT_MAX_VALUE, i.e., LLONG_MAX(i.e., 2^63-1) */
    *currval = dc_currval->data;
    cm_spin_unlock(&sequence->entry->lock);
    dc_seq_close(&dc);
    return GS_SUCCESS;
}

/*
 * generate the nextval of the sequence
 * @param[in]  sequence in dc
 * @param[out]  sequence in dc
 * @return
 * - GS_SUCCESS
 * - GS_ERROR
 */
static status_t seq_get_next_value(dc_sequence_t *sequence)
{
    int64 res;

    if (sequence->lastval > sequence->maxval || sequence->lastval < sequence->minval) {
        if (sequence->is_cyclable == GS_FALSE) {
            if (sequence->step > 0) {
                GS_THROW_ERROR(ERR_SEQ_INVALID, "sequence NEXTVAL exceeds MAXVALUE");
            } else {
                GS_THROW_ERROR(ERR_SEQ_INVALID, "sequence NEXTVAL goes below MINVALUE");
            }
            return GS_ERROR;
        }

        sequence->lastval = sequence->step > 0 ? sequence->minval : sequence->maxval;
    }

#ifdef _MSC_VER
    if (opr_int64add_overflow(sequence->rsv_nextval, sequence->step, res))
#else
    if (opr_int64add_overflow(sequence->rsv_nextval, sequence->step, &res))
#endif
    {
        if(sequence->is_cyclable == GS_FALSE){
            if (sequence->step > 0) {
                GS_THROW_ERROR(ERR_SEQ_INVALID, "sequence NEXTVAL exceeds MAXVALUE");
            } else {
                GS_THROW_ERROR(ERR_SEQ_INVALID, "sequence NEXTVAL goes below MINVALUE");
            }
            return GS_ERROR;
        }
        sequence->lastval = sequence->step > 0 ? sequence->minval : sequence->maxval;
    }

    sequence->rsv_nextval = sequence->lastval;
    sequence->lastval += sequence->step;

    return GS_SUCCESS;
}

static bool32 db_reach_sequence_boundary(dc_sequence_t *sequence, uint32 batch_count, int64 *end_val,
    uint32 group_order, uint32 group_cnt)
{
    int64 val;

    val = sequence->lastval + batch_count * sequence->step;
    if ((sequence->step > 0 && val < sequence->maxval) || (sequence->step < 0 && val > sequence->minval)) {
        sequence->lastval = val;
        *end_val = val - sequence->step;
        return GS_FALSE;
    }

    *end_val = sequence->step > 0 ? sequence->maxval : sequence->minval;

    if (sequence->is_cyclable) {
        sequence->lastval = ((sequence->step > 0) ? sequence->minval : sequence->maxval);
        return GS_TRUE;
    }

    /* sequence->lastval cannot exceed the boundary */
    if (sequence->step > 0) {
        sequence->lastval += ((sequence->maxval - sequence->lastval) / sequence->step + 1) * sequence->step;
    } else if (sequence->step < 0) {
        sequence->lastval += ((sequence->minval - sequence->lastval) / sequence->step + 1) * sequence->step;
    }

    return GS_TRUE;
}

/*
 * generate value of last_number which will be recoreded in sequence$
 * @param[in]   sequence - dictionary of sequence
 * @param[out]  last_number - value of last_number
 * @return void
 */
static void seq_generate_last_number(dc_sequence_t *sequence, int64 *last_number)
{
    int64 val, cache_size;

    cache_size = sequence->cache_size > 0 ? sequence->cache_size : 1;

    /* Note: defination check guarantee cache * step can not exceed the boundary */
    val = *last_number + cache_size * sequence->step;
    if ((sequence->step > 0 && val <= sequence->maxval) || (sequence->step < 0 && val >= sequence->minval)) {
        *last_number = val;
        return;
    }

    knl_panic(sequence->step != 0);
    if (sequence->is_cyclable) {
        /* val and *last_number cannot exceed the boundary */
        val = sequence->step > 0 ? sequence->maxval : sequence->minval;
        val = (cache_size - cm_abs64((val - *last_number) / sequence->step) - 1) * sequence->step;
        *last_number = (sequence->step > 0 ? sequence->minval : sequence->maxval) + val;
        return;
    }

    /* *last_number cannot exceed the boundary */
    if (sequence->step > 0) {
        *last_number += ((sequence->maxval - *last_number) / sequence->step + 1) * sequence->step;
    } else if (sequence->step < 0) {
        *last_number += ((sequence->minval - *last_number) / sequence->step + 1) * sequence->step;
    }

    return;
}

/*
 * fetch description of specified sequence from sequence$
 * @param[in]   session - kernel reserved session for handle sequences
 * @param[in]   cursor  - kernel of the cursor
 * @param[in]   uid  - user id
 * @param[in]   name - name of sequence
 * @param[out]  cursor
 * @return
 * - GS_SUCCESS
 * - GS_ERROR
 */
status_t db_fetch_seq_description(knl_session_t *session, knl_cursor_t *cursor, uint32 uid, text_t *name)
{
    knl_open_sys_cursor(session, cursor, CURSOR_ACTION_UPDATE, SYS_SEQ_ID, 0);
    knl_init_index_scan(cursor, GS_TRUE);
    knl_set_scan_key(INDEX_DESC(cursor->index), &cursor->scan_range.l_key, GS_TYPE_INTEGER, &uid, sizeof(uint32), 0);
    knl_set_scan_key(INDEX_DESC(cursor->index), &cursor->scan_range.l_key, GS_TYPE_STRING, name->str, name->len, 1);

    if (GS_SUCCESS != knl_fetch(session, cursor)) {
        return GS_ERROR;
    }

    if (cursor->eof) {
        GS_THROW_ERROR(ERR_SEQ_INVALID, "sequence does not exist");
        return GS_ERROR;
    }

    return GS_SUCCESS;
}

status_t seq_generate_cache(knl_session_t *session, dc_sequence_t *sequence, text_t *name)
{
    uint16 size;
    int64 lastval;
    row_assist_t ra;
    knl_cursor_t *cursor = NULL;

    if (++sequence->cache_pos >= sequence->cache_size) {
        if (knl_begin_auton_rm(session) != GS_SUCCESS) {
            return GS_ERROR;
        }

        CM_SAVE_STACK(session->stack);

        cursor = knl_push_cursor(session);

        if (GS_SUCCESS != db_fetch_seq_description(session, cursor, sequence->uid, name)) {
            CM_RESTORE_STACK(session->stack);
            knl_end_auton_rm(session, GS_ERROR);
            return GS_ERROR;
        }

        lastval = *(int64 *)CURSOR_COLUMN_DATA(cursor, LASTVAL_COLUMN_ID);

        (void)seq_generate_last_number(sequence, &lastval);

        row_init(&ra, cursor->update_info.data, HEAP_MAX_ROW_SIZE, 1);
        (void)row_put_int64(&ra, lastval);
        cursor->update_info.count = 1;
        cursor->update_info.columns[0] = LASTVAL_COLUMN_ID;
        cm_decode_row(cursor->update_info.data, cursor->update_info.offsets, cursor->update_info.lens, &size);
        if (GS_SUCCESS != knl_internal_update(session, cursor)) {
            CM_RESTORE_STACK(session->stack);
            knl_end_auton_rm(session, GS_ERROR);
            return GS_ERROR;
        }

        sequence->cache_pos = 0;
        CM_RESTORE_STACK(session->stack);

        knl_end_auton_rm(session, GS_SUCCESS);
    }

    return GS_SUCCESS;
}

status_t db_next_seq_value(knl_session_t *session, text_t *user, text_t *name, int64 *nextval)
{
    dc_sequence_t *sequence = NULL;
    sequence_entry_t *entry = NULL;
    dc_currval_t *currval = NULL;
    knl_dictionary_t dc_seq;

    if (GS_SUCCESS != dc_seq_open(session, user, name, &dc_seq)) {
        return GS_ERROR;
    }

    sequence = (dc_sequence_t *)dc_seq.handle;
    entry = sequence->entry;
    cm_spin_lock(&entry->lock, NULL);

    if (!sequence->valid || entry->org_scn > DB_CURR_SCN(session)) {
        dc_seq_close(&dc_seq);
        cm_spin_unlock(&entry->lock);
        GS_THROW_ERROR(ERR_SEQ_NOT_EXIST, T2S(user), T2S_EX(name));
        return GS_ERROR;
    }

    if (seq_generate_cache(session, sequence, name) != GS_SUCCESS) {
        dc_seq_close(&dc_seq);
        cm_spin_unlock(&entry->lock);
        return GS_ERROR;
    }

    if (GS_SUCCESS != seq_get_next_value(sequence)) {
        dc_seq_close(&dc_seq);
        cm_spin_unlock(&entry->lock);
        return GS_ERROR;
    }

    *nextval = sequence->rsv_nextval;

    /* Initialize the currval in current session */
    if (sequence->currvals[session->id / DC_GROUP_CURRVAL_COUNT] == NULL) {
        if (dc_alloc_mem(&session->kernel->dc_ctx, entry->entity->memory, GS_SHARED_PAGE_SIZE,
            (void **)&sequence->currvals[session->id / DC_GROUP_CURRVAL_COUNT]) != GS_SUCCESS) {
            dc_seq_close(&dc_seq);
            cm_spin_unlock(&entry->lock);
            return GS_ERROR;
        }
    }

    /* save the currval on the that user session's sequence slot */
    currval = DC_GET_SEQ_CURRVAL(sequence, session->id);
    currval->data = *nextval;
    currval->serial_id = session->serial_id;

    dc_seq_close(&dc_seq);
    cm_spin_unlock(&entry->lock);

    return GS_SUCCESS;
}

status_t db_get_nextval_for_cn(knl_session_t *session, text_t *user, text_t *name, int64 *value)
{
    dc_sequence_t *sequence = NULL;
    sequence_entry_t *entry = NULL;
    knl_dictionary_t dc_seq;

    if (GS_SUCCESS != dc_seq_open(session, user, name, &dc_seq)) {
        return GS_ERROR;
    }

    sequence = (dc_sequence_t *)dc_seq.handle;
    entry = sequence->entry;
    cm_spin_lock(&entry->lock, NULL);

    if (!sequence->valid || entry->org_scn > DB_CURR_SCN(session)) {
        GS_THROW_ERROR(ERR_SEQ_NOT_EXIST, T2S(user), T2S_EX(name));
        dc_seq_close(&dc_seq);
        cm_spin_unlock(&entry->lock);
        return GS_ERROR;
    }

    if (seq_generate_cache(session, sequence, name) != GS_SUCCESS) {
        dc_seq_close(&dc_seq);
        cm_spin_unlock(&entry->lock);
        return GS_ERROR;
    }

    *value = sequence->lastval;
    sequence->rsv_nextval = sequence->lastval;
    dc_seq_close(&dc_seq);
    cm_spin_unlock(&entry->lock);
    return GS_SUCCESS;
}

status_t db_multi_seq_value(knl_session_t *session, knl_sequence_def_t *def, uint32 group_order, uint32 group_cnt,
    uint32 count)
{
    dc_sequence_t *sequence = NULL;
    sequence_entry_t *entry = NULL;
    knl_dictionary_t dc_seq;
    uint32 remain_size = count;
    uint32 batch_count;
    bool32 is_first = GS_TRUE;
    int64 start_val = 0;
    int64 end_val = 0;
    bool32 allow_cycle = GS_TRUE;

    if (GS_SUCCESS != dc_seq_open(session, &def->user, &def->name, &dc_seq)) {
        return GS_ERROR;
    }

    sequence = (dc_sequence_t *)dc_seq.handle;
    entry = sequence->entry;
    cm_spin_lock(&entry->lock, NULL);

    if (!sequence->valid || entry->org_scn > DB_CURR_SCN(session)) {
        dc_seq_close(&dc_seq);
        cm_spin_unlock(&entry->lock);
        GS_THROW_ERROR(ERR_SEQ_NOT_EXIST, T2S(&def->user), T2S_EX(&def->name));
        return GS_ERROR;
    }

    while (remain_size) {
        if (seq_generate_cache(session, sequence, &def->name) != GS_SUCCESS) {
            dc_seq_close(&dc_seq);
            cm_spin_unlock(&entry->lock);
            return GS_ERROR;
        }

        batch_count = (uint32)(sequence->cache_size - sequence->cache_pos);
        batch_count = (batch_count > 0) ? batch_count : 1;
        batch_count = (remain_size > batch_count) ? batch_count : remain_size;

        if (sequence->lastval > sequence->maxval || sequence->lastval < sequence->minval) {
            if (sequence->is_cyclable == GS_FALSE) {
                dc_seq_close(&dc_seq);
                cm_spin_unlock(&entry->lock);
                GS_THROW_ERROR(ERR_SEQ_INVALID, "sequence exceeds MINVALUE or MAXVALUE");
                return GS_ERROR;
            }
        }

        if (is_first) {
            start_val = sequence->lastval;
            is_first = GS_FALSE;
        }

        if (db_reach_sequence_boundary(sequence, batch_count, &end_val, group_order, group_cnt)) {
            if (sequence->is_cyclable && allow_cycle) {
                allow_cycle = GS_FALSE;
                if ((sequence->step > 0 && start_val > end_val) || (sequence->step < 0 && start_val < end_val)) {
                    if (sequence->step > 0) {
                        start_val = sequence->minval;
                    } else {
                        start_val = sequence->maxval;
                    }
                    continue;
                }
            }
            break;
        }

        sequence->cache_pos += batch_count - 1;
        remain_size -= batch_count;
    }

    sequence->rsv_nextval = sequence->lastval;
    def->step = sequence->step;
    def->start = start_val;
    def->max_value = end_val;
    dc_seq_close(&dc_seq);
    cm_spin_unlock(&entry->lock);

    return GS_SUCCESS;
}

/*
 * implement of drop sequence
 * @param[in]   session - user session
 * @param[in]   user - username
 * @param[in]   name - sequence name
 * @return
 * - GS_SUCCESS
 * - GS_ERROR
 */
status_t db_drop_sequence(knl_session_t *session, knl_dictionary_t *dc, bool32 *exists)
{
    knl_cursor_t *cursor = NULL;
    text_t name;
    dc_sequence_t *seq = (dc_sequence_t *)dc->handle;
    rd_seq_t redo;
    errno_t ret;
    obj_info_t obj_addr;
    bool32 has_logic = session->kernel->db.ctrl.core.lrep_mode == LOG_REPLICATION_ON;
    uint32 op_type = RD_DROP_SEQUENCE;
    cm_str2text(seq->name, &name);

    knl_set_session_scn(session, GS_INVALID_ID64);

    CM_SAVE_STACK(session->stack);

    cursor = knl_push_cursor(session);

    knl_open_sys_cursor(session, cursor, CURSOR_ACTION_DELETE, SYS_SEQ_ID, 0);

    knl_init_index_scan(cursor, GS_TRUE);
    knl_set_scan_key(INDEX_DESC(cursor->index), &cursor->scan_range.l_key, GS_TYPE_INTEGER, &seq->uid, sizeof(uint32),
        0);
    knl_set_scan_key(INDEX_DESC(cursor->index), &cursor->scan_range.l_key, GS_TYPE_STRING, name.str, name.len, 1);

    if (GS_SUCCESS != knl_fetch(session, cursor)) {
        CM_RESTORE_STACK(session->stack);
        return GS_ERROR;
    }

    if (cursor->eof) {
        *exists = GS_FALSE;
        CM_RESTORE_STACK(session->stack);
        return GS_SUCCESS;
    }

    *exists = GS_TRUE;

    log_append_lrep_seq(session, op_type, has_logic, name);

    if (GS_SUCCESS != knl_internal_delete(session, cursor)) {
        session->rm->is_ddl_op = GS_FALSE;
        CM_RESTORE_STACK(session->stack);
        return GS_ERROR;
    }

    /* drop the privileges on the sequence granted to user and roles */
    if (GS_SUCCESS != db_drop_object_privs(session, seq->uid, seq->name, OBJ_TYPE_SEQUENCE)) {
        session->rm->is_ddl_op = GS_FALSE;
        CM_RESTORE_STACK(session->stack);
        return GS_ERROR;
    }

    redo.op_type = RD_DROP_SEQUENCE;
    redo.id = seq->id;
    redo.uid = seq->uid;
    ret = strcpy_sp(redo.user_name, GS_NAME_BUFFER_SIZE, seq->entry->user->desc.name);
    knl_securec_check(ret);
    ret = strcpy_sp(redo.seq_name, GS_NAME_BUFFER_SIZE, seq->name);
    knl_securec_check(ret);
    log_put(session, RD_LOGIC_OPERATION, &redo, sizeof(rd_seq_t),
        has_logic ? LOG_ENTRY_FLAG_WITH_LOGIC_OID : LOG_ENTRY_FLAG_NONE);

    if (has_logic) {
        session->rm->is_ddl_op = GS_FALSE;
    }

    obj_addr.oid = dc->oid;
    obj_addr.uid = dc->uid;
    obj_addr.tid = OBJ_TYPE_SEQUENCE;
    if (g_knl_callback.update_depender && g_knl_callback.update_depender(session, &obj_addr) != GS_SUCCESS) {
        CM_RESTORE_STACK(session->stack);
        return GS_ERROR;
    }
    knl_commit(session);

    dc_drop_object_privs(&session->kernel->dc_ctx, seq->uid, seq->name, OBJ_TYPE_SEQUENCE);
    dc_sequence_drop(session, seq->entry);
    CM_RESTORE_STACK(session->stack);
    return GS_SUCCESS;
}

void print_drop_sequence(log_entry_t *log)
{
    rd_seq_t *rd = (rd_seq_t *)log->data;
    printf("drop sequence uid:%d,seq_name:%s\n", rd->uid, rd->seq_name);
}

/*
 * implement of drop sequence
 * @param[in]   session - user session
 * @param[in]   def - sequence defination
 * @return
 * - GS_SUCCESS
 * - GS_ERROR
 */
status_t db_alter_sequence(knl_session_t *session, knl_sequence_def_t *def)
{
    row_assist_t ra;
    knl_dictionary_t dc_seq;
    knl_cursor_t *cursor = NULL;
    text_t *name = NULL;
    text_t *user = NULL;
    dc_sequence_t *sequence = NULL;
    sequence_entry_t *entry = NULL;
    uint16 update_cols = 0;
    uint16 size;
    bool32 lastnum_changed = GS_FALSE;
    rd_seq_t redo;
    bool32 has_logic = session->kernel->db.ctrl.core.lrep_mode == LOG_REPLICATION_ON;
    uint32 op_type = RD_ALTER_SEQUENCE;

    if (!def->is_option_set) {
        GS_THROW_ERROR(ERR_SEQ_INVALID, "no options specified for alter sequence");
        return GS_ERROR;
    }

    user = &def->user;
    name = &def->name;

    if (GS_SUCCESS != dc_seq_open(session, user, name, &dc_seq)) {
        return GS_ERROR;
    }

    sequence = (dc_sequence_t *)dc_seq.handle;
    entry = sequence->entry;
    cm_spin_lock(&entry->lock, NULL);

    CM_SAVE_STACK(session->stack);
    cursor = knl_push_cursor(session);

    if (GS_SUCCESS != db_fetch_seq_description(session, cursor, sequence->uid, name)) {
        cm_spin_unlock(&entry->lock);
        CM_RESTORE_STACK(session->stack);
        dc_seq_close(&dc_seq);
        return GS_ERROR;
    }

    log_append_lrep_info(session, op_type, has_logic);

    row_init(&ra, cursor->update_info.data, HEAP_MAX_ROW_SIZE, 12);

    if (def->is_minval_set) {
        cursor->update_info.columns[update_cols++] = MINVAL_COLUMN_ID;
        (void)row_put_int64(&ra, def->min_value);
        sequence->minval = def->min_value;
    }

    if (def->is_maxval_set) {
        cursor->update_info.columns[update_cols++] = MAXVAL_COLUMN_ID;
        (void)row_put_int64(&ra, def->max_value);
        sequence->maxval = def->max_value;
    }

    if (def->is_step_set) {
        cursor->update_info.columns[update_cols++] = STEP_COLUMN_ID;
        (void)row_put_int64(&ra, def->step);
        lastnum_changed = GS_TRUE;
    }

    if (def->is_cache_set) {
        cursor->update_info.columns[update_cols++] = CACHESIZE_COLUMN_ID;
        (void)row_put_int64(&ra, def->cache);
        sequence->cache_size = def->cache;
        lastnum_changed = GS_TRUE;
    }

    if (def->is_cycle != sequence->is_cyclable) {
        cursor->update_info.columns[update_cols++] = CYCLE_FLAG_COLUMN_ID;
        (void)row_put_int32(&ra, def->is_cycle);
        sequence->is_cyclable = def->is_cycle;
    }

    if (def->is_order != sequence->is_order) {
        cursor->update_info.columns[update_cols++] = ORDER_FALG_COLUMN_ID;
        (void)row_put_int32(&ra, def->is_order);
        sequence->is_order = def->is_order;
    }

    cursor->update_info.columns[update_cols++] = CHG_SCN_COLUMN_ID;
    (void)row_put_int64(&ra, db_inc_scn(session));

    /* regenerate lastnumber when step or cache size changed */
    if (lastnum_changed) {
        sequence->lastval += def->step - sequence->step;
        sequence->step = def->step;
        cursor->update_info.columns[update_cols++] = LASTVAL_COLUMN_ID;
        (void)row_put_int64(&ra, sequence->lastval);

        /* regenerate last_number when nextval */
        sequence->cache_pos = sequence->cache_size;
    }

    cursor->update_info.count = update_cols;
    ra.head->column_count = update_cols;
    cm_decode_row(cursor->update_info.data, cursor->update_info.offsets, cursor->update_info.lens, &size);

    if (GS_SUCCESS != knl_internal_update(session, cursor)) {
        session->rm->is_ddl_op = GS_FALSE;
        cm_spin_unlock(&entry->lock);
        CM_RESTORE_STACK(session->stack);
        dc_seq_close(&dc_seq);
        return GS_ERROR;
    }

    redo.op_type = RD_ALTER_SEQUENCE;
    redo.uid = dc_seq.uid;
    redo.id = dc_seq.oid;
    log_put(session, RD_LOGIC_OPERATION, &redo, sizeof(rd_seq_t),
        has_logic ? LOG_ENTRY_FLAG_WITH_LOGIC_OID : LOG_ENTRY_FLAG_NONE);

    if (has_logic) {
        session->rm->is_ddl_op = GS_FALSE;
    }

    knl_commit(session);

    cm_spin_unlock(&entry->lock);

    CM_RESTORE_STACK(session->stack);
    dc_seq_close(&dc_seq);

    return GS_SUCCESS;
}

status_t seq_update_seq_nextval(knl_session_t *session, dc_sequence_t *sequence, text_t *name, int64 value)
{
    uint16 size;
    row_assist_t ra;
    knl_cursor_t *cursor = NULL;

    if (knl_begin_auton_rm(session) != GS_SUCCESS) {
        return GS_ERROR;
    }

    CM_SAVE_STACK(session->stack);

    cursor = knl_push_cursor(session);

    if (GS_SUCCESS != db_fetch_seq_description(session, cursor, sequence->uid, name)) {
        CM_RESTORE_STACK(session->stack);
        knl_end_auton_rm(session, GS_ERROR);
        return GS_ERROR;
    }

    row_init(&ra, cursor->update_info.data, HEAP_MAX_ROW_SIZE, 1);
    (void)row_put_int64(&ra, value);
    cursor->update_info.count = 1;
    cursor->update_info.columns[0] = LASTVAL_COLUMN_ID;
    cm_decode_row(cursor->update_info.data, cursor->update_info.offsets, cursor->update_info.lens, &size);
    if (GS_SUCCESS != knl_internal_update(session, cursor)) {
        CM_RESTORE_STACK(session->stack);
        knl_end_auton_rm(session, GS_ERROR);
        return GS_ERROR;
    }

    sequence->lastval = value;
    sequence->cache_pos = sequence->cache_size;
    CM_RESTORE_STACK(session->stack);

    knl_end_auton_rm(session, GS_SUCCESS);

    return GS_SUCCESS;
}

status_t db_get_seq_def(knl_session_t *session, text_t *user, text_t *name, knl_sequence_def_t *def)
{
    dc_sequence_t *sequence = NULL;
    sequence_entry_t *entry = NULL;
    knl_dictionary_t dc_seq;

    if (GS_SUCCESS != dc_seq_open(session, user, name, &dc_seq)) {
        return GS_ERROR;
    }

    sequence = (dc_sequence_t *)dc_seq.handle;
    entry = sequence->entry;
    cm_spin_lock(&entry->lock, NULL);

    if (!sequence->valid || entry->org_scn > DB_CURR_SCN(session)) {
        dc_seq_close(&dc_seq);
        cm_spin_unlock(&entry->lock);
        GS_THROW_ERROR(ERR_SEQ_NOT_EXIST, T2S(user), T2S_EX(name));
        return GS_ERROR;
    }

    def->step = sequence->step;
    def->max_value = sequence->maxval;
    def->min_value = sequence->minval;
    def->cache = sequence->cache_size;
    def->is_cycle = sequence->is_cyclable;
    def->start = sequence->lastval;

    dc_seq_close(&dc_seq);
    cm_spin_unlock(&entry->lock);

    return GS_SUCCESS;
}

status_t db_alter_seq_nextval(knl_session_t *session, knl_sequence_def_t *def, int64 value)
{
    dc_sequence_t *sequence = NULL;
    sequence_entry_t *entry = NULL;
    knl_dictionary_t dc_seq;
    text_t *user = &def->user;
    text_t *name = &def->name;

    if (GS_SUCCESS != dc_seq_open(session, user, name, &dc_seq)) {
        return GS_ERROR;
    }

    sequence = (dc_sequence_t *)dc_seq.handle;
    entry = sequence->entry;
    cm_spin_lock(&entry->lock, NULL);

    if (!sequence->valid || entry->org_scn > DB_CURR_SCN(session)) {
        dc_seq_close(&dc_seq);
        cm_spin_unlock(&entry->lock);
        GS_THROW_ERROR(ERR_SEQ_NOT_EXIST, T2S(user), T2S_EX(name));
        return GS_ERROR;
    }

    if (seq_update_seq_nextval(session, sequence, name, value) != GS_SUCCESS) {
        dc_seq_close(&dc_seq);
        cm_spin_unlock(&entry->lock);
        return GS_ERROR;
    }

    dc_seq_close(&dc_seq);
    cm_spin_unlock(&entry->lock);

    return GS_SUCCESS;
}

void print_alter_sequence(log_entry_t *log)
{
    rd_seq_t *rd = (rd_seq_t *)log->data;
    printf("alter sequence uid:%d,oid:%d\n", rd->uid, rd->id);
}

/*
 * implement of drop all sequences owned by user
 * @param[in]   session - user session
 * @param[in]   user - username
 * @return
 * - GS_SUCCESS
 * - GS_ERROR
 */
static status_t db_fetch_seq_by_uid(knl_session_t *session, uint32 uid, sequence_desc_t *desc, bool32 *found)
{
    knl_cursor_t *cursor = NULL;
    text_t seq_name;

    CM_SAVE_STACK(session->stack);

    cursor = knl_push_cursor(session);

    knl_open_sys_cursor(session, cursor, CURSOR_ACTION_SELECT, SYS_SEQ_ID, SYS_SEQ001_ID);

    knl_init_index_scan(cursor, GS_FALSE);
    knl_set_scan_key(INDEX_DESC(cursor->index), &cursor->scan_range.l_key, GS_TYPE_INTEGER, &uid, sizeof(uint32), 0);
    knl_set_scan_key(INDEX_DESC(cursor->index), &cursor->scan_range.r_key, GS_TYPE_INTEGER, &uid, sizeof(uint32), 0);
    knl_set_key_flag(&cursor->scan_range.l_key, SCAN_KEY_LEFT_INFINITE, 1);
    knl_set_key_flag(&cursor->scan_range.r_key, SCAN_KEY_RIGHT_INFINITE, 1);

    if (GS_SUCCESS != knl_fetch(session, cursor)) {
        CM_RESTORE_STACK(session->stack);
        *found = GS_FALSE;
        return GS_SUCCESS;
    }

    if (!cursor->eof) {
        seq_name.str = CURSOR_COLUMN_DATA(cursor, SYS_SEQUENCE_COL_NAME);
        seq_name.len = CURSOR_COLUMN_SIZE(cursor, SYS_SEQUENCE_COL_NAME);
        (void)cm_text2str(&seq_name, desc->name, GS_MAX_NAME_LEN + 1);
        *found = GS_TRUE;
    } else {
        *found = GS_FALSE;
    }

    CM_RESTORE_STACK(session->stack);
    return GS_SUCCESS;
}

/*
 * implement of drop all sequences owned by user
 * @param[in]   session - user session
 * @param[in]   user - username
 * @return
 * - GS_SUCCESS
 * - GS_ERROR
 */
status_t db_drop_sequence_by_user(knl_session_t *session, text_t *user, uint32 uid)
{
    bool32 found = GS_FALSE;
    sequence_desc_t desc;
    knl_dictionary_t dc;
    text_t seq_name;
    bool32 seq_exists = GS_FALSE;

    if (db_fetch_seq_by_uid(session, uid, &desc, &found) != GS_SUCCESS) {
        return GS_ERROR;
    }

    while (found) {
        cm_str2text(desc.name, &seq_name);
        if (GS_SUCCESS != dc_seq_open(session, user, &seq_name, &dc)) {
            return GS_ERROR;
        }

        if (db_drop_sequence(session, &dc, &seq_exists) != GS_SUCCESS) {
            dc_seq_close(&dc);
            return GS_ERROR;
        }
        dc_seq_close(&dc);

        knl_set_session_scn(session, GS_INVALID_ID64);

        if (db_fetch_seq_by_uid(session, uid, &desc, &found) != GS_SUCCESS) {
            return GS_ERROR;
        }
    }

    return GS_SUCCESS;
}
