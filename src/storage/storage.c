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
 * storage.c
 *    storage
 *
 * IDENTIFICATION
 *    src/storage/storage.c
 *
 * -------------------------------------------------------------------------
 */
#include "storage.h"

#include "db_handle.h"
#include "storage/gstor/zekernel/kernel/table/knl_table.h"

#define G_STOR_MAX_CURSOR 5
typedef struct st_lob_buf
{
    char *buf;
    uint32 size;
} lob_buf_t;
typedef struct st_ec_handle
{
    lob_buf_t lob_buf;
    knl_cursor_t *cursor;
    knl_session_t *session;
    knl_dictionary_t dc;
    knl_cursor_t *cursors[G_STOR_MAX_CURSOR];
} ec_handle_t;

typedef void(*db_shutdown_t)(void* cc_instance);
typedef int(*db_startup_t)(void** cc_instance, char *path);
typedef void(*db_free_t)(void *handle);
typedef void(*db_clean_t)(void *handle);
typedef int(*db_alloc_t)(void* cc_instance, void **handle);
typedef int(*db_open_t)(void *handle, const char *table_name);
typedef int(*db_begin_t)(void *handle);
typedef int(*db_commit_t)(void *handle);
typedef int(*db_rollback_t)(void *handle);
typedef int(*db_del_t)(void *handle, char *key, uint32 key_len, bool32 prefix, uint32 *count);
typedef int(*db_put_t)(void *handle, char *key, uint32 key_len, char *val, uint32 val_len);
typedef int(*db_get_t)(void *handle, char *key, uint32 key_len, char **val, uint32 *val_len, bool32 *eof);
typedef int(*db_cursor_next_t)(void *handle, bool32 *eof, size_t cursor_idx);
typedef int(*db_open_cursor_t)(void *handle, const char* table_name,
    int cond_column_count, exp_column_def_t* cond_column_list, bool32 *eof, int idx_slot);
typedef int(*db_open_cursor_ex_t)(void *handle, const char* table_name,int col_count,
    int count, condition_def_t* cond_column_list, bool32 *eof, int idx_slot,scan_action_t action, size_t cursor_idx);

typedef int(*db_cursor_fetch_t)(void *handle, int sel_column_count, exp_column_def_t *sel_column_list,
    int *res_row_count, res_row_def_t *res_row_list, size_t cursor_idx);
typedef int(*db_insert_row_t)(void* handle, const char* table_name, int column_count, exp_column_def_t* column_list);
typedef int(*db_update_row_t)(void* handle, const char* table_name, int upd_column_count, 
    exp_column_def_t* upd_column_list, int cond_column_count, exp_column_def_t* cond_column_list);
typedef int(*db_delete_row_t)(void* handle, const char* table_name, int *del_count,
    int cond_column_count, exp_column_def_t* cond_column_list);
typedef int(*db_delete_t)(void* handle, size_t cursor_idx);
typedef int(*db_update_t)(void* handle, int column_count, exp_column_def_t* column_list, size_t cursor_idx);
typedef int(*db_truncate_t)(void* handle, char *owner, char *table_name);

typedef int(*db_alter_table_t)(void* handle, 
            const char* table_name, 
            const exp_altable_def_t* alter_table, 
            error_info_t* err_info);

typedef int(*db_create_index_t)(void* handle, const char* table_name, const exp_index_def_t* index_def, 
              error_info_t* err_info);

typedef int(*db_create_table_t)(void* handle, const char* table_name, 
    int column_count, exp_column_def_t* column_list,
    int index_count, exp_index_def_t* index_list,
    int cons_count, exp_constraint_def_t* cons_list, 
    exp_attr_def_t attr, error_info_t* err_info);

typedef int(*db_get_table_info_t)(void* handle,const char* table_name,
                                             exp_table_meta* table_info, error_info_t* err_info);

typedef int (*db_create_sequence_t)(void *handle, char *owner, sequence_def_t *sequence_info);
typedef int (*db_seq_currval_t)(void *handle,const char* name,int64_t* value);
typedef int (*db_seq_nextval_t)(void *handle,const char* name,int64_t* value);
typedef int (*db_alter_seq_nextval_t)(void *handle, const char* name, int64_t value);

typedef int (*db_batch_insert_t)(void *handle, const char* table_name, int row_count, res_row_def_t *row_list, uint32 part_no);

typedef int(*db_create_view_t)(void* handle, exp_view_def_t *view_def, int columnCount, exp_column_def_t* column_defs);

typedef int (*db_drop_t)(void *handle, char *owner, drop_def_t *drop_info);

typedef int (*db_modified_partno_t)(void *handle,size_t cursor_idx,int partno);

typedef int (*db_force_checkpoint)(void *handle);
typedef int (*db_comment_on_t)(void *handle, exp_comment_def_t *comment_def);

typedef struct st_db {
    db_put_t          put;
    db_del_t          del;
    db_get_t          get;
    db_free_t         free;
    db_alloc_t        alloc;
    db_open_t         open_table;
    db_open_t         open_mem_table;
    db_clean_t        clean;
    db_begin_t        begin;
    db_commit_t       commit;
    db_startup_t      startup;
    db_shutdown_t     shutdown;
    db_rollback_t     rollback;
    db_cursor_next_t  cursor_next;
    db_cursor_fetch_t cursor_fetch;
    //
    db_open_t         open_user_table;
    db_insert_row_t   insert_row;
    db_update_row_t   update_row;
    db_delete_row_t   delete_row;
    db_delete_t       delete;
    db_update_t       update;

    db_open_cursor_ex_t open_cursor_ex;
    db_truncate_t truncate;

    db_alter_table_t alter_table;
    db_create_index_t create_index;
    db_create_table_t create_table;
    db_get_table_info_t get_table_info;

    db_create_sequence_t create_sequence;

    db_batch_insert_t batch_insert;

    db_create_view_t create_view;

    db_seq_currval_t seq_currval;
    db_seq_nextval_t seq_nextval;
    db_alter_seq_nextval_t alter_seq_nextval;

    db_drop_t drop;

    db_modified_partno_t modified_partno;

    db_force_checkpoint force_checkpoint;
    db_comment_on_t comment_on;
    
}db_t;

static const db_t g_dbs[] = {
    {gstor_put, gstor_del, gstor_get, gstor_free, gstor_alloc, gstor_open_table, gstor_open_mem_table, gstor_clean, gstor_begin,
     gstor_commit, gstor_startup, gstor_shutdown, gstor_rollback , gstor_cursor_next,
     gstor_cursor_fetch, gstor_open_user_table, gstor_executor_insert_row, gstor_executor_update_row,
     gstor_executor_delete_row, gstor_executor_delete, gstor_executor_update, gstor_open_cursor_ex, gstor_truncate_table, sqlapi_gstor_alter_table,
     sqlapi_gstor_create_index, sqlapi_gstor_create_user_table, gstor_get_table_info, gstor_create_sequence, gstor_batch_insert_row, 
     gstor_create_user_view, gstor_seq_currval, gstor_seq_nextval, gstor_alter_seq_nextval, gstor_drop,
     gstor_modified_partno, gstor_force_checkpoint, gstor_comment_on}
};

static const db_t *g_curr_db = NULL;
#define STG_HANDLE  (g_curr_db)

static handle_pool_t g_handle_pool;
#define HANDLE_POOL (&g_handle_pool)

#define STG_CHECK_DB_STARTUP                                     \
    do {                                                         \
        if (STG_HANDLE == NULL) {                                \
            GS_LOG_RUN_ERR("[STG] Please call db_startup first"); \
            return GS_ERROR;                                     \
        }                                                        \
    } while (0)

static inline void init_g_handle_pool(void)
{
    HANDLE_POOL->hwm  = 0;
    HANDLE_POOL->lock = 0;
    biqueue_init(&HANDLE_POOL->idle_list);
}

static inline void destroy_db_handle(db_handle_t *db_handle)
{
    STG_HANDLE->free(db_handle->handle);
    CM_FREE_PTR(db_handle);
}

static inline void deinit_g_handle_pool(void)
{
    for (uint32 i = 0; i < HANDLE_POOL->hwm; ++i) {
        db_handle_t *db_handle = HANDLE_POOL->handles[i];
        if (db_handle == NULL) {
            continue;
        }
        destroy_db_handle(db_handle);
    }
}

status_t kv_startup(void** cc_instance, int dbtype, char *path) //delete
{
    return db_startup(cc_instance, dbtype, path);
}
status_t db_startup(void** cc_instance, int dbtype, char *path)
{
    char real_data_path[GS_FILE_NAME_BUFFER_SIZE] = {0};

    if (CM_IS_EMPTY_STR(path)) {
        GS_LOG_RUN_ERR("[STG] data path is empty");
        return GS_ERROR;
    }
    GS_RETURN_IFERR(realpath_file(path, real_data_path, GS_FILE_NAME_BUFFER_SIZE));

    if (g_dbs[dbtype].startup(cc_instance, real_data_path) != GS_SUCCESS) {
        GS_LOG_RUN_ERR("[STG] db %u startup failed", dbtype);
        return GS_ERROR;
    }

    STG_HANDLE = &g_dbs[dbtype];
    init_g_handle_pool();
    return GS_SUCCESS;
}

void kv_shutdown(void* cc_instance)  //delete
{
    db_shutdown(cc_instance);
}
void db_shutdown(void* cc_instance)
{
    deinit_g_handle_pool();
    if (STG_HANDLE == NULL) {
        return;
    }
    STG_HANDLE->shutdown(cc_instance);
}

static inline void return_free_handle(db_handle_t *db_handle)
{
    cm_spin_lock(&HANDLE_POOL->lock, NULL);
    biqueue_add_tail(&HANDLE_POOL->idle_list, QUEUE_NODE_OF(db_handle));
    cm_spin_unlock(&HANDLE_POOL->lock);
}

static inline bool32 reuse_handle(db_handle_t **handle)
{
    if (biqueue_empty(&HANDLE_POOL->idle_list)) {
        return GS_FALSE;
    }

    cm_spin_lock(&HANDLE_POOL->lock, NULL);
    biqueue_node_t *node = biqueue_del_head(&HANDLE_POOL->idle_list);
    if (node == NULL) {
        cm_spin_unlock(&HANDLE_POOL->lock);
        return GS_FALSE;
    }

    *handle = OBJECT_OF(db_handle_t, node);
    cm_spin_unlock(&HANDLE_POOL->lock);
    return GS_TRUE;
}

status_t kv_alloc(void* cc_instance, void **handle)
{
    return db_alloc(cc_instance, handle);
}
status_t db_alloc(void* cc_instance, void **handle)
{
    STG_CHECK_DB_STARTUP;

    if (reuse_handle((db_handle_t**)handle)) {
        return GS_SUCCESS;
    }

    db_handle_t *db_handle = (db_handle_t*)malloc(sizeof(db_handle_t));
    if (db_handle == NULL) {
        GS_LOG_DEBUG_ERR("[STG] alloc memory failed");
        return GS_ERROR;
    }
    if (STG_HANDLE->alloc(cc_instance, &db_handle->handle) != GS_SUCCESS) {
        GS_LOG_DEBUG_ERR("[STG] alloc handle from db failed");
        CM_FREE_PTR(db_handle);
        return GS_ERROR;
    }
    db_handle->next = db_handle->prev = NULL;

    cm_spin_lock(&HANDLE_POOL->lock, NULL);
    HANDLE_POOL->handles[HANDLE_POOL->hwm++] = db_handle;
    cm_spin_unlock(&HANDLE_POOL->lock);
    *handle = db_handle;
    return GS_SUCCESS;
}

void kv_free(void *handle)
{
    db_free(handle);
}
void db_free(void *handle)
{
    db_handle_t *db_handle = ((db_handle_t*)handle);

    if (STG_HANDLE != NULL) {
        STG_HANDLE->clean(db_handle->handle);
    }
    return_free_handle(db_handle);
}

status_t kv_open_table(void *handle, const char *table_name)
{
    STG_CHECK_DB_STARTUP;
    return STG_HANDLE->open_table(((db_handle_t*)handle)->handle, table_name);
}
status_t db_open_mem_table(void *handle, const char *table_name)
{
    STG_CHECK_DB_STARTUP;
    return STG_HANDLE->open_mem_table(((db_handle_t*)handle)->handle, table_name);
}

status_t db_open_table(void *handle, const char *table_name)
{
    STG_CHECK_DB_STARTUP;
    return STG_HANDLE->open_user_table(((db_handle_t*)handle)->handle, table_name);
}

status_t db_open_cursor_ex(void* handle,const char* table_name, int col_count,
    int condition_count, condition_def_t* conditions,bool32* eof,int idx_slot,scan_action_t action,size_t cursor_idx) {
    STG_CHECK_DB_STARTUP;
    return STG_HANDLE->open_cursor_ex(((db_handle_t*)handle)->handle, table_name,col_count,
        condition_count, conditions, eof, idx_slot,action,cursor_idx);
}

status_t db_cursor_next(void *handle, bool32 *eof, size_t cursor_idx)
{
    STG_CHECK_DB_STARTUP;
    return STG_HANDLE->cursor_next(((db_handle_t*)handle)->handle, eof, cursor_idx);
}

status_t db_cursor_fetch(void *handle, int sel_column_count, exp_column_def_t *sel_column_list,
    int *res_row_count, res_row_def_t *res_row_list, size_t cursor_idx)
{
    STG_CHECK_DB_STARTUP;
    return STG_HANDLE->cursor_fetch(((db_handle_t*)handle)->handle, sel_column_count, sel_column_list,
        res_row_count, res_row_list, cursor_idx);
}

status_t kv_put(void *handle, text_t *key, text_t *val)
{
    STG_CHECK_DB_STARTUP;
    status_t ret = STG_HANDLE->put(((db_handle_t*)handle)->handle, key->str, key->len, val->str, val->len);
    return ret;
}

status_t kv_del(void *handle, text_t *key, bool32 prefix, uint32 *count)
{
    STG_CHECK_DB_STARTUP;
    status_t ret = STG_HANDLE->del(((db_handle_t*)handle)->handle, key->str, key->len, prefix, count);
    return ret;
}

status_t kv_get(void *handle, text_t *key, text_t *val, bool32 *eof)
{
    STG_CHECK_DB_STARTUP;
    status_t ret = STG_HANDLE->get(((db_handle_t*)handle)->handle, key->str, key->len, &val->str, &val->len, eof);
    return ret;
}

status_t gstor_insert_row(void* handle, const char* table_name,
    int column_count, exp_column_def_t* column_list)
{
    STG_CHECK_DB_STARTUP;
    status_t ret = STG_HANDLE->insert_row(((db_handle_t*)handle)->handle, table_name, column_count, column_list);
    return ret;
}

status_t batch_insert(void* handle, const char* table_name, int row_count, res_row_def_t *row_list, uint32 part_no) {
    STG_CHECK_DB_STARTUP;
    status_t ret = STG_HANDLE->batch_insert(((db_handle_t*)handle)->handle, table_name, row_count, row_list, part_no);
    return ret;
}

status_t gstor_update_row(void* handle, const char* table_name, 
    int upd_column_count, exp_column_def_t* upd_column_list,
    int cond_column_count, exp_column_def_t* cond_column_list)
{
    STG_CHECK_DB_STARTUP;
    status_t ret = STG_HANDLE->update_row(((db_handle_t*)handle)->handle, table_name,
        upd_column_count, upd_column_list, cond_column_count, cond_column_list);
    return ret;
}

status_t gstor_update(void* handle, int upd_column_count, exp_column_def_t* upd_column_list, size_t cursor_idx)
{
    STG_CHECK_DB_STARTUP;
    status_t ret = STG_HANDLE->update(((db_handle_t*)handle)->handle, 
        upd_column_count, upd_column_list,cursor_idx);
    return ret;
}

status_t gstor_delete_row(void* handle, const char* table_name, int *del_count,
    int cond_column_count, exp_column_def_t* cond_column_list)
{
    STG_CHECK_DB_STARTUP;
    status_t ret = STG_HANDLE->delete_row(((db_handle_t*)handle)->handle, table_name, del_count,
        cond_column_count, cond_column_list);
    return ret;
}

status_t gstor_delete(void* handle,size_t cursor_idx)
{
    STG_CHECK_DB_STARTUP;
    status_t ret = STG_HANDLE->delete(((db_handle_t*)handle)->handle,cursor_idx);
    return ret;
}

status_t gstor_truncate(void* handle, char *owner, char *table_name)
{
    STG_CHECK_DB_STARTUP;
    status_t ret = STG_HANDLE->truncate(((db_handle_t*)handle)->handle, owner, table_name);
    return ret;
}

status_t gstor_alter_table(void* handle, 
            const char* table_name, 
            const exp_altable_def_t* alter_table, 
            error_info_t* err_info)
{
    STG_CHECK_DB_STARTUP;
    status_t ret = STG_HANDLE->alter_table(((db_handle_t*)handle)->handle, table_name, alter_table, err_info);
    return ret;
}

status_t gstor_create_index_sql(void* handle, const char* table_name, const exp_index_def_t* index_def, error_info_t* err_info)
{
    STG_CHECK_DB_STARTUP;
    status_t ret = STG_HANDLE->create_index(((db_handle_t*)handle)->handle, table_name, index_def, err_info);
    return ret;
}

status_t gstor_create_user_table_sql(void* handle, const char* table_name, 
    int column_count, exp_column_def_t* column_list,
    int index_count, exp_index_def_t* index_list,
    int cons_count, exp_constraint_def_t* cons_list, 
    exp_attr_def_t attr, error_info_t* err_info)
{
    STG_CHECK_DB_STARTUP;
    status_t ret = STG_HANDLE->create_table(((db_handle_t*)handle)->handle, table_name, 
                                            column_count, column_list, index_count, index_list,
                                            cons_count, cons_list, attr, err_info);
    return ret;
}

status_t gstor_get_table_info_sql(void* handle,const char* table_name,
                                             exp_table_meta* table_info, error_info_t* err_info)
{
    STG_CHECK_DB_STARTUP;
    status_t ret = STG_HANDLE->get_table_info(((db_handle_t*)handle)->handle, table_name, 
                                            table_info, err_info);
    return ret;
}

status_t kv_begin(void *handle)
{
    return db_begin(handle);
}
status_t db_begin(void *handle)
{
    STG_CHECK_DB_STARTUP;
    return STG_HANDLE->begin(((db_handle_t*)handle)->handle);
}

status_t kv_commit(void *handle)
{
    return db_commit(handle);
}
status_t db_commit(void *handle)
{
    STG_CHECK_DB_STARTUP;
    return STG_HANDLE->commit(((db_handle_t*)handle)->handle);
}

status_t kv_rollback(void *handle)
{
    return db_rollback(handle);
}
status_t db_rollback(void *handle)
{
    STG_CHECK_DB_STARTUP;
    return STG_HANDLE->rollback(((db_handle_t*)handle)->handle);
}

status_t create_sequence(void *handle, char *owner, sequence_def_t *sequence_info)
{
    STG_CHECK_DB_STARTUP;
    status_t ret = STG_HANDLE->create_sequence(((db_handle_t *)handle)->handle, owner, sequence_info);
    return ret;
}

EXPORT_API status_t create_view(void* handle, exp_view_def_t *view_def, int columnCount, exp_column_def_t* column_defs)
{
    STG_CHECK_DB_STARTUP;
    status_t ret = STG_HANDLE->create_view(((db_handle_t *)handle)->handle, view_def, columnCount, column_defs);
    return ret;
}
status_t seq_currval(void *handle,const char *name, int64_t* val)
{
    STG_CHECK_DB_STARTUP;
    status_t ret = STG_HANDLE->seq_currval(((db_handle_t *)handle)->handle, name, val);
    return ret;
}

status_t seq_nextval(void *handle,const char *name, int64_t *val)
{
    STG_CHECK_DB_STARTUP;
    status_t ret = STG_HANDLE->seq_nextval(((db_handle_t *)handle)->handle, name, val);
    return ret;
}

status_t alter_seq_nextval(void *handle,const char *name, int64_t val) {
    STG_CHECK_DB_STARTUP;
    status_t ret = STG_HANDLE->alter_seq_nextval(((db_handle_t *)handle)->handle, name, val);
    return ret;
}

status_t drop(void *handle, char *owner, drop_def_t *drop_info)
{
    STG_CHECK_DB_STARTUP;
    status_t ret = STG_HANDLE->drop(((db_handle_t *)handle)->handle, owner, drop_info);
    return ret;
}

status_t modified_partno(void *handle, size_t cursor_idx ,int part_no) {
    STG_CHECK_DB_STARTUP;
    status_t ret = STG_HANDLE->modified_partno(((db_handle_t *)handle)->handle, cursor_idx, part_no);
    return ret;
}

status_t force_checkpoint(void* handle){
    STG_CHECK_DB_STARTUP;
    status_t ret = STG_HANDLE->force_checkpoint(((db_handle_t *)handle)->handle);
    return ret;
}

status_t comment_on(void *handle, exp_comment_def_t *comment_def) {
    STG_CHECK_DB_STARTUP;
    status_t ret = STG_HANDLE->comment_on(((db_handle_t *)handle)->handle, comment_def);
    return ret;
}

