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
 * storage.h
 *    storage header file
 *
 * IDENTIFICATION
 *    src/storage/storage.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef __STORAGE_H__
#define __STORAGE_H__

#include "kv_executor/kv_executor.h"
#include "relational_executor/rela_executor.h"

#ifdef __cplusplus
extern "C" {
#endif

EXPORT_API void kv_shutdown(void* cc_instance);  //need delete
EXPORT_API void db_shutdown(void* cc_instance);

EXPORT_API status_t kv_startup(void** cc_instance, int dbtype, char *path); //need delete
EXPORT_API status_t db_startup(void** cc_instance, int dbtype, char *path);

EXPORT_API void kv_free(void *handle);  //need delete
EXPORT_API void db_free(void *handle);

EXPORT_API status_t kv_alloc(void* cc_instance, void **handle);    //need delete
EXPORT_API status_t db_alloc(void* cc_instance, void **handle);

EXPORT_API status_t db_open_table(void *handle, const char *table_name);
EXPORT_API status_t db_open_mem_table(void *handle, const char *table_name);

EXPORT_API status_t db_open_cursor_ex(void *handle, const char* table_name,int index_column_size,
    int count, condition_def_t* cond_column_list, bool32 *eof, int idx_slot,scan_action_t action,size_t cursor_idx);

EXPORT_API status_t db_cursor_next(void *handle, bool32 *eof, size_t cursor_idx);

EXPORT_API status_t db_cursor_fetch(void *handle, int sel_column_count, exp_column_def_t *sel_column_list,
    int *res_row_count, res_row_def_t *res_row_list, size_t cursor_idx);

EXPORT_API status_t gstor_insert_row(void* handle, const char* table_name,
    int column_count, exp_column_def_t* column_list);
EXPORT_API status_t batch_insert(void* handle, const char* table_name, int row_count,
    res_row_def_t *row_list, uint32 part_no);

EXPORT_API status_t gstor_update_row(void* handle, const char* table_name, 
    int upd_column_count, exp_column_def_t* upd_column_list,
    int cond_column_count, exp_column_def_t* cond_column_list);

EXPORT_API status_t gstor_update(void* handle,int column_count, exp_column_def_t* column_list, size_t cursor_idx);

EXPORT_API status_t gstor_delete_row(void* handle, const char* table_name, int *del_count,
    int cond_column_count, exp_column_def_t* cond_column_list);

EXPORT_API status_t gstor_delete(void* handle, size_t cursor_idx);

EXPORT_API status_t gstor_truncate(void* handle, char *owner, char *table_name);

EXPORT_API status_t gstor_create_user_table_sql(void* handle, const char* table_name, 
    int column_count, exp_column_def_t* column_list,
    int index_count, exp_index_def_t* index_list,
    int cons_count, exp_constraint_def_t* cons_list, 
    exp_attr_def_t attr, error_info_t* err_info);

EXPORT_API status_t gstor_create_index_sql(void* handle, const char* table_name, 
                                        const exp_index_def_t* index_def, error_info_t* err_info);

EXPORT_API status_t gstor_alter_table(void* handle, 
            const char* table_name, 
            const exp_altable_def_t* alter_table, 
            error_info_t* err_info);

EXPORT_API status_t gstor_get_table_info_sql(void* handle,const char* table_name,
                                             exp_table_meta* table_info, error_info_t* err_info);

EXPORT_API status_t db_begin(void *handle);

EXPORT_API status_t db_commit(void *handle);

EXPORT_API status_t db_rollback(void *handle);

EXPORT_API status_t create_sequence(void *handle, char *owner, sequence_def_t *sequence_info);
EXPORT_API status_t seq_currval(void *handle,const char *name, int64_t *val);
EXPORT_API status_t seq_nextval(void *handle,const char *name, int64_t *val);
EXPORT_API status_t alter_seq_nextval(void *handle,const char *name, int64_t val);

EXPORT_API status_t drop(void *handle, char *owner, drop_def_t *drop_info);

EXPORT_API status_t create_view(void* handle, exp_view_def_t *view_def, int columnCount, exp_column_def_t* column_defs);

EXPORT_API status_t modified_partno(void *handle, size_t cursor_idx, int part_no);

EXPORT_API status_t force_checkpoint(void* handle);

EXPORT_API status_t comment_on(void *handle, exp_comment_def_t *comment_def);

#ifdef __cplusplus
}
#endif

#endif
