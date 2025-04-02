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
 * knl_ctlg.h
 *    implement of catalog table
 *
 * IDENTIFICATION
 *    src/storage/gstor/zekernel/kernel/catalog/knl_ctlg.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef __KNL_CTLG_TBL_H__
#define __KNL_CTLG_TBL_H__

#include "knl_dc.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct st_rd_update_core_index {
    logic_op_t op_type;
    uint32 table_id;
    uint32 index_id;
    page_id_t entry;
}rd_update_core_index_t;

index_t *db_sys_index(uint32 id);
table_t *db_sys_table(uint32 id);
status_t db_alter_user_field(knl_session_t *session, knl_user_desc_t *desc, knl_cursor_t *cursor, uint32 update_flag);
status_t db_fill_builtin_indexes(knl_session_t *session);
status_t db_write_systable(knl_session_t *session, knl_cursor_t *cursor, knl_table_desc_t *desc);
status_t db_write_sysstorage(knl_session_t *session, knl_cursor_t *cursor, knl_scn_t org_scn, knl_storage_desc_t *desc);
status_t db_write_sysindex(knl_session_t *session, knl_cursor_t *cursor, knl_index_desc_t *desc);
status_t db_write_syscolumn(knl_session_t *session, knl_cursor_t *cursor, knl_column_t *column);
status_t db_write_syslob(knl_session_t *session, knl_cursor_t *cursor, knl_lob_desc_t *desc);
status_t db_write_sysview(knl_session_t *session, knl_cursor_t *cursor, knl_view_t *view, knl_view_def_t *def);
status_t db_write_sysview_column(knl_session_t *session, knl_cursor_t *cursor, knl_column_t *column);
status_t db_write_sysrb(knl_session_t *session, knl_rb_desc_t *desc);
status_t db_insert_sys_user(knl_session_t *session, knl_cursor_t *cursor, knl_user_desc_t *desc);
status_t db_insert_sys_tenants(knl_session_t *session, knl_cursor_t *cursor, knl_tenant_desc_t *desc);
status_t db_build_sys_column(knl_session_t *session, knl_cursor_t *cursor);
status_t db_build_sys_index(knl_session_t *session, knl_cursor_t *cursor);
status_t db_build_sys_user(knl_session_t *session, knl_cursor_t *cursor);
void db_garbage_segment_init(knl_session_t *session);
status_t db_garbage_segment_clean(knl_session_t *session);
status_t db_garbage_segment_handle(knl_session_t *session, uint32 uid, uint32 oid, bool32 is_purge_truncate);
status_t db_write_garbage_segment(knl_session_t *session, knl_seg_desc_t *seg);
status_t db_update_garbage_segment_entry(knl_session_t *session, knl_table_desc_t *desc, page_id_t entry);
void db_update_index_clean_option(knl_session_t *session, knl_alindex_def_t *def, knl_index_desc_t desc);
void db_purge_garbage_segment(knl_session_t *session);
void db_clean_garbage_partition(knl_session_t *session);
status_t db_load_core_entity_by_id(knl_session_t *session, memory_context_t *memory, table_t *table);
void db_update_core_index(knl_session_t *session, rd_update_core_index_t *redo_index_info);
void db_get_sys_dc(knl_session_t *session, uint32 id, knl_dictionary_t *dc);
void rd_db_update_core_index(knl_session_t *session, log_entry_t *log);
void print_db_update_core_index(log_entry_t *log);
status_t db_alter_tenant_field(knl_session_t *session, knl_tenant_desc_t *desc);
void db_delay_clean_segments(knl_session_t *session);
void db_clean_garbage_subpartition(knl_session_t *session);
status_t knl_internal_repair_catalog(knl_session_t *session);
#ifdef __cplusplus
}
#endif

#endif
