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
 * knl_database.h
 * implement of database
 *
 * IDENTIFICATION
 * src/storage/gstor/zekernel/kernel/knl_database.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef __KNL_DATABASE_H__
#define __KNL_DATABASE_H__

#include "cm_defs.h"
#include "cm_latch.h"
#include "cm_utils.h"
#include "knl_space.h"
#include "knl_log.h"
#include "knl_datafile.h"
#include "knl_interface.h"
#include "knl_session.h"
#include "knl_heap.h"
#include "knl_dc.h"
#include "knl_archive.h"
#include "knl_backup.h"
#include "knl_core_table_defs.h"
#include "knl_db_ctrl.h"

#ifdef __cplusplus
extern "C" {
#endif

#define DB_SYS_USER_ID 0
#define DB_PUB_USER_ID 1
#define SYS_VIEW_TEXT_COLUMN 8

#define FIXED_TEMP_SPACE_ID 1
#define FIXED_UNDO_SPACE_ID 2
#define FIXED_USER_SPACE_ID 3

#define SYS_GARBAGE_SEGMENT_COLS 15

#define SYS_TABLE_SERIAL_START 22

typedef enum en_sys_table_id {
    SYS_TABLE_ID = 0,
    SYS_COLUMN_ID,
    SYS_INDEX_ID,
    SYS_USER_ID,
    SYS_SEQ_ID,
    SYS_LOB_ID,
    SYS_RB_ID,
    SYS_CONSDEF_ID,
    SYS_VIEW_ID,
    SYS_VIEWCOL_ID,
    DUAL_ID,
    SYS_PROC_ID,
    SYS_EXTERNAL_ID,
    SYS_PENDING_TRANS_ID,
    SYS_SYN_ID,
    SYS_COMMENT_ID,
    SYS_PRIVS_ID,
    OBJECT_PRIVS_ID,
    SYS_USER_ROLES_ID,
    SYS_ROLES_ID,
    SYS_HISTGRM_ID,
    SYS_HIST_HEAD_ID,
    SYS_PARTOBJECT_ID,
    SYS_PARTCOLUMN_ID,
    SYS_TABLEPART_ID,
    SYS_INDEXPART_ID,
    SYS_LOBPART_ID,
    SYS_SHADOW_INDEX_ID,
    SYS_PROFILE_ID,
    SYS_SHADOW_INDEXPART_ID,
    SYS_BACKUP_SET_ID,
    SYS_DATA_NODES_ID,
    SYS_PENDING_DISTRIBUTED_TRANS_ID,
    SYS_DISTRIBUTE_STRATEGY_ID,
    SYS_GARBAGE_SEGMENT_ID,
    SYS_PARTSTORE_ID,
    SYS_USER_HISTORY_ID,
    SYS_PROC_ARGS_ID,
    SYS_LOGIC_REP_ID,
    SYS_MON_MODS_ALL_ID,
    SYS_DEPENDENCY_ID,
    SYS_DISTRIBUTE_RULE_ID,
    SYS_LINK_ID,
    SYS_TMP_SEG_STAT_ID,
    SYS_ICOL_ID,
    SYS_JOB_ID,
    SYS_SQL_MAP_ID,
    SYS_SYNC_INFO_ID,
    SYS_DIST_DDL_LOGINFO,
    SYS_AUDIT_ID,
    SYS_REBALANCE_TASK_ID,
    SYS_RSRC_PLAN_ID,
    SYS_RSRC_GROUP_ID,
    SYS_RSRC_GROUP_MAPPING_ID,
    SYS_RSRC_PLAN_RULE_ID,
    SYS_DIRECTORY_ID,
    SYS_SUB_TABLE_PARTS_ID,
    SYS_SUB_PARTCOLUMN_ID,
    SYS_SUB_LOB_PARTS_ID,
    SYS_SUB_INDEX_PARTS_ID,
    SYS_SUB_PART_TEMPLATE_ID,
    SYS_STORAGE_ID,
    SYS_TYPE_ID = 1024,
    SYS_TYPE_ATTR_ID = 1025,
    SYS_TYPE_METHOD_ID = 1026,
    SYS_COLL_TYPE_ID = 1027,
    SYS_TYPE_VERSION_ID = 1028,
    SYS_LIBRARY_ID = 1029,
    SYS_DDM_ID = 1030,
    SYS_POLICY_ID = 1031,
    SYS_USER_PRIVS_ID = 1032,
    SYS_CONSIS_HASH_STRATEGY_ID = 1033,
    SYS_TENANTS_ID = 1034,
    SYS_INSTANCE_INFO_ID = 1035,
    SYS_COMPRESS_ID = 1036,
    SYS_TEMP_HISTGRAM_ID = 1037,
    SYS_TEMP_HIST_HEAD_ID = 1038,
    SYS_TRIGGER_ID = 1039,
    SYS_LOGIN_USER_ID = 1040,
    SYS_KV_ID = 1041,
    SYS_TABLE_COUNT
} sys_table_id_t;

/* role id define */
#define SYS_DBA_ROLE_ID 0
#define SYS_RESOURCE_ROLE_ID 1
#define SYS_CONNECT_ROLE_ID 2
// #define SYS_STATISTICS_ROLE_ID 3     // 2024-9-3
#define SYS_ROLE_ID_COUNT 3             // 2024-9-3  4->3 for "is system role, can not drop"

/* tenant$root id define */
#define SYS_TENANTROOT_ID 0

typedef void (*seg_exec_proc)(knl_session_t *session, knl_seg_desc_t *seg);

typedef struct st_seg_executor {
    seg_op_t type;
    seg_exec_proc proc;
} seg_executor_t;

#define CORE_SYS_TABLE_CEIL SYS_USER_ID
#define IS_CORE_SYS_TABLE(uid, id) ((uid) == (uint32)DB_SYS_USER_ID && (id) <= (uint32)CORE_SYS_TABLE_CEIL)
/* max system table,view and dynamic views */
#define MAX_SYS_OBJECTS (GS_SHARED_PAGE_SIZE / sizeof(pointer_t))

#define IX_SYS_TABLE1_ID 0
#define IX_SYS_TABLE2_ID 1
#define IX_SYS_COLUMN_ID 2
#define IX_SYS_INDEX1_ID 3
#define IX_SYS_INDEX2_ID 4
#define IX_SYS_USER1_ID 5
#define IX_SYS_USER2_ID 6

#define STAT_TABLES_PER_TIME 1000

#define DBA_ROLE_ID 0
#define RESOURCE_ROLE_ID 1
#define CONNECT_ROLE_ID 2

#define PRIVS_GRANTEE_TYPE_USER 0
#define PRIVS_GRANTEE_TYPE_ROLE 1
#define PRIVS_ADMIN_OPTION_FALSE 0
#define PRIVS_ADMIN_OPTION_TRUE 1

typedef struct st_database {
    spinlock_t lock;
    spinlock_t ctrl_lock;
    latch_t ddl_latch;
    db_status_t status;
    ctrlfile_set_t ctrlfiles; // ctrl/log/space/device info should been moved to ctrl space, should not been here
    logfile_set_t logfiles;
    space_t spaces[GS_MAX_SPACES];
    datafile_t datafiles[GS_MAX_DATA_FILES];
    database_ctrl_t ctrl;
    charset_t *charset;
    uint32 charset_id;
    volatile bool32 recover_for_restore; /* recover by command */
    volatile bool32 is_readonly;
    uint32 readonly_reason;
    volatile bool32 has_load_role;
    db_open_status_t open_status;
    uint64 terminate_lfn;
} database_t;

#define DB_NOT_READY(session) ((session)->kernel->db.status <= DB_STATUS_RECOVERY)
#define DB_TO_RECOVERY(session) ((session)->kernel->db.status >= DB_STATUS_RECOVERY)
#define DB_IS_OPEN(session) ((session)->kernel->db.status == DB_STATUS_OPEN)
#define DB_STATUS(session) ((session)->kernel->db.status)

typedef struct st_proc_name_node {
    uint16 name_len;
    char name[GS_NAME_BUFFER_SIZE];
    uint16 pack_len;
    char pack[GS_NAME_BUFFER_SIZE];
    uint16 trig_tab_len;
    char trig_tab[GS_NAME_BUFFER_SIZE];
    uint16 trig_tab_user_len;
    char trig_tab_user[GS_NAME_BUFFER_SIZE];
    uint32 uid;
    int64 oid;
    char pl_type;
} proc_name_node_t;

typedef struct st_trig_name_list {
    uint32 count;
    proc_name_node_t item[GS_MAX_TRIGGER_COUNT];
} trig_name_list_t;

#define DB_CURR_SCN(session) KNL_GET_SCN(&(session)->kernel->scn)
#define DB_DEFER_RECYLE_TIME(session) ((session)->kernel->attr.index_defer_recycle_time)

knl_scn_t db_inc_scn(knl_session_t *session);
knl_scn_t db_next_scn(knl_session_t *session);

#if defined(WIN32) || defined(__arm__) || defined(__aarch64__)
#define DB_GET_LSN(p_lsn) ((uint64)cm_atomic_get(p_lsn))
#define DB_SET_LSN(p_lsn, lsn) (cm_atomic_set((atomic_t *)&p_lsn, (int64)lsn))
#else
#define DB_GET_LSN(p_lsn) (uint64)(*(p_lsn))
#define DB_SET_LSN(p_lsn, lsn) ((p_lsn) = (lsn))
#endif

#define DB_CURR_LSN(session) DB_GET_LSN(&(session)->kernel->db.ctrl.core.lsn)
#define DB_INC_LSN(session) cm_atomic_inc(&(session)->kernel->db.ctrl.core.lsn)

#define DB_INIT_TIME(session) ((session)->kernel->db.ctrl.core.init_time)

#define DB_INC_LFN(lfn) (++(lfn))
#define DB_SET_LFN(p_lfn, lfn) (*(p_lfn) = (lfn))

#define DB_IS_READONLY(session) ((session)->kernel->db.is_readonly)
#define DB_IS_RESTRICT(session) ((session)->kernel->db.open_status == DB_OPEN_STATUS_RESTRICT)
#define DB_IS_UPGRADE(session) ((session)->kernel->db.open_status >= DB_OPEN_STATUS_UPGRADE)
#define DB_IS_MAINTENANCE(session) ((session)->kernel->db.open_status >= DB_OPEN_STATUS_RESTRICT)
#define DB_IS_PRIMARY(db) ((db)->ctrl.core.db_role == REPL_ROLE_PRIMARY)
#define DB_IS_PHYSICAL_STANDBY(db) ((db)->ctrl.core.db_role == REPL_ROLE_PHYSICAL_STANDBY)
#ifdef _REPLICATION
#define DB_IS_SINGLE(session)                                                           \
    (!DB_IS_RAFT_ENABLED((session)->kernel) && DB_IS_PRIMARY(&(session)->kernel->db) && \
        (session)->kernel->lsnd_ctx.standby_num == 0)
#endif
#define DB_IS_RCY_CHECK_PCN(session) ((session)->kernel->attr.rcy_check_pcn)
#define DB_IS_CASCADED_PHYSICAL_STANDBY(db) ((db)->ctrl.core.db_role == REPL_ROLE_CASCADED_PHYSICAL_STANDBY)
#define DB_IN_BG_ROLLBACK(session) ((session)->kernel->tran_ctx.rollback_num > 0)
#define DB_IS_BG_ROLLBACK_SE(session) \
    ((session)->id >= SESSION_ID_ROLLBACK && (session)->id < SESSION_ID_ROLLBACK + GS_MAX_ROLLBACK_PROC)
#ifdef _REPLICATION
#define SESSION_IS_LOG_ANALYZE(session) ((session)->id > 0 && (session)->id == (session)->kernel->gbp_aly_ctx.sid)
#else
#define SESSION_IS_LOG_ANALYZE(session) 0
#endif
#define SESSION_IS_KERNEL(session) ((session)->id == SESSION_ID_KERNEL)
#define SESSION_IS_GBP_BG(session) ((session)->gbp_queue_index > 0)
#define LRPL_OR_GBPALY(session) (SESSION_IS_LOG_ANALYZE(session) ? "GBP" : "LRPL")

#define MODE_MAX_PERFORMANCE(db) ((db)->ctrl.core.protect_mode == MAXIMUM_PERFORMANCE)
#define MODE_MAX_AVAILABILITY(db) ((db)->ctrl.core.protect_mode == MAXIMUM_AVAILABILITY)
#define MODE_MAX_PROTECTION(db) ((db)->ctrl.core.protect_mode == MAXIMUM_PROTECTION)

#define DB_IS_CHECKSUM_OFF(session) ((session)->kernel->attr.db_block_checksum == CKS_OFF)
#define DB_IS_CHECKSUM_TYPICAL(session) ((session)->kernel->attr.db_block_checksum == CKS_TYPICAL)
#define DB_IS_CHECKSUM_FULL(session) ((session)->kernel->attr.db_block_checksum == CKS_FULL)
#define NULL_2_STR(ptr) (((ptr) != NULL && (*(char *)(ptr)) != '\0') ? (ptr) : "(null)")

status_t db_init(knl_session_t *session);
status_t db_mount_ctrl(knl_session_t *session);
status_t db_mount(knl_session_t *session);
status_t db_recover(knl_session_t *session, knl_scn_t max_recover_scn);
status_t db_open(knl_session_t *session, db_open_opt_t *options);
void db_close(knl_session_t *session, bool32 need_ckpt);
void db_close_log_files(knl_session_t *session);
status_t db_callback_function(knl_session_t *session);

void db_close_log_files(knl_session_t *session);
static inline void db_store_core(database_t *db)
{
    core_ctrl_t *core = (core_ctrl_t *)db->ctrl.pages[CORE_CTRL_PAGE_ID].buf;
    *core = db->ctrl.core;
}

static inline void db_load_core(database_t *db)
{
    db->ctrl.core = *(core_ctrl_t *)&db->ctrl.pages[CORE_CTRL_PAGE_ID].buf[0];
}


status_t db_build_baseline(knl_session_t *session, knl_build_def_t *def);
static inline bool32 db_in_switch(switch_ctrl_t *ctrl)
{
    return (bool32)(ctrl->request != SWITCH_REQ_NONE);
}

status_t db_build_ex_systables(knl_session_t *session);

status_t db_analyze_schema(knl_session_t *session, knl_analyze_schema_def_t *def);
status_t db_delete_schema_stats(knl_session_t *session, text_t *schema_name);

char *db_get_switchover_status(knl_session_t *session);
char *db_get_failover_status(knl_session_t *session);
char *db_get_status(knl_session_t *session);
char *db_get_condition(knl_session_t *session);
char *db_get_needrepair_reason(knl_session_t *session);
char *db_get_readonly_reason(knl_session_t *session);
void db_reset_log(knl_session_t *session, uint32 switch_asn, bool32 reset_recover, bool32 reset_archive);

// 2024-10-25  吴锦锋   增加：原函数不对外提供(static)，现改成对外提供声明，在knl_recovery.c的rcy_recover_check函数中使用
status_t db_reset_ignore_log(knl_session_t *session, bool32 resetlogs, bool32 force_ignore_logs);

uint64 db_get_datafiles_size(knl_session_t *session);
uint64 db_get_logfiles_size(knl_session_t *session);
uint64 db_get_datafiles_used_size(knl_session_t *session);

status_t db_clean_nologging_all(knl_session_t *session);
void db_update_name_by_path(const char *path, char *name, uint32 len);
status_t db_update_ctrl_filename(knl_session_t *session);
status_t db_update_config_ctrl_name(knl_session_t *session);
status_t db_update_storage_filename(knl_session_t *session);
status_t db_change_storage_path(file_convert_t *convert, char *name, uint32 name_size);
void db_set_with_switchctrl_lock(switch_ctrl_t *ctrl, volatile bool32 *working);
void db_update_seg_scn(knl_session_t *session, knl_dictionary_t *dc);
void db_segments_stats_record(knl_session_t *session, seg_stat_t temp_stat, seg_stat_t *seg_stat);
void db_segment_stats_init(knl_session_t *session, seg_stat_t *temp_stat);
void db_convert_temp_path(knl_session_t *session, const char *path);
status_t db_purge(knl_session_t *session, knl_purge_def_t *def);
status_t dump_ctrl_page(database_ctrl_t *page, cm_dump_t *dump);
status_t dump_rebuild_ctrl_statement(database_ctrl_t *ctrl, cm_dump_t *dump);
status_t db_load_ctrlspace(knl_session_t *session, text_t *files);
void db_save_corrupt_info(knl_session_t *session, page_id_t page_id, knl_corrupt_info_t *info);
bool32 db_check_backgroud_blocked(knl_session_t *session, bool32 demote, bool32 sync);
void db_set_ctrl_restored(knl_session_t *session, bool32 is_restored);

status_t file_name_preproc(knl_session_t *session, char *suffix, char *name);
#define DB_CORE_CTRL(session) (&(session)->kernel->db.ctrl.core)

#ifdef __cplusplus
}
#endif

#endif
