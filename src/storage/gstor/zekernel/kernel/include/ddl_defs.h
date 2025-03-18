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
 * ddl_defs.h
 *    Data Definition Language defines
 *
 * IDENTIFICATION
 *    src/storage/gstor/zekernel/kernel/include/ddl_defs.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef __KNL_TABLE_DEFS_H__
#define __KNL_TABLE_DEFS_H__

#include "knl_defs.h"

#ifdef __cplusplus
extern "C" {
#endif

#define IS_ALTABLE_NOLOGGING_ACTION(action)                                                              \
        ((action) == ALTABLE_ENABLE_NOLOGGING || (action) == ALTABLE_DISABLE_NOLOGGING ||                 \
        (action) == ALTABLE_ENABLE_PART_NOLOGGING || (action) == ALTABLE_DISABLE_PART_NOLOGGING ||       \
        (action) == ALTABLE_ENABLE_SUBPART_NOLOGGING || (action) == ALTABLE_DISABLE_SUBPART_NOLOGGING)

/* shrink table option */
#define SHRINK_SPACE 0x00000000
#define SHRINK_COMPACT 0x00000001
#define SHRINK_CASCADE 0x00000002

typedef struct st_knl_storage_def {
    int64 initial;
    int64 next;
    int64 maxsize;
} knl_storage_def_t;

typedef struct st_knl_alt_table_prop {
    text_t new_name;
    uint32 pctfree;
    uint32 initrans;
    uint32 appendonly;
    bool32 enable_row_move;
    uint32 shrink_opt;
    uint32 shrink_percent;
    uint32 shrink_timeout;
    int64 serial_start;
    knl_storage_def_t storage_def;
} knl_alt_table_prop_t;

typedef struct st_knl_constraint_state {
    /*
     * if any of the "[NOT] DEFERRABLE", "ENABLE | DISABLE", "INITIALLY {IMMEDIATE | DEFERRED}", "RELY | NORELY",
     * "VALIDATE | NOVALIDATE" or "USING INDEX" specified, it means constraint_state clause existed in the constraint
     * statement.
     * use the state_setting_flag field to check if any constraint_state clause
     *
     * @NOTE:
     *    1. the properties specified in the constraint_state clause are implemented just for the syntax compatibility
     *    and would not take effect (except for the "USING INDEX"),
     *    because currently the storage engine does not support those properties.
     *    2. the "USING INDEX" properties are stored in the "knl_index_def_t" structure defined in
     *    "knl_constraint_def_t".
     */
    union {
        struct {
            uint32 is_use_index : 1;
            uint32 is_enable : 1;
            uint32 is_validate : 1;
            uint32 is_anonymous : 1;  // caution: forbid to change position, DB_TAB_COLS use it.
            uint32 deferrable_ops : 2;
            uint32 initially_ops : 2;
            uint32 rely_ops : 2;
            uint32 is_encode : 1; // deprecated field
            uint32 unused_ops : 21;
        };
        uint32 option;
    };
} knl_constraint_state_t;

typedef struct st_knl_part_def {
    text_t name;
    text_t space;
    uint32 initrans;
    uint32 pctfree;
    bool32 is_parent;
    text_t hiboundval;
    part_key_t *partkey;
    galist_t value_list;
    knl_storage_def_t storage_def;
    galist_t group_subkeys;
    galist_t subparts;
    uint8 is_csf; // mark if the part is csf format row
    uint8 support_csf;
    uint8 exist_subparts; // mark if the part has subpartitions
    compress_type_t compress_type;
    compress_algo_t compress_algo;
} knl_part_def_t;

typedef struct st_knl_part_column_def {
    uint32 column_id;
    gs_type_t datatype;
    bool32 is_char;
    uint16 size;
    uint8 precision;
    int8 scale;
} knl_part_column_def_t;

typedef struct st_knl_store_in_set {
    galist_t space_list;  
    uint32 part_cnt;      
    uint32 space_cnt;     
    bool32 is_store_in;   
} knl_store_in_set_t;

typedef struct st_knl_part_obj_def {
    part_type_t part_type;
    galist_t part_keys;
    galist_t parts;
    part_type_t subpart_type;  // for composite partition
    galist_t subpart_keys;     // for composite partition
    bool32 is_composite;       // for composite partition
    text_t interval;
    binary_t binterval;
    galist_t group_keys;
    bool32 has_default;
    bool32 sub_has_default;
    bool32 is_interval;
    knl_store_in_set_t part_store_in;
    knl_store_in_set_t subpart_store_in;
    uint32 interval_spc_num;
    bool32 delay_partition;        // for as select
    sql_text_t save_key;           // for as select
    sql_text_t save_subkey;           // for as select
    sql_text_t save_interval_part; // for as select
    sql_text_t save_part;          // for as select
    bool32 is_slice;
    bool32 auto_addpart;        // if automatic add new part or not, yes for interval
    bool32 is_crosspart;        // if data storaged of cross part or not
} knl_part_obj_def_t;

typedef struct st_knl_index_def {
    text_t user;
    text_t table;
    text_t name;
    text_t space;
    index_type_t type;
    bool32 primary;
    bool32 unique;
    galist_t columns;  // knl_index_col_def_t
    uint32 initrans;
    uint32 pctfree;
    uint8 cr_mode;
    uint32 parted;
    knl_part_obj_def_t *part_def;
    bool32 online;
    bool32 use_existed;  // true only if primary key or unique constraint specified to using an existed index.
    uint32 options;
    bool32 is_func;
    uint32 parallelism;
} knl_index_def_t;

#define GS_MAX_INDEX_COUNT_PERSQL    8
typedef struct st_knl_indexes_def {
    knl_index_def_t indexes_def[GS_MAX_INDEX_COUNT_PERSQL];
    uint32 index_count;
} knl_indexes_def_t;

typedef enum en_knl_ref_refactor {
    REF_DEL_NOT_ALLOWED = 0,
    REF_DEL_CASCADE = 1,
    REF_DEL_SET_NULL = 2,
} knl_refactor_t;

typedef struct st_knl_reference_def {
    text_t ref_user;
    text_t ref_table;
    knl_refactor_t refactor;
    knl_dictionary_t ref_dc;
    galist_t ref_columns;
} knl_reference_def_t;

typedef struct st_knl_check_def {
    text_t text;
    void *cond;  // condition tree for check
} knl_check_def_t;

#define IS_CONS_STATE_FLAG_SPECIFIED(constraint_def) (bool32)((constraint_def)->cons_state.option != 0)
#define IS_USEINDEX_FLAG_SPECIFIED(constraint_def)   (bool32)((constraint_def)->cons_state.is_use_index)

/* truncate table option */
#define TRUNC_RECYCLE_STORAGE 0x00000000
#define TRUNC_PURGE_STORAGE 0x00000001
#define TRUNC_REUSE_STORAGE 0x00000002
#define TRUNC_DROP_STORAGE 0x00000004

typedef struct st_knl_constraint_def {
    constraint_type_t type;
    text_t name;
    galist_t columns;
    union {
        knl_index_def_t index;  // primary / unique
        knl_reference_def_t ref;
        knl_check_def_t check;
    };
    knl_constraint_state_t cons_state;
} knl_constraint_def_t;

typedef struct st_knl_alt_cstr_prop {
    text_t name;
    uint32 opts;
    knl_constraint_def_t new_cons;  // for add constraint
} knl_alt_cstr_prop_t;

typedef struct st_knl_alt_part_prop {
    uint32 initrans;
    uint32 pctfree;
} knl_alt_part_prop_t;

typedef struct st_knl_alt_part_interval_t {
    text_t interval;
    binary_t binterval;
} knl_alt_part_interval_t;

typedef struct st_knl_alt_part {
    text_t name;
    knl_part_obj_def_t *obj_def;
    union {
        uint32 option;                          // for truncate partition
        text_t new_name;                        // for rename partition
        knl_alt_part_prop_t part_prop;          // for modify partition
        knl_alt_part_interval_t part_interval;  // for set interval
        bool32 global_index_option;             // for update global index
#ifdef Z_SHARDING
        bool32 is_sys_interval_part;           // for drop interval partition
#endif
        knl_storage_def_t storage_def;
    };
    bool32 is_garbage_clean; // for smon thread to clean the garbage partition
} knl_alt_part_t;

typedef enum en_lrep_key_type_def {
    LOGICREP_KEY_TYPE_PRIMARY_KEY,
    LOGICREP_KEY_TYPE_UNIQUE,
} lrep_key_type_def;

typedef struct st_knl_add_logical_log {
    lrep_key_type_def key_type;
    text_t idx_name;
    galist_t parts;
    bool32 is_parts_logical; // for partitions logical log
} knl_add_logical_log_t;

typedef enum en_modify_lob_action {
    MODIFY_LOB_SHRINK = 1,
    MODIFY_LOB_STORAGE = 2,
    MODIFY_LOB_PCTVERSION = 3,
}modify_lob_action_t;

typedef struct st_knl_modify_lob_def {
    text_t              name;
    modify_lob_action_t action;
}knl_modify_lob_def_t;

// definition of column in table
typedef struct st_knl_column_def {
    text_t name;
    union {
        /*
         * These definitions is same as the `typmode_t`, thus they should
         * be replaced by typmode_t for unifying the definition of columns
         */
        struct {
            gs_type_t datatype;
            uint16 size;
            uint8 precision;
            int8 scale;
        };
        typmode_t typmod; /* datatype, size, precision, scale, etc. */
    };
    union {
        struct {
            bool32 nullable : 1;
            bool32 primary : 1;  // if it is a primary key
            bool32 unique : 1;
            bool32 is_serial : 1;
            bool32 is_check : 1;
            bool32 is_ref : 1;
            bool32 is_default : 1;
            bool32 is_update_default : 1;
            bool32 is_comment : 1;
            bool32 is_collate : 1;
            bool32 has_null : 1;
            bool32 has_quote : 1;  // if column name wrapped with double quotation
            bool32 is_dummy : 1;   // if it is a dummy column for index
            bool32 is_default_null : 1; // empty string treat as null or ''
            bool32 unused_ops : 18;
        };
        bool32 is_option_set;
    };
    uint32 col_id;   // the position of column in alter table add column
    text_t inl_pri_cons_name;
    text_t inl_chk_cons_name;
    text_t inl_uq_cons_name;
    text_t inl_ref_cons_name;
    text_t ref_user;
    text_t ref_table;
    galist_t ref_columns;
    knl_refactor_t refactor;
    text_t default_text;
    bool32 delay_verify;  // after parse as select verify default expr
    bool32 delay_verify_auto_increment;  // after parse as select verify auto increment
    void *insert_expr;    // default expr
    void *update_expr;    // on update expr
    text_t check_text;
    void *check_cond;  // condition tree for check
    text_t comment;
    void *table;
} knl_column_def_t;

typedef struct st_knl_alt_column_prop {
    text_t name;
    text_t new_name;
    knl_column_def_t new_column;    // for add column or modify column
    galist_t constraints;
} knl_alt_column_prop_t;

typedef enum en_drop_option {
    DROP_DIRECTLY = 0x00000000,
    DROP_IF_EXISTS = 0x00000001,
    DROP_KEEP_FILES = 0x00000002,    // for tablespace
    DROP_CASCADE_CONS = 0x00000004,  // for tablespace
    DROP_TYPE_FORCE   = 0x00000008,  // for type
} drop_option_t;

typedef enum en_create_option {
    CREATE_IF_NOT_EXISTS = 0x00000001,
    CREATE_OR_REPLACE = 0x00000002,
    CREATE_TYPE_FORCE = 0x00000004,  // for create type spec force
} create_option_t;

/* alter table definition */
typedef struct st_knl_altable_def {
    altable_action_t action;
    uint32 options;
    text_t user;
    text_t name;
    union {
        knl_alt_table_prop_t table_def;
        /*
         * for ALTER TABLE's "ADD"(COLUMN), "MODIFY"(COLUMN), "RENAME"(COLUMN) & "DROP"(COLUMN)
         * the type of the galist's element is (knl_alt_column_prop_t *)
         * @NOTE: "column_defs" should be initialized with cm_galist_init() right after
         * the "action" field was set to ALTABLE_ADD_COLUMN/ALTABLE_MODIFY_COLUMN/
         * ALTABLE_RENAME_COLUMN/ALTABLE_DROP_COLUMN
         */
        galist_t column_defs;
        knl_alt_cstr_prop_t cons_def;
        knl_alt_part_t part_def;
        knl_add_logical_log_t logical_log_def;
        knl_modify_lob_def_t modify_lob_def;
    };
} knl_altable_def_t;

typedef struct st_loginfo_base_rec {
    uint32 group_id;
    uint32 datanode_id;
    text_t ddl_info;
    timestamp_t create_time;
    timestamp_t expired_time;
    uint32 retry_times;
    uint32 status;
}loginfo_base_rec_t;

typedef struct st_knl_dist_ddl_loginfo {
    text_t dist_ddl_id;
    loginfo_base_rec_t rec;
} knl_dist_ddl_loginfo_t;

typedef enum en_knl_ext_type {
    LOADER = 0,
    DATAPUMP = 1,
} knl_ext_type_t;

typedef struct st_knl_ext_def {
    knl_ext_type_t external_type;
    text_t directory;
    text_t location;
    char records_delimiter;
    char fields_terminator;
} knl_ext_def_t;

typedef struct st_knl_table_def {
    text_t schema;
    text_t name;
    text_t space;
    galist_t columns;
    galist_t constraints;
    galist_t lob_stores;
    uint32 initrans;
    uint32 maxtrans;
    uint32 pctfree;
    uint32 parted;
    uint32 sysid;
    uint8 cr_mode;
    uint8 csf;
    bool8 is_memory;   //memory table
    knl_storage_def_t storage_def;
    table_type_t type;
    knl_ext_def_t external_def;
    bool32 appendonly;
    knl_part_obj_def_t *part_def;
    struct {
        bool32 pk_inline : 1;   // inline primary key
        bool32 uq_inline : 1;   // inline unique constraint
        bool32 rf_inline : 1;   // inline reference constraint
        bool32 chk_inline : 1;  // inline check constraint
        bool32 create_as_select : 1; // create table as select
        bool32 unused : 27;
    };

    uint32 options;      // if not exists
    int64 serial_start;  // init auto incremnet value
    uint8 collate;
    uint8 charset;
    compress_type_t compress_type;
    compress_algo_t compress_algo;

#ifdef Z_SHARDING
    bool32 is_distribute_rule_def;  // use for create distribute rule
    uint32 distribute_type;
    galist_t distribute_groups;
    galist_t distribute_exprs;
    galist_t distribute_values;  // item of galist_t is values of per group (child-list).
    text_t distribute_text;      // distribute info
    binary_t distribute_data;
    binary_t distribute_buckets;
    uint32 slice_count;
    bool8 is_create;
    text_t ref_user;
    text_t ref_tab;
#endif
    bool32 is_timescale;
    bool32 has_retention;
    char* retention;
    bool32 has_comment;
    char* comment;
} knl_table_def_t;

status_t knl_create_table(knl_handle_t session, knl_table_def_t *def);
status_t knl_create_table_as_select(knl_handle_t session, knl_handle_t stmt, knl_table_def_t *def);

status_t knl_ddl_enabled(knl_handle_t session, bool32 forbid_in_rollback);
status_t knl_insert_ddl_loginfo(knl_handle_t knl_session, knl_dist_ddl_loginfo_t *info);
status_t knl_query_ddl_loginfo(knl_handle_t knl_session, text_t *ddl_id, text_t *ddl_info, uint32 *used_encrypt);
status_t knl_delete_ddl_loginfo(knl_handle_t knl_session, text_t *ddl_id);
status_t knl_clean_ddl_loginfo(knl_handle_t knl_session, text_t *ddl_id, uint32 *rows);
void knl_clean_before_commit(knl_handle_t knl_session);
void knl_set_ddl_id(knl_handle_t knl_session, text_t *id);
bool32 knl_is_dist_ddl(knl_handle_t knl_session);
status_t knl_internal_drop_table(knl_handle_t session, knl_drop_def_t *def);

status_t knl_alter_table(knl_handle_t session, knl_handle_t stmt, knl_altable_def_t *def);
status_t knl_perform_alter_table(knl_handle_t session, knl_handle_t stmt, knl_altable_def_t *def);
status_t knl_alter_table_shrink(knl_handle_t session, knl_altable_def_t *def);
status_t knl_drop_table(knl_handle_t session, knl_drop_def_t *def);
status_t knl_drop_view(knl_handle_t session, knl_drop_def_t *def);

typedef struct st_knl_trunc_def {
    text_t owner;  // owner's name
    text_t name;   // object name
    uint32 option;
} knl_trunc_def_t;

typedef enum st_flashback_type {
    FLASHBACK_INVALID_TYPE = 0,
    FLASHBACK_TO_SCN = 1,
    FLASHBACK_TO_TIMESTAMP = 2,
    FLASHBACK_DROP_TABLE = 3,
    FLASHBACK_TRUNCATE_TABLE = 4,
    FLASHBACK_TABLE_PART = 5,
    FLASHBACK_TABLE_SUBPART = 6,
} flashback_type_t;

typedef struct st_knl_flashback_def {
    text_t owner;     // owner's name
    text_t name;      // table name
    text_t ext_name;  // table new name or partition name
    bool32 force;
    knl_scn_t scn;
    void *expr;
    flashback_type_t type;
} knl_flashback_def_t;

status_t knl_truncate_table(knl_handle_t session, knl_trunc_def_t *def);
status_t knl_flashback_table(knl_handle_t session, knl_flashback_def_t *def);


/* Kernel set txn wait time definition */
typedef struct st_knl_lockwait_def {
    uint32 lock_wait_timeout;
} knl_lockwait_def_t;

typedef enum en_lock_table_mode {
    LOCK_MODE_SHARE = 0, /* SHARE */
    LOCK_MODE_EXCLUSIVE  /* EXCLUSIVE */
} lock_table_mode_t;

typedef enum en_wait_mode {
    WAIT_MODE_NO_WAIT = 0, /* NO WAIT */
    WAIT_MODE_WAIT,        /* WAIT */
} wait_mode_t;

typedef struct st_lock_table {
    text_t name;
    text_t schema;
} lock_table_t;

typedef struct st_lock_tables_def {
    galist_t tables;  // table list entry lock_table_t
    lock_table_mode_t lock_mode;
    wait_mode_t wait_mode;
    uint32 wait_time;  // unit second, only wait_mode = WAIT_MODE_WAIT,
    // the value is meaning
    // (wait_mode = WAIT_MODE_WAIT, value = 0) equal (wait_mode = WAIT_MODE_NO_WAIT)
} lock_tables_def_t;

typedef enum en_purge_type {
    PURGE_TABLE = 0,
    PURGE_INDEX = 1,
    PURGE_PART = 2,
    PURGE_TABLE_OBJECT = 3,
    PURGE_INDEX_OBJECT = 4,
    PURGE_PART_OBJECT = 5,
    PURGE_TABLESPACE = 6,
    PURGE_RECYCLEBIN = 7,
} purge_type_t;

typedef struct st_knl_purge_def {
    text_t owner;
    text_t name;
    text_t part_name;
    text_t ext_name;
    purge_type_t type;
} knl_purge_def_t;

void knl_set_lockwait_timeout(knl_handle_t session, knl_lockwait_def_t *def);
status_t knl_update_serial_value(knl_handle_t session, knl_handle_t dc_entity, int64 value);
status_t knl_lock_tables(knl_handle_t session, lock_tables_def_t *def);
status_t knl_purge(knl_handle_t session, knl_purge_def_t *def);
#ifdef __cplusplus
}
#endif

#endif
