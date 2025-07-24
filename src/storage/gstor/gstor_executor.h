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
 * gstor_executor.h
 *
 *
 * IDENTIFICATION
 * src/storage/gstor/gstor_executor.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef __KNL_EXECUTOR_H__
#define __KNL_EXECUTOR_H__

#include "storage/gstor/zekernel/common/cm_types.h"
#include "storage/gstor/zekernel/common/cm_defs.h"
#include "storage/gstor/zekernel/common/cm_error.h"
#include "storage/gstor/zekernel/common/cm_list.h"
#include "storage/gstor/zekernel/common/cm_text.h"
#include "storage/gstor/zekernel/common/cm_interval.h"
#include "storage/gstor/zekernel/common/cm_binary.h"
#include "storage/gstor/zekernel/common/cm_thread.h"
#include "storage/gstor/zekernel/kernel/include/dcl_defs.h"
#include "storage/gstor/zekernel/kernel/include/ddl_defs.h"
#include "storage/gstor/zekernel/kernel/include/dml_defs.h"
// #include "storage/gstor/zekernel/kernel/table/knl_heap.h"

#ifdef __cplusplus
extern "C" {
#endif

#define G_STOR_MAX_CURSOR 5

typedef struct st_lob_buf {
    char *buf;
    uint32 size;
} lob_buf_t;

typedef struct st_ec_handle {
    lob_buf_t lob_buf;
    knl_cursor_t *cursor;
    struct st_knl_session *session;
    knl_dictionary_t dc;
    knl_cursor_t* cursors[G_STOR_MAX_CURSOR];
} ec_handle_t;

#define EC_LOBBUF(handle) (&((ec_handle_t *)(handle))->lob_buf)
#define EC_CURSOR(handle) ((ec_handle_t *)(handle))->cursor
#define EC_CURSOR_IDX(handle,idx) ((ec_handle_t*)(handle))->cursors[idx]
#define EC_SESSION(handle) ((ec_handle_t *)(handle))->session
#define EC_DC(handle) (&(((ec_handle_t *)(handle))->dc))

struct st_instance;

#define GS_TIME_SUFFIX_HOUR ('h')
#define GS_TIME_SUFFIX_DAY ('d')
#define GS_TIME_SUFFIX_WEEK ('w')

#define GS_WAIT_REFLASH_DC 100
#define GS_WAIT_ALLOC_HANDLE 1000

typedef enum en_assign_type {
    ASSIGN_TYPE_EQUAL = 0,                          // 等于
    ASSIGN_TYPE_LESS = ASSIGN_TYPE_EQUAL + 1,       // 小于
    ASSIGN_TYPE_MORE = ASSIGN_TYPE_EQUAL + 2,       // 大于
    ASSIGN_TYPE_LESS_EQUAL = ASSIGN_TYPE_EQUAL + 3, // 小于等于
    ASSIGN_TYPE_MORE_EQUAL = ASSIGN_TYPE_EQUAL + 4, // 大于等于
    ASSIGN_TYPE_UNEQUAL = ASSIGN_TYPE_EQUAL + 5,    // 不等于
} assign_type_t;

typedef struct st_col_text {
    char *str;            // 名称、值
    uint32 len;           // 长度
    assign_type_t assign; // 赋值方式
} col_text_t;

typedef struct st_exp_column_def {
    col_text_t name;        // 字段名
    gs_type_t col_type;     // 字段类型
    uint16 col_slot;        // 字段槽位
    uint16 size;            // 字段长度
    bool32 nullable;        // 是否可空
    bool32 is_primary;      // 是否主键字段
    bool32 is_default;      // 是否有默认值
    col_text_t default_val; // 默认值
    col_text_t crud_value;  // 列值（查询、插入、修改、删除时用到）
    uint16 precision;       // 精度（即小数点后的位数）
    bool32 is_comment;      // 是否有列备注
    col_text_t comment;     // 备注
    uint16 scale;
    bool32 is_unique; // 是否唯一键字段
    bool32 is_check;  // 是否包含check约束
    bool32 is_autoincrement;
} exp_column_def_t;

enum SCAN_EDGE {
    SCAN_EDGE_LE = 0, // 小于等于
    SCAN_EDGE_GE = 1, // 大于等于
    SCAN_EDGE_EQ = 2, // 等于
};

typedef struct st_condition_def {
    gs_type_t col_type;    // column type
    const char *left_buff; // buff store raw data
    const char *right_buff;
    uint16 left_size; // len of data buff
    uint16 right_size;
    enum SCAN_EDGE scan_edge;
} condition_def_t;

typedef enum gstor_scan_action {
    GSTOR_CURSOR_ACTION_FOR_UPDATE_SCAN = 1,
    GSTOR_CURSOR_ACTION_SELECT = 2,
    GSTOR_CURSOR_ACTION_UPDATE = 3,
    GSTOR_CURSOR_ACTION_INSERT = 4,
    GSTOR_CURSOR_ACTION_DELETE = 5,
} scan_action_t;

// rowmark_type_t in dml_defs.h
typedef enum gstor_rowmark_type {
    GSTOR_ROWMARK_WAIT_BLOCK = 0,
    GSTOR_ROWMARK_WAIT_SECOND,
    GSTOR_ROWMARK_NOWAIT,
    GSTOR_ROWMARK_SKIP_LOCKED
} gstor_rowmark_type_t;

typedef struct st_lock_clause_def {
    bool32 is_select_for_update;
    gstor_rowmark_type_t wait_policy;
    uint32 wait_n;
} lock_clause_t;

typedef struct st_exp_part_column_def {
    uint32 column_id;
    gs_type_t datatype;
    uint16 size;
    uint8 precision;
    int8 scale;
} exp_part_column_def_t;

typedef struct st_exp_part_obj_def {
    part_type_t part_type;
    uint32 part_col_count;
    exp_part_column_def_t part_keys[GS_MAX_PART_COLUMN_SIZE]; // exp_part_column_def_t
    text_t interval;
    bool32 has_default;
    bool32 is_interval;
    bool32 is_slice;
    bool32 auto_addpart;        // if automatic add new part or not, yes for interval
    bool32 is_crosspart;        // if data storaged of cross part or not
} exp_part_obj_def_t;

typedef struct st_exp_index_def {
    col_text_t  name;       //索引名
    col_text_t *cols;       //索引列列表
    uint32      col_count;  //索引列列数
    bool32      is_unique;  //是否唯一索引
    bool32      is_primary; //是否主键索引
    uint16      index_slot; //索引槽位，表示在table_t.index_set.items中的位置
    uint16      col_ids[GS_MAX_INDEX_COLUMNS]; // 索引包含的列对应到表中的列id，方便索引选取时比较
    bool32      parted;
    exp_part_obj_def_t *part_obj;

    // uint32 dep_set_count;           // 被依赖的外键数量
    // cons_dep_set_t cons_dep_set;    // 被依赖的外键集合
}exp_index_def_t;

typedef struct st_exp_constraint_def {
    constraint_type_t type; // type in constraint_type_t  constraint_type_t
    col_text_t name;
    uint32 col_count;
    col_text_t *cols;

    // fk
    uint32 fk_ref_col_count;
    text_t *fk_ref_cols;
    text_t fk_ref_user;
    text_t fk_ref_table;
    knl_refactor_t fk_upd_action;
    knl_refactor_t fk_del_action;
} exp_constraint_def_t;

typedef struct st_exp_al_column_def {
    col_text_t name;                  // 列名
    col_text_t new_name;              // 新列名
    exp_column_def_t col_def;         // 列定义， for add column or modify column
    uint32 constr_count;              // 约束的数量
    exp_constraint_def_t *constr_def; // 约束定义
} exp_al_column_def_t;

typedef struct st_exp_alt_table_prop {
    col_text_t new_name;
} exp_alt_table_prop_t;

typedef struct st_exp_alt_table_part {
    col_text_t part_name;  // partition name
    part_type_t part_type;
    col_text_t hiboundval;  // 分区的上边界，手动创建分区时，上边界来源于分区名 :表名_20230821 or 表名_2023082116
} exp_alt_table_part_t;

typedef struct st_exp_altable_def {
    altable_action_t        action;
    col_text_t              name;       //表名
    exp_alt_table_prop_t    altable;
    exp_al_column_def_t     *cols;      //列列表
    uint32                  col_count;  //列数
    exp_alt_table_part_t    part_opt;   // 分区相关操作
    exp_constraint_def_t    *constraint; //约束
    char                    *comment;   // 表备注
}exp_altable_def_t;

typedef struct st_res_row_def {
    int column_count;                  // 行数
    exp_column_def_t *row_column_list; // 行包含的列列表
} res_row_def_t;

typedef enum en_exp_dict_type {
    DIC_TYPE_UNKNOWN = 0,
    DIC_TYPE_TABLE = 1,
    DIC_TYPE_TEMP_TABLE_TRANS = 2,
    DIC_TYPE_TEMP_TABLE_SESSION = 3,
    DIC_TYPE_TABLE_NOLOGGING = 4,

    /* this is must be the last one of table type */
    DIC_TYPE_TABLE_EXTERNAL = 5,

    DIC_TYPE_VIEW = 6,
    DIC_TYPE_DYNAMIC_VIEW = 7,
    DIC_TYPE_GLOBAL_DYNAMIC_VIEW = 8,

    DIC_TYPE_SYNONYM = 9,
    DIC_TYPE_DISTRIBUTE_RULE = 10,
    DIC_TYPE_SEQUENCE = 11,
} exp_dict_type_t;

typedef struct st_exp_part_desc_t {
    uint32 uid;
    uint32 table_id;
    uint32 index_id;
    part_type_t parttype;
    uint32 partcnt;
    uint32 slot_num;
    uint32 partkeys;
    uint32 flags;
    text_t interval;
    binary_t binterval;
    uint32 transition_no;
    uint32 interval_num;
    uint32 interval_spc_num;
    uint32 real_partcnt;
    uint32 not_ready_partcnt; // for split partition
    bool32 is_slice;
    bool32 auto_addpart; // if automatic add new part or not, yes for interval
    bool32 is_crosspart; // if data storaged of cross part or not
} exp_part_desc_t;

/* partition column description */
typedef struct st_exp_part_column_desc_t {
    uint32 uid;
    uint32 table_id;
    uint32 column_id;
    uint32 pos_id;
    gs_type_t datatype;
} exp_part_column_desc_t;

typedef struct st_exp_part_bucket {
    uint32 first;
} exp_part_bucket_t;

/* table partition description */
typedef struct st_exp_table_part_desc {
    uint32 uid;
    uint32 table_id;
    uint32 part_id;
    uint32 space_id;
    char name[GS_TS_PART_NAME_BUFFER_SIZE];
    union {
        uint32 flags;
        struct {
            uint32 not_ready : 1;
            uint32 storaged : 1;  // specified storage parameter
            uint32 is_parent : 1; // specified the part if is a parent part
            uint32 is_csf : 1;
            uint32 is_nologging : 1;
            uint32 compress : 1;
            uint32 unused : 26;
        };
    };
    text_t hiboundval;
    binary_t bhiboundval;
    uint8 compress_algo;
} exp_table_part_desc_t;

/* table partition entity */
typedef struct st_exp_table_part {
    uint32 part_no;
    uint32 parent_partno;
    uint32 global_partno; // part no in the global array consisted of all subparts
    exp_table_part_desc_t desc;
    bool32 is_ready; // stand for whether the dc is loading completely and this table part is ready to access
} exp_table_part_t;

typedef struct st_exp_part_table_t {
    exp_part_desc_t desc;
    exp_part_column_desc_t *keycols;
    exp_part_bucket_t *pbuckets; // 分区名 hash-> part no
    exp_table_part_t *entitys;
} exp_part_table_t;

typedef struct st_exp_table_meta_def {
    uint32 id;                         // table id
    char name[GS_NAME_BUFFER_SIZE];    // table name
    uint32 uid;                        // user id
    uint32 space_id;                   // table space
    uint32 oid;                        // object id
    uint32 column_count;               // column count
    uint32 index_count;                // index count
    exp_column_def_t *columns;         // columns defs
    exp_index_def_t *indexes;          // index defs
    uint32 cons_count;                 // constraint count
    exp_constraint_def_t *constraints; // table constraints
    // uint32 ref_count;
    // ref_cons_t *ref_cons;
    text_t sql;                        // for views
    exp_dict_type_t dict_type;         // table/view/...

    // about partition
    bool32 parted;               // if partition table or not
    bool32 appendonly;           // if appendonly or not
    bool32 is_timescale;         // if a timescale table or not
    bool32 has_retention;        // if having seted retetion or not
    interval_detail_t retention; // how long time of part data storaged, e.g. 30d
    exp_part_table_t part_table;
    bool32 has_autoincrement;    // if having autoincrement column
} exp_table_meta;

typedef struct st_table_option_def {
    bool32 is_memory;
} table_option_def_t;

typedef struct st_sequence_def {
    char *name;
    int64_t increment;
    int64_t min_value;
    int64_t max_value;
    int64_t start_value;
    bool32 is_cycle;
} sequence_def_t;

// e_object_type的子集
typedef enum en_drop_type {
    DROP_TYPE_TABLE = 0,
    DROP_TYPE_VIEW = 1,
    DROP_TYPE_SEQUENCE = 2,
    DROP_TYPE_INDEX = 10,
} drop_type_t;

typedef struct st_drop_def {
    char *name;
    bool32 if_exists;
    drop_type_t type;
} drop_def_t;

typedef struct st_exp_view_def {
    uint32 uid;
    text_t name;
    text_t user;
    galist_t columns;
    text_t sub_sql;
    sql_style_t sql_tpye;
    bool32 is_replace;
    galist_t *ref_objects;
    void *select;
} exp_view_def_t;

typedef struct st_exp_attr_def {
    bool32 is_timescale;
    char* retention;
    bool32 parted;
    exp_part_obj_def_t * part_def;
    char* comment;
}exp_attr_def_t;


typedef enum exp_comment_on_type {
    EXP_COMMENT_ON_TABLE = 0, /* TABLE */
    EXP_COMMENT_ON_COLUMN,    /* COLUMN */
} exp_comment_on_type_t;
typedef struct st_exp_comment_def {
    exp_comment_on_type_t type;
    text_t owner;
    text_t name;
    text_t column;
    text_t comment;
    uint32 uid;
    uint32 id;
    uint32 column_id;
} exp_comment_def_t;

EXPORT_API void gstor_shutdown(void* cc_instance);

EXPORT_API int gstor_startup(void **cc_instance, char *data_path);

EXPORT_API int gstor_alloc(void *cc_instance, void **handle);

EXPORT_API int gstor_create_user_table(void *handle, const char *table_name, int column_count,
    exp_column_def_t *column_list, int index_count, exp_index_def_t *index_list, int cons_count,
    exp_constraint_def_t *cons_list);

EXPORT_API int sqlapi_gstor_create_user_table(void* handle, const char* schema_name, const char* table_name, 
    int column_count, exp_column_def_t* column_list,
    int index_count, exp_index_def_t* index_list,
    int cons_count, exp_constraint_def_t* cons_list, 
    exp_attr_def_t attr, error_info_t* err_info);

EXPORT_API int sqlapi_gstor_create_index(void *handle, const char* schema_name, const char *table_name,
    const exp_index_def_t *index_def, error_info_t *err_info);
EXPORT_API int sqlapi_gstor_alter_table(void *handle, const char* schema_name, const char *table_name,
    const exp_altable_def_t *alter_table, error_info_t *err_info);

EXPORT_API int gstor_create_index(void *handle, const char *table_name, const exp_index_def_t *index_def);

int gstor_set_session_info(void *handle);
EXPORT_API int gstor_open_user_table(void *handle, const char *table_name);
EXPORT_API int gstor_open_user_table_with_user(void *handle, const char *schema_name, const char *table_name);

EXPORT_API int gstor_open_table(void *handle, const char *table_name);

EXPORT_API int gstor_open_mem_table(void *handle, const char *table_name);

EXPORT_API void gstor_free(void *handle);

EXPORT_API void gstor_clean(void *handle);

// EXPORT_API int gstor_set_param(char *name, char *value, char *data_path);

EXPORT_API int gstor_put(void *handle, char *key, unsigned int key_len, char *val, unsigned int val_len);

EXPORT_API int gstor_del(void *handle, char *key, unsigned int key_len, unsigned int prefix, unsigned int *count);

EXPORT_API int gstor_get(void *handle, char *key, unsigned int key_len, char **val, unsigned int *val_len,
    unsigned int *eof);

EXPORT_API int rust_gstor_get(void *handle, char *key);

EXPORT_API status_t gstor_executor_insert_row(void *handle, const char *table_name, int column_count,
    exp_column_def_t *column_list);

EXPORT_API status_t gstor_batch_insert_row(void* handle, const char* table_name,
    int row_count, res_row_def_t *row_list, uint32 part_no,
    bool32 is_ignore);

EXPORT_API status_t gstor_executor_update_row(void *handle, const char *table_name, int upd_column_count,
    exp_column_def_t *upd_column_list, int cond_column_count, exp_column_def_t *cond_column_list);

EXPORT_API status_t gstor_executor_update(void *handle, int column_count, exp_column_def_t *column_list, size_t cursor_idx);

EXPORT_API status_t gstor_executor_delete_row(void *handle, const char *table_name, int *del_count,
    int cond_column_count, exp_column_def_t *cond_column_list);

EXPORT_API status_t gstor_executor_delete(void *handle, size_t cursor_idx);

EXPORT_API status_t gstor_truncate_table(void *handle, char *owner, char *table_name);

EXPORT_API int gstor_open_cursor_ex(void *handle, const char *table_name, int index_column_size, int condition_size,
    condition_def_t *cond_column_list, bool32 *eof, int idx_slot, scan_action_t action, size_t cursor_idx,
    lock_clause_t lock_clause);

EXPORT_API int gstor_cursor_next(void *handle, unsigned int *eof, size_t cursor_idx);

EXPORT_API int gstor_cursor_fetch(void *handle, int sel_column_count, exp_column_def_t *sel_column_list,
    int *res_row_count, res_row_def_t *res_row_list,size_t cursor_idx);

EXPORT_API int gstor_fast_count_table_row(void *handle, const char* table_name, size_t cursor_idx, int64_t* row);

EXPORT_API int gstor_begin(void *handle);

EXPORT_API int gstor_commit(void *handle);

EXPORT_API int gstor_rollback(void *handle);

EXPORT_API int gstor_vm_alloc(void *handle, unsigned int *vmid);
EXPORT_API int gstor_vm_open(void *handle, unsigned int vmid, void **page);
EXPORT_API void gstor_vm_close(void *handle, unsigned int vmid);
EXPORT_API void gstor_vm_free(void *handle, unsigned int vmid);
EXPORT_API int gstor_vm_swap_out(void *handle, void *page, unsigned long long *swid, unsigned int *cipher_len);
EXPORT_API int gstor_vm_swap_in(void *handle, unsigned long long swid, unsigned int cipher_len, void *page);
EXPORT_API int gstor_xa_start(void *handle, unsigned char gtrid_len, const char *gtrid);
EXPORT_API int gstor_xa_status(void *handle);
EXPORT_API int gstor_xa_shrink(void *handle);
EXPORT_API int gstor_xa_end(void *handle);
EXPORT_API int gstor_detach_suspend_rm(void *handle);
EXPORT_API int gstor_attach_suspend_rm(void *handle);
EXPORT_API int gstor_detach_pending_rm(void *handle);
EXPORT_API int gstor_attach_pending_rm(void *handle);

EXPORT_API int gstor_get_table_info(void *handle, const char *schema_name, const char *table_name,
    exp_table_meta *table_info, error_info_t *err_info);
EXPORT_API void free_table_info(exp_table_meta *);

EXPORT_API int32 gstore_var_compare_data(const void *data1, const void *data2, gs_type_t type, uint32 size1,
    uint32 size2);

EXPORT_API knl_cursor_t *gstor_get_cursor(void *handle, size_t cursor_idx);

EXPORT_API status_t gstor_create_sequence(void *handle, const char *owner, sequence_def_t *sequence_info);
EXPORT_API status_t gstor_seq_currval(void *handle, const char *owner, const char *name, int64_t *val);
EXPORT_API status_t gstor_seq_nextval(void *handle, const char *owner, const char *name, int64_t *val);
EXPORT_API status_t gstor_alter_seq_nextval(void *handle, const char *owner, const char *name, int64_t val);
EXPORT_API status_t gstor_is_sequence_exist(void *handle, const char *owner, const char *name, bool32 *is_exist);

EXPORT_API status_t gstor_drop(void *handle, char *owner, drop_def_t *drop_info);

EXPORT_API status_t gstor_create_user_view(void *handle, exp_view_def_t *view_def, int columnCount,
    exp_column_def_t *column_defs);

EXPORT_API status_t gstor_modified_partno(void *handle,size_t cursor_idx,int partno);

EXPORT_API status_t gstor_force_checkpoint(void *handle);

EXPORT_API status_t gstor_comment_on(void *handle, exp_comment_def_t *comment_def);

int32_t db_is_open(void *db_instance);
uint32_t get_streamagg_threadpool_num(void *db_instance);

thread_t * get_streamagg_main_thread(void *db_instance);
char * get_database_home_path(void *db_instance);
int32_t get_ts_cagg_switch_on(void *db_instance);

EXPORT_API status_t gstor_autoincrement_nextval(void *handle, uint32 slot, int64_t *nextval);

EXPORT_API status_t gstor_autoincrement_updateval(void *handle, uint32 slot, int64_t nextval);

EXPORT_API status_t gstor_set_is_begin_transaction(void *handle, uint8 flag);

int64 gstor_get_sql_engine_memory_limit(void *handle);
uint32_t gstor_get_max_connections(void *handle);
uint32_t gstor_set_max_connections(void *handle, uint32_t max_conn);

bool32 gstor_get_user_id(void *handle, const char *user_name, uint32 *id);
bool32 gstor_get_role_id(void *handle, const char *role_name, uint32 *id);
bool32 gstor_verify_user_password(void *handle, const char *user_name, const char *password);

EXPORT_API status_t gstor_create_role(void *handle, knl_role_def_t *def);
EXPORT_API status_t gstor_create_user(void *handle, knl_user_def_t *def);
EXPORT_API status_t gstor_drop_role(void *handle, knl_drop_def_t *def);
EXPORT_API status_t gstor_drop_user(void *handle, knl_drop_user_t *def);
EXPORT_API status_t gstor_grant_privilege(void *handle, knl_grant_def_t *def);
EXPORT_API status_t gstor_revoke_privilege(void *handle, knl_revoke_def_t *def);
#ifdef __cplusplus
}
#endif

#endif
