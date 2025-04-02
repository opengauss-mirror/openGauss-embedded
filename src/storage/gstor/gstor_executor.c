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
 * gstor_executor.c
 *
 *
 * IDENTIFICATION
 * src/storage/gstor/gstor_executor.c
 *
 * -------------------------------------------------------------------------
 */
#include "cm_defs.h"
#include "cm_file.h"
#include "cm_buddy.h"
#include "cm_row.h"
#include "dc_tbl.h"
#include "dml_defs.h"
#include "gstor_param.h"
#include "gstor_handle.h"
#include "gstor_sys_def.h"
#include "gstor_executor.h"
#include "gstor_instance.h"
#include "storage/gstor/zekernel/kernel/table/knl_table.h"
#include "storage/gstor/zekernel/kernel/catalog/knl_comment.h"
#include "storage/kv_executor/gstor_adpt.h"
#include <stdio.h>
#ifdef __cplusplus
extern "C" {
#endif

#define G_STOR_SEQUENCE_9 '9'
#define G_STOR_PREFIX_FLAG (1 << 0)
#define G_STOR_SEQUENCE_FLAG (1 << 1)
#define G_STOR_DEFAULT_FLAG (0)
#define G_STOR_SEQUENCE_OFFSET (10)
#define GSTOR_IDX_EXT_NAME1 ("IX_")
#define GSTOR_IDX_EXT_NAME2 ("_001")
#define G_STOR_TABLE_EXT_SIZE (7)


#define G_STOR_DEFAULT_COLS (2)
#define G_STOR_DEFAULT_IDX_CNT (1)
#define G_SOTR_DEFAULT_TBL_ID ((uint32)64)

#define GSTOR_PUT_TRY_TIMES 10

static uint32 g_cur_table_id = G_SOTR_DEFAULT_TBL_ID;
static const char *g_inst_name = "intarkdb";
static const char *g_lock_file = "intarkdb.lck";
static const text_t g_user_table_col1 = {
    .str = (char *)"KEY",
    .len = 3
};
static const text_t g_user_table_col2 = {
    .str = (char *)"VALUE",
    .len = 5
};

#define GS_MAX_KEY_LEN (uint32)4000

static status_t gstor_init_config(instance_t *cc_instance, char *data_path)
{
    char cfg_path[GS_FILE_NAME_BUFFER_SIZE];

    PRTS_RETURN_IFERR(sprintf_s(cfg_path, GS_FILE_NAME_BUFFER_SIZE, "%s/intarkdb/cfg", data_path));
    if (!cm_dir_exist(cfg_path)) {
        GS_RETURN_IFERR(cm_create_dir_ex(cfg_path));
    }

    cc_instance->cc_config = (config_t *)malloc(sizeof(config_t));
    if (cc_instance->cc_config == NULL) {
        GS_LOG_DEBUG_ERR("alloc config object failed");
        return GS_ERROR;
    }

    uint32 param_count = 0;
    config_item_t *params = NULL;

    // copy g_parameters to params 
    knl_param_get_config_info(&params, &param_count);
    // init cc_config and init cc_config by params
    cm_init_config(params, param_count, cc_instance->cc_config);

    errno_t err = sprintf_s(cc_instance->cc_config->file_name, GS_FILE_NAME_BUFFER_SIZE, "%s/%s", cfg_path, "intarkdb.ini");
    if(err == -1 ) {
        CM_FREE_PTR(cc_instance->cc_config);
        return GS_ERROR;
    }

    bool32 file_exist = cm_file_exist(cc_instance->cc_config->file_name); 
    cc_instance->gstorini_exists = file_exist ? GS_TRUE : GS_FALSE;
    if(file_exist){
        // load config from file
        GS_RETURN_IFERR(cm_load_config(params, param_count, cc_instance->cc_config->file_name, cc_instance->cc_config, GS_FALSE));
        return GS_SUCCESS;
    } else {
        int32 fd = -1;
        GS_RETURN_IFERR(cm_create_file(cc_instance->cc_config->file_name, O_BINARY | O_SYNC | O_RDWR | O_EXCL, &fd));
        cm_close_file(fd);
    }
    return GS_SUCCESS;
}

static void gstor_deinit_config(instance_t *cc_instance)
{
    uint32 param_count = 0;
    config_item_t *params = NULL;

    knl_param_get_config_info(&params, &param_count);
    for (uint32 i = 0; i < param_count; ++i) {
        params[i].is_default = GS_TRUE;
    }
    if (cc_instance->cc_config == NULL) {
        return;
    }
    CM_FREE_PTR(cc_instance->cc_config->value_buf);
    CM_FREE_PTR(cc_instance->cc_config);
}

static inline void gstor_init_lob_buf(lob_buf_t *lob_buf)
{
    lob_buf->buf = NULL;
    lob_buf->size = 0;
}

static inline void gstor_free_lob_buf(lob_buf_t *lob_buf)
{
    lob_buf->size = 0;
    BUDDY_FREE_PTR(lob_buf->buf);
}

static inline status_t gstor_realloc_log_buf(instance_t *cc_instance, lob_buf_t *lob_buf, uint32 size)
{
    gstor_free_lob_buf(lob_buf);
    lob_buf->buf = galloc((&cc_instance->sga.buddy_pool), size);
    if (lob_buf->buf == NULL) {
        GS_LOG_DEBUG_ERR("alloc lob buf %u failed, mem pool remain %llu", size,
            (&cc_instance->sga.buddy_pool)->max_size - (&cc_instance->sga.buddy_pool)->used_size);
        return GS_ERROR;
    }
    lob_buf->size = size;
    return GS_SUCCESS;
}

static inline status_t gstor_open_kv_table(void *handle, const char *tablename, knl_dictionary_t *dc)
{
    text_t user = {
        .str = (char *)"SYS",
        .len = 3
    };
    text_t table = {
        .str = (char *)tablename,
        .len = strlen(tablename)
    };
    knl_session_t *session = EC_SESSION(handle);
    return knl_open_dc(session, &user, &table, dc);
}

int gstor_alloc(void *cc_instance, void **handle)
{
    // check db status and dc status
    uint32 wait_times = 0;
    while (!db_is_open(cc_instance)) {
        cm_sleep(1000);
        wait_times++;
        if (wait_times >= GS_WAIT_ALLOC_HANDLE) {
            GS_LOG_RUN_ERR("alloc handle failed, the database is not available!");
            return GS_ERROR;
        }
        GS_LOG_RUN_WAR("The database is not available at this moment, wait fot 1000 ms...");
    }

    ec_handle_t *ec_handle = (ec_handle_t *)malloc(sizeof(ec_handle_t));
    if (ec_handle == NULL) {
        GS_LOG_DEBUG_ERR("alloc exec handle memory failed");
        return GS_ERROR;
    }

    if (knl_alloc_session(cc_instance, &ec_handle->session) != GS_SUCCESS) {
        CM_FREE_PTR(ec_handle);
        return GS_ERROR;
    }

    if (knl_alloc_cursor(cc_instance, &ec_handle->cursor) != GS_SUCCESS) {
        knl_free_session(ec_handle->session);
        CM_FREE_PTR(ec_handle);
        return GS_ERROR;
    }

    for(int i = 0 ; i < G_STOR_MAX_CURSOR; ++i) {
        ec_handle->cursors[i] = NULL;
    }

    ec_handle->dc.handle = NULL;
    gstor_init_lob_buf(&ec_handle->lob_buf);
    *handle = ec_handle;
    return GS_SUCCESS;
}

static status_t gstor_create_table(void *handle, const char *table_name)
{
    column_def_t user_table_cols[] = {
        { g_user_table_col1, GS_TYPE_VARCHAR, GS_MAX_KEY_LEN, GS_FALSE },
        { g_user_table_col2, GS_TYPE_CLOB,    GS_MAX_KEY_LEN, GS_TRUE },
    };

    uint32 table_len = (uint32)strlen(table_name);
    uint32 idx_len = table_len + G_STOR_TABLE_EXT_SIZE;
#ifdef _MSC_VER
    char *idx_name = (char *)malloc(idx_len + 1);
    if (idx_name == NULL) {
        return GS_ERROR;
    }
#else
    char idx_name[idx_len + 1];
#endif
    PRTS_RETURN_IFERR(sprintf_s(idx_name, idx_len + 1, "%s%s%s", GSTOR_IDX_EXT_NAME1, table_name, GSTOR_IDX_EXT_NAME2));
    idx_name[idx_len] = '\0';

    index_def_t user_kv_indexes[] = {
        { {.str = idx_name, .len = idx_len}, (text_t*)&g_user_table_col1, 1, GS_TRUE}
    };

    bool8 is_memory = 0;
    table_def_t user_table = { {
        .str = (char *)table_name,
        .len = (uint32)strlen(table_name) 
        },
                               user_table_cols,
                               G_STOR_DEFAULT_COLS,
                               strcmp(table_name, EXC_DCC_KV_TABLE) == 0 ? (text_t *)&g_system : (text_t *)&g_users,
                               GS_INVALID_ID32,
                               G_STOR_DEFAULT_IDX_CNT,
                               user_kv_indexes,
                               TABLE_TYPE_HEAP,
                               is_memory,
                               0,
        NULL 
                             };
#ifdef _MSC_VER
    if (idx_name)free(idx_name);
#endif
    status_t ret = knl_create_user_table(EC_SESSION(handle), &user_table);
    if (ret == GS_SUCCESS) {
        ++g_cur_table_id;
        return GS_SUCCESS;
    }
    return GS_ERROR;
}

static status_t gstor_create_mem_table(void *handle, const char *table_name)
{
    column_def_t user_table_cols[] = {
        { g_user_table_col1, GS_TYPE_VARCHAR, GS_MAX_KEY_LEN, GS_FALSE },
        { g_user_table_col2, GS_TYPE_CLOB,    GS_MAX_KEY_LEN, GS_TRUE },
    };

    uint32 table_len = (uint32)strlen(table_name);
    uint32 idx_len = table_len + G_STOR_TABLE_EXT_SIZE;
#ifdef _MSC_VER
    char *idx_name = (char *)malloc(idx_len + 1);
    if (idx_name == NULL) {
        return GS_ERROR;
    }
#else
    char idx_name[idx_len + 1];
#endif
    PRTS_RETURN_IFERR(sprintf_s(idx_name, idx_len + 1, "%s%s%s", GSTOR_IDX_EXT_NAME1, table_name, GSTOR_IDX_EXT_NAME2));
    idx_name[idx_len] = '\0';

    index_def_t user_kv_indexes[] = {
        { {.str = idx_name, .len = idx_len}, (text_t*)&g_user_table_col1, 1, GS_TRUE}
    };

    bool8 is_memory = 1;
    table_def_t user_table = { {
        .str = (char *)table_name,
        .len = (uint32)strlen(table_name) 
        },
                               user_table_cols,
                               G_STOR_DEFAULT_COLS,
                               (text_t *)&g_users,
                               GS_INVALID_ID32,
                               G_STOR_DEFAULT_IDX_CNT,
                               user_kv_indexes,
                               TABLE_TYPE_HEAP,
                               is_memory,
                               0,
        NULL 
                             };
    status_t ret = knl_create_user_table(EC_SESSION(handle), &user_table);
    if (ret == GS_SUCCESS) {
        ++g_cur_table_id;
        return GS_SUCCESS;
    }
#ifdef _MSC_VER
    free(idx_name);
#endif
    return GS_ERROR;
}

int gstor_create_user_table(void *handle, const char *table_name, int column_count, exp_column_def_t *column_list,
    int index_count, exp_index_def_t *index_list, int cons_count, exp_constraint_def_t *cons_list)
{
    knl_constraint_def_t *user_table_cons = NULL;
    knl_session_t *session = EC_SESSION(handle);

    column_def_t *user_table_cols = (column_def_t *)malloc(sizeof(column_def_t) * column_count);
    if (user_table_cols == NULL) {
        GS_LOG_DEBUG_ERR("alloc user_table_cols object failed");
        return GS_ERROR;
    }
    index_def_t *user_table_idxs = (index_def_t *)malloc(sizeof(index_def_t) * index_count);
    if (user_table_idxs == NULL) {
        GS_LOG_DEBUG_ERR("alloc user_table_idxs object failed");
        if (user_table_cols)
            free(user_table_cols);
        return GS_ERROR;
    }
    for (uint32 i = 0; i < column_count; i++) {
        column_def_t *table_cols = user_table_cols + i;
        table_cols->name.str = column_list[i].name.str;
        table_cols->name.len = column_list[i].name.len;
        table_cols->type = column_list[i].col_type;
        table_cols->size = column_list[i].size;
        table_cols->nullable = column_list[i].nullable;
        table_cols->is_primary = column_list[i].is_primary;
        table_cols->is_default = column_list[i].is_default;
        if (table_cols->is_default) {
            table_cols->default_val.str = column_list[i].default_val.str;
            table_cols->default_val.len = column_list[i].default_val.len;
        }
        if (column_list[i].comment.len > 0) {
            table_cols->comment.str = column_list[i].comment.str;
            table_cols->comment.len = column_list[i].comment.len;
        }
    }
    for (uint32 i = 0; i < index_count; i++) {
        index_def_t *table_idxs = user_table_idxs + i;
        table_idxs->name.str = index_list[i].name.str;
        table_idxs->name.len = index_list[i].name.len;
        text_t *cols = (text_t *)malloc(sizeof(text_t) * index_list[i].col_count);
        if (cols == NULL) {
            GS_LOG_DEBUG_ERR("alloc cols object failed");
            if (user_table_cols)
                free(user_table_cols);
            if (user_table_idxs)
                free(user_table_idxs);
            for (uint32 i_free = 0; i_free < i; i_free++) {
                if (user_table_idxs[i_free].cols)
                    free(user_table_idxs[i_free].cols);
            }
            return GS_ERROR;
        }
        for (uint32 j = 0; j < index_list[i].col_count; j++) {
            text_t *column = cols + j;
            column->str = index_list[i].cols[j].str;
            column->len = index_list[i].cols[j].len;
        }
        table_idxs->cols = cols;
        table_idxs->col_count = index_list[i].col_count;
        table_idxs->is_unique = index_list[i].is_unique;
        table_idxs->is_primary = index_list[i].is_primary;
    }

    CM_SAVE_STACK(session->stack);
    if (cons_count > 0) {
        user_table_cons = (knl_constraint_def_t *)malloc(sizeof(knl_constraint_def_t) * cons_count);
        if (user_table_cons == NULL) {
            GS_LOG_DEBUG_ERR("alloc user_table_cons object failed");
            CM_RESTORE_STACK(session->stack);
            return GS_ERROR;
        }
        for (uint32 i = 0; i < cons_count; i++) {
            knl_constraint_def_t *table_cons = user_table_cons + i;
            knl_index_col_def_t *cons_col = NULL;
            table_cons->type = cons_list[i].type;
            table_cons->name.str = cons_list[i].name.str;
            table_cons->name.len = cons_list[i].name.len;
            cm_galist_init(&table_cons->columns, session->stack, cm_stack_alloc);
            for (int col_idx = 0; col_idx < cons_list[i].col_count; col_idx++) {
                GS_RETURN_IFERR(
                    cm_galist_new(&table_cons->columns, sizeof(knl_index_col_def_t), (pointer_t *)&cons_col));
                MEMS_RETURN_IFERR(memset_s(cons_col, sizeof(knl_index_col_def_t), 0, sizeof(knl_index_col_def_t)));
                cons_col->name.str = cons_list[i].cols[col_idx].str;
                cons_col->name.len = cons_list[i].cols[col_idx].len;
                cons_col->mode = SORT_MODE_ASC;
                cons_col->is_func = GS_FALSE;
            }
            if (table_cons->type == CONS_TYPE_PRIMARY || table_cons->type == CONS_TYPE_UNIQUE) {
                MEMS_RETURN_IFERR(
                    memset_sp(&table_cons->index, sizeof(table_cons->index), 0, sizeof(table_cons->index)));
                table_cons->index.name = table_cons->name;
                table_cons->index.table.str = (char *)table_name;
                table_cons->index.table.len = strlen(table_name);
                #ifdef _MSC_VER
                const text_t *space = &g_users;
                table_cons->index.space = *space;
                #else
                table_cons->index.space = (text_t)g_users;
                #endif
                table_cons->index.primary = (table_cons->type == CONS_TYPE_PRIMARY) ? GS_TRUE : GS_FALSE;
                table_cons->index.unique = (table_cons->type == CONS_TYPE_UNIQUE) ? GS_TRUE : GS_FALSE;
                table_cons->index.user.str = SYS_USER_NAME;
                table_cons->index.user.len = SYS_USER_NAME_LEN;
                table_cons->index.cr_mode = CR_PAGE;
                table_cons->index.options |= CREATE_IF_NOT_EXISTS;
            }
        }
    }

    bool8 is_memory = 0;
    table_def_t user_table = { {
        .str = (char *)table_name,
        .len = (uint32)strlen(table_name) 
        },
                               user_table_cols,
                               column_count,
                               (text_t *)&g_users,
                               GS_INVALID_ID32,
                               index_count,
                               user_table_idxs,
                               TABLE_TYPE_HEAP,
                               is_memory,
                               cons_count,
        user_table_cons 
                             };
    status_t ret = knl_create_user_table(session, &user_table);
    if (ret == GS_SUCCESS) {
        ++g_cur_table_id;
    }

    for (uint32 i = 0; i < index_count; i++) {
        free(user_table_idxs[i].cols);
    }
    free(user_table_idxs);
    free(user_table_cols);
    free(user_table_cons);
    CM_RESTORE_STACK(session->stack);
    return ret;
}

status_t part_table_time_trans(char *time_str, uint32 len, date_t *data){
    // 这里把他们转化为以微秒为单位的整数进行保存
    switch(time_str[len-1]){
        case GS_TIME_SUFFIX_HOUR:{
            time_str[len-1] = '\0';
            int32 hours = atoi(time_str);
            *data = hours * SECONDS_PER_HOUR * MICROSECS_PER_SECOND_LL;
            time_str[len-1] = GS_TIME_SUFFIX_HOUR;
            break;
        }
        case GS_TIME_SUFFIX_DAY:{
            time_str[len-1] = '\0';
            int32 days = atoi(time_str);
            *data = days * SECONDS_PER_DAY * MICROSECS_PER_SECOND_LL;
            time_str[len-1] = GS_TIME_SUFFIX_DAY;
            break;
        }
        case GS_TIME_SUFFIX_WEEK:{
            time_str[len-1] = '\0';
            int32 weeks = atoi(time_str);
            *data = weeks * DAYS_PER_WEEK * SECONDS_PER_DAY * MICROSECS_PER_SECOND_LL;
            time_str[len-1] = GS_TIME_SUFFIX_WEEK;
            break;
        }
        default:{
            GS_LOG_RUN_ERR("the time suffix do not support!");
            return GS_ERROR;
        }
    }
    return GS_SUCCESS;
}

status_t convert_part_obj_def(knl_session_t *session, knl_part_obj_def_t **part_def, exp_part_obj_def_t *part_obj){
    *part_def = (knl_part_obj_def_t *)cm_push(session->stack, sizeof(knl_part_obj_def_t));
    MEMS_RETURN_IFERR(memset_s(*part_def, sizeof(knl_part_obj_def_t), 0, sizeof(knl_part_obj_def_t)));
    (*part_def)->part_type = part_obj->part_type;
    (*part_def)->part_keys.count = part_obj->part_col_count;
    cm_galist_init(&(*part_def)->part_keys, session->stack, cm_stack_alloc);
    knl_part_column_def_t* part_col = NULL;
    for (uint32 i = 0; i < part_obj->part_col_count; i++) {
        GS_RETURN_IFERR(cm_galist_new(&(*part_def)->part_keys, sizeof(knl_part_column_def_t), (pointer_t *)&part_col));
        MEMS_RETURN_IFERR(memset_s(part_col, sizeof(knl_part_column_def_t), 0, sizeof(knl_part_column_def_t)));
        part_col->column_id = part_obj->part_keys[i].column_id;
        part_col->datatype = part_obj->part_keys[i].datatype;
        part_col->is_char = part_col->datatype==GS_TYPE_VARCHAR;
        part_col->size = part_obj->part_keys[i].size;
        part_col->precision = part_obj->part_keys[i].precision;
        part_col->scale = part_obj->part_keys[i].scale;
    }
    if (part_obj->is_interval && part_obj->interval.len>0){
        if (part_obj->part_col_count != 1){
            GS_LOG_RUN_ERR("the interval key count can only be one!");
            return GS_ERROR;
        }
        (*part_def)->is_interval = GS_TRUE;
        // interval e.g. 1d/1h/1w
        (*part_def)->interval.str = part_obj->interval.str;
        (*part_def)->interval.len = part_obj->interval.len;
        // binterval 
        part_key_t *part_key = (part_key_t *)cm_push(session->stack, GS_MAX_COLUMN_SIZE);
        (*part_def)->binterval.bytes = (uint8 *)part_key;
        MEMS_RETURN_IFERR(memset_s(part_key, GS_MAX_COLUMN_SIZE, 0, GS_MAX_COLUMN_SIZE));
        part_key_init(part_key, part_obj->part_col_count);
        date_t data;  // 这里把他们转化为以微秒为单位的整数进行保存
        if (part_table_time_trans(part_obj->interval.str, part_obj->interval.len, &data) != GS_SUCCESS) {
            return GS_ERROR;
        }
        if (part_put_data(part_key, &data, part_obj->part_keys[0].size, part_obj->part_keys[0].datatype) != GS_SUCCESS) {
            return GS_ERROR;
        }
        (*part_def)->binterval.size = sizeof(part_key_t);
        GS_LOG_RUN_INF("interval time:%lld, binterval.size:%d, datatype:%d,"
                    "part_key->size:%d, part_key->column_count:%d\n", 
                    data, (*part_def)->binterval.size, part_obj->part_keys[0].datatype,
                    part_key->size, part_key->column_count);
    }
    (*part_def)->auto_addpart = part_obj->auto_addpart;
    (*part_def)->is_crosspart = part_obj->is_crosspart;
    return GS_SUCCESS;
}

status_t convert_table_def(knl_session_t *session, const char* schema_name, const char* table_name, bool8 is_memory,
    const int column_count, const exp_column_def_t* column_list,
    const int cons_count, const exp_constraint_def_t* cons_list, 
    bool32 is_timescale, char* retention, bool32 parted, exp_part_obj_def_t *part_obj, 
    knl_table_def_t* def, bool32* need_partkey_index, char* comment)
{
    knl_column_def_t *column = NULL;
    knl_constraint_def_t *constra = NULL;
    *need_partkey_index = is_timescale && parted; // 时序表需要创建基于分区键索引

    text_t schema = { schema_name, strlen(schema_name) };
    def->name.str = table_name;
    def->name.len = (uint32)strlen(table_name);
    def->sysid = GS_INVALID_ID32;
    def->space = g_users;
    def->type = TABLE_TYPE_HEAP;
    def->schema = schema;
    def->cr_mode = CR_PAGE;
    def->options |= CREATE_IF_NOT_EXISTS;
    def->is_memory = is_memory;

    cm_galist_init(&def->columns, session->stack, cm_stack_alloc);
    cm_galist_init(&def->constraints, session->stack, cm_stack_alloc);

    for (uint32 i = 0; i < column_count; i++) {
        GS_RETURN_IFERR(cm_galist_new(&def->columns, sizeof(knl_column_def_t), (pointer_t *)&column));
        MEMS_RETURN_IFERR(memset_s(column, sizeof(knl_column_def_t), 0, sizeof(knl_column_def_t)));
        column->name.str = column_list[i].name.str;
        column->name.len = column_list[i].name.len;
        cm_galist_init(&column->ref_columns, session->stack, cm_stack_alloc);
        column->table = (void *)&def;
        column->has_null = GS_TRUE;
        column->primary = GS_FALSE;
        column->nullable = column_list[i].nullable;
        column->typmod.size = column_list[i].size;
        column->typmod.datatype = column_list[i].col_type;
        column->primary = column_list[i].is_primary;
        column->is_default = column_list[i].is_default;
        column->is_default_null = GS_TRUE;
        column->is_serial = column_list[i].is_autoincrement;
        if (column->is_default) {
            column->default_text.str = column_list[i].default_val.str;
            column->default_text.len = column_list[i].default_val.len;
            column->is_default_null = GS_FALSE;
        }
        if (column_list[i].is_comment && column_list[i].comment.len > 0) {
            column->is_comment = GS_TRUE;
            column->comment.str = column_list[i].comment.str;
            column->comment.len = column_list[i].comment.len;
        }
        if (column->typmod.datatype == GS_TYPE_DECIMAL || column->typmod.datatype == GS_TYPE_NUMBER) {
            column->typmod.precision = column_list[i].precision;
            column->typmod.scale = column_list[i].scale;
        }
    }

    // constraints
    for (uint32 i = 0; i < cons_count; i++) {
        GS_RETURN_IFERR(cm_galist_new(&def->constraints, sizeof(knl_constraint_def_t), (pointer_t *)&constra));
        MEMS_RETURN_IFERR(memset_s(constra, sizeof(knl_constraint_def_t), 0, sizeof(knl_constraint_def_t)));

        knl_index_col_def_t *cons_col = NULL;
        constra->type = cons_list[i].type;
        constra->name.str = cons_list[i].name.str;
        constra->name.len = cons_list[i].name.len;
        cm_galist_init(&constra->columns, session->stack, cm_stack_alloc);
        for (int col_idx = 0; col_idx < cons_list[i].col_count; col_idx++) {
            GS_RETURN_IFERR(cm_galist_new(&constra->columns, sizeof(knl_index_col_def_t), (pointer_t *)&cons_col));
            MEMS_RETURN_IFERR(memset_s(cons_col, sizeof(knl_index_col_def_t), 0, sizeof(knl_index_col_def_t)));
            cons_col->name.str = cons_list[i].cols[col_idx].str;
            cons_col->name.len = cons_list[i].cols[col_idx].len;
            cons_col->mode = SORT_MODE_ASC;
            cons_col->is_func = GS_FALSE;
        }
        if (constra->type == CONS_TYPE_PRIMARY || constra->type == CONS_TYPE_UNIQUE) {
            MEMS_RETURN_IFERR(memset_sp(&constra->index, sizeof(constra->index), 0, sizeof(constra->index)));
            if (constra->name.len >= GS_NAME_BUFFER_SIZE) {
                GS_LOG_RUN_ERR("index name is to long, max length:%u\n", GS_NAME_BUFFER_SIZE - 1);
                GS_THROW_ERROR(ERR_NAME_TOO_LONG, constra->name.str, constra->name.len, GS_NAME_BUFFER_SIZE - 1);
                return GS_ERROR;
            }
            constra->index.name = constra->name;
            constra->index.table.str = (char *)table_name;
            constra->index.table.len = strlen(table_name);
            #ifdef _MSC_VER
            const text_t *space = &g_users;
            constra->index.space = *space;
            #else
            constra->index.space = (text_t)g_users;
            #endif
            constra->index.primary = (constra->type == CONS_TYPE_PRIMARY) ? GS_TRUE : GS_FALSE;
            constra->index.unique = 
                (constra->type == CONS_TYPE_UNIQUE 
                 || constra->type == CONS_TYPE_PRIMARY) ? GS_TRUE : GS_FALSE;
            constra->index.user.str = schema_name;
            constra->index.user.len = strlen(schema_name);
            constra->index.cr_mode = CR_PAGE;
            constra->index.options |= CREATE_IF_NOT_EXISTS;

            if (*need_partkey_index && part_obj!=NULL && part_obj->part_keys!=NULL &&
                0==strncmp(cons_list[i].cols[0].str, 
                           column_list[part_obj->part_keys[0].column_id].name.str, 
                           cons_list[i].cols[0].len)){
                *need_partkey_index = GS_FALSE;
            }
        }

        // fk
        if (constra->type == CONS_TYPE_REFERENCE) {
            knl_column_def_t *cons_col = NULL;
            constra->type = cons_list[i].type;
            constra->name.str = cons_list[i].name.str;
            constra->name.len = cons_list[i].name.len;
            cm_galist_init(&constra->columns, session->stack, cm_stack_alloc);
            for (int col_idx = 0; col_idx < cons_list[i].col_count; col_idx++) {
                GS_RETURN_IFERR(cm_galist_new(&constra->columns, sizeof(knl_column_def_t), (pointer_t *)&cons_col));
                MEMS_RETURN_IFERR(memset_s(cons_col, sizeof(knl_column_def_t), 0, sizeof(knl_column_def_t)));
                cons_col->name.str = cons_list[i].cols[col_idx].str;
                cons_col->name.len = cons_list[i].cols[col_idx].len;
            }

            MEMS_RETURN_IFERR(memset_sp(&constra->ref, sizeof(constra->ref), 0, sizeof(constra->ref)));

            knl_column_def_t *ref_columns = NULL;
            cm_galist_init(&constra->ref.ref_columns, session->stack, cm_stack_alloc);
            for (int col_i = 0; col_i < cons_list[i].fk_ref_col_count; col_i++) {
                GS_RETURN_IFERR(cm_galist_new(&constra->ref.ref_columns, sizeof(knl_column_def_t), (pointer_t *)&ref_columns));
                MEMS_RETURN_IFERR(memset_s(ref_columns, sizeof(knl_column_def_t), 0, sizeof(knl_column_def_t)));
                ref_columns->name.str = cons_list[i].fk_ref_cols[col_i].str;
                ref_columns->name.len = cons_list[i].fk_ref_cols[col_i].len;
            }
            
            constra->ref.ref_user.str = cons_list[i].fk_ref_user.str;
            constra->ref.ref_user.len = cons_list[i].fk_ref_user.len;
            constra->ref.ref_table.str = cons_list[i].fk_ref_table.str;
            constra->ref.ref_table.len = cons_list[i].fk_ref_table.len;
            constra->ref.refactor = cons_list[i].fk_del_action;
            constra->ref.ref_dc.type = DICT_TYPE_TABLE;
            constra->cons_state.is_use_index = 1;
            constra->cons_state.is_enable = 1;
            constra->cons_state.is_validate = 1;
            constra->cons_state.is_anonymous = 0;
        }
    }

    // partition
    def->parted = parted;
    def->is_timescale = is_timescale;
    if (retention != NULL){
        def->has_retention = GS_TRUE;
        def->retention = retention;
    }
    if (parted && part_obj!=NULL){
        if(convert_part_obj_def(session, &def->part_def, part_obj) != GS_SUCCESS){
            return GS_ERROR;
        }

        if (def->has_retention && def->part_def->is_interval){
            date_t retent, interv;
            GS_RETURN_IFERR(part_table_time_trans(def->retention, strlen(def->retention), &retent));
            GS_RETURN_IFERR(part_table_time_trans(def->part_def->interval.str, def->part_def->interval.len, &interv));
            if (retent < interv){
                GS_LOG_RUN_ERR("retention %s must be larger than interval %s!", def->retention, def->part_def->interval.str);
                GS_THROW_ERROR(ERR_PARAMETER_TOO_SMALL, def->retention, interv);
                return GS_ERROR;
            }
        }
    }
    // TABLE COMMENT
    if (comment != NULL && strlen(comment) > 0){
        def->has_comment = GS_TRUE;
        def->comment = comment;
    }
    return GS_SUCCESS;
}

int sqlapi_gstor_create_user_table(void* handle, const char* schema_name, const char* table_name, 
    int column_count, exp_column_def_t* column_list,
    int index_count, exp_index_def_t* index_list,
    int cons_count, exp_constraint_def_t* cons_list, 
    exp_attr_def_t attr, error_info_t* err_info)
{
    cm_reset_error(); // 初始化异常信息存储变量
    knl_session_t *session = EC_SESSION(handle);
    bool8 is_memory = 0;
    knl_table_def_t def;
    bool32 need_partkey_index = GS_FALSE;
    MEMS_RETURN_IFERR(memset_s(&def, sizeof(knl_table_def_t), 0, sizeof(knl_table_def_t)));

    CM_SAVE_STACK(session->stack);
    status_t ret = convert_table_def(session, schema_name, table_name, is_memory, column_count, 
                        column_list, cons_count, cons_list, attr.is_timescale, attr.retention, 
                        attr.parted, attr.part_def, &def, &need_partkey_index, attr.comment);
    if (ret != GS_SUCCESS) {
        if (err_info != NULL) {
            err_info->code = GS_ERRNO;
            memcpy_s(err_info->message, GS_MESSAGE_BUFFER_SIZE, g_tls_error.message, GS_MESSAGE_BUFFER_SIZE);
        }
        cm_reset_error();
        CM_RESTORE_STACK(session->stack);
        return ret;
    }

    ret = knl_create_table(session, &def);
    if (ret == GS_SUCCESS) {
        ++g_cur_table_id;
    } else {
        if (err_info != NULL) {
            err_info->code = GS_ERRNO;
            memcpy_s(err_info->message, GS_MESSAGE_BUFFER_SIZE, g_tls_error.message, GS_MESSAGE_BUFFER_SIZE);
            GS_LOG_RUN_WAR("sqlapi_gstor_create_user_table err,code:%d,msg:%s\n", err_info->code, err_info->message);
        }
        cm_reset_error();
        CM_RESTORE_STACK(session->stack);
        return ret;
    }
    if (need_partkey_index) {
        index_count=1; //建表时只存在唯一键或主键索引，且它们都是由对应约束进行创建
        index_list = (exp_index_def_t *)cm_push(session->stack, sizeof(exp_index_def_t));
        MEMS_RETURN_IFERR(memset_s(index_list, sizeof(exp_index_def_t), 0, sizeof(exp_index_def_t)));
        uint32 str_len = strlen(table_name) + column_list[attr.part_def->part_keys[0].column_id].name.len + 1;
        index_list[0].name.str = (char *)cm_push(session->stack, str_len);
        MEMS_RETURN_IFERR(memset_s(index_list[0].name.str, str_len, 0, str_len));
        strcat_s(index_list[0].name.str, str_len, table_name);
        strcat_s(index_list[0].name.str, str_len, column_list[attr.part_def->part_keys[0].column_id].name.str);
        index_list[0].name.len = str_len-1;
        index_list[0].name.assign = ASSIGN_TYPE_EQUAL;
        index_list[0].cols = (col_text_t *)cm_push(session->stack, sizeof(col_text_t));
        index_list[0].cols[0] = column_list[attr.part_def->part_keys[0].column_id].name;
        index_list[0].col_count = 1;
        index_list[0].parted = attr.parted;
        index_list[0].part_obj = attr.part_def;
        if (index_count > 0 && 
            sqlapi_gstor_create_index(handle, schema_name, table_name, index_list, err_info) != GS_SUCCESS) {
            CM_RESTORE_STACK(session->stack);
            return GS_ERROR;
        }
    }

    // fk index
    uint32 fk_index_idx = 0;
    for (uint32 i = 0; i < cons_count; i++) {
        if (cons_list[i].type != CONS_TYPE_REFERENCE) {
            continue;
        }
        fk_index_idx++;
        char idx_buf[5];
        sprintf(idx_buf, "%04d", fk_index_idx);

        exp_index_def_t* index_list = (exp_index_def_t *)cm_push(session->stack, sizeof(exp_index_def_t));
        MEMS_RETURN_IFERR(memset_s(index_list, sizeof(exp_index_def_t), 0, sizeof(exp_index_def_t)));
        uint32 str_len = strlen(table_name) + 8 + 1;    // fk_tablename_0001
        index_list[0].name.str = (char *)cm_push(session->stack, str_len);
        MEMS_RETURN_IFERR(memset_s(index_list[0].name.str, str_len, 0, str_len));
        strcat_s(index_list[0].name.str, str_len, "fk_");
        strcat_s(index_list[0].name.str, str_len, table_name);
        strcat_s(index_list[0].name.str, str_len, "_");
        strcat_s(index_list[0].name.str, str_len, idx_buf);
        index_list[0].name.len = str_len - 1;
        index_list[0].name.assign = ASSIGN_TYPE_EQUAL;

        uint32 fk_col_count = cons_list[i].col_count;
        index_list[0].cols = (col_text_t *)cm_push(session->stack, sizeof(col_text_t) * fk_col_count);
        for (uint32 fk_i = 0; fk_i < fk_col_count; fk_i++) {
            index_list[0].col_count = fk_col_count;
            index_list[0].cols[fk_i] = cons_list[i].cols[fk_i];
        }
        if (sqlapi_gstor_create_index(handle, schema_name, table_name, index_list, err_info) != GS_SUCCESS) {
            CM_RESTORE_STACK(session->stack);
            return GS_ERROR;
        }
    }

    CM_RESTORE_STACK(session->stack);
    return ret;
}

int gstor_create_index(void *handle, const char *table_name, const exp_index_def_t *index_def)
{
    if (!index_def) {
        return GS_ERROR;
    }

    column_def_t* user_table_cols[] = {};
    index_def_t *user_table_idx = (index_def_t *)malloc(sizeof(index_def_t));
    if (user_table_idx == NULL) {
        GS_LOG_DEBUG_ERR("alloc user_table_idx object failed");
        return GS_ERROR;
    }

    user_table_idx->name.str = index_def->name.str;
    user_table_idx->name.len = index_def->name.len;
    text_t *cols = (text_t *)malloc(sizeof(text_t) * index_def->col_count);
    if (cols == NULL) {
        GS_LOG_DEBUG_ERR("alloc cols object failed");
        if (user_table_idx)
            free(user_table_idx);
        return GS_ERROR;
    }
    for (uint32 i = 0; i < index_def->col_count; i++) {
        text_t *column = cols + i;
        column->str = index_def->cols[i].str;
        column->len = index_def->cols[i].len;
    }
    user_table_idx->cols = cols;
    user_table_idx->col_count = index_def->col_count;
    user_table_idx->is_unique = index_def->is_unique;
    user_table_idx->is_primary = index_def->is_primary;

    bool8 is_memory = 0;
    table_def_t user_table = { {
        .str = (char *)table_name,
        .len = (uint32)strlen(table_name) 
        },
                               user_table_cols,
                               0,
                               (text_t *)&g_users,
                               0,
                               1,
                               user_table_idx,
                               TABLE_TYPE_HEAP,
        is_memory 
                             };

    knl_session_t *knl_session = EC_SESSION(handle);
    status_t status = knl_create_sys_index(knl_session, &user_table, user_table_idx);
    if (status != GS_SUCCESS) {
        free(user_table_idx->cols);
        free(user_table_idx);
        return status;
    }
    status_t ret = knl_load_sys_def(knl_session, &user_table.name);

    free(user_table_idx->cols);
    free(user_table_idx);
    return ret;
}

int sqlapi_gstor_create_index(void *handle, const char* schema_name, const char *table_name,
    const exp_index_def_t *index_def, error_info_t *err_info)
{
    cm_reset_error(); // 初始化异常信息存储变量
    if (!index_def) {
        return GS_ERROR;
    }

    knl_session_t *session = EC_SESSION(handle);
    knl_index_def_t def;
    MEMS_RETURN_IFERR(memset_s(&def, sizeof(knl_index_def_t), 0, sizeof(knl_index_def_t)));

    CM_SAVE_STACK(session->stack);
    knl_index_col_def_t *column = NULL;
    text_t user = { schema_name, strlen(schema_name) };
    def.user = user;
    def.name.str = index_def->name.str;
    def.name.len = index_def->name.len;
    def.table.str = (char *)table_name;
    def.table.len = (uint32)strlen(table_name);
    def.space = g_users;
    def.unique = index_def->is_unique;
    def.primary = index_def->is_primary;
    def.cr_mode = CR_PAGE;
    def.options |= CREATE_IF_NOT_EXISTS;
    def.online = GS_TRUE;
    def.parted = index_def->parted;

    cm_galist_init(&def.columns, session->stack, cm_stack_alloc);
    for (uint32 i = 0; i < index_def->col_count; i++) {
        GS_RETURN_IFERR(cm_galist_new(&def.columns, sizeof(knl_index_col_def_t), (void **)&column));
        MEMS_RETURN_IFERR(memset_s(column, sizeof(knl_index_col_def_t), 0, sizeof(knl_index_col_def_t)));
        column->name.str = index_def->cols[i].str;
        column->name.len = index_def->cols[i].len;
        column->mode = SORT_MODE_ASC;
    }
    if (index_def->parted && index_def->part_obj!=NULL){
        if(convert_part_obj_def(session, &def.part_def, index_def->part_obj) != GS_SUCCESS){
            return GS_ERROR;
        }
    }
    status_t status = knl_create_index(session, &def);
    CM_RESTORE_STACK(session->stack);
    if (status != GS_SUCCESS) {
        if (err_info != NULL) {
            err_info->code = GS_ERRNO;
            memcpy_s(err_info->message, GS_MESSAGE_BUFFER_SIZE, g_tls_error.message, GS_MESSAGE_BUFFER_SIZE);
            GS_LOG_RUN_WAR("sqlapi_gstor_create_index err,code:%d,msg:%s\n", err_info->code, err_info->message);
        }
        cm_reset_error();
        return status;
    }

    return GS_SUCCESS;
}

int gstor_open_user_table(void *handle, const char *table_name)
{
    knl_dictionary_t *dc = EC_DC(handle);
    knl_close_dc(dc);

    text_t user = {
        .str = (char *)"SYS",
        .len = 3
    };
    text_t table = {
        .str = (char *)table_name,
        .len = strlen(table_name)
    };
    knl_session_t *session = EC_SESSION(handle);
    if (knl_open_dc(session, &user, &table, dc) == GS_SUCCESS) {
        gstor_set_session_info(handle);
        return GS_SUCCESS;
    }
    return GS_ERROR;
}

int gstor_open_user_table_with_user(void *handle, const char *schema_name, const char *table_name)
{
    knl_dictionary_t *dc = EC_DC(handle);
    knl_close_dc(dc);

    text_t user = {
        .str = (char *)schema_name,
        .len = strlen(schema_name)
    };
    text_t table = {
        .str = (char *)table_name,
        .len = strlen(table_name)
    };
    knl_session_t *session = EC_SESSION(handle);
    if (knl_open_dc(session, &user, &table, dc) == GS_SUCCESS) {
        gstor_set_session_info(handle);
        return GS_SUCCESS;
    } else {
        int32_t err_code;
        const char *message = NULL;
        cm_get_error(&err_code, &message, NULL);
        GS_LOG_RUN_INF("gstor_open_user_table_with_user err_code=%d, message=%s\n", err_code, message);
        cm_reset_error();
    }
    return GS_ERROR;
}

int gstor_set_session_info(void *handle)
{
    knl_dictionary_t *dc = EC_DC(handle);
    knl_session_t *new_session = EC_SESSION(handle);
    new_session->is_resident = GS_FALSE;
    new_session->is_timescale = GS_FALSE;
    if (dc->type == DICT_TYPE_TABLE && dc->handle != NULL) {
        dc_entity_t *entity = DC_ENTITY(dc);
        new_session->is_resident = entity->table.desc.is_memory;
        new_session->is_timescale = entity->table.desc.is_timescale;
        //new_session->is_timescale = GS_TRUE;
        //entity->table.desc.appendonly = GS_TRUE;
        GS_LOG_RUN_INF("gstor_open_table tablename=%s, resident=%d, is_timescale=%d,entity->table.desc.appendonly = %d \n",
                       entity->table.desc.name, new_session->is_resident, new_session->is_timescale, entity->table.desc.appendonly);
    }
    return 0;
}

int gstor_open_table(void *handle, const char *table_name)
{
    knl_session_t *session = EC_SESSION(handle);
    instance_t *cc_instance = session->kernel->server;
    knl_dictionary_t *dc = EC_DC(handle);
    knl_close_dc(dc);
    if (gstor_open_kv_table(handle, table_name, dc) != GS_SUCCESS) {
        // if (cm_get_error_code() == ERR_TABLE_OR_VIEW_NOT_EXIST) {
            cm_reset_error();
            status_t ret = gstor_create_table(handle, table_name);
            if (ret != GS_SUCCESS) {
                GS_LOG_RUN_ERR("create table failed, error code %d", cm_get_error_code());
                return GS_ERROR;
            }

            if (gstor_open_kv_table(handle, table_name, dc) == GS_SUCCESS) {
                gstor_set_session_info(handle);
                return GS_SUCCESS;
            }
            return GS_ERROR;
        // }
    }

    gstor_set_session_info(handle);
    return GS_SUCCESS;
}

int gstor_open_mem_table(void *handle, const char *table_name)
{
    knl_dictionary_t *dc = EC_DC(handle);
    knl_close_dc(dc);
    if (gstor_open_kv_table(handle, table_name, dc) != GS_SUCCESS) {
        if (cm_get_error_code() == ERR_TABLE_OR_VIEW_NOT_EXIST) {
            cm_reset_error();
            status_t ret = gstor_create_mem_table(handle, table_name);
            if (ret != GS_SUCCESS) {
                GS_LOG_RUN_ERR("create table failed, error code %d", cm_get_error_code());
                return GS_ERROR;
            }

            if (gstor_open_kv_table(handle, table_name, dc) == GS_SUCCESS) {
                gstor_set_session_info(handle);
                return GS_SUCCESS;
            }
            return GS_ERROR;
        }
    }
    gstor_set_session_info(handle);
    return GS_SUCCESS;
}

void gstor_clean(void *handle)
{
    gstor_free_lob_buf(EC_LOBBUF(handle));

    knl_cleanup_session(EC_SESSION(handle));

    knl_close_cursor(EC_SESSION(handle), EC_CURSOR(handle));
    
    for(int i = 0 ; i < G_STOR_MAX_CURSOR; ++i) {
        if(EC_CURSOR_IDX(handle,i)!=NULL){
            knl_close_cursor(EC_SESSION(handle), EC_CURSOR_IDX(handle,i));
        }
    }

}

void gstor_free(void *handle)
{
    if (!handle) {
        return;
    }

    knl_close_dc(EC_DC(handle));

    gstor_free_lob_buf(EC_LOBBUF(handle));

    knl_free_session(EC_SESSION(handle));

    CM_FREE_PTR(EC_CURSOR(handle));

    for(int i = 0 ; i < G_STOR_MAX_CURSOR; ++i) {
        CM_FREE_PTR(EC_CURSOR_IDX(handle,i));
    }

    CM_FREE_PTR(handle);
}

static inline bool32 gstor_check_db(knl_session_t *session)
{
    text_t ctrlfiles;
    bool32 is_found = GS_FALSE;
    bool32 db_exists = GS_FALSE;

    // log_param_t *log_param = cm_log_param_instance();
    // uint32 log_level = log_param->log_level;

    // close log for db check
    // log_param->log_level = 0;
    db_exists = (db_check(session, &ctrlfiles, &is_found) == GS_SUCCESS && is_found);
    // log_param->log_level = log_level;
    cm_reset_error();
    return db_exists;
}

static status_t gstor_try_build_sys_tables(knl_handle_t handle, const char *file_name, bool8 is_necessary)
{
    instance_t *cc_instance = ((knl_session_t *)handle)->kernel->server;
    if (!cc_instance->sys_defined) {
        GS_RETURN_IFERR(knl_build_sys_objects(handle));
        cc_instance->sys_defined = GS_TRUE;
    }
    return GS_SUCCESS;
}

static void rsrc_accumate_io(knl_handle_t sess, io_type_t type) {}

static void sql_pool_recycle_all() {}

static bool32 knl_have_ssl(void)
{
    return GS_FALSE;
}

static void clean_open_cursors(knl_handle_t sess, uint64 lsn) {}

static void clean_open_temp_cursors(knl_handle_t sess, void *temp_cache) {}

static status_t return_callback(knl_handle_t sess)
{
    return GS_SUCCESS;
}

static void void_callback(knl_handle_t sess) {}

static inline void knl_init_mtrl_vmc(knl_handle_t sess, handle_t *mtrl)
{
    instance_t *cc_instance = ((knl_session_t *)sess)->kernel->server;
    mtrl_context_t *ctx = (mtrl_context_t *)mtrl;
    vmc_init(&cc_instance->vmp, &ctx->vmc);
}

static void gstor_set_callback(void)
{
    g_knl_callback.alloc_rm = knl_alloc_rm;
    g_knl_callback.release_rm = knl_release_rm;
    g_knl_callback.alloc_auton_rm = knl_alloc_auton_rm;
    g_knl_callback.release_auton_rm = knl_release_auton_rm;
    g_knl_callback.get_xa_xid = knl_get_xa_xid;
    g_knl_callback.add_xa_xid = knl_add_xa_xid;
    g_knl_callback.delete_xa_xid = knl_delete_xa_xid;
    g_knl_callback.attach_suspend_rm = knl_attach_suspend_rm;
    g_knl_callback.detach_suspend_rm = knl_detach_suspend_rm;
    g_knl_callback.attach_pending_rm = knl_attach_pending_rm;
    g_knl_callback.detach_pending_rm = knl_detach_pending_rm;
    g_knl_callback.shrink_xa_rms = knl_shrink_xa_rms;
    g_knl_callback.before_commit = (knl_before_commit_t)knl_clean_before_commit;
    g_knl_callback.accumate_io = rsrc_accumate_io;
    g_knl_callback.sql_pool_recycle_all = sql_pool_recycle_all;
    g_knl_callback.load_scripts = gstor_try_build_sys_tables;
    g_knl_callback.set_min_scn = void_callback;
    g_knl_callback.have_ssl = knl_have_ssl;
    g_knl_callback.invalidate_cursor = clean_open_cursors;
    g_knl_callback.pl_init = return_callback;
    g_knl_callback.init_shard_resource = return_callback;
    g_knl_callback.init_sql_maps = return_callback;
    g_knl_callback.init_resmgr = return_callback;
    g_knl_callback.init_vmc = knl_init_mtrl_vmc;
    g_knl_callback.invalidate_temp_cursor = clean_open_temp_cursors;
    g_knl_callback.parse_default_from_text = knl_parse_default_from_text;
    g_knl_callback.exec_default = knl_exec_default;
    g_knl_callback.keep_stack_variant = cm_keep_stack_variant;
}

static status_t gstor_init_db_home(instance_t *cc_instance, char *data_path)
{
    if (data_path == NULL) {
        return GS_ERROR;
    }

    char home[GS_MAX_PATH_BUFFER_SIZE];
    GS_RETURN_IFERR(realpath_file(data_path, home, GS_MAX_PATH_BUFFER_SIZE));

    if (cm_check_exist_special_char(home, (uint32)strlen(home))) {
        GS_THROW_ERROR(ERR_INVALID_DIR, home);
        return GS_ERROR;
    }
    cm_trim_home_path(home, (uint32)strlen(home));

    PRTS_RETURN_IFERR(sprintf_s(cc_instance->home, GS_MAX_PATH_BUFFER_SIZE, "%s/intarkdb", home));
    cc_instance->kernel.home = cc_instance->home;

    PRTS_RETURN_IFERR(sprintf_s(home, GS_MAX_PATH_BUFFER_SIZE, "%s/data", cc_instance->home));
    if (!cm_dir_exist(home)) {
        GS_RETURN_IFERR(cm_create_dir_ex(home));
    }
    return GS_SUCCESS;
}

static inline status_t gstor_lock_db(instance_t *cc_instance)
{
    char file_name[GS_FILE_NAME_BUFFER_SIZE] = { 0 };

    PRTS_RETURN_IFERR(snprintf_s(file_name, GS_FILE_NAME_BUFFER_SIZE, GS_FILE_NAME_BUFFER_SIZE - 1, "%s/%s",
        cc_instance->home, g_lock_file));

    if (cm_open_file(file_name, O_CREAT | O_RDWR | O_BINARY, &cc_instance->lock_fd) != GS_SUCCESS) {
        return GS_ERROR;
    }

    return cm_lock_fd(cc_instance->lock_fd);
}

static void gstor_init_default_size(knl_attr_t *attr)
{
    attr->vma_size = DEFAULT_VMA_SIZE;
    attr->large_vma_size = DEFAULT_LARGE_VMA_SIZE;
    attr->shared_area_size = DEFAULT_SHARE_AREA_SIZE;
    attr->sql_pool_factor = DEFAULT_SQL_POOL_FACTOR;
    attr->large_pool_size = DEFAULT_LARGE_POOL_SIZE;
    attr->temp_buf_size = DEFAULT_TEMP_BUF_SIZE;
    attr->temp_pool_num = DEFAULT_TEMP_POOL_NUM;
    attr->cr_pool_size = DEFAULT_CR_POOL_SIZE;
    attr->cr_pool_count = DEFAULT_CR_POOL_COUNT;
    attr->index_buf_size = DEFAULT_INDEX_BUF_SIZE;
    attr->max_rms = GS_MAX_RMS;
    attr->ckpt_interval = DEFAULT_CKPT_INTERVAL;
    attr->ckpt_io_capacity = DEFAULT_CKPT_IO_CAPACITY;
    attr->log_replay_processes = DEFAULT_LOG_REPLAY_PROCESSES;
    attr->rcy_sleep_interval = DEFAULT_RCY_SLEEP_INTERVAL;
    attr->dbwr_processes = DEFAULT_DBWR_PROCESSES;
    attr->undo_reserve_size = DEFAULT_UNDO_RESERVER_SIZE;
    attr->undo_retention_time = DEFAULT_UNDO_RETENTION_TIME;
    attr->undo_segments = DEFAULT_UNDO_SEGMENTS;
    attr->undo_active_segments = DEFAULT_UNDO_ACTIVE_SEGMENTS;
    attr->undo_auton_trans_segments = DEFAULT_UNDO_AUTON_TRANS_SEGMENTS;
    attr->tx_rollback_proc_num = DEFAULT_TX_ROLLBACK_PROC_NUM;
    attr->default_extents = DEFAULT_EXTENTS;
    attr->alg_iter = DEFAULT_ALG_ITER;
    attr->max_column_count = DEFAULT_ALG_ITER;
    attr->stats_sample_size = DEFAULT_STATS_SAMPLE_SIZE;
    attr->private_key_locks = DEFAULT_PRIVATE_KEY_LOCKS;
    attr->private_row_locks = DEFAULT_PRIVATE_ROW_LOCKS;
    attr->spc_usage_alarm_threshold = DEFAULT_SPC_USAGE_ALARM_THRESHOLD;
    attr->stats_max_buckets = DEFAULT_STATS_MAX_BUCKETS;
    attr->lob_reuse_threshold = DEFAULT_LOG_REUSE_THRESHOLD;
    attr->init_lockpool_pages = DEFAULT_INIT_LOCKPOOL_PAGES;
    attr->max_temp_tables = DEFAULT_MAX_TEMP_TABLES;
    attr->buddy_init_size = BUDDY_INIT_BLOCK_SIZE;
    attr->buddy_max_size = BUDDY_MEM_POOL_INIT_SIZE;
    attr->lgwr_head_buf_size = GS_SHARED_PAGE_SIZE;
    attr->lgwr_async_buf_size = GS_SHARED_PAGE_SIZE;
    attr->buf_iocbs_size = sizeof(buf_iocb_t) * BUF_IOCBS_MAX_NUM;
}

static status_t gstor_init_default_params(knl_attr_t* attr)
{
    gstor_init_default_size(attr);

    attr->spin_count = DEFAULT_SPIN_COUNT;
    attr->cpu_count = cm_sys_get_nprocs();
    attr->enable_double_write = GS_TRUE;
    attr->rcy_check_pcn = GS_TRUE;
    attr->ashrink_wait_time = DEFAULT_ASHRINK_WAIT_TIME;
    attr->db_block_checksum = (uint32)CKS_FULL;
    attr->db_isolevel = (uint8)ISOLATION_READ_COMMITTED;
    attr->ckpt_timeout = DEFAULT_CKPT_TIMEOUT;
    attr->page_clean_period = DEFAULT_PAGE_CLEAN_PERIOD;
    attr->enable_OSYNC = GS_TRUE;
    attr->enable_logdirectIO = GS_TRUE;
    attr->enable_fdatasync = GS_FALSE;
    attr->undo_auto_shrink = GS_TRUE;
    attr->repl_wait_timeout = DEFAULT_REPL_WAIT_TIMEOUT;
    attr->restore_check_version = GS_TRUE;
    attr->nbu_backup_timeout = DEFAULT_NBU_BACKUP_TIMEOUT;
    attr->check_sysdata_version = GS_TRUE;
    attr->xa_suspend_timeout = DEFAULT_XA_SUSPEND_TIMEOUT;
    attr->build_keep_alive_timeout = DEFAULT_BUILD_KEEP_ALIVE_TIMEOUT;
    attr->enable_upper_case_names = GS_TRUE;
    attr->recyclebin = GS_TRUE;
    attr->alg_iter = DEFAULT_ALG_ITER;
    attr->enable_idx_key_len_check = GS_TRUE;
    attr->initrans = DEFAULT_INITTRANS;
    attr->cr_mode = CR_PAGE;
    attr->idx_auto_recycle = GS_TRUE;
    attr->lsnd_wait_time = DEFAULT_LSND_WAIT_TIME;
    attr->ddl_lock_timeout = DEFAULT_DDL_LOCK_TIMEOUT;
    attr->lock_wait_timeout = DEFAULT_LOCK_WAIT_TIMEOUT;
    attr->enable_auto_inherit_role = GS_TRUE;
    attr->max_conn_num = DEFAULT_MAX_CONN_NUM;

    attr->systime_inc_threshold = (int64)DAY2SECONDS(FIX_NUM_DAYS_YEAR);
    attr->enable_degrade_search = GS_TRUE;
    attr->delay_cleanout = GS_TRUE;
    attr->ctrllog_backup_level = CTRLLOG_BACKUP_LEVEL_FULL;
    attr->max_sql_engine_memory = GS_SQL_ENGINE_MEMORY_SIZE;
    attr->synchronous_commit = GS_TRUE;
    attr->dbwr_fsync_timeout = DEFAULT_DBWR_FSYNC_TIMEOUT;
    attr->isolation_level = DEFAULT_ISOLATION_LEVEL;
    attr->timer = g_timer();
    PRTS_RETURN_IFERR(sprintf_s(attr->pwd_alg, GS_NAME_BUFFER_SIZE, "%s", "PBKDF2"));
    return GS_SUCCESS;
}

static status_t gstor_init_runtime_params(instance_t *cc_instance)
{
    knl_attr_t *attr = &cc_instance->kernel.attr;

    attr->config = cc_instance->cc_config;
    uint32 page_size = attr->page_size;
    attr->max_row_size = page_size - 256;
    /* the max value of page_size is 32768 and GS_PLOG_PAGES is 7 */
    attr->plog_buf_size = page_size * GS_PLOG_PAGES;
    attr->cursor_size = (uint32)(sizeof(knl_cursor_t) + page_size * 2 + attr->max_column_count * sizeof(uint16) * 2);
    /* the min value of inst->attr.max_map_nodes is 8192 */
    attr->max_map_nodes = (page_size - sizeof(map_page_t) - sizeof(page_tail_t)) / sizeof(map_node_t);
    attr->xpurpose_buf = cm_aligned_buf(cc_instance->xpurpose_buf);

    attr->dbwr_buf_size = (uint64)GS_CKPT_GROUP_SIZE * attr->page_size;
    attr->lgwr_cipher_buf_size = attr->log_buf_size / 2 + sizeof(cipher_ctrl_t);
    attr->lgwr_cipher_buf_size = CM_CALC_ALIGN(attr->lgwr_cipher_buf_size, SIZE_K(4));

    attr->lgwr_buf_size = attr->lgwr_cipher_buf_size;
    attr->tran_buf_size = knl_txn_buffer_size(attr->page_size, attr->undo_segments);

    char *conf_control_files = cm_get_config_value(cc_instance->cc_config, "CONTROL_FILES");
    char control_files[GS_MAX_CONFIG_LINE_SIZE];
    if (conf_control_files != NULL && strlen(conf_control_files) > 0) {
        PRTS_RETURN_IFERR(sprintf_s(control_files, GS_MAX_CONFIG_LINE_SIZE, conf_control_files, cc_instance->home,
            cc_instance->home, cc_instance->home));
    } else {
        PRTS_RETURN_IFERR(sprintf_s(control_files, GS_MAX_CONFIG_LINE_SIZE,
            "(%s/data/ctrl1,%s/data/ctrl2,%s/data/ctrl3)", cc_instance->home, cc_instance->home, cc_instance->home));
    }
    return cm_alter_config(cc_instance->cc_config, "CONTROL_FILES", control_files, CONFIG_SCOPE_MEMORY, GS_TRUE);
}

static status_t load_tscagg_switch_param(instance_t *cc_instance, bool32 *enable_ts_cagg)
{
    char *value = cm_get_config_value(cc_instance->cc_config, "TS_CAGG_SWITCH_ITEM");

    if (cm_str_equal_ins(value, "TRUE")) {
        *enable_ts_cagg = GS_TRUE;
    } else if (cm_str_equal_ins(value, "FALSE")) {
        *enable_ts_cagg = GS_FALSE;
    } else {
        GS_THROW_ERROR(ERR_INVALID_PARAMETER, "TS_CAGG_SWITCH_ITEM");
        return GS_ERROR;
    }

    return GS_SUCCESS;
}

static status_t load_ts_update_switch_param(instance_t *cc_instance, bool32 *enable_ts_update_support)
{
    char *value = cm_get_config_value(cc_instance->cc_config, "TS_UPDATE_SUPPORT");

    if (cm_str_equal_ins(value, "TRUE")) {
        *enable_ts_update_support = GS_TRUE;
    } else if (cm_str_equal_ins(value, "FALSE")) {
        *enable_ts_update_support = GS_FALSE;
    } else {
        GS_THROW_ERROR(ERR_INVALID_PARAMETER, "TS_UPDATE_SUPPORT");
        return GS_ERROR;
    }

    return GS_SUCCESS;
}

/************************ 
    date: 20241022   
    author: 吴锦锋    
    desc: 增加是否在启动遇到日志损坏时忽略损坏文件修复的配置，这是为了兜底保证数据库文件在日志损坏后还能打开
    modify:  20241101 吴锦锋  改成统一的可以加载bool32的函数 
********************/
static status_t load_bool32_param(char* tag, instance_t *cc_instance, bool32 *ignore_corrupted_logs)
{
    char *value = cm_get_config_value(cc_instance->cc_config, tag);

    if (cm_str_equal_ins(value, "TRUE")) {
        *ignore_corrupted_logs = GS_TRUE;
    } else if (cm_str_equal_ins(value, "FALSE")) {
        *ignore_corrupted_logs = GS_FALSE;
    } else {
        GS_THROW_ERROR(ERR_INVALID_PARAMETER, tag);
        return GS_ERROR;
    }

    return GS_SUCCESS;
}

static status_t gstor_load_param_config(instance_t *cc_instance)
{
    knl_attr_t *attr = &cc_instance->kernel.attr;

    GS_RETURN_IFERR(knl_param_get_size_uint64(cc_instance->cc_config, "DATA_BUFFER_SIZE", &attr->data_buf_size));
    if (attr->data_buf_size < GS_MIN_DATA_BUFFER_SIZE) {
        GS_THROW_ERROR(ERR_PARAMETER_TOO_SMALL, "DATA_BUFFER_SIZE", GS_MIN_DATA_BUFFER_SIZE);
        return GS_ERROR;
    }
    GS_RETURN_IFERR(knl_param_get_uint32(cc_instance->cc_config, "BUF_POOL_NUM", &attr->buf_pool_num));
    if (attr->buf_pool_num > GS_MAX_BUF_POOL_NUM || attr->buf_pool_num <= 0) {
        GS_THROW_ERROR(ERR_PARAMETER_OVER_RANGE, "BUF_POOL_NUM", (int64)1, (int64)GS_MAX_BUF_POOL_NUM);
        return GS_ERROR;
    }
    GS_RETURN_IFERR(knl_param_get_size_uint64(cc_instance->cc_config, "LOG_BUFFER_SIZE", &attr->log_buf_size));
    if (attr->log_buf_size < GS_MIN_LOG_BUFFER_SIZE || attr->log_buf_size > GS_MAX_LOG_BUFFER_SIZE) {
        GS_THROW_ERROR(ERR_PARAMETER_OVER_RANGE, "LOG_BUFFER_SIZE", GS_MIN_LOG_BUFFER_SIZE, GS_MAX_LOG_BUFFER_SIZE);
        return GS_ERROR;
    }
    GS_RETURN_IFERR(knl_param_get_uint32(cc_instance->cc_config, "LOG_BUFFER_COUNT", &attr->log_buf_count));
    if (!(attr->log_buf_count > 0 && attr->log_buf_count <= GS_MAX_LOG_BUFFERS)) {
        GS_THROW_ERROR(ERR_INVALID_PARAMETER, "LOG_BUFFER_COUNT");
        return GS_ERROR;
    }
    GS_RETURN_IFERR(knl_param_get_size_uint32(cc_instance->cc_config, "PAGE_SIZE", &attr->page_size));
    if (!(attr->page_size == 8192 || attr->page_size == 16384 || attr->page_size == 32768)) {
        GS_THROW_ERROR(ERR_INVALID_PARAMETER, "PAGE_SIZE");
        return GS_ERROR;
    }
    GS_RETURN_IFERR(knl_param_get_size_uint64(cc_instance->cc_config, "SPACE_SIZE", &cc_instance->attr.space_size));
    if (cc_instance->attr.space_size < SIZE_M(16)) {
        GS_THROW_ERROR(ERR_INVALID_PARAMETER, "SPACE_SIZE");
        return GS_ERROR;
    }
    GS_RETURN_IFERR(knl_param_get_size_uint64(cc_instance->cc_config, "REDO_SPACE_SIZE", &cc_instance->attr.redo_space_size));
    if (cc_instance->attr.redo_space_size < SIZE_M(2)) {
        GS_THROW_ERROR(ERR_INVALID_PARAMETER, "REDO_SPACE_SIZE");
        return GS_ERROR;
    }
    GS_RETURN_IFERR(knl_param_get_size_uint64(cc_instance->cc_config, "USER_SPACE_SIZE", &cc_instance->attr.user_space_size));
    if (cc_instance->attr.user_space_size < SIZE_M(2)) {
        GS_THROW_ERROR(ERR_INVALID_PARAMETER, "USER_SPACE_SIZE");
        return GS_ERROR;
    }
    GS_RETURN_IFERR(knl_param_get_size_uint64(cc_instance->cc_config, "UNDO_SPACE_SIZE", &cc_instance->attr.undo_space_size));
    if (cc_instance->attr.undo_space_size < SIZE_M(2)) {
        GS_THROW_ERROR(ERR_INVALID_PARAMETER, "UNDO_SPACE_SIZE");
        return GS_ERROR;
    }
    GS_RETURN_IFERR(knl_param_get_size_uint64(cc_instance->cc_config, "SWAP_SPACE_SIZE", &cc_instance->attr.swap_space_size));
    if (cc_instance->attr.swap_space_size < SIZE_M(1)) {
        GS_THROW_ERROR(ERR_INVALID_PARAMETER, "SWAP_SPACE_SIZE");
        return GS_ERROR;
    }
    GS_RETURN_IFERR(knl_param_get_size_uint64(cc_instance->cc_config, "SYSTEM_SPACE_SIZE", &cc_instance->attr.system_space_size));
    if (cc_instance->attr.system_space_size < SIZE_M(2)) {
        GS_THROW_ERROR(ERR_INVALID_PARAMETER, "SYSTEM_SPACE_SIZE");
        return GS_ERROR;
    }
    GS_RETURN_IFERR(knl_param_get_size_uint32(cc_instance->cc_config, "STACK_SIZE", &cc_instance->attr.stack_size));

    GS_RETURN_IFERR(knl_param_get_uint32(cc_instance->cc_config, "CTRL_LOG_BACK_LEVEL", &attr->ctrllog_backup_level));
    GS_RETURN_IFERR(knl_param_get_uint32(cc_instance->cc_config, "ARCHIVE_MODE", &attr->arch_mode));
    if (attr->arch_mode !=0 && attr->arch_mode != 1) {
        GS_THROW_ERROR(ERR_PARAMETER_OVER_RANGE, "ARCHIVE_MODE", (int64)ARCHIVE_LOG_OFF, (int64)ARCHIVE_LOG_ON);
        return GS_ERROR;
    }
    GS_RETURN_IFERR(knl_param_get_size_uint64(cc_instance->cc_config, "MAX_ARCH_FILES_SIZE", &attr->max_arch_files_size));
    if (attr->max_arch_files_size > cc_instance->attr.redo_space_size * GS_MAX_ARCH_NUM) {
        GS_THROW_ERROR(ERR_PARAMETER_OVER_RANGE, "MAX_ARCH_FILES_SIZE", (int64)0, (int64)cc_instance->attr.redo_space_size * GS_MAX_ARCH_NUM);
        return GS_ERROR;
    }
    GS_RETURN_IFERR(knl_param_get_uint32(cc_instance->cc_config, "REDO_SAVE_TIME", &attr->redo_save_time));
    GS_LOG_RUN_INF("arch_mode = %d, redo_save_time = %d, ctrllog_backup_level = %d\n", attr->arch_mode,
        attr->redo_save_time, attr->ctrllog_backup_level);

    GS_RETURN_IFERR(knl_param_get_size_uint64(cc_instance->cc_config, "VMA_SIZE", &attr->vma_size));
    GS_RETURN_IFERR(knl_param_get_size_uint64(cc_instance->cc_config, "LARGE_VMA_SIZE", &attr->large_vma_size));
    GS_RETURN_IFERR(knl_param_get_size_uint64(cc_instance->cc_config, "SHARED_AREA_SIZE", &attr->shared_area_size));
    GS_RETURN_IFERR(knl_param_get_size_uint64(cc_instance->cc_config, "LARGE_POOL_SIZE", &attr->large_pool_size));
    GS_RETURN_IFERR(knl_param_get_size_uint64(cc_instance->cc_config, "TEMP_BUF_SIZE", &attr->temp_buf_size));
    GS_RETURN_IFERR(knl_param_get_uint32(cc_instance->cc_config, "TEMP_POOL_NUM", &attr->temp_pool_num));
    GS_RETURN_IFERR(knl_param_get_size_uint64(cc_instance->cc_config, "CR_POOL_SIZE", &attr->cr_pool_size));
    GS_RETURN_IFERR(knl_param_get_uint32(cc_instance->cc_config, "CR_POOL_COUNT", &attr->cr_pool_count));
    GS_RETURN_IFERR(knl_param_get_size_uint64(cc_instance->cc_config, "INDEX_BUF_SIZE", &attr->index_buf_size));
    GS_RETURN_IFERR(knl_param_get_uint32(cc_instance->cc_config, "CKPT_INTERVAL", &attr->ckpt_interval));
    GS_RETURN_IFERR(knl_param_get_uint32(cc_instance->cc_config, "CKPT_IO_CAPACITY", &attr->ckpt_io_capacity));
    GS_RETURN_IFERR(knl_param_get_uint32(cc_instance->cc_config, "LOG_REPLAY_PROCESSES", &attr->log_replay_processes));
    GS_RETURN_IFERR(knl_param_get_uint32(cc_instance->cc_config, "RCY_SLEEP_INTERVAL", &attr->rcy_sleep_interval));
    GS_RETURN_IFERR(knl_param_get_uint32(cc_instance->cc_config, "DBWR_PROCESSES", &attr->dbwr_processes));
    if (attr->dbwr_processes < 1 || attr->dbwr_processes > GS_MAX_DBWR_PROCESS) {
        GS_THROW_ERROR(ERR_PARAMETER_OVER_RANGE, "DBWR_PROCESSES", (int64)1, (int64)GS_MAX_DBWR_PROCESS);
        return GS_ERROR;
    }
    GS_RETURN_IFERR(knl_param_get_uint32(cc_instance->cc_config, "UNDO_RESERVE_SIZE", &attr->undo_reserve_size));
    GS_RETURN_IFERR(knl_param_get_uint32(cc_instance->cc_config, "UNDO_RETENTION_TIME", &attr->undo_retention_time));
    // [20s,3600s]
    if (attr->undo_retention_time < 20 || attr->undo_retention_time > 3600) {
        GS_THROW_ERROR(ERR_PARAMETER_OVER_RANGE, "UNDO_RETENTION_TIME", (int64)20, (int64)3600);
        return GS_ERROR;
    }
    GS_RETURN_IFERR(knl_param_get_uint32(cc_instance->cc_config, "UNDO_SEGMENTS", &attr->undo_segments));
    GS_RETURN_IFERR(knl_param_get_uint32(cc_instance->cc_config, "TX_ROLLBACK_PROC_NUM", &attr->tx_rollback_proc_num));
    GS_RETURN_IFERR(knl_param_get_uint32(cc_instance->cc_config, "MAX_COLUMN_COUNT", &attr->max_column_count));
    GS_RETURN_IFERR(knl_param_get_uint32(cc_instance->cc_config, "MAX_TEMP_TABLES", &attr->max_temp_tables));
    GS_RETURN_IFERR(knl_param_get_uint32(cc_instance->cc_config, "INIT_LOCKPOOL_PAGES", &attr->init_lockpool_pages));
    GS_RETURN_IFERR(knl_param_get_uint32(cc_instance->cc_config, "DEFAULT_EXTENTS", &attr->default_extents));
    GS_RETURN_IFERR(knl_param_get_uint32(cc_instance->cc_config, "EXEC_AGG_THREAD_NUM", &attr->exec_agg_thread_num));
    GS_RETURN_IFERR(knl_param_get_uint32(cc_instance->cc_config, "LOCK_WAIT_TIMEOUT", &attr->lock_wait_timeout));
    GS_RETURN_IFERR(knl_param_get_size_uint64(cc_instance->cc_config, "SQL_ENGINE_MEMORY_SIZE", &attr->max_sql_engine_memory));
    // [1000ms,1000000ms] || 0
    if (attr->lock_wait_timeout != 0 && 
        (attr->lock_wait_timeout < (int32)GS_MIN_LOCK_WAIT_TIMEOUT 
        || attr->lock_wait_timeout > (int32)GS_MAX_LOCK_WAIT_TIMEOUT)) {
        GS_THROW_ERROR(ERR_PARAMETER_OVER_RANGE, "LOCK_WAIT_TIMEOUT", (int64)GS_MIN_LOCK_WAIT_TIMEOUT, (int64)GS_MAX_LOCK_WAIT_TIMEOUT);
        return GS_ERROR;
    }
    GS_RETURN_IFERR(knl_param_get_uint32(cc_instance->cc_config, "MAX_CONN_NUM", &attr->max_conn_num));
    // [1,4096]
    if (attr->max_conn_num < (int32)GS_MIN_CONN_NUM || attr->max_conn_num > (int32)GS_MAX_CONN_NUM) {
        GS_THROW_ERROR(ERR_PARAMETER_OVER_RANGE, "MAX_CONN_NUM", (int64)GS_MIN_CONN_NUM, (int64)GS_MAX_CONN_NUM);
        return GS_ERROR;
    }
    if (load_ts_update_switch_param(cc_instance, &attr->enable_ts_update) != GS_SUCCESS) {
        return GS_ERROR;
    }
    // 20241210吴锦锋：增加
    if (load_bool32_param("ENFORCED_IGNORE_ALL_REDO_LOGS", cc_instance, &attr->enforced_ignore_all_redo_logs) != GS_SUCCESS) {
        return GS_ERROR;
    }
    // 20241022吴锦锋：增加读取是否忽略损坏文件的配置
    if (load_bool32_param("IGNORE_CORRUPTED_LOGS", cc_instance, &attr->ignore_corrupted_logs) != GS_SUCCESS) {
        return GS_ERROR;
    }
    // 20241101吴锦锋：增加读取是否忽略损坏undo文件的配置
    if (load_bool32_param("SMON_LOOP_IN_IGNORE_MODE", cc_instance, &attr->smon_loop_in_ignore_mode) != GS_SUCCESS) {
        return GS_ERROR;
    }

    char *sys_pwd_tmp = cm_get_config_value(cc_instance->cc_config, "SYS_USER_PASSWORD");
    size_t pwd_len = strlen(sys_pwd_tmp) /2 ;
    for (size_t i = 0; i < pwd_len; i++) {
        char c = 0;
        sscanf(&sys_pwd_tmp[2 * i], "%2hhx", &c);
        attr->sys_pwd[i] = c;
        //GS_LOG_DEBUG_INF("param :%d %02x", i, attr->sys_pwd[i] & 0xFF);
    }
    attr->sys_pwd[pwd_len] = '\0';

    // synchronous_commit
    char *synchronous_commit = cm_get_config_value(cc_instance->cc_config, "SYNCHRONOUS_COMMIT");
    char sync_commit[4] = {};
    memcpy_s(sync_commit, 4, synchronous_commit, strlen(synchronous_commit));
    cm_str_lower(sync_commit);
    if (cm_str_equal(sync_commit, "off")) {
        attr->synchronous_commit = GS_FALSE;
    } else if (cm_str_equal(sync_commit, "on")) {
        attr->synchronous_commit = GS_TRUE;
    } else {
        GS_THROW_ERROR(ERR_INVALID_PARAMETER, "SYNCHRONOUS_COMMIT");
        return GS_ERROR;
    }

    // isolation_level
    GS_RETURN_IFERR(knl_param_get_uint32(cc_instance->cc_config, "ISOLATION_LEVEL", &attr->isolation_level));
    if (attr->isolation_level != ISOLATION_READ_COMMITTED && attr->isolation_level != ISOLATION_CURR_COMMITTED) {
        GS_THROW_ERROR(ERR_PARAMETER_OVER_RANGE, "ISOLATION_LEVEL", (int64)ISOLATION_READ_COMMITTED, (int64)ISOLATION_CURR_COMMITTED);
        return GS_ERROR;
    }

    return GS_SUCCESS;
}

static inline void gstor_check_file_errno(instance_t *cc_instance)
{
    if (errno == EMFILE || errno == ENFILE) {
        GS_LOG_ALARM(WARN_FILEDESC, "'instance-name':'%s'}", cc_instance->kernel.instance_name);
    }
}

static status_t gstor_init_loggers(instance_t *cc_instance)
{
    char file_name[GS_FILE_NAME_BUFFER_SIZE] = { '\0' };
    cm_log_allinit();

    log_param_t *log_param = cm_log_param_instance();

    PRTS_RETURN_IFERR(
        snprintf_s(log_param->log_home, GS_MAX_PATH_BUFFER_SIZE, GS_MAX_PATH_LEN, "%s/log", cc_instance->home));

    MEMS_RETURN_IFERR(strcpy_sp(log_param->instance_name, GS_MAX_NAME_LEN, cc_instance->kernel.instance_name));

    log_param->log_backup_file_count = 10;
              
    GS_RETURN_IFERR(knl_param_get_size_uint32(cc_instance->cc_config, "LOG_LEVEL", &(log_param->log_level)));
    GS_RETURN_IFERR(knl_param_get_size_uint32(cc_instance->cc_config, "LOG_FILE_COUNT", 
                                              &(log_param->log_backup_file_count)));
    GS_RETURN_IFERR(knl_param_get_size_uint32(cc_instance->cc_config, "MAX_LOG_FILE_SIZE", 
                                              &(log_param->max_log_file_size)));

    if (log_param->log_level < 0 || log_param->log_level > (int64)LOG_ODBC_INF_LEVEL) {
        GS_THROW_ERROR(ERR_PARAMETER_OVER_RANGE, "LOG_LEVEL", (int64)0, (int64)LOG_ODBC_INF_LEVEL);
        return GS_ERROR;
    }
    cm_log_set_file_permissions(600);
    cm_log_set_path_permissions(700);

    // RUN
    PRTS_RETURN_IFERR(snprintf_s(file_name, GS_FILE_NAME_BUFFER_SIZE, GS_MAX_FILE_NAME_LEN, "%s/run/%s",
        log_param->log_home, "intarkdb.rlog"));
    cm_log_init(LOG_RUN, file_name);

    // DEBUG
    PRTS_RETURN_IFERR(snprintf_s(file_name, GS_FILE_NAME_BUFFER_SIZE, GS_MAX_FILE_NAME_LEN, "%s/debug/%s",
        log_param->log_home, "intarkdb.dlog"));
    cm_log_init(LOG_DEBUG, file_name);

    // ALARM
    PRTS_RETURN_IFERR(snprintf_s(file_name, GS_FILE_NAME_BUFFER_SIZE, GS_MAX_FILE_NAME_LEN, "%s/%s_alarm.log",
        log_param->log_home, log_param->instance_name));
    cm_log_init(LOG_ALARM, file_name);

    log_file_handle_t *log_file_handle = cm_log_logger_file(LOG_ALARM);
    cm_log_open_file(log_file_handle);

    // TRACE
    PRTS_RETURN_IFERR(snprintf_s(file_name, GS_FILE_NAME_BUFFER_SIZE, GS_MAX_FILE_NAME_LEN, "%s/%s_smon_%05u.trc",
        log_param->log_home, log_param->instance_name, (uint32)SESSION_ID_SMON));

    cm_log_init(LOG_TRACE, file_name);
    log_file_handle = cm_log_logger_file(LOG_TRACE);
    cm_log_open_file(log_file_handle);

    // callback
    cm_init_error_handler(cm_set_sql_error);
    g_check_file_error = gstor_check_file_errno;

    return GS_SUCCESS;
}

static inline status_t gstor_load_params(instance_t *cc_instance, char *data_path)
{
    if (cc_instance->cc_config == NULL) {
        // read params from intarkdb.ini , save to cc_config
        GS_RETURN_IFERR(gstor_init_config(cc_instance, data_path));
    }

    if (gstor_init_default_params(&cc_instance->kernel.attr) != GS_SUCCESS) {
        return GS_ERROR;
    }
    if (gstor_load_param_config(cc_instance) != GS_SUCCESS) {
        return GS_ERROR;
    }
    if (gstor_init_runtime_params(cc_instance) != GS_SUCCESS) {
        return GS_ERROR;
    }
    if (!cc_instance->gstorini_exists && cm_save_config(cc_instance->cc_config)!=GS_SUCCESS){
        GS_LOG_RUN_ERR("save config to  intarkdb.ini failed");
        return GS_ERROR;
    }
    return gstor_init_loggers(cc_instance);
}

static status_t gstor_init_instance(instance_t** cc_instance, char *data_path)
{
    *cc_instance = (instance_t *)malloc(sizeof(instance_t));
    if (cc_instance == NULL) {
        GS_LOG_RUN_ERR("[knl_init_instance] alloc instance failed");
        return GS_ERROR;
    }

    MEMS_RETURN_IFERR(memset_s(*cc_instance, sizeof(instance_t), 0, sizeof(instance_t)));

    MEMS_RETURN_IFERR(strncpy_s((*cc_instance)->kernel.instance_name, GS_MAX_NAME_LEN, g_inst_name, strlen(g_inst_name)));

    gstor_set_callback();

    GS_RETURN_IFERR(gstor_init_db_home(*cc_instance, data_path));

    GS_RETURN_IFERR(gstor_load_params(*cc_instance, data_path));

    GS_RETURN_IFERR(knl_create_sga(*cc_instance));

    GS_RETURN_IFERR(vmp_create(&(*cc_instance)->sga.vma, 0, &(*cc_instance)->vmp));

    rm_pool_init(&(*cc_instance)->rm_pool);

    GS_RETURN_IFERR(knl_alloc_sys_sessions(*cc_instance));

    (*cc_instance)->lock_fd = -1;
    return GS_SUCCESS;
}

static inline status_t gstor_start_db(knl_instance_t *kernel)
{
    knl_session_t *session = kernel->sessions[SESSION_ID_KERNEL];

    if (gstor_check_db(session)) {
        return knl_open_sys_database(session);
    }

    return knl_create_sys_database(session, kernel->home);
}

void gstor_shutdown(void* instance)
{
    instance_t* cc_instance = (instance_t*)instance;
    cm_close_timer(g_timer());

    if (cc_instance == NULL) {
        return;
    }

    while (GS_TRUE) {
        if (cc_instance->shutdown_ctx.phase == SHUTDOWN_PHASE_DONE || cm_spin_try_lock(&cc_instance->kernel.db.lock)) {
            break;
        }
        cm_sleep(5);
        GS_LOG_RUN_INF("wait for shutdown to complete");
    }

    if (cc_instance->shutdown_ctx.phase == SHUTDOWN_PHASE_DONE) {
        return;
    }

    cc_instance->shutdown_ctx.phase = SHUTDOWN_PHASE_INPROGRESS;
    cc_instance->shutdown_ctx.mode = SHUTDOWN_MODE_ABORT;

    knl_shutdown(NULL, &cc_instance->kernel, GS_TRUE);

    rm_pool_deinit(cc_instance, &cc_instance->rm_pool);
    GS_LOG_RUN_INF("shutdown checkPoint: asn =%d, block_id=%d, rst_id =%d \n",
        cc_instance->kernel.redo_ctx.curr_point.asn, cc_instance->kernel.redo_ctx.curr_point.block_id,
        cc_instance->kernel.redo_ctx.curr_point.rst_id);
    knl_free_sys_sessions(cc_instance);

    knl_destroy_sga(cc_instance);

    cc_instance->shutdown_ctx.phase = SHUTDOWN_PHASE_DONE;
    // CM_FREE_PTR(cc_instance);

    gstor_deinit_config(cc_instance);
    CM_FREE_PTR(cc_instance);
}

int gstor_startup(void** cc_instance, char *data_path)
{
    do {
        GS_BREAK_IF_ERROR(cm_start_timer(g_timer()));
        GS_BREAK_IF_ERROR(gstor_init_instance(cc_instance, data_path));
        GS_BREAK_IF_ERROR(gstor_lock_db((instance_t*)*cc_instance));
        GS_BREAK_IF_ERROR(alck_init_ctx(&((instance_t*)*cc_instance)->kernel));
        GS_BREAK_IF_ERROR(knl_startup(&((instance_t*)*cc_instance)->kernel));
        GS_BREAK_IF_ERROR(gstor_start_db(&((instance_t*)*cc_instance)->kernel));
        // sleep 1s wait for child thread
        // cm_sleep(1000);
        cm_set_db_timezone(TIMEZONE_OFFSET_DEFAULT);
        GS_LOG_RUN_INF("gstore started successfully!");
        return GS_SUCCESS;
    } while (GS_FALSE);

    gstor_shutdown((instance_t*)*cc_instance);
    GS_LOG_RUN_INF("gstore started failed!");
    GS_LOG_RUN_INF("code:%d, errmsg:%s\n", GS_ERRNO, g_tls_error.message);
    return GS_ERROR;
}

static inline void gstor_prepare(knl_session_t *session, knl_cursor_t *cursor, lob_buf_t *lob_buf)
{
    gstor_free_lob_buf(lob_buf);
    knl_close_cursor(session, cursor);
    knl_set_session_scn(session, GS_INVALID_ID64);
}

static inline status_t gstor_open_cursor_internal(knl_session_t *session, knl_cursor_t *cursor, knl_dictionary_t *dc,
    knl_cursor_action_t action, uint32 index_slot)
{
    cursor->action = action;
    if (index_slot == GS_INVALID_ID32) {
        cursor->scan_mode = SCAN_MODE_TABLE_FULL;
    } else {
        cursor->index_slot = index_slot;
        cursor->scan_mode = SCAN_MODE_INDEX;
    }
    uint32 cache_is_insert = session->is_insert;
    session->is_insert = action == CURSOR_ACTION_INSERT ? GS_TRUE : GS_FALSE;
    knl_inc_session_ssn(session);
    status_t ret = knl_open_cursor(session, cursor, dc);
    if (ret != GS_SUCCESS) {
        session->is_insert = cache_is_insert;
    }
    return ret;
}

static inline status_t gstor_set_key(char *key, uint32 key_len, row_assist_t *ra)
{
    text_t data;
    data.str = key;
    data.len = key_len;
    return row_put_text(ra, &data);
}

static inline status_t gstor_set_value(knl_session_t *session, knl_cursor_t *cursor, knl_dictionary_t *dc, text_t *val,
    row_assist_t *ra)
{
    if (val == NULL) {
        return row_put_null(ra);
    }

    knl_column_t *column = knl_get_column(dc->handle, SYS_KV_VALUE_COL_ID);
    return knl_row_put_lob(session, cursor, column, (void *)val, ra);
}

static inline status_t gstor_insert(knl_session_t *session, knl_cursor_t *cursor, knl_dictionary_t *dc, char *key,
    uint32 key_len, char *val, uint32 val_len)
{
    GS_RETURN_IFERR(gstor_open_cursor_internal(session, cursor, dc, CURSOR_ACTION_INSERT, GS_INVALID_ID32));

    row_assist_t ra;
    uint32 column_count = knl_get_column_count(dc->handle);
    row_init(&ra, (char *)cursor->row, session->kernel->attr.max_row_size, column_count);

    // set key
    GS_RETURN_IFERR(gstor_set_key(key, key_len, &ra));

    // set value
    text_t setval = {
        .str = val,
        .len = val_len
    };
    GS_RETURN_IFERR(gstor_set_value(session, cursor, dc, &setval, &ra));

    return knl_internal_insert(session, cursor);
}

static inline status_t gstor_update_core(knl_session_t *session, knl_cursor_t *cursor, knl_dictionary_t *dc, char *val,
    uint32 val_len)
{
    row_assist_t ra;
    knl_update_info_t *ui = &cursor->update_info;

    ui->count = 1;
    ui->columns[0] = SYS_KV_VALUE_COL_ID;
    row_init(&ra, ui->data, session->kernel->attr.max_row_size, ui->count);

    text_t setval = {
        .str = val,
        .len = val_len
    };
    GS_RETURN_IFERR(gstor_set_value(session, cursor, dc, &setval, &ra));

    cm_decode_row(ui->data, ui->offsets, ui->lens, NULL);
    return knl_internal_update(session, cursor);
}

static inline status_t gstor_update(knl_session_t *session, knl_cursor_t *cursor, knl_dictionary_t *dc, char *key,
    uint32 key_len, char *val, uint32 val_len, bool32 *updated)
{
    GS_RETURN_IFERR(gstor_open_cursor_internal(session, cursor, dc, CURSOR_ACTION_UPDATE, IX_SYS_KV_01_ID));

    knl_init_index_scan(cursor, GS_TRUE);
    knl_set_scan_key(INDEX_DESC(cursor->index), &cursor->scan_range.l_key, GS_TYPE_STRING, key, key_len,
        SYS_KV_KEY_COL_ID);
    GS_RETURN_IFERR(knl_fetch(session, cursor));
    if (cursor->eof) {
        return GS_SUCCESS;
    }
    *updated = GS_TRUE;
    return gstor_update_core(session, cursor, dc, val, val_len);
}

status_t row_put_default_value(knl_session_t *session, knl_cursor_t *cursor, knl_dictionary_t *dc, row_assist_t *ra, knl_column_t *column) {
    switch(column->datatype) {
        case GS_TYPE_BOOLEAN:
            row_put_bool(ra, *((int32 *)((internal_default_value_t*)column->default_text.str)->data));
            break;
        case GS_TYPE_UTINYINT:
        case GS_TYPE_USMALLINT:
        case GS_TYPE_UINT32:
            row_put_uint32(ra, *((uint32 *)((internal_default_value_t*)column->default_text.str)->data));
            break;
        case GS_TYPE_INTEGER:
        case GS_TYPE_SMALLINT:
        case GS_TYPE_TINYINT:
            row_put_int32(ra, *((int32 *)((internal_default_value_t*)column->default_text.str)->data));
            break;
        case GS_TYPE_BIGINT:
        case GS_TYPE_UINT64:
            row_put_int64(ra, *((int64 *)((internal_default_value_t*)column->default_text.str)->data));
            break;
        case GS_TYPE_REAL:
        case GS_TYPE_FLOAT:
            row_put_real(ra, *((double *)((internal_default_value_t*)column->default_text.str)->data));
            break;
        case GS_TYPE_TIMESTAMP:
        case GS_TYPE_DATE:
            row_put_int64(ra, (date_t) cm_utc_now() - CM_UNIX_EPOCH);
            break;
        case GS_TYPE_BLOB:
        case GS_TYPE_CLOB: {
            text_t data;
            data.str = ((internal_default_value_t*)column->default_text.str)->data;
            data.len = ((internal_default_value_t*)column->default_text.str)->size;
            knl_row_put_lob(session, cursor, column, (void *)(&data), ra);
            break;
        }
        case GS_TYPE_CHAR:
        case GS_TYPE_VARCHAR:
        case GS_TYPE_STRING: {
            text_t data;
            data.str = ((internal_default_value_t*)column->default_text.str)->data;
            data.len = ((internal_default_value_t*)column->default_text.str)->size;
            row_put_text(ra, &data);
            break;
        }
        case GS_TYPE_NUMBER:
        case GS_TYPE_DECIMAL: {
            row_put_dec4(ra, (dec4_t *)((internal_default_value_t*)column->default_text.str)->data);
            break;
        }
        case GS_TYPE_BINARY:
        case GS_TYPE_RAW: {
            binary_t bin;
            bin.bytes = (uint8*)((internal_default_value_t*)column->default_text.str)->data;
            bin.is_hex_const = GS_FALSE;
            bin.size = ((internal_default_value_t*)column->default_text.str)->size;
            row_put_bin(ra, &bin);
            break;
        }
        default: {
            return GS_ERROR;
        }
    }
    return GS_SUCCESS;
}

status_t executor_insert_row(knl_session_t *session, knl_cursor_t *cursor, knl_dictionary_t *dc, const char *table_name,
    int column_count, exp_column_def_t *column_list)
{
    GS_RETURN_IFERR(gstor_open_cursor_internal(session, cursor, dc, CURSOR_ACTION_INSERT, GS_INVALID_ID32));

    row_assist_t ra;
    uint32 knl_column_count = knl_get_column_count(dc->handle);
    uint32 max_size = session->kernel->attr.max_row_size;
    row_init(&ra, (char *)cursor->row, max_size, knl_column_count);

    for (uint32 ra_i = 0; ra_i < knl_column_count; ra_i++) {
        bool32 is_bound_col = GS_FALSE;
        uint32 i = 0;
        for (; i < column_count; i++) {
            if (ra_i == column_list[i].col_slot) {
                is_bound_col = GS_TRUE;
                break;
            }
        }
        
        knl_column_t *column = knl_get_column(dc->handle, ra_i);
        if (!is_bound_col) {
            if(KNL_COLUMN_IS_SERIAL(column)){
                int64 *auto_val;
                row_put_autoincrement(session, dc, &ra, column, &auto_val);
            }
            else if(!KNL_COLUMN_IS_DEFAULT_NULL(column)){
                row_put_default_value(session, cursor, dc, &ra, column);
            }
            else{
                row_put_null(&ra);
            }
            continue;
        }

        if (!column_list[i].crud_value.str) {
            if(KNL_COLUMN_IS_SERIAL(column)){
                int64 *auto_val = 1;
                row_put_autoincrement(session, dc, &ra, column, &auto_val);
            }
            else{
                row_put_null(&ra);
            }
            continue;
        }

        switch (column_list[i].col_type) {
            case GS_TYPE_BOOLEAN:
                row_put_bool(&ra, *((uint32 *)(column_list[i].crud_value.str)));
                break;
            case GS_TYPE_UTINYINT:
            case GS_TYPE_USMALLINT:
            case GS_TYPE_UINT32:
                row_put_uint32(&ra, *((uint32 *)column_list[i].crud_value.str));
                break;
            case GS_TYPE_INTEGER:
            case GS_TYPE_SMALLINT:
            case GS_TYPE_TINYINT:
                row_put_int32(&ra, *((int32 *)column_list[i].crud_value.str));
                break;
            case GS_TYPE_BIGINT:
            case GS_TYPE_UINT64:
                row_put_int64(&ra, *((int64 *)column_list[i].crud_value.str));
                break;
            case GS_TYPE_REAL:
            case GS_TYPE_FLOAT:
                row_put_real(&ra, *((double *)column_list[i].crud_value.str));
                break;
            case GS_TYPE_TIMESTAMP:
            case GS_TYPE_DATE:
                row_put_int64(&ra, *((int64 *)column_list[i].crud_value.str));
                break;
            case GS_TYPE_BLOB:
            case GS_TYPE_CLOB: {
                knl_column_t *column = knl_get_column(dc->handle, ra_i);
                text_t data;
                data.str = column_list[i].crud_value.str;
                data.len = column_list[i].crud_value.len;
                knl_row_put_lob(session, cursor, column, (void *)(&data), &ra);
                break;
            }
            case GS_TYPE_CHAR:
            case GS_TYPE_VARCHAR:
            case GS_TYPE_STRING: {
                row_put_text(&ra, &column_list[i].crud_value);
                break;
            }
            case GS_TYPE_NUMBER:
            case GS_TYPE_DECIMAL: {
                row_put_dec4(&ra, (dec4_t *)column_list[i].crud_value.str);
                break;
            }
            case GS_TYPE_BINARY:
            case GS_TYPE_RAW: {
                binary_t bin;
                bin.bytes = (uint8*)(column_list[i].crud_value.str);
                bin.is_hex_const = GS_FALSE;
                bin.size = column_list[i].crud_value.len;
                row_put_bin(&ra, &bin);
                break;
            }
            default: {
                return GS_ERROR;
            }
        }
    }
    if (cursor->row->size < PCRH_MIN_ROW_SIZE) {
        cursor->row->size = PCRH_MIN_ROW_SIZE;
    }
    status_t ret_status = knl_internal_insert(session, cursor);
    session->is_insert = GS_FALSE;
    return ret_status;
}

status_t gstor_executor_insert_row(void *handle, const char *table_name, int column_count,
    exp_column_def_t *column_list)
{
    cm_reset_error();
    knl_cursor_t *cursor = EC_CURSOR(handle);
    knl_session_t *session = EC_SESSION(handle);
    knl_dictionary_t *dc = EC_DC(handle);

    gstor_prepare(session, cursor, EC_LOBBUF(handle));

    for (;;) {
        cm_set_ignore_log(GS_TRUE);
        if (executor_insert_row(session, cursor, dc, table_name, column_count, column_list) == GS_SUCCESS) {
            cm_set_ignore_log(GS_FALSE);
            return GS_SUCCESS;
        }
        cm_set_ignore_log(GS_FALSE);
        if (GS_ERRNO != ERR_DUPLICATE_KEY) {
            return GS_ERROR;
        } else {
            return GS_ERROR;
        }
    }
}

status_t row_put_autoincrement(knl_session_t *session, knl_dictionary_t *dc, row_assist_t *ra, knl_column_t *column,
    int64 *auto_val) {
    // 获取自增列的值
    int64_t val = 0;
    status_t ret = knl_get_serial_value(session, DC_ENTITY(dc),&val); 
    if( ret != GS_SUCCESS) {
        return ret;
    }
    *auto_val = val;
    switch(column->datatype) {
        case GS_TYPE_BIGINT:
            row_put_int64(ra, val);
            break;
        case GS_TYPE_INTEGER:
            if( val > INT32_MAX) {
                GS_LOG_RUN_ERR("autoincrement value out of range!! val = %ld \n", val);
                return GS_ERROR;
            }
            int32_t val32 = (int32_t)val;
            row_put_int32(ra, val32);
            break;
        case GS_TYPE_SMALLINT:
            if( val > INT16_MAX) {
                GS_LOG_RUN_ERR("autoincrement value out of range!! val = %ld \n", val);
                return GS_ERROR;
            }
            int32_t smallint = (int32_t)val; // smallint在内部存储时，实际上是int32
            row_put_int32(ra, smallint);
            break;
        case GS_TYPE_TINYINT:
            if( val > INT8_MAX) {
                GS_LOG_RUN_ERR("autoincrement value out of range!! val = %ld \n", val);
                return GS_ERROR;
            }
            int32_t tinyint = (int32_t)val; // tinyint在内部存储时，实际上是int32
            row_put_int32(ra, tinyint);
            break;
        case GS_TYPE_UINT64:
            if( val < 0) {
                GS_LOG_RUN_ERR("autoincrement value out of range!! val = %ld \n", val);
                return GS_ERROR;
            }
            row_put_int64(ra, val);
            break;
        case GS_TYPE_UINT32:
            if( val < 0 || val > UINT32_MAX) {
                GS_LOG_RUN_ERR("autoincrement value out of range!! val = %ld \n", val);
                return GS_ERROR;
            }
            uint32_t uint32_val = (uint32_t)val;
            row_put_uint32(ra, uint32_val);
            break;
        case GS_TYPE_USMALLINT:
            if( val < 0 || val > UINT16_MAX) {
                GS_LOG_RUN_ERR("autoincrement value out of range!! val = %ld \n", val);
                return GS_ERROR;
            }
            uint32_t usmallint = (uint32_t)val; // usmallint在内部存储时，实际上是uint32
            row_put_uint32(ra, usmallint);
            break;
        case GS_TYPE_UTINYINT:
            if( val < 0 || val > UINT8_MAX) {
                GS_LOG_RUN_ERR("autoincrement value out of range!! val = %ld \n", val);
                return GS_ERROR;
            }
            uint32_t utinyint = (uint32_t)val; // utinyint在内部存储时，实际上是uint32
            row_put_uint32(ra, utinyint);
            break;
        default:
            GS_LOG_RUN_ERR("autoincrement value type error!! type = %d \n", column->datatype);
            return GS_ERROR;
    }
    return GS_SUCCESS;
}

status_t gstor_autoincrement_nextval(void *handle, uint32 slot, int64_t *nextval) {
    knl_session_t *session = EC_SESSION(handle);
    knl_dictionary_t *dc = EC_DC(handle);

    knl_column_t *column = knl_get_column(dc->handle, slot);
    if (KNL_COLUMN_IS_SERIAL(column)) {
        return knl_get_serial_value(session, DC_ENTITY(dc), nextval);
    }
    return GS_ERROR;
}

status_t gstor_autoincrement_updateval(void *handle, uint32 slot, int64_t nextval) {
    knl_session_t *session = EC_SESSION(handle);
    knl_dictionary_t *dc = EC_DC(handle);

    knl_column_t *column = knl_get_column(dc->handle, slot);
    if (KNL_COLUMN_IS_SERIAL(column)) {
        return knl_update_serial_value(session, DC_ENTITY(dc), nextval);
    }
    return GS_ERROR;
}

status_t executor_batch_insert(knl_session_t* session, knl_cursor_t* cursor,knl_dictionary_t* dc,
                            const char* table_name, int row_count, res_row_def_t *row_list,
                            uint32 part_no, bool32 is_ignore) {
    cursor->part_loc.part_no = part_no;
    GS_RETURN_IFERR(gstor_open_cursor_internal(session, cursor, dc, CURSOR_ACTION_INSERT, GS_INVALID_ID32));

    cursor->rowid_count = row_count;
    row_head_t *row_addr = cursor->row;
    uint32 knl_column_count = knl_get_column_count(dc->handle);
    uint32 max_size = session->kernel->attr.max_row_size;
    int column_count = row_list[0].column_count;
    int use_row_size = 0;
    for (uint32 row_i = 0; row_i < row_count; row_i++) {
        row_assist_t ra;
        row_init(&ra, (char *)cursor->row, max_size, knl_column_count);

        for (uint32 ra_i = 0; ra_i < knl_column_count; ra_i++) {
            bool32 is_bound_col = GS_FALSE;
            uint32 i = 0;
            for (; i < column_count; i++) { // TODO: 优化，避免每次都遍历
                if (ra_i == row_list[row_i].row_column_list[i].col_slot) {
                    is_bound_col = GS_TRUE;
                    break;
                }
            }

            if (!is_bound_col) {
                knl_column_t *column = knl_get_column(dc->handle, ra_i);
                if(KNL_COLUMN_IS_SERIAL(column)){
                    int64 *auto_val;
                    row_put_autoincrement(session, dc, &ra, column, &auto_val);
                }
                else if(!KNL_COLUMN_IS_DEFAULT_NULL(column)){
                    row_put_default_value(session, cursor, dc, &ra, column);
                }
                else{
                    row_put_null(&ra);
                }
                continue;
            }

            if (!row_list[row_i].row_column_list[i].crud_value.str) {
                row_put_null(&ra);
                continue;
            }

            switch (row_list[row_i].row_column_list[i].col_type) {
                case GS_TYPE_BOOLEAN:
                    row_put_bool(&ra, *((uint32 *)(row_list[row_i].row_column_list[i].crud_value.str)));
                    break;
                case GS_TYPE_UTINYINT:
                case GS_TYPE_USMALLINT:
                case GS_TYPE_UINT32:
                    row_put_uint32(&ra, *((uint32 *)row_list[row_i].row_column_list[i].crud_value.str));
                    break;
                case GS_TYPE_INTEGER:
                case GS_TYPE_SMALLINT:
                case GS_TYPE_TINYINT:
                    row_put_int32(&ra, *((int32 *)row_list[row_i].row_column_list[i].crud_value.str));
                    break;
                case GS_TYPE_BIGINT:
                case GS_TYPE_UINT64:
                    row_put_int64(&ra, *((int64 *)row_list[row_i].row_column_list[i].crud_value.str));
                    break;
                case GS_TYPE_REAL:
                case GS_TYPE_FLOAT:
                    row_put_real(&ra, *((double *)row_list[row_i].row_column_list[i].crud_value.str));
                    break;
                case GS_TYPE_TIMESTAMP:
                case GS_TYPE_DATE:
                    row_put_int64(&ra, *((int64 *)row_list[row_i].row_column_list[i].crud_value.str));
                    break;
                case GS_TYPE_BLOB:
                case GS_TYPE_CLOB: {
                    knl_column_t *column = knl_get_column(dc->handle, ra_i);
                    text_t data;
                    data.str = row_list[row_i].row_column_list[i].crud_value.str;
                    data.len = row_list[row_i].row_column_list[i].crud_value.len;
                    knl_row_put_lob(session, cursor, column, (void *)(&data), &ra);
                    break;
                }
                case GS_TYPE_CHAR:
                case GS_TYPE_VARCHAR:
                case GS_TYPE_STRING: {
                    row_put_text(&ra, &row_list[row_i].row_column_list[i].crud_value);
                    break;
                }

                case GS_TYPE_NUMBER:
                case GS_TYPE_DECIMAL: {
                    row_put_dec4(&ra, (dec4_t *)row_list[row_i].row_column_list[i].crud_value.str);
                    break;
                }
                case GS_TYPE_BINARY:
                case GS_TYPE_RAW: {
                    binary_t bin;
                    bin.bytes = (uint8*)(row_list[row_i].row_column_list[i].crud_value.str);
                    bin.is_hex_const = GS_FALSE;
                    bin.size = row_list[row_i].row_column_list[i].crud_value.len;
                    row_put_bin(&ra, &bin);
                    break;
                }
                default: {
                    return GS_ERROR;
                }
            }
        }
        if (cursor->row->size < PCRH_MIN_ROW_SIZE) {
            cursor->row->size = PCRH_MIN_ROW_SIZE;
        }
        use_row_size += cursor->row->size;
        if (use_row_size > max_size) {
            GS_LOG_RUN_ERR("insert size out of one page range!! use_row_size = %d \n", use_row_size);
            cursor->row = row_addr;
            return GS_ERROR;
        }
        cursor->row = (row_head_t *)((char *)cursor->row + cursor->row->size);
    }
    cursor->row = row_addr;
    cursor->is_insert_ignore = is_ignore;
    status_t ret_status = knl_insert(session, cursor);
    session->is_insert = GS_FALSE;
    return ret_status;
}

status_t gstor_batch_insert_row(void* handle, const char* table_name, int row_count, res_row_def_t *row_list,
                                uint32 part_no, bool32 is_ignore) {
    knl_cursor_t  *cursor  = EC_CURSOR(handle);
    knl_session_t *session = EC_SESSION(handle);
    knl_dictionary_t *dc = EC_DC(handle);
    cm_reset_error();
    gstor_prepare(session, cursor, EC_LOBBUF(handle));

    for (;;) {
        cm_set_ignore_log(GS_TRUE);
        if (executor_batch_insert(session, cursor, dc, table_name, row_count, row_list, part_no, is_ignore) == GS_SUCCESS) {
            cm_set_ignore_log(GS_FALSE);
            return GS_SUCCESS;
        }
        cm_set_ignore_log(GS_FALSE);
        if (GS_ERRNO != ERR_DUPLICATE_KEY) {
            return GS_ERROR;
        } else {
            return GS_ERROR;
        }
    }
}



status_t executor_update_row(knl_session_t *session, knl_cursor_t *cursor, knl_dictionary_t *dc, const char *table_name,
    int upd_column_count, exp_column_def_t *upd_column_list, int cond_column_count, exp_column_def_t *cond_column_list,
    bool32 *updated)
{
    GS_RETURN_IFERR(gstor_open_cursor_internal(session, cursor, dc, CURSOR_ACTION_UPDATE, 0));

    knl_init_index_scan(cursor, GS_TRUE);
    for (uInt i = 0; i < cond_column_count; i++) {
        knl_set_scan_key(INDEX_DESC(cursor->index), &cursor->scan_range.l_key, GS_TYPE_STRING,
            cond_column_list[i].crud_value.str, cond_column_list[i].crud_value.len, i);
    }
    GS_RETURN_IFERR(knl_fetch(session, cursor));

    if (cursor->eof) {
        return GS_SUCCESS;
    }
    *updated = GS_TRUE;

    row_assist_t ra;
    knl_update_info_t *ui = &cursor->update_info;

    ui->count = upd_column_count;
    for (uint32 i = 0; i < upd_column_count; i++) {
        ui->columns[i] = i;
    }
    row_init(&ra, ui->data, session->kernel->attr.max_row_size, ui->count);

    for (uint32 i = 0; i < upd_column_count; i++) {
        if (upd_column_list[i].crud_value.str == NULL) {
            row_put_null(&ra);
        } else {
            row_put_text(&ra, &upd_column_list[i].crud_value);
        }
    }

    cm_decode_row(ui->data, ui->offsets, ui->lens, NULL);
    return knl_internal_update(session, cursor);
}

status_t gstor_executor_update_row(void *handle, const char *table_name, int upd_column_count,
    exp_column_def_t *upd_column_list, int cond_column_count, exp_column_def_t *cond_column_list)
{
    knl_cursor_t *cursor = EC_CURSOR(handle);
    knl_session_t *session = EC_SESSION(handle);
    knl_dictionary_t *dc = EC_DC(handle);
    cm_reset_error();
    gstor_prepare(session, cursor, EC_LOBBUF(handle));

    bool32 updated = GS_FALSE;
    status_t status = executor_update_row(session, cursor, dc, table_name, upd_column_count, upd_column_list,
        cond_column_count, cond_column_list, &updated);
    if (updated) {
        return GS_SUCCESS;
    } else {
        printf("status=%d\n", status);
    }
    return status;
}

// gstor_cmp_exp_column_def  asc
static int gstor_cmp_exp_column_def(const void *a, const void *b) {
    return ((exp_column_def_t *)a)->col_slot - ((exp_column_def_t *)b)->col_slot;
}

status_t gstor_executor_update(void *handle, int column_count, exp_column_def_t *column_list,size_t cursor_idx)
{
    knl_cursor_t *cursor = EC_CURSOR_IDX(handle,cursor_idx);
    knl_session_t *session = EC_SESSION(handle);
    knl_dictionary_t *dc = EC_DC(handle);
    row_assist_t ra;
    knl_update_info_t *ui = &cursor->update_info;
    cm_reset_error();
    // sort upd_column_list  by col_slot , for update case
    qsort(column_list, column_count, sizeof(exp_column_def_t), gstor_cmp_exp_column_def);
    
    uint32_t total = 0;
    ui->count = column_count;
    for (uint32 i = 0; i < column_count; i++) {
        ui->columns[i] = column_list[i].col_slot;
        if (column_list[i].col_type == GS_TYPE_CLOB 
            || column_list[i].col_type == GS_TYPE_BLOB) {
                total += CM_ALIGN4(sizeof(lob_locator_t));
        } else {
            total += column_list[i].crud_value.len;
        }
    }
    if(total > session->kernel->attr.max_row_size) {
        GS_LOG_RUN_ERR("update size out of one page range!! total = %d \n", total);
        return GS_ERROR;
    }
    row_init(&ra, ui->data, session->kernel->attr.max_row_size, ui->count);

    for (uint32 i = 0; i < column_count; i++) {
        if (column_list[i].crud_value.str == NULL) {
            row_put_null(&ra);
        } else {
            switch (column_list[i].col_type) {
                case GS_TYPE_BOOLEAN:
                    row_put_bool(&ra, *((uint32 *)(column_list[i].crud_value.str)));
                    break;
                case GS_TYPE_UTINYINT:
                case GS_TYPE_USMALLINT:
                case GS_TYPE_UINT32:
                    row_put_uint32(&ra, *((uint32 *)column_list[i].crud_value.str));
                    break;
                case GS_TYPE_INTEGER:
                case GS_TYPE_SMALLINT:
                case GS_TYPE_TINYINT:
                    row_put_int32(&ra, *((int32 *)column_list[i].crud_value.str));
                    break;
                case GS_TYPE_BIGINT:
                case GS_TYPE_UINT64:
                    row_put_int64(&ra, *((int64 *)column_list[i].crud_value.str));
                    break;
                case GS_TYPE_REAL:
                case GS_TYPE_FLOAT:
                    row_put_real(&ra, *((double *)column_list[i].crud_value.str));
                    break;
                case GS_TYPE_TIMESTAMP:
                case GS_TYPE_DATE:
                    row_put_int64(&ra, *((int64 *)column_list[i].crud_value.str));
                    break;
                case GS_TYPE_BLOB:
                case GS_TYPE_CLOB: {
                    knl_column_t *column = knl_get_column(dc->handle, column_list[i].col_slot);
                    text_t data;
                    data.str = column_list[i].crud_value.str;
                    data.len = column_list[i].crud_value.len;
                    knl_row_put_lob(session, cursor, column, (void *)(&data), &ra);
                    break;
                }
                case GS_TYPE_CHAR:
                case GS_TYPE_VARCHAR:
                case GS_TYPE_STRING: {
                    row_put_text(&ra, &column_list[i].crud_value);
                    break;
                }
                case GS_TYPE_NUMBER:
                case GS_TYPE_DECIMAL: {
                    row_put_dec4(&ra, (dec4_t *)column_list[i].crud_value.str);
                    break;
                }
                case GS_TYPE_BINARY:
                case GS_TYPE_RAW: {
                    binary_t bin;
                    bin.bytes = (uint8*)(column_list[i].crud_value.str);
                    bin.is_hex_const = GS_FALSE;
                    bin.size = column_list[i].crud_value.len;
                    row_put_bin(&ra, &bin);
                    break;
                }
                default: {
                    return GS_ERROR;
                    break;
                }
            }
        }
    }

    cm_decode_row(ui->data, ui->offsets, ui->lens, NULL);
    return knl_internal_update(session, cursor);
}

status_t gstor_executor_delete(void *handle, size_t cursor_idx)
{
    cm_reset_error();
    knl_cursor_t *cursor = EC_CURSOR_IDX(handle, cursor_idx);
    knl_session_t *session = EC_SESSION(handle);
    GS_RETURN_IFERR(knl_internal_delete(session, cursor));
    return GS_SUCCESS;
}

status_t gstor_executor_delete_row(void *handle, const char *table_name, int *del_count, int cond_column_count,
    exp_column_def_t *cond_column_list)
{
    knl_cursor_t *cursor = EC_CURSOR(handle);
    knl_session_t *session = EC_SESSION(handle);
    knl_dictionary_t *dc = EC_DC(handle);
    cm_reset_error();
    gstor_prepare(session, cursor, EC_LOBBUF(handle));

    GS_RETURN_IFERR(gstor_open_cursor_internal(session, cursor, dc, CURSOR_ACTION_DELETE, 0));

    knl_init_index_scan(cursor, GS_TRUE);
    for (uInt i = 0; i < cond_column_count; i++) {
        knl_set_scan_key(INDEX_DESC(cursor->index), &cursor->scan_range.l_key, GS_TYPE_STRING,
            cond_column_list[i].crud_value.str, cond_column_list[i].crud_value.len, i);
    }
    GS_RETURN_IFERR(knl_fetch(session, cursor));

    *del_count = 0;
    while (!cursor->eof) {
        GS_RETURN_IFERR(knl_internal_delete(session, cursor));
        GS_RETURN_IFERR(knl_fetch(session, cursor));
        (*del_count)++;
    }
    return GS_SUCCESS;
}

status_t gstor_truncate_table(void *handle, char *owner, char *table_name)
{
    cm_reset_error();
    knl_session_t *session = EC_SESSION(handle);
    knl_trunc_def_t def;

    if (!owner) {
        text_t owner_sys = { (char *)"SYS", 3 };
        def.owner = owner_sys;
    } else {
        def.owner.str = owner;
        def.owner.len = strlen(owner);
    }

    if (!table_name) {
        return GS_ERROR;
    } else {
        def.name.str = table_name;
        def.name.len = strlen(table_name);
    }
    def.option = TRUNC_RECYCLE_STORAGE;

    return knl_truncate_table(session, &def);
}

static status_t gstor_make_scan_key(knl_session_t *session, knl_cursor_t *cursor, char *key, uint32 len, uint32 flags)
{
    GS_LOG_DEBUG_INF("make scan key: %u, key: %s, len: %u", flags, key, len);
    bool32 prefix = ((flags & G_STOR_PREFIX_FLAG) > 0) ? GS_TRUE : GS_FALSE;
    bool32 sequence = ((flags & G_STOR_SEQUENCE_FLAG) > 0) ? GS_TRUE : GS_FALSE;
    knl_init_index_scan(cursor, !(prefix || sequence));
    knl_set_scan_key(INDEX_DESC(cursor->index), &cursor->scan_range.l_key, GS_TYPE_STRING, key, (uint16)len,
        SYS_KV_KEY_COL_ID);

    if (flags == G_STOR_DEFAULT_FLAG) {
        return GS_SUCCESS;
    }

    CM_SAVE_STACK(session->stack);
    char *r_key = (char *)cm_push(session->stack, GS_MAX_KEY_LEN);
    if (r_key == NULL) {
        GS_LOG_DEBUG_ERR("make scan key alloc mem failed");
        return GS_ERROR;
    }
    int32 ret = strncpy_s(r_key, GS_MAX_KEY_LEN, key, len);
    if (ret != EOK) {
        GS_LOG_DEBUG_ERR("make scan key system call failed for strncpy %d", ret);
        CM_RESTORE_STACK(session->stack);
        return GS_ERROR;
    }
    if (prefix) {
        // fill padding for key's right range
        r_key[len++] = (char)255;
        if (len < GS_MAX_KEY_LEN) {
            r_key[len++] = (char)255;
        }
    } else {
        if (sequence) {
            for (uint32 i = 0; i < G_STOR_SEQUENCE_OFFSET; i++) {
                r_key[(len - G_STOR_SEQUENCE_OFFSET) + i] = G_STOR_SEQUENCE_9;
            }
        }
    }

    knl_set_scan_key(INDEX_DESC(cursor->index), &cursor->scan_range.r_key, GS_TYPE_STRING, r_key, (uint16)len,
        SYS_KV_KEY_COL_ID);
    CM_RESTORE_STACK(session->stack);
    return GS_SUCCESS;
}

static status_t gstor_get_table_row_kv(void *handle, char **key, unsigned int *key_len, char **val,
    unsigned int *val_len)
{
    knl_cursor_t *cursor = EC_CURSOR(handle);
    knl_session_t *session = EC_SESSION(handle);
    instance_t *cc_instance = session->kernel->server;

    // key
    if (key != NULL) {
        *key = CURSOR_COLUMN_DATA(cursor, SYS_KV_KEY_COL_ID);
    }

    if (key_len != NULL) {
        *key_len = CURSOR_COLUMN_SIZE(cursor, SYS_KV_KEY_COL_ID);
    }

    *val_len = CURSOR_COLUMN_SIZE(cursor, SYS_KV_VALUE_COL_ID);
    if (*val_len == GS_NULL_VALUE_LEN) {
        *val = NULL;
        *val_len = 0;
        return GS_SUCCESS;
    }

    // value
    lob_locator_t *locator = (lob_locator_t *)CURSOR_COLUMN_DATA(cursor, SYS_KV_VALUE_COL_ID);
    *val_len = locator->head.size;

    // inline
    if (!locator->head.is_outline) {
        *val = (char *)locator + OFFSET_OF(lob_locator_t, data);
        return GS_SUCCESS;
    }

    // outline
    if (*val_len > EC_LOBBUF(handle)->size) {
        GS_RETURN_IFERR(gstor_realloc_log_buf(cc_instance, EC_LOBBUF(handle), (*val_len)));
    }

    *val = EC_LOBBUF(handle)->buf;
    GS_RETURN_IFERR(knl_read_lob(session, locator, 0, (void *)(*val), (*val_len), NULL));
    return GS_SUCCESS;
}

int gstor_put(void *handle, char *key, unsigned int key_len, char *val, unsigned int val_len)
{
    knl_cursor_t *cursor = EC_CURSOR(handle);
    knl_session_t *session = EC_SESSION(handle);
    knl_dictionary_t *dc = EC_DC(handle);

    gstor_prepare(session, cursor, EC_LOBBUF(handle));

    uint32 try_times = 0;
    for (;;) {
        cm_set_ignore_log(GS_TRUE);
        if (gstor_insert(session, cursor, dc, key, key_len, val, val_len) == GS_SUCCESS) {
            cm_set_ignore_log(GS_FALSE);
            return GS_SUCCESS;
        }
        cm_set_ignore_log(GS_FALSE);
        if (GS_ERRNO != ERR_DUPLICATE_KEY) {
            return GS_ERROR;
        }

        cm_reset_error();
        bool32 updated = GS_FALSE;

        GS_RETURN_IFERR(gstor_update(session, cursor, dc, key, key_len, val, val_len, &updated));
        if (updated) {
            return GS_SUCCESS;
        } else {
            // This situation must be a primary key conflict in different session (different threads)
            // Because of read committed is not visible in different session, it needs to add session scn
            try_times++;
            if (try_times <= GSTOR_PUT_TRY_TIMES) {
                db_next_scn(session);
                knl_set_session_scn(session, DB_CURR_SCN(session));
            } else {
                return GS_ERROR;
            }
        }
    }
}

int gstor_del(void *handle, char *key, unsigned int key_len, unsigned int prefix, unsigned int *count)
{
    knl_cursor_t *cursor = EC_CURSOR(handle);
    knl_session_t *session = EC_SESSION(handle);
    knl_dictionary_t *dc = EC_DC(handle);

    gstor_prepare(session, cursor, EC_LOBBUF(handle));

    GS_RETURN_IFERR(gstor_open_cursor_internal(session, cursor, dc, CURSOR_ACTION_DELETE, IX_SYS_KV_01_ID));

    GS_RETURN_IFERR(gstor_make_scan_key(session, cursor, key, key_len, prefix));

    GS_RETURN_IFERR(knl_fetch(session, cursor));

    *count = 0;
    while (!cursor->eof) {
        GS_RETURN_IFERR(knl_internal_delete(session, cursor));
        GS_RETURN_IFERR(knl_fetch(session, cursor));
        (*count)++;
    }
    return GS_SUCCESS;
}

int gstor_get(void *handle, char *key, unsigned int key_len, char **val, unsigned int *val_len, unsigned int *eof)
{
    knl_cursor_t *cursor = EC_CURSOR(handle);
    knl_session_t *session = EC_SESSION(handle);
    knl_dictionary_t *dc = EC_DC(handle);

    gstor_prepare(session, cursor, EC_LOBBUF(handle));

    GS_RETURN_IFERR(gstor_open_cursor_internal(session, cursor, dc, CURSOR_ACTION_SELECT, IX_SYS_KV_01_ID));

    GS_RETURN_IFERR(gstor_make_scan_key(session, cursor, key, key_len, G_STOR_DEFAULT_FLAG));

    GS_RETURN_IFERR(knl_fetch(session, cursor));
    *eof = cursor->eof;
    if (*eof) {
        return GS_SUCCESS;
    }
    return gstor_get_table_row_kv(handle, NULL, NULL, val, val_len);
}

int rust_gstor_get(void *handle, char *key)
{
    knl_cursor_t *cursor = EC_CURSOR(handle);
    knl_session_t *session = EC_SESSION(handle);
    knl_dictionary_t *dc = EC_DC(handle);
    char *val;
    unsigned int eof;
    unsigned int val_len;

    gstor_prepare(session, cursor, EC_LOBBUF(handle));

    GS_RETURN_IFERR(gstor_open_cursor_internal(session, cursor, dc, CURSOR_ACTION_SELECT, IX_SYS_KV_01_ID));

    GS_RETURN_IFERR(gstor_make_scan_key(session, cursor, key, strlen(key), G_STOR_DEFAULT_FLAG));

    GS_RETURN_IFERR(knl_fetch(session, cursor));
    eof = cursor->eof;
    if (eof) {
        return GS_SUCCESS;
    }
    /* if (gstor_get_table_row_kv(handle, NULL, NULL, &val, &val_len) == GS_SUCCESS) {
        return val;
    } */
    return GS_ERROR;
}

int gstor_open_cursor_ex(void *handle, const char *table_name, int index_column_size, int condition_size,
    condition_def_t *conditions, bool32 *eof, int idx_slot, scan_action_t action, size_t cursor_idx,
    lock_clause_t lock_clause)
{
    knl_session_t *session = EC_SESSION(handle);
    instance_t *cc_instance = session->kernel->server;

    GS_RETURN_IF_FALSE(cursor_idx < G_STOR_MAX_CURSOR);
    if(EC_CURSOR_IDX(handle,cursor_idx) == NULL) {
        if(knl_alloc_cursor(cc_instance, &EC_CURSOR_IDX(handle,cursor_idx)) != GS_SUCCESS) {
            return GS_ERROR;
        }
    }
    knl_cursor_t* cursor = EC_CURSOR_IDX(handle,cursor_idx);
    knl_dictionary_t *dc = EC_DC(handle);

    gstor_prepare(session, cursor, EC_LOBBUF(handle));

    // select for update
    if (action == CURSOR_ACTION_SELECT && lock_clause.is_select_for_update == GS_TRUE) {
        action = CURSOR_ACTION_UPDATE;
        cursor->rowmark.type = lock_clause.wait_policy;
        cursor->rowmark.wait_seconds = lock_clause.wait_n;
    }

    if (idx_slot >= 0) {
        GS_RETURN_IFERR(gstor_open_cursor_internal(session, cursor, dc, (knl_cursor_action_t)action, idx_slot));

        knl_init_index_scan(cursor, GS_FALSE);
        knl_scan_key_t *left = &cursor->scan_range.l_key;
        knl_scan_key_t *right = &cursor->scan_range.r_key;
        size_t i = 0;
        for (; i < condition_size; i++) {
            knl_set_scan_key(INDEX_DESC(cursor->index), left, conditions[i].col_type, conditions[i].left_buff,
                conditions[i].left_size, i);
            knl_set_scan_key(INDEX_DESC(cursor->index), right, conditions[i].col_type, conditions[i].right_buff,
                conditions[i].right_size, i);
            if (conditions[i].scan_edge == SCAN_EDGE_LE) {
                knl_set_key_flag(left, SCAN_KEY_LEFT_INFINITE, i);
            } else if (conditions[i].scan_edge == SCAN_EDGE_GE) {
                knl_set_key_flag(right, SCAN_KEY_RIGHT_INFINITE, i);
            }
        }
        // 未设置的条件
        while (i < index_column_size) {
            knl_set_key_flag(left, SCAN_KEY_LEFT_INFINITE, i);
            knl_set_key_flag(right, SCAN_KEY_RIGHT_INFINITE, i);
            i++;
        }
    } else {
        GS_RETURN_IFERR(gstor_open_cursor_internal(session, cursor, dc, (knl_cursor_action_t)action, GS_INVALID_ID32));
    }
    return 0;
}

int gstor_cursor_next(void *handle, unsigned int *eof, size_t cursor_idx)
{
    knl_cursor_t *cursor = EC_CURSOR_IDX(handle,cursor_idx);
    GS_RETURN_IFERR(knl_fetch(EC_SESSION(handle), cursor));
    *eof = cursor->eof;
    return GS_SUCCESS;
}

int gstor_fast_count_table_row(void *handle, const char* table_name, size_t cursor_idx, int64_t* rows) {
    knl_session_t *session = EC_SESSION(handle);
    instance_t *cc_instance = session->kernel->server;

    GS_RETURN_IF_FALSE(cursor_idx < G_STOR_MAX_CURSOR);
    if(EC_CURSOR_IDX(handle,cursor_idx) == NULL) {
        if(knl_alloc_cursor(cc_instance, &EC_CURSOR_IDX(handle,cursor_idx)) != GS_SUCCESS) {
            return GS_ERROR;
        }
    }
    knl_cursor_t  *cursor  = EC_CURSOR_IDX(handle,cursor_idx);
    knl_dictionary_t *dc = EC_DC(handle);

    gstor_prepare(session, cursor, EC_LOBBUF(handle));

    GS_RETURN_IFERR(gstor_open_cursor_internal(session, cursor, dc, CURSOR_ACTION_SELECT, GS_INVALID_ID32));
    knl_cursor_operator2_t do_fetch_page = TABLE_ACCESSOR(cursor)->do_fetch_page;
    if(!do_fetch_page) {
        GS_LOG_RUN_ERR("not do fetch page method for table: %s\n", table_name);
        return GS_ERROR;
    }
    while(1) {
        int64_t count = 0;
        if(do_fetch_page(session,cursor,&count) != GS_SUCCESS) {
            GS_LOG_RUN_ERR("fetch page fail with table: %s\n", table_name);
            return GS_ERROR;
        }
        *rows += count;
        if(cursor->eof) {
            break;
        }
    }
    return 0;
}

int gstor_cursor_fetch(void *handle, int sel_column_count, exp_column_def_t *sel_column_list, int *res_row_count,
    res_row_def_t *res_row_list,size_t cursor_idx)
{
    knl_cursor_t  *cursor  = EC_CURSOR_IDX(handle,cursor_idx);
    knl_session_t *session = EC_SESSION(handle);
    instance_t *cc_instance = session->kernel->server;

    *res_row_count = 1;
    res_row_list->column_count = sel_column_count;
    for (uint32 i = 0; i < sel_column_count; i++) {
        exp_column_def_t *col = res_row_list->row_column_list + i;
        col->name = sel_column_list[i].name;
        col->col_type = sel_column_list[i].col_type;
        col->crud_value.assign = 0;
        col->precision = sel_column_list[i].precision;
        col->scale = sel_column_list[i].scale;
        col->crud_value.len = CURSOR_COLUMN_SIZE(cursor, sel_column_list[i].col_slot);
        if (col->crud_value.len != GS_NULL_VALUE_LEN) {
            if (col->col_type != GS_TYPE_CLOB && col->col_type != GS_TYPE_BLOB) {
                col->crud_value.str = CURSOR_COLUMN_DATA(cursor, sel_column_list[i].col_slot);
            } else {
                lob_locator_t *locator = (lob_locator_t *)CURSOR_COLUMN_DATA(cursor, sel_column_list[i].col_slot);
                col->crud_value.len = locator->head.size;
                if (!locator->head.is_outline) {
                    // inline
                    col->crud_value.str = (char *)locator + OFFSET_OF(lob_locator_t, data);
                } else {
                    // outline
                    if (col->crud_value.len > EC_LOBBUF(handle)->size) {
                        GS_RETURN_IFERR(gstor_realloc_log_buf(cc_instance, EC_LOBBUF(handle), col->crud_value.len));
                    }
                    col->crud_value.str = EC_LOBBUF(handle)->buf;
                    GS_RETURN_IFERR(knl_read_lob(session, locator, 0, (void *)(col->crud_value.str), col->crud_value.len, NULL));
                }
            }
        } else {
            col->crud_value.str = NULL;
        }
    }
    return GS_SUCCESS;
}

int gstor_begin(void *handle)
{
    knl_session_t *session = EC_SESSION(handle);
    knl_set_session_trans(session, ISOLATION_READ_COMMITTED);
    return GS_SUCCESS;
}

int gstor_commit(void *handle)
{
    knl_commit(EC_SESSION(handle));
    return GS_SUCCESS;
}

int gstor_rollback(void *handle)
{
    knl_rollback(EC_SESSION(handle), NULL);
    return GS_SUCCESS;
}

int gstor_vm_alloc(void *handle, unsigned int *vmid)
{
    return vm_alloc(EC_SESSION(handle), EC_SESSION(handle)->temp_pool, vmid);
}

int gstor_vm_open(void *handle, unsigned int vmid, void **page)
{
    return vm_open(EC_SESSION(handle), EC_SESSION(handle)->temp_pool, vmid, (vm_page_t **)page);
}

void gstor_vm_close(void *handle, unsigned int vmid)
{
    vm_close(EC_SESSION(handle), EC_SESSION(handle)->temp_pool, vmid, VM_ENQUE_TAIL);
}

void gstor_vm_free(void *handle, unsigned int vmid)
{
    vm_free(EC_SESSION(handle), EC_SESSION(handle)->temp_pool, vmid);
}

int gstor_vm_swap_out(void *handle, void *page, unsigned long long *swid, unsigned int *cipher_len)
{
    knl_session_t *session = EC_SESSION(handle);
    return session->temp_pool->swapper.out(session, (vm_page_t *)page, swid, cipher_len);
}

int gstor_vm_swap_in(void *handle, unsigned long long swid, unsigned int cipher_len, void *page)
{
    knl_session_t *session = EC_SESSION(handle);
    return session->temp_pool->swapper.in(session, swid, cipher_len, (vm_page_t *)page);
}

int gstor_xa_start(void *handle, unsigned char gtrid_len, const char *gtrid)
{
    knl_session_t *session = EC_SESSION(handle);
    instance_t *cc_instance = session->kernel->server;
    session->rm->xa_xid.fmt_id = 0;
    session->rm->xa_xid.gtrid_len = gtrid_len;
    MEMS_RETURN_IFERR(memcpy_sp(session->rm->xa_xid.gtrid, GS_MAX_XA_BASE16_GTRID_LEN, gtrid, gtrid_len));
    session->rm->xa_xid.bqual_len = 0;
    return (int)knl_add_xa_xid(cc_instance, &session->rm->xa_xid, session->rm->id, XA_START);
}

int gstor_xa_status(void *handle)
{
    knl_session_t *session = EC_SESSION(handle);
    instance_t *cc_instance = session->kernel->server;

    uint16 rmid = knl_get_xa_xid(cc_instance, &session->rm->xa_xid);
    if (rmid == GS_INVALID_ID16) {
        return XACT_END;
    }

    txn_t *txn = session->kernel->rms[rmid]->txn;
    if (txn == NULL) {
        return XACT_END;
    }

    return (int)txn->status;
}

int gstor_xa_shrink(void *handle)
{
    knl_shrink_xa_rms(EC_SESSION(handle), GS_TRUE);
    return GS_SUCCESS;
}

int gstor_xa_end(void *handle)
{
    knl_session_t *session = EC_SESSION(handle);
    instance_t *cc_instance = session->kernel->server;
    knl_delete_xa_xid(cc_instance, &session->rm->xa_xid);
    return GS_SUCCESS;
}

int gstor_detach_suspend_rm(void *handle)
{
    uint16 rmid;
    knl_session_t *session = EC_SESSION(handle);
    instance_t *cc_instance = session->kernel->server;
    GS_RETURN_IFERR(knl_alloc_rm(cc_instance, &rmid));
    knl_detach_suspend_rm(session, rmid);
    return GS_SUCCESS;
}

int gstor_attach_suspend_rm(void *handle)
{
    knl_session_t *session = EC_SESSION(handle);
    (void)knl_attach_suspend_rm(session, &session->rm->xa_xid, XA_PHASE1, GS_FALSE);
    return GS_SUCCESS;
}

int gstor_detach_pending_rm(void *handle)
{
    knl_session_t *session = EC_SESSION(handle);
    uint16 rmid = session->rmid;
    knl_detach_pending_rm(session, rmid);
    return GS_SUCCESS;
}

int gstor_attach_pending_rm(void *handle)
{
    knl_session_t *session = EC_SESSION(handle);
    (void)knl_attach_pending_rm(session, &session->rm->xa_xid);
    return GS_SUCCESS;
}

void free_table_info(exp_table_meta *meta)
{
    if (meta->columns != NULL) {
        free(meta->columns);
    }
    if(meta->indexes!=NULL){
        free(meta->indexes);
    }
    if(meta->part_table.entitys){
        free(meta->part_table.entitys);
    }
    if(meta->part_table.keycols){
        free(meta->part_table.keycols);
    }
    if(meta->part_table.pbuckets){
        free(meta->part_table.pbuckets);
    }
}

static void col_format_trans(exp_column_def_t *col_info, knl_column_t *col)
{
    col_info->name.str = col->name;
    col_info->name.len = strlen(col->name);
    col_info->col_type = col->datatype;
    col_info->size = col->size;
    col_info->precision = col->precision;
    col_info->nullable = col->nullable;
    col_info->scale = col->scale;
    if (col->default_text.len > 0) {
        col_info->is_default = GS_TRUE;
        col_info->default_val.str = col->default_text.str;
        col_info->default_val.len = col->default_text.len;
        col_info->default_val.assign = ASSIGN_TYPE_EQUAL;
    }
    col_info->is_autoincrement = KNL_COLUMN_IS_SERIAL(col);
}

static void index_format_trans(exp_index_def_t *idx_info, index_t *index, knl_handle_t handle)
{
    idx_info->name.str = index->desc.name;
    idx_info->name.len = strlen(index->desc.name);
    idx_info->col_count = index->desc.column_count;
    idx_info->is_primary = index->desc.primary;
    idx_info->is_unique = index->desc.unique;
    for (int i = 0; i < idx_info->col_count; i++) {
        idx_info->col_ids[i] = index->desc.columns[i];
    }
    idx_info->parted = index->desc.parted;

    // fk
    // if (index->dep_set.count > 0) {
    //     idx_info->dep_set_count = index->dep_set.count;
    //     idx_info->cons_dep_set = index->dep_set;
    // } else {
    //     idx_info->dep_set_count = 0;
    // }
}

static void partdesc_format_trans(exp_part_desc_t *desc_info, knl_part_desc_t *desc)
{
    desc_info->uid = desc->uid;
    desc_info->table_id = desc->table_id;
    desc_info->index_id = desc->index_id;
    desc_info->parttype = desc->parttype;
    desc_info->partcnt = desc->partcnt;
    desc_info->slot_num = desc->slot_num;
    desc_info->partkeys = desc->partkeys;
    desc_info->flags = desc->flags;
    desc_info->interval = desc->interval;
    desc_info->binterval = desc->binterval;
    desc_info->real_partcnt = desc->real_partcnt;
    desc_info->transition_no = desc->transition_no;
    desc_info->interval_num = desc->interval_num;
    desc_info->interval_spc_num = desc->interval_spc_num;
    desc_info->auto_addpart = desc->auto_addpart;
    desc_info->is_crosspart = desc->is_crosspart;
    binary_t *bin = &desc_info->binterval;
    GS_LOG_RUN_INF("bin->size:%d, bin->bytes->size:%d, bin->bytes->column_count:%d\n", 
                bin->size, ((part_key_t*)bin->bytes)->size, ((part_key_t*)bin->bytes)->column_count);
}

static void tablepart_format_trans(exp_table_part_t *table_part, table_part_t *entity)
{
    table_part->part_no = entity->part_no;
    table_part->parent_partno = entity->parent_partno;
    table_part->global_partno = entity->global_partno;
    table_part->is_ready = entity->is_ready;

    table_part->desc.uid = entity->desc.uid;
    table_part->desc.table_id = entity->desc.table_id;
    table_part->desc.part_id = entity->desc.part_id;
    memcpy_sp(table_part->desc.name, GS_TS_PART_NAME_BUFFER_SIZE, entity->desc.name, GS_TS_PART_NAME_BUFFER_SIZE);
    table_part->desc.flags = entity->desc.flags;
    table_part->desc.compress_algo = entity->desc.compress_algo;
    table_part->desc.hiboundval.str = entity->desc.hiboundval.str;
    table_part->desc.hiboundval.len = entity->desc.hiboundval.len;
    table_part->desc.bhiboundval = entity->desc.bhiboundval;
}

// get table meta info
int gstor_get_table_info(void *handle, const char *schema_name, const char *table_name,
                        exp_table_meta *table_info, error_info_t *err_info)
{
    cm_reset_error();
    // TODO get user name from other place
    text_t user = { schema_name, strlen(schema_name) };
    text_t table = { table_name, strlen(table_name) };
    knl_session_t *session = EC_SESSION(handle);

    knl_dictionary_t table_dc; // 表字典
    status_t result = dc_open(session, &user, &table, &table_dc);
    if (result != GS_SUCCESS) {
        if (err_info != NULL) {
            err_info->code = GS_ERRNO;
            memcpy_s(err_info->message, GS_MESSAGE_BUFFER_SIZE, g_tls_error.message, GS_MESSAGE_BUFFER_SIZE);
        }
        cm_reset_error();
        return result;
    }

    if (table_dc.type == DICT_TYPE_VIEW) {
        knl_view_t *view = &((dc_entity_t *)(table_dc.handle))->view;

        strcpy(table_info->name, view->name);

        table_info->id = view->id;
        table_info->uid = view->uid;
        table_info->column_count = view->column_count;
        // status_t knl_get_view_sub_sql(knl_handle_t session, knl_dictionary_t *dc, text_t *sql, uint32 *page_id)
        table_info->sql = view->sub_sql;
        table_info->dict_type = DICT_TYPE_VIEW;

        size_t col_mem_len = table_info->column_count * sizeof(exp_column_def_t);
        table_info->columns = (exp_column_def_t *)malloc(col_mem_len);
        MEMS_RETURN_IFERR(memset_sp(table_info->columns, col_mem_len, 0, col_mem_len));
        exp_column_def_t *cur_col = table_info->columns;
        uint32 visible_col_count = 0; // 可见列数量
        for (int i = 0; i < table_info->column_count; ++i) {
            knl_column_t *col = knl_get_column(table_dc.handle, i);
            if (KNL_COLUMN_INVISIBLE(col)) { // 列不可见，跳过
                continue;
            }
            col_format_trans(cur_col, col);
            cur_col->col_slot = i;
            ++cur_col;
            ++visible_col_count;
        }
        table_info->column_count = visible_col_count;
        dc_close(&table_dc);
        return GS_SUCCESS;
    } else if (table_dc.type == DICT_TYPE_TABLE) {
        knl_table_desc_t *desc = &(DC_TABLE(&table_dc)->desc);

        strcpy(table_info->name, desc->name);

        table_info->id = desc->id;
        table_info->uid = desc->uid;
        table_info->space_id = desc->space_id;
        table_info->oid = desc->oid;
        table_info->column_count = desc->column_count;
        table_info->index_count = desc->index_count;
        table_info->dict_type = DICT_TYPE_TABLE;
        table_info->has_autoincrement = DC_ENTITY(&table_dc)->has_serial_col;

        // TODO entity->column_count 与 desc->column_count 有区别吗？
        size_t col_mem_len = table_info->column_count * sizeof(exp_column_def_t);
        table_info->columns = (exp_column_def_t *)malloc(col_mem_len);
        MEMS_RETURN_IFERR(memset_sp(table_info->columns, col_mem_len, 0, col_mem_len));
        exp_column_def_t *cur_col = table_info->columns;
        uint32 visible_col_count = 0; // 可见列数量
        for (int i = 0; i < table_info->column_count; ++i) {
            knl_column_t *col = knl_get_column(table_dc.handle, i);
            if (KNL_COLUMN_INVISIBLE(col)) { // 列不可见，跳过
                continue;
            }
            col_format_trans(cur_col, col);
            cur_col->col_slot = i;
            ++cur_col;
            ++visible_col_count;
        }
        table_info->column_count = visible_col_count;

        table_info->indexes = (exp_index_def_t *)malloc(table_info->index_count * sizeof(exp_index_def_t));
        exp_index_def_t *cur_index = table_info->indexes;
        for (int i = 0; i < table_info->index_count; ++i) {
            index_t *index = DC_INDEX(&table_dc, i);
            index_format_trans(cur_index, index, table_dc.handle);
            cur_index->index_slot = i;
            ++cur_index;
        }

        // constraints-fk
        // cons_set_t cons_set = DC_TABLE(&table_dc)->cons_set;
        // table_info->ref_count = cons_set.ref_count;
        // if (table_info->ref_count > 0) {
        //     table_info->ref_cons = cons_set.ref_cons;
        // }

        /* about partition */
        table_info->appendonly = desc->appendonly;
        table_info->is_timescale = desc->is_timescale;
        table_info->has_retention = desc->has_retention;
        if (table_info->has_retention) {
            table_info->retention = desc->retention;
        }
        table_info->parted = desc->parted;
        if (table_info->parted) {
            exp_part_desc_t *part_desc = &table_info->part_table.desc;
            part_table_t *part_table = DC_TABLE_PART(&table_dc);
            partdesc_format_trans(part_desc, &part_table->desc);

            size_t keycols_mem_len = part_table->desc.partkeys * sizeof(exp_part_column_desc_t);
            table_info->part_table.keycols = (exp_part_column_desc_t *)malloc(keycols_mem_len);
            MEMS_RETURN_IFERR(
                memcpy_sp(table_info->part_table.keycols, keycols_mem_len, part_table->keycols, keycols_mem_len));
            table_info->part_table.pbuckets = (exp_part_bucket_t *)malloc(GS_SHARED_PAGE_SIZE);
            MEMS_RETURN_IFERR(memset_sp(table_info->part_table.pbuckets, GS_SHARED_PAGE_SIZE, 0, GS_SHARED_PAGE_SIZE));
            table_info->part_table.entitys = (exp_table_part_t *)malloc(part_desc->partcnt * sizeof(exp_table_part_t));
            exp_table_part_t *part_entitys = table_info->part_table.entitys;
            uint32 part_idx = 0;
            for (uint32 i = 0; i < PART_NAME_HASH_SIZE; i++) {
                table_info->part_table.pbuckets[i].first = part_table->pbuckets[i].first;
                if (table_info->part_table.pbuckets[i].first == GS_INVALID_ID32)
                    continue;
                uint32 part_no = table_info->part_table.pbuckets[i].first;
                while(part_no!=GS_INVALID_ID32){
                    table_part_t* entity = PART_GET_ENTITY(part_table, part_no);
                    tablepart_format_trans(&part_entitys[part_idx], entity);
                    part_idx++;
                    part_no = entity->pnext;
                }
            }
            if (part_idx != part_desc->partcnt) {
                GS_LOG_RUN_ERR("part_idx is not equal to partcnt!\n");
                dc_close(&table_dc);
                return GS_ERROR;
            }
        }

        dc_close(&table_dc);
        return GS_SUCCESS;
    }
    dc_close(&table_dc);
    return GS_ERROR;
}

int32 gstore_var_compare_data(const void *data1, const void *data2, gs_type_t type, uint32 size1, uint32 size2)
{
    return var_compare_data(data1, data2, type, size1, size2);
}

knl_cursor_t *gstor_get_cursor(void *handle, size_t cursor_idx)
{
    return EC_CURSOR_IDX(handle,cursor_idx);
}

static void alter_col_format_trans(exp_column_def_t *col, knl_column_def_t *alt_column)
{
    alt_column->name.str = col->name.str;
    alt_column->name.len = col->name.len;
    alt_column->typmod.datatype = col->col_type;
    alt_column->typmod.size = col->size;
    alt_column->nullable = col->nullable;
    alt_column->primary = col->is_primary;
    // 这里要保证is_default 和 is_default_null一个为true一个为false，不能同事为true或false
    alt_column->is_default = col->is_default;
    alt_column->is_default_null = GS_TRUE;
    if (alt_column->is_default) {
        alt_column->default_text.str = col->default_val.str;
        alt_column->default_text.len = col->default_val.len;
        alt_column->is_default_null = GS_FALSE;
    }
    if (col->comment.len > 0) {
        alt_column->comment.str = col->comment.str;
        alt_column->comment.len = col->comment.len;
    }
    alt_column->typmod.precision = col->precision;
    alt_column->typmod.scale = col->scale;
    alt_column->unique = col->is_unique;
    alt_column->is_check = col->is_check;
}

int sqlapi_gstor_alter_table(void *handle, const char* schema_name, const char *table_name,
    const exp_altable_def_t *alter_table, error_info_t *err_info)
{
    cm_reset_error(); // 初始化异常信息存储变量
    knl_session_t *session = EC_SESSION(handle);
    knl_altable_def_t altable_def;

    altable_def.action = alter_table->action;
    altable_def.user.str = schema_name;
    altable_def.user.len = strlen(schema_name);
    altable_def.name.str = (char *)table_name;
    altable_def.name.len = (uint32)strlen(table_name);

    CM_SAVE_STACK(session->stack);
    switch (altable_def.action) {
        case ALTABLE_ADD_COLUMN:
        case ALTABLE_MODIFY_COLUMN: {
            knl_alt_column_prop_t *alt_column = NULL;
            cm_galist_init(&altable_def.column_defs, session->stack, cm_stack_alloc);
            for (int col_idx = 0; col_idx < alter_table->col_count; col_idx++) {
                GS_RETURN_IFERR(
                    cm_galist_new(&altable_def.column_defs, sizeof(knl_alt_column_prop_t), (pointer_t *)&alt_column));
                MEMS_RETURN_IFERR(
                    memset_s(alt_column, sizeof(knl_alt_column_prop_t), 0, sizeof(knl_alt_column_prop_t)));
                exp_al_column_def_t *al_column_def = &alter_table->cols[col_idx];
                alt_column->name.str = al_column_def->name.str;
                alt_column->name.len = al_column_def->name.len;
                alter_col_format_trans(&al_column_def->col_def, &alt_column->new_column);
                if ((!alt_column->new_column.is_check && !alt_column->new_column.unique &&
                    !alt_column->new_column.primary) ||
                    (al_column_def->constr_count == 0)) {
                    continue;
                }
                // 约束 alt_column->constraints fill
                knl_constraint_def_t *constraint = NULL;
                cm_galist_init(&alt_column->constraints, session->stack, cm_stack_alloc);
                for (uint32 constridx = 0; constridx < al_column_def->constr_count; constridx++) {
                    GS_RETURN_IFERR(cm_galist_new(&alt_column->constraints, sizeof(knl_constraint_def_t),
                        (pointer_t *)&constraint));
                    MEMS_RETURN_IFERR(
                        memset_s(constraint, sizeof(knl_constraint_def_t), 0, sizeof(knl_constraint_def_t)));
                    exp_constraint_def_t *cosntr = &al_column_def->constr_def[constridx];
                    constraint->type = cosntr->type;
                    constraint->name.str = cosntr->name.str;
                    constraint->name.len = cosntr->name.len;
                    knl_index_col_def_t *cons_col = NULL;
                    cm_galist_init(&constraint->columns, session->stack, cm_stack_alloc);
                    for (int conscol_idx = 0; conscol_idx < cosntr->col_count; conscol_idx++) {
                        GS_RETURN_IFERR(
                            cm_galist_new(&constraint->columns, sizeof(knl_index_col_def_t), (pointer_t *)&cons_col));
                        MEMS_RETURN_IFERR(
                            memset_s(cons_col, sizeof(knl_index_col_def_t), 0, sizeof(knl_index_col_def_t)));
                        cons_col->name.str = cosntr->cols[conscol_idx].str;
                        cons_col->name.len = cosntr->cols[conscol_idx].len;
                        cons_col->mode = SORT_MODE_ASC;
                        cons_col->is_func = GS_FALSE;
                    }
                    if (constraint->type == CONS_TYPE_PRIMARY || constraint->type == CONS_TYPE_UNIQUE) {
                        MEMS_RETURN_IFERR(
                            memset_sp(&constraint->index, sizeof(constraint->index), 0, sizeof(constraint->index)));
                        constraint->index.name = constraint->name;
                        constraint->index.table.str = (char *)table_name;
                        constraint->index.table.len = strlen(table_name);
                        #ifdef _MSC_VER
                        const text_t *space = &g_users;
                        constraint->index.space = *space;
                        #else
                        constraint->index.space = (text_t)g_users;
                        #endif
                        constraint->index.primary = (constraint->type == CONS_TYPE_PRIMARY) ? GS_TRUE : GS_FALSE;
                        constraint->index.unique = (constraint->type == CONS_TYPE_UNIQUE) ? GS_TRUE : GS_FALSE;
                        constraint->index.user.str = schema_name;
                        constraint->index.user.len = strlen(schema_name);
                        constraint->index.cr_mode = CR_PAGE;
                        constraint->index.options |= CREATE_IF_NOT_EXISTS;
                    }
                }
            }
            break;
        }
        case ALTABLE_RENAME_COLUMN: {
            knl_alt_column_prop_t *alt_column = NULL;
            cm_galist_init(&altable_def.column_defs, session->stack, cm_stack_alloc);
            for (int col_idx = 0; col_idx < alter_table->col_count; col_idx++) {
                GS_RETURN_IFERR(
                    cm_galist_new(&altable_def.column_defs, sizeof(knl_alt_column_prop_t), (pointer_t *)&alt_column));
                MEMS_RETURN_IFERR(
                    memset_s(alt_column, sizeof(knl_alt_column_prop_t), 0, sizeof(knl_alt_column_prop_t)));
                alt_column->name.str = alter_table->cols[col_idx].name.str;
                alt_column->name.len = alter_table->cols[col_idx].name.len;

                alt_column->new_name.str = alter_table->cols[col_idx].new_name.str;
                alt_column->new_name.len = alter_table->cols[col_idx].new_name.len;
            }
            break;
        }
        case ALTABLE_DROP_COLUMN: {
            knl_alt_column_prop_t *alt_column = NULL;
            cm_galist_init(&altable_def.column_defs, session->stack, cm_stack_alloc);
            for (int col_idx = 0; col_idx < alter_table->col_count; col_idx++) {
                GS_RETURN_IFERR(
                    cm_galist_new(&altable_def.column_defs, sizeof(knl_alt_column_prop_t), (pointer_t *)&alt_column));
                MEMS_RETURN_IFERR(
                    memset_s(alt_column, sizeof(knl_alt_column_prop_t), 0, sizeof(knl_alt_column_prop_t)));
                alt_column->name.str = alter_table->cols[col_idx].name.str;
                alt_column->name.len = alter_table->cols[col_idx].name.len;
            }
            break;
        }
        case ALTABLE_RENAME_TABLE:{
            altable_def.table_def.new_name.str = alter_table->altable.new_name.str;
            altable_def.table_def.new_name.len = alter_table->altable.new_name.len;
            break;
        }
        case ALTABLE_ADD_PARTITION:{
            text_t user = {schema_name, strlen(schema_name)};
            text_t table = {table_name,strlen(table_name)};
            knl_dictionary_t table_dc; // 表字典
            status_t result = dc_open(session,&user,&table,&table_dc);
            if(result != GS_SUCCESS) {
                if (err_info != NULL){
                    err_info->code = GS_ERRNO;
                    memcpy_s(err_info->message, GS_MESSAGE_BUFFER_SIZE, g_tls_error.message, GS_MESSAGE_BUFFER_SIZE);
                    GS_LOG_RUN_WAR("dc_open err,code:%d,msg:%s\n", err_info->code, err_info->message);
                }
                cm_reset_error();
                return result;
            }    
            altable_def.part_def.name.str = alter_table->part_opt.part_name.str;
            altable_def.part_def.name.len = alter_table->part_opt.part_name.len;
            altable_def.part_def.obj_def = (knl_part_obj_def_t *)cm_push(session->stack, sizeof(knl_part_obj_def_t));
            knl_part_obj_def_t* part_obj = altable_def.part_def.obj_def;
            MEMS_RETURN_IFERR(memset_s(part_obj, sizeof(knl_part_obj_def_t), 0, sizeof(knl_part_obj_def_t)));
            part_obj->part_type = alter_table->part_opt.part_type;
            knl_part_def_t * part = NULL;
            cm_galist_init(&part_obj->parts, session->stack, cm_stack_alloc);
            GS_RETURN_IFERR(cm_galist_new(&part_obj->parts, sizeof(knl_part_def_t), (pointer_t *)&part));
            MEMS_RETURN_IFERR(memset_s(part, sizeof(knl_part_def_t), 0, sizeof(knl_part_def_t)));
            part->name.str = alter_table->part_opt.part_name.str;
            part->name.len = alter_table->part_opt.part_name.len;
            part->initrans = 0;
            part->pctfree = GS_INVALID_ID32;
            part->is_csf = GS_INVALID_ID8;
            part->space.len = 0;
            part->space.str = NULL;
            part->compress_algo = COMPRESS_NONE;
            part->is_parent = GS_FALSE;
            part->storage_def.initial = 0;
            part->storage_def.maxsize = 0;
            part->hiboundval.str = alter_table->part_opt.hiboundval.str;
            part->hiboundval.len = alter_table->part_opt.hiboundval.len;
            part->partkey = (part_key_t *)cm_push(session->stack, GS_MAX_COLUMN_SIZE);
            MEMS_RETURN_IFERR(memset_s(part->partkey, GS_MAX_COLUMN_SIZE, 0, GS_MAX_COLUMN_SIZE));
            part_key_init(part->partkey, 1);
            date_t data;  // 这里把他们转化为以微秒为单位的整数进行保存
            if (cm_text2bigint(&part->hiboundval, &data) != GS_SUCCESS) {
                dc_close(&table_dc);
                return GS_ERROR;
            }
            // 以下填充的数字8是这里用来占位的，可以是其他值，但这里仅限于类型为时间类型
            if (part_put_data(part->partkey, &data, 8, DC_TABLE_PART(&table_dc)->keycols[0].datatype) != GS_SUCCESS) {
                dc_close(&table_dc);
                return GS_ERROR;
            }
            dc_close(&table_dc);
            break;
        }
        case ALTABLE_DROP_PARTITION:{  
            altable_def.part_def.name.str = alter_table->part_opt.part_name.str;
            altable_def.part_def.name.len = alter_table->part_opt.part_name.len;
            break;
        }
        case ALTABLE_MODIFY_TABLE_COMMENT:{
            // TABLE COMMENT
            text_t user = {schema_name, strlen(schema_name)};
            text_t obj_name = {table_name, strlen(table_name)};
            knl_dictionary_t dc;
            if (dc_open(session, &user, &obj_name, &dc) != GS_SUCCESS) {
                if (err_info != NULL){
                    err_info->code = GS_ERRNO;
                    memcpy_s(err_info->message, GS_MESSAGE_BUFFER_SIZE, g_tls_error.message, GS_MESSAGE_BUFFER_SIZE);
                    GS_LOG_RUN_ERR("dc_open err,code:%d,msg:%s\n", err_info->code, err_info->message);
                }
                cm_reset_error();
                return GS_ERROR;
            }
            table_t *table = DC_TABLE(&dc);

            if (alter_table->comment != NULL && strlen(alter_table->comment) > 0) {
                knl_comment_def_t comment_def;
                comment_def.uid = table->desc.uid;
                comment_def.id = table->desc.id;
                comment_def.column_id = GS_INVALID_ID32;
                text_t COMMENT = {alter_table->comment, strlen(alter_table->comment)};
                comment_def.comment = COMMENT;
                comment_def.type = COMMENT_ON_TABLE;
                if (GS_SUCCESS != db_comment_on(session, &comment_def)) {
                    if (err_info != NULL){
                        err_info->code = GS_ERRNO;
                        memcpy_s(err_info->message, GS_MESSAGE_BUFFER_SIZE, g_tls_error.message, GS_MESSAGE_BUFFER_SIZE);
                    }
                    cm_reset_error();
                    dc_close(&dc);
                    return GS_ERROR;
                }
                dc_close(&dc);
                return GS_SUCCESS;
            }
            dc_close(&dc);
            return GS_ERROR;
        }
        case ALTABLE_ADD_CONSTRAINT:{
            altable_def.cons_def.name.str = alter_table->constraint->name.str;
            altable_def.cons_def.name.len = alter_table->constraint->name.len;

            altable_def.cons_def.new_cons.type = alter_table->constraint->type;
            // name of constraint
            altable_def.cons_def.new_cons.name.str = alter_table->constraint->name.str;
            altable_def.cons_def.new_cons.name.len = alter_table->constraint->name.len;

            cm_galist_init(&altable_def.cons_def.new_cons.columns, session->stack, cm_stack_alloc);
            for(int i = 0; i < alter_table->constraint->col_count; i++){
                knl_index_col_def_t *cons_col = NULL;
                GS_RETURN_IFERR(cm_galist_new(&altable_def.cons_def.new_cons.columns, sizeof(knl_index_col_def_t), (pointer_t *)&cons_col));
                MEMS_RETURN_IFERR(memset_s(cons_col, sizeof(knl_index_col_def_t), 0, sizeof(knl_index_col_def_t)));
                cons_col->name.str = alter_table->constraint->cols[i].str;
                cons_col->name.len = alter_table->constraint->cols[i].len;
                cons_col->mode = SORT_MODE_ASC;
                cons_col->is_func = GS_FALSE;
            }
            text_t user = {schema_name, strlen(schema_name)};
            altable_def.cons_def.new_cons.index.user = user;

            // table of constraint
            altable_def.cons_def.new_cons.index.table.str = (char *)table_name;
            altable_def.cons_def.new_cons.index.table.len = strlen(table_name);

            altable_def.cons_def.new_cons.index.name.str = alter_table->constraint->name.str;
            altable_def.cons_def.new_cons.index.name.len = alter_table->constraint->name.len;

            altable_def.cons_def.new_cons.index.space = g_users;

            altable_def.cons_def.new_cons.index.primary = (alter_table->constraint->type == CONS_TYPE_PRIMARY) ? GS_TRUE : GS_FALSE;
            altable_def.cons_def.new_cons.index.unique = (alter_table->constraint->type == CONS_TYPE_UNIQUE) ? GS_TRUE : (alter_table->constraint->type == CONS_TYPE_PRIMARY) ? GS_TRUE : GS_FALSE;

            altable_def.cons_def.new_cons.index.cr_mode = CR_PAGE;
            altable_def.cons_def.new_cons.index.options |= CREATE_IF_NOT_EXISTS;
            altable_def.cons_def.new_cons.index.online = GS_FALSE;    
            altable_def.cons_def.new_cons.index.is_func = GS_FALSE;
            altable_def.cons_def.new_cons.index.parted = GS_FALSE;
            altable_def.cons_def.new_cons.index.parallelism = GS_FALSE;

            cm_galist_init(&altable_def.cons_def.new_cons.index.columns, session->stack, cm_stack_alloc);
            for(int i = 0; i < alter_table->constraint->col_count; i++){
                knl_index_col_def_t *idx_col = NULL;
                GS_RETURN_IFERR(cm_galist_new(&altable_def.cons_def.new_cons.index.columns, sizeof(knl_index_col_def_t), (pointer_t *)&idx_col));
                MEMS_RETURN_IFERR(memset_s(idx_col, sizeof(knl_index_col_def_t), 0, sizeof(knl_index_col_def_t)));
                idx_col->name.str = alter_table->constraint->cols[i].str;
                idx_col->name.len = alter_table->constraint->cols[i].len;
                idx_col->mode = SORT_MODE_ASC;
                idx_col->is_func = GS_FALSE;
            }
            break;    
        }   
        default:{
            GS_THROW_ERROR(ERR_UNSUPPORT_OPER_TYPE, "alter table action", altable_def.action);
            if (err_info != NULL) {
                err_info->code = GS_ERRNO;
                memcpy_s(err_info->message, GS_MESSAGE_BUFFER_SIZE, g_tls_error.message, GS_MESSAGE_BUFFER_SIZE);
                GS_LOG_RUN_WAR("sqlapi_gstor_alter_table err,code:%d,msg:%s\n", err_info->code, err_info->message);
            }
            cm_reset_error();
            CM_RESTORE_STACK(session->stack);
            return GS_ERROR;
        }
    }

    status_t ret = knl_alter_table(session, session->stack, &altable_def);
    if (ret != GS_SUCCESS) {
        if (err_info != NULL) {
            err_info->code = GS_ERRNO;
            memcpy_s(err_info->message, GS_MESSAGE_BUFFER_SIZE, g_tls_error.message, GS_MESSAGE_BUFFER_SIZE);
            GS_LOG_RUN_WAR("knl_alter_table err,code:%d,msg:%s\n", err_info->code, err_info->message);
        }
        cm_reset_error();
    }

    CM_RESTORE_STACK(session->stack);
    return ret;
}

status_t gstor_create_sequence(void *handle, const char *owner, sequence_def_t *sequence_info)
{
    cm_reset_error();
    knl_session_t *session = EC_SESSION(handle);
    knl_sequence_def_t def = {0};

    if (!owner) {
        text_t owner_sys = { (char *)"SYS", 3 };
        def.user = owner_sys;
    } else {
        def.user.str = owner;
        def.user.len = strlen(owner);
    }

    if (!sequence_info->name) {
        return GS_ERROR;
    } else {
        def.name.str = sequence_info->name;
        def.name.len = strlen(sequence_info->name);
    }

    def.start = sequence_info->start_value;
    def.step = sequence_info->increment;
    def.min_value = sequence_info->min_value;
    def.max_value = sequence_info->max_value;
    def.is_cycle = sequence_info->is_cycle;
    def.nocache = 1;

    return knl_create_sequence(session, &def);
}

status_t gstor_create_user_view(void *handle, exp_view_def_t *view_def, int columnCount, exp_column_def_t *column_defs)
{
    cm_reset_error();
    knl_session_t *session = EC_SESSION(handle);

    CM_SAVE_STACK(session->stack);
	cm_galist_init(&view_def->columns, session->stack, cm_stack_alloc);
    knl_column_def_t* column_t = NULL;
    for (int idx = 0; idx < columnCount; idx ++) {
		GS_RETURN_IFERR(cm_galist_new(&view_def->columns, sizeof(knl_column_def_t), (pointer_t *)&column_t));
        MEMS_RETURN_IFERR(memset_s(column_t, sizeof(knl_column_def_t), 0, sizeof(knl_column_def_t)));
        column_t->name.len = column_defs[idx].name.len;
        column_t->name.str = column_defs[idx].name.str;
        column_t->datatype = column_defs[idx].col_type;
        column_t->size = column_defs[idx].size;
        column_t->nullable = column_defs[idx].nullable;          
        column_t->precision = column_defs[idx].precision;
        column_t->scale = column_defs[idx].scale;
    }

    CM_RESTORE_STACK(session->stack);
    knl_view_def_t knl_view_def = { view_def->uid,         view_def->name,     view_def->user,       view_def->columns,
                                    view_def->sub_sql,     view_def->sql_tpye, view_def->is_replace, OBJ_STATUS_VALID,
                                    view_def->ref_objects, view_def->select };
    return knl_create_view(session, &knl_view_def);
}
status_t gstor_seq_currval(void *handle, const char *owner, const char *name, int64_t *val)
{
    text_t seq_name;
    seq_name.str = (char *)name;
    seq_name.len = strlen(name);
    text_t owner_sys = { (char *)"SYS", 3 };
    return knl_seq_currval(EC_SESSION(handle), &owner_sys, &seq_name, val);
}

status_t gstor_seq_nextval(void *handle, const char *owner, const char *name, int64_t *val)
{
    text_t seq_name;
    seq_name.str = (char *)name;
    seq_name.len = strlen(name);
    text_t owner_sys = { (char *)"SYS", 3 };
    return knl_seq_nextval(EC_SESSION(handle), &owner_sys, &seq_name, val);
}

status_t gstor_alter_seq_nextval(void *handle, const char *owner, const char *name, int64_t val)
{
    knl_sequence_def_t def = {0};

    text_t owner_sys = { (char *)"SYS", 3 };
    def.user = owner_sys;
    def.name.str = (char *)name;
    def.name.len = strlen(name);
    return knl_alter_seq_nextval(EC_SESSION(handle), &def, val);
}

status_t gstor_is_sequence_exist(void *handle, const char *owner, const char *name, bool32 *is_exist) {
    knl_sequence_def_t def = {0};
    text_t owner_sys = { (char *)"SYS", 3 };
    text_t seq_name = { (char *)name, strlen(name) }; 
    status_t ret = knl_get_seq_def(EC_SESSION(handle), &owner_sys, &seq_name, &def);
    if(ret == GS_SUCCESS) {
        *is_exist = GS_TRUE;
    } else if(ret == GS_ERROR){
        if(GS_ERRNO == ERR_SEQ_NOT_EXIST) {
            *is_exist = GS_FALSE;
            return GS_SUCCESS;
        }
    }
    return ret;
}

status_t gstor_drop(void *handle, char *owner, drop_def_t *drop_info)
{
    cm_reset_error();
    knl_session_t *session = EC_SESSION(handle);
    knl_drop_def_t def;
    status_t status;

    if (!owner) {
        text_t owner_sys = { (char *)"SYS", 3 };
        def.owner = owner_sys;
    } else {
        def.owner.str = owner;
        def.owner.len = strlen(owner);
    }

    if (!drop_info->name) {
        return GS_ERROR;
    } else {
        def.name.str = drop_info->name;
        def.name.len = strlen(drop_info->name);
    }

    if (drop_info->if_exists) {
        def.options = DROP_IF_EXISTS;
    } else {
        def.options = DROP_DIRECTLY;
    }
    if (drop_info->type == DROP_TYPE_TABLE) {
        def.purge = GS_TRUE;
    }
    def.temp = GS_FALSE;

    def.ex_name.len = 0;
    def.ex_name.str = NULL;

    switch (drop_info->type) {
        case DROP_TYPE_TABLE:
            status = knl_drop_table(session, &def);
            break;
        case DROP_TYPE_VIEW:
            status = knl_drop_view(session, &def);
            break;
        case DROP_TYPE_SEQUENCE:
            status = knl_drop_sequence(session, &def);
            break;
        case DROP_TYPE_INDEX:
            status = knl_drop_index(session, &def);
            break;
        default:
            status = GS_ERROR;
    }

    return status;
}


status_t gstor_modified_partno(void *handle,size_t cursor_idx,int partno) {
    if(EC_CURSOR_IDX(handle,cursor_idx)){
        EC_CURSOR_IDX(handle,cursor_idx)->part_loc.part_no = partno;
    } else {
        knl_session_t *session = EC_SESSION(handle);
        instance_t *cc_instance = session->kernel->server; 
        if(knl_alloc_cursor(cc_instance, &EC_CURSOR_IDX(handle,cursor_idx)) != GS_SUCCESS) {
            return GS_ERROR;
        }
        EC_CURSOR_IDX(handle,cursor_idx)->part_loc.part_no = partno;
    }
    return GS_SUCCESS;
}

status_t gstor_force_checkpoint(void *handle){
    knl_session_t *session = EC_SESSION(handle);
    return knl_checkpoint(session,CKPT_TYPE_LOCAL);
}

status_t gstor_comment_on(void *handle, exp_comment_def_t *comment_def) {
    knl_session_t *session = EC_SESSION(handle);

    knl_comment_def_t def;
    def.type = comment_def->type;
    def.owner = comment_def->owner;
    def.name = comment_def->name;
    def.column = comment_def->column;
    def.comment = comment_def->comment;
    def.uid = comment_def->uid;
    def.id = comment_def->id;
    def.column_id = comment_def->column_id;
    if (def.owner.len < 1) {
        def.owner.str = (char *)"SYS";
        def.owner.len = 3;
    }
    return knl_comment_on(session, &def);
}

int32_t db_is_open(void *db_instance) {
    instance_t *ins = (instance_t*)db_instance;

    return ins->kernel.db.status == DB_STATUS_OPEN 
                    && ins->kernel.dc_ctx.completed;
}

uint32_t get_streamagg_threadpool_num(void *db_instance) {
    instance_t *ins = (instance_t*)db_instance;

    return ins->kernel.attr.exec_agg_thread_num;
}

int32_t get_ts_cagg_switch_on(void *db_instance) {
    instance_t *ins = (instance_t*)db_instance;

    return ins->kernel.attr.enable_ts_cagg;
}

char * get_database_home_path(void *db_instance) {
    instance_t *ins = (instance_t*)db_instance;
    
    return &ins->kernel.home;
}

status_t gstor_set_is_begin_transaction(void *handle, uint8 flag) {
    knl_session_t *session = EC_SESSION(handle);
    session->is_begin_transaction = flag;

    return GS_SUCCESS;
}


int64 gstor_get_sql_engine_memory_limit(void *handle) {
    instance_t *ins = (instance_t*)handle;
    return ins->kernel.attr.max_sql_engine_memory;
}

uint32_t gstor_get_max_connections(void *handle) {
    knl_session_t *session = EC_SESSION(handle);
    instance_t *ins = session->kernel->server;

    return ins->kernel.attr.max_conn_num;
}

uint32_t gstor_set_max_connections(void *handle, uint32_t max_conn) {
    knl_session_t *session = EC_SESSION(handle);
    instance_t *ins = session->kernel->server;

    ins->kernel.attr.max_conn_num = max_conn;

    char conn_num[5] = {};
    sprintf(conn_num, "%ld", max_conn);
    cm_alter_config(ins->cc_config, "MAX_CONN_NUM", conn_num, CONFIG_SCOPE_BOTH, GS_TRUE);

    return ins->kernel.attr.max_conn_num;
}


int32_t get_ts_update_switch_on(void *handle) {
    knl_session_t *session = EC_SESSION(handle);
    instance_t *ins = session->kernel->server;    
    return ins->kernel.attr.enable_ts_update;
}

// ---------------------------------for user-----------------------------//
bool32 gstor_get_user_id(void *handle, const char *user_name, uint32 *id) {
    knl_session_t *session = EC_SESSION(handle);

    text_t user = {user_name, strlen(user_name)};
    return knl_get_user_id(session, &user, id);
}

bool32 gstor_get_role_id(void *handle, const char *role_name, uint32 *id) {
    knl_session_t *session = EC_SESSION(handle);

    text_t role = {role_name, strlen(role_name)};
    return knl_get_role_id(session, &role, id);
}

bool32 gstor_verify_user_password(void *handle, const char *user_name, const char *password) {
    knl_session_t *session = EC_SESSION(handle);

    text_t user = {user_name, strlen(user_name)};
    return dc_verify_user_password(session, &user, password);
}

status_t gstor_create_role(void *handle, knl_role_def_t *def) {
    knl_session_t *session = EC_SESSION(handle);

    return knl_create_role(session, def);
}

status_t gstor_create_user(void *handle, knl_user_def_t *def) {
    knl_session_t *session = EC_SESSION(handle);

    return knl_create_user(session, def);
}

status_t gstor_drop_role(void *handle, knl_drop_def_t *def) {
    knl_session_t *session = EC_SESSION(handle);

    return knl_drop_role(session, def);
}

status_t gstor_drop_user(void *handle, knl_drop_user_t *def) {
    knl_session_t *session = EC_SESSION(handle);

    return knl_drop_user(session, def);
}

status_t gstor_grant_privilege(void *handle, knl_grant_def_t *def) {
    knl_session_t *session = EC_SESSION(handle);

    return knl_exec_grant_privs(session, def);
}

status_t gstor_revoke_privilege(void *handle, knl_revoke_def_t *def) {
    knl_session_t *session = EC_SESSION(handle);

    return knl_exec_revoke_privs(session, def);
}

// -------------------------------end for user-----------------------------//

#ifdef __cplusplus
}
#endif
