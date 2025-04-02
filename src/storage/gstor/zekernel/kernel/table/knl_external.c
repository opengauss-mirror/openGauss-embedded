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
 * knl_external.c
 *    implement of external table
 *
 * IDENTIFICATION
 *    src/storage/gstor/zekernel/kernel/table/knl_external.c
 *
 * -------------------------------------------------------------------------
 */
#include "knl_external.h"
#include "cm_file.h"
#include "knl_privilege.h"


static status_t external_fill_column_to_row(knl_session_t *session, knl_cursor_t *cursor, row_assist_t *ra,
    text_t *column, uint16 col_id)
{
    dc_entity_t *entity = (dc_entity_t *)cursor->dc_entity;
    knl_column_t *dc_column = dc_get_column(entity, col_id);
    status_t status = GS_SUCCESS;
    variant_t tmp_value;
    int64 val_bigint;
    int32 val_int;
    uint32 val_uint32;
    double val_double;
    binary_t tmp_binary;
    errno_t ret;

    cm_trim_text(column);
    if (column->len == 0) {
        if (dc_column->nullable != GS_TRUE) {
            GS_THROW_ERROR(ERR_COLUMN_NOT_NULL, dc_column->name);
            return GS_ERROR;
        }

        row_put_null(ra);
        return GS_SUCCESS;
    }

    switch (dc_column->datatype) {
        case GS_TYPE_BOOLEAN:
            if (cm_text2bool(column, &tmp_value.v_bool) != GS_SUCCESS) {
                return GS_ERROR;
            }
            status = row_put_bool(ra, tmp_value.v_bool);
            break;
        case GS_TYPE_UINT32:
            if (cm_text2uint32(column, &val_uint32) != GS_SUCCESS) {
                return GS_ERROR;
            }
            status = row_put_uint32(ra, val_uint32);
            break;
        case GS_TYPE_INTEGER:
            if (cm_text2int(column, &val_int) != GS_SUCCESS) {
                return GS_ERROR;
            }
            status = row_put_int32(ra, val_int);
            break;
        case GS_TYPE_BIGINT:
            if (cm_text2bigint(column, &val_bigint) != GS_SUCCESS) {
                return GS_ERROR;
            }
            status = row_put_int64(ra, val_bigint);
            break;
        case GS_TYPE_REAL:
            if (cm_text2real(column, &val_double) != GS_SUCCESS) {
                return GS_ERROR;
            }

            if (cm_adjust_double(&val_double, dc_column->precision, dc_column->scale) != GS_SUCCESS) {
                return GS_ERROR;
            }

            status = row_put_real(ra, val_double);
            break;
        case GS_TYPE_NUMBER:
        case GS_TYPE_DECIMAL:
            if (cm_text_to_dec8(column, &tmp_value.v_dec) != GS_SUCCESS) {
                return GS_ERROR;
            }

            if (cm_adjust_dec8(&tmp_value.v_dec, dc_column->precision, dc_column->scale) != GS_SUCCESS) {
                return GS_ERROR;
            }

            status = row_put_dec8(ra, &tmp_value.v_dec);
            break;
        case GS_TYPE_DATE:
        case GS_TYPE_TIMESTAMP:
        case GS_TYPE_TIMESTAMP_TZ_FAKE:
        case GS_TYPE_TIMESTAMP_LTZ:
            if (cm_text2date(column, NULL, &tmp_value.v_date) != GS_SUCCESS) {
                return GS_ERROR;
            }
            status = row_put_date(ra, tmp_value.v_date);
            break;

        case GS_TYPE_TIMESTAMP_TZ:
            if (cm_text2timestamp_tz(column, NULL, cm_get_local_tzoffset(), &tmp_value.v_tstamp_tz) != GS_SUCCESS) {
                return GS_ERROR;
            }
            status = row_put_timestamp_tz(ra, &tmp_value.v_tstamp_tz);
            break;

        case GS_TYPE_INTERVAL_DS:
            if (cm_text2dsinterval(column, &tmp_value.v_itvl_ds) != GS_SUCCESS) {
                return GS_ERROR;
            }
            status = row_put_dsinterval(ra, tmp_value.v_itvl_ds);
            break;

        case GS_TYPE_INTERVAL_YM:
            if (cm_text2yminterval(column, &tmp_value.v_itvl_ym) != GS_SUCCESS) {
                return GS_ERROR;
            }
            status = row_put_yminterval(ra, tmp_value.v_itvl_ym);
            break;

        case GS_TYPE_CHAR:
        case GS_TYPE_VARCHAR:
        case GS_TYPE_STRING:
            if (column->len > dc_column->size) {
                GS_THROW_ERROR(ERR_EXCEED_MAX_FIELD_LEN, dc_column->name, column->len, dc_column->size);
                return GS_ERROR;
            }
            status = row_put_text(ra, column);
            break;
        case GS_TYPE_BINARY:
            if (column->len > dc_column->size) {
                GS_THROW_ERROR(ERR_EXCEED_MAX_FIELD_LEN, dc_column->name, column->len, dc_column->size);
                return GS_ERROR;
            } else if (column->len < dc_column->size) {
                CM_SAVE_STACK(session->stack);
                tmp_binary.bytes = (uint8 *)cm_push(session->stack, dc_column->size);
                tmp_binary.size = dc_column->size;

                ret = memcpy_sp(tmp_binary.bytes, tmp_binary.size, column->str, column->len);
                knl_securec_check(ret);

                ret = memset_sp((char *)tmp_binary.bytes + column->len, dc_column->size - column->len, 0,
                                dc_column->size - column->len);
                knl_securec_check(ret);
                status = row_put_bin(ra, &tmp_binary);
                CM_RESTORE_STACK(session->stack);
            } else {
                tmp_binary.bytes = (uint8 *)column->str;
                tmp_binary.size = column->len;
                status = row_put_bin(ra, &tmp_binary);
            }
            break;
        case GS_TYPE_VARBINARY:
        case GS_TYPE_RAW:
            if (column->len > dc_column->size) {
                GS_THROW_ERROR(ERR_EXCEED_MAX_FIELD_LEN, dc_column->name, column->len, dc_column->size);
                return GS_ERROR;
            }

            tmp_binary.bytes = (uint8 *)column->str;
            tmp_binary.size = column->len;
            status = row_put_bin(ra, &tmp_binary);
            break;
        case GS_TYPE_CLOB:
        case GS_TYPE_BLOB:
        case GS_TYPE_IMAGE:
            GS_THROW_ERROR(ERR_OPERATIONS_NOT_SUPPORT, "lob type", "external organized table");
            return GS_ERROR;
        default:
            row_put_null(ra);
            break;
    }
    
    return status;
}

static status_t external_heap_get_row(knl_session_t *session, knl_cursor_t *cursor, bool32 *is_found)
{
    row_assist_t ra;
    text_t line;
    text_t column;
    uint32 column_count;
    uint32 column_id = 0;
    dc_entity_t *entity = (dc_entity_t *)cursor->dc_entity;
    knl_ext_desc_t *external_desc = ((table_t *)cursor->table)->desc.external_desc;

    *is_found = GS_FALSE;

    CM_NULL_TERM(&cursor->text);
    cm_trim_text(&cursor->text);

    if (!cm_fetch_text(&cursor->text, external_desc->records_delimiter, 0, &line)) {
        cursor->text.len = 0;
        return GS_SUCCESS;
    }

    column_count = knl_get_column_count(entity);
    row_init(&ra, (char *)cursor->row, DEFAULT_PAGE_SIZE, column_count);

    while (cm_fetch_text(&line, external_desc->fields_terminator, 0, &column)) {
        if (column_id >= column_count) {
            GS_THROW_ERROR(ERR_LINE_SIZE_TOO_LONG, column_count);
            return GS_ERROR;
        }
        /* the value of column_id is no larger than 65535 */
        if (external_fill_column_to_row(session, cursor, &ra, &column, column_id) != GS_SUCCESS) {
            return GS_ERROR;
        }

        column_id++;
    }

    if (column_id < column_count) {
        GS_THROW_ERROR(ERR_LINE_SIZE_TOO_LONG, column_count);
        return GS_ERROR;
    }

    *is_found = GS_TRUE;

    return GS_SUCCESS;
}

static status_t external_heap_fetch_by_page(knl_session_t *session, knl_cursor_t *cursor, bool32 *is_found)
{
    int32 read_size;
    knl_ext_desc_t *external_desc = ((table_t *)cursor->table)->desc.external_desc;

    *is_found = GS_FALSE;

    for (;;) {
        if (cursor->text.len == 0) {
            if (cm_read_file(cursor->fd, cursor->page_buf, DEFAULT_PAGE_SIZE, &read_size) != GS_SUCCESS) {
                GS_LOG_RUN_ERR("[EXTERNAL] failed to read data from %s/%s",
                               external_desc->directory, external_desc->location);
                return GS_ERROR;
            }

            if (read_size == 0) {
                cursor->eof = GS_TRUE;
                cm_close_file(cursor->fd);
                cursor->fd = -1;
                break;
            }

            cursor->text.str = (char *)cursor->page_buf;
            cursor->text.len = (uint32)read_size;

            if (read_size == DEFAULT_PAGE_SIZE) {
                while (cursor->text.len > 0 &&
                       cursor->text.str[cursor->text.len - 1] != external_desc->records_delimiter) {
                    cursor->text.len--;
                }

                if (cursor->text.len == 0) {
                    GS_THROW_ERROR(ERR_VALUE_ERROR, "the size of row is too large.");
                    return GS_ERROR;
                }

                if (cm_seek_file(cursor->fd, (int32)cursor->text.len - read_size, SEEK_CUR) == -1) {
                    GS_THROW_ERROR(ERR_SEEK_FILE, (int32)cursor->text.len - read_size, SEEK_SET, errno);
                    return GS_ERROR;
                }
            }
        }

        if (external_heap_get_row(session, cursor, is_found) != GS_SUCCESS) {
            return GS_ERROR;
        }

        if (*is_found) {
            return GS_SUCCESS;
        }
    }

    return GS_SUCCESS;
}

status_t external_heap_fetch(knl_handle_t handle, knl_cursor_t *cursor)
{
    knl_session_t *session = (knl_session_t *)handle;

    for (;;) {
        if (cursor->eof) {
            return GS_SUCCESS;
        }

        if (external_heap_fetch_by_page(session, cursor, &cursor->is_found) != GS_SUCCESS) {
            cm_close_file(cursor->fd);
            cursor->fd = -1;
            return GS_ERROR;
        }

        if (!cursor->is_found) {
            continue;
        }

        if (knl_match_cond(session, cursor, &cursor->is_found) != GS_SUCCESS) {
            cm_close_file(cursor->fd);
            cursor->fd = -1;
            return GS_ERROR;
        }

        if (!cursor->is_found) {
            continue;
        }

        return GS_SUCCESS;
    }
}

static status_t db_write_sys_directory(knl_session_t *session, knl_directory_desc_t *desc)
{
    row_assist_t ra;
    table_t *table = NULL;
    knl_cursor_t *cursor = NULL;

    CM_SAVE_STACK(session->stack);
    cursor = knl_push_cursor(session);
    knl_open_sys_cursor(session, cursor, CURSOR_ACTION_INSERT, SYS_DIRECTORY_ID, GS_INVALID_ID32);
    table = (table_t *)cursor->table;
    row_init(&ra, (char *)cursor->row, HEAP_MAX_ROW_SIZE, table->desc.column_count);

    (void)row_put_uint32(&ra, desc->uid);
    (void)row_put_str(&ra, desc->name);
    (void)row_put_str(&ra, desc->path);

    if (knl_internal_insert(session, cursor) != GS_SUCCESS) {
        CM_RESTORE_STACK(session->stack);
        return GS_ERROR;
    }

    CM_RESTORE_STACK(session->stack);
    return GS_SUCCESS;
}

static status_t db_delete_sys_directory(knl_session_t *session, char *name)
{
    knl_cursor_t *cursor = NULL;

    CM_SAVE_STACK(session->stack);
    cursor = knl_push_cursor(session);
    knl_open_sys_cursor(session, cursor, CURSOR_ACTION_DELETE, SYS_DIRECTORY_ID, IX_DIRECTORY_001_ID);
    knl_init_index_scan(cursor, GS_TRUE);
    knl_set_scan_key(INDEX_DESC(cursor->index), &cursor->scan_range.l_key, GS_TYPE_STRING, name,
                     (uint16)strlen(name), IX_COL_SYS_DIRECTORY_NAME);

    if (knl_fetch(session, cursor) != GS_SUCCESS) {
        CM_RESTORE_STACK(session->stack);
        return GS_ERROR;
    }

    if (cursor->eof) {
        GS_THROW_ERROR(ERR_OBJECT_NOT_EXISTS, "directory", name);
        CM_RESTORE_STACK(session->stack);
        return GS_ERROR;
    }

    if (knl_internal_delete(session, cursor) != GS_SUCCESS) {
        CM_RESTORE_STACK(session->stack);
        return GS_ERROR;
    }

    CM_RESTORE_STACK(session->stack);
    return GS_SUCCESS;
}

static status_t db_revoke_privs_on_directory(knl_session_t *session, assist_obj_priv_item_t *item)
{
    uint32 grantee_id, grantor_id;
    uint32 priv_id;
    uint32 grantee_type;
    uint32 own_uid = DB_SYS_USER_ID;
    uint32 obj_type = OBJ_TYPE_DIRECTORY;
    knl_cursor_t *cursor = NULL;
    
    CM_SAVE_STACK(session->stack);
    cursor = knl_push_cursor(session);
    knl_open_sys_cursor(session, cursor, CURSOR_ACTION_SELECT, OBJECT_PRIVS_ID, IX_SYS_OBJECT_PRIVS_002_ID);
    knl_init_index_scan(cursor, GS_TRUE);

    knl_set_scan_key(INDEX_DESC(cursor->index), &cursor->scan_range.l_key, GS_TYPE_INTEGER, (void *)&own_uid,
                     sizeof(uint32), IX_COL_SYS_OBJECT_PRIVS_002_OBJECT_OWNER);
    knl_set_scan_key(INDEX_DESC(cursor->index), &cursor->scan_range.l_key, GS_TYPE_STRING, (void *)item->objname,
                     (uint16)strlen(item->objname), IX_COL_SYS_OBJECT_PRIVS_002_OBJECT_NAME);
    knl_set_scan_key(INDEX_DESC(cursor->index), &cursor->scan_range.l_key, GS_TYPE_INTEGER, (void *)&obj_type,
                     sizeof(uint32), IX_COL_SYS_OBJECT_PRIVS_002_OBJECT_TYPE);

    if (knl_fetch(session, cursor) != GS_SUCCESS) {
        CM_RESTORE_STACK(session->stack);
        return GS_ERROR;
    }

    while (!cursor->eof) {
        grantee_type = *(uint32 *)CURSOR_COLUMN_DATA(cursor, OBJECT_PRIVS_COL_GRANTEE_TYPE);
        grantee_id = *(uint32 *)CURSOR_COLUMN_DATA(cursor, OBJECT_PRIVS_COL_GRANTEE);
        grantor_id = *(uint32 *)CURSOR_COLUMN_DATA(cursor, OBJECT_PRIVS_COL_GRANTOR);
        priv_id = *(uint32 *)CURSOR_COLUMN_DATA(cursor, OBJECT_PRIVS_COL_PRIVILEGE);
        item->privid = priv_id;
        if (db_revoke_dirpriv_from_grantee(session, grantor_id, grantee_id, grantee_type, item) != GS_SUCCESS) {
            CM_RESTORE_STACK(session->stack);
            return GS_ERROR;
        }

        if (knl_fetch(session, cursor) != GS_SUCCESS) {
            CM_RESTORE_STACK(session->stack);
            return GS_ERROR;
        }
    }

    CM_RESTORE_STACK(session->stack);
    return GS_SUCCESS;
}

status_t db_update_directory_path(knl_session_t *session, knl_directory_desc_t *desc)
{
    row_assist_t ra;
    knl_cursor_t *cursor = NULL;

    CM_SAVE_STACK(session->stack);
    cursor = knl_push_cursor(session);
    knl_open_sys_cursor(session, cursor, CURSOR_ACTION_UPDATE, SYS_DIRECTORY_ID, IX_DIRECTORY_001_ID);
    knl_init_index_scan(cursor, GS_TRUE);
    knl_set_scan_key(INDEX_DESC(cursor->index), &cursor->scan_range.l_key, GS_TYPE_STRING, desc->name,
                     (uint16)strlen(desc->name), IX_COL_SYS_DIRECTORY_NAME);

    if (knl_fetch(session, cursor) != GS_SUCCESS) {
        CM_RESTORE_STACK(session->stack);
        return GS_ERROR;
    }

    knl_panic_log(!cursor->eof, "data is not found, panic info: page %u-%u type %u table %s index %s",
                  cursor->rowid.file, cursor->rowid.page, ((page_head_t *)cursor->page_buf)->type,
                  ((table_t *)cursor->table)->desc.name, ((index_t *)cursor->index)->desc.name);

    row_init(&ra, cursor->update_info.data, HEAP_MAX_ROW_SIZE, 1);
    (void)row_put_str(&ra, desc->path);
    cursor->update_info.count = 1;
    cursor->update_info.columns[0] = SYS_DIRECTORIES_COL_DIRE_PATH;
    cm_decode_row(cursor->update_info.data, cursor->update_info.offsets, cursor->update_info.lens, NULL);

    if (knl_internal_update(session, cursor) != GS_SUCCESS) {
        CM_RESTORE_STACK(session->stack);
        return GS_ERROR;
    }

    CM_RESTORE_STACK(session->stack);
    return GS_SUCCESS;
}

static status_t db_prepare_create_directory(knl_directory_def_t *def, knl_directory_desc_t *desc)
{
    if (def->name.len > GS_MAX_NAME_LEN - 1) {
        GS_THROW_ERROR(ERR_NAME_TOO_LONG, "directory", def->name.len, GS_MAX_NAME_LEN - 1);
        return GS_ERROR;
    }

    if (def->path.len > GS_MAX_PATH_BUFFER_SIZE - 1) {
        GS_THROW_ERROR(ERR_FILE_PATH_TOO_LONG, GS_MAX_PATH_BUFFER_SIZE - 1);
        return GS_ERROR;
    }

    desc->uid = 0;
    if (cm_text2str(&def->name, desc->name, GS_MAX_NAME_LEN) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (cm_text2str(&def->path, desc->path, GS_MAX_PATH_BUFFER_SIZE) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (strlen(desc->path) == 0 || !cm_dir_exist(desc->path)) {
        GS_THROW_ERROR(ERR_PATH_NOT_EXIST, desc->path);
        return GS_ERROR;
    }

    if (access(desc->path, R_OK) != 0) {
        GS_THROW_ERROR_EX(ERR_SQL_SYNTAX_ERROR, "%s is not an readable folder", desc->path);
        return GS_ERROR;
    }

#ifndef WIN32
    if (cm_verify_file_host(desc->path) != GS_SUCCESS) {
        GS_THROW_ERROR(ERR_FILE_EXEC_PRIV, desc->path);
        return GS_ERROR;
}
#endif

    return GS_SUCCESS;
}

static status_t db_create_directory_with_replace(knl_session_t *session, knl_directory_desc_t *desc)
{
    text_t dir_name;
    dc_user_t *user = NULL;
    bool32 has_priv = GS_FALSE;
    dc_obj_priv_entry_t *entry = NULL;  
    
    if (db_update_directory_path(session, desc) != GS_SUCCESS) {
        return GS_ERROR;
    }
        
    cm_str2text(desc->name, &dir_name);

    if (dc_open_user_by_id(session, session->uid, &user) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (user->desc.id == DB_SYS_USER_ID) {
        return GS_SUCCESS;
    }
    
    if (dc_find_objpriv_entry(&user->obj_privs, DB_SYS_USER_ID, &dir_name, OBJ_TYPE_DIRECTORY, &entry)) {
        if (DC_HAS_OBJ_PRIV(entry->priv_item.direct_grant, GS_PRIV_DIRE_READ)) {
            has_priv = GS_TRUE;
        }
    }

    if (!has_priv) {
        if (db_grant_dirpriv_to_user(session, desc->name, session->uid, GS_PRIV_DIRE_READ) != GS_SUCCESS) {
            return GS_ERROR;
        }
    }
    
    return GS_SUCCESS;
}

status_t db_create_directory(knl_session_t *session, knl_directory_def_t *def)
{
    knl_directory_desc_t desc;
    bool32 is_found = GS_FALSE;
    dc_context_t *ctx = &session->kernel->dc_ctx;
    
    if (DB_NOT_READY(session)) {
        GS_THROW_ERROR(ERR_NO_DB_ACTIVE);
        return GS_ERROR;
    }

    cm_spin_lock(&ctx->paral_lock, NULL);

    if (db_prepare_create_directory(def, &desc) != GS_SUCCESS) {
        cm_spin_unlock(&ctx->paral_lock);
        return GS_ERROR;
    }

    if (db_fetch_directory_path(session, desc.name, NULL, 0, &is_found) != GS_SUCCESS) {
        cm_spin_unlock(&ctx->paral_lock);
        return GS_ERROR;
    }

    if (is_found && def->is_replace) {
        if (db_create_directory_with_replace(session, &desc) != GS_SUCCESS) {
            cm_spin_unlock(&ctx->paral_lock);
            return GS_ERROR;
        }
    } else if (is_found && !def->is_replace) {
        GS_THROW_ERROR(ERR_OBJECT_EXISTS, "directory", desc.name);
        cm_spin_unlock(&ctx->paral_lock);
        return GS_ERROR;
    } else {
        if (db_write_sys_directory(session, &desc) != GS_SUCCESS) {
            cm_spin_unlock(&ctx->paral_lock);
            return GS_ERROR;
        }

        /* the user who create the directory has all directory privilege on it. 
         * but on current, we only surpport read on directory */
        if (db_grant_dirpriv_to_user(session, desc.name, session->uid, GS_PRIV_DIRE_READ) != GS_SUCCESS) {
            cm_spin_unlock(&ctx->paral_lock);
            return GS_ERROR;
        }
    }

    cm_spin_unlock(&ctx->paral_lock);
    return GS_SUCCESS;
}

status_t db_drop_directory(knl_session_t *session, knl_drop_def_t *def)
{
    dc_context_t *ctx = &session->kernel->dc_ctx;
    assist_obj_priv_item_t item;
    item.objowner = DB_SYS_USER_ID;
    item.objtype = OBJ_TYPE_DIRECTORY;

    if (DB_NOT_READY(session)) {
        GS_THROW_ERROR(ERR_NO_DB_ACTIVE);
        return GS_ERROR;
    }
    
    cm_spin_lock(&ctx->paral_lock, NULL);
    if (def->name.len > GS_MAX_NAME_LEN - 1) {
        GS_THROW_ERROR(ERR_NAME_TOO_LONG, "directory", def->name.len, GS_MAX_NAME_LEN - 1);
        cm_spin_unlock(&ctx->paral_lock);
        return GS_ERROR;
    }

    if (cm_text2str(&def->name, item.objname, GS_MAX_NAME_LEN) != GS_SUCCESS) {
        cm_spin_unlock(&ctx->paral_lock);
        return GS_ERROR;
    }
    
    if (db_delete_sys_directory(session, item.objname) != GS_SUCCESS) {
        cm_spin_unlock(&ctx->paral_lock);
        return GS_ERROR;
    }

    /* revoke all privilege on the directory */
    if (db_revoke_privs_on_directory(session, &item) != GS_SUCCESS) {
        cm_spin_unlock(&ctx->paral_lock);
        return GS_ERROR;
    }

    cm_spin_unlock(&ctx->paral_lock);
    return GS_SUCCESS;
}

status_t db_fetch_directory_path(knl_session_t *session, const char *dir_name, char *dire_path,
    uint32 dire_len, bool32 *is_found)
{
    errno_t ret;
    char *path_buffer = NULL;
    uint32 path_len;
    knl_cursor_t *cursor = NULL;
    char name_buffer[GS_FILE_NAME_BUFFER_SIZE] = { 0 };

    *is_found = GS_FALSE;
    size_t dir_name_size = strlen(dir_name);
    ret = memcpy_sp(name_buffer, GS_FILE_NAME_BUFFER_SIZE, dir_name, dir_name_size);
    knl_securec_check(ret);
    if (session->kernel->attr.enable_upper_case_names) {
        cm_str_upper(name_buffer);
    }
    
    CM_SAVE_STACK(session->stack);
    cursor = knl_push_cursor(session);
    knl_open_sys_cursor(session, cursor, CURSOR_ACTION_SELECT, SYS_DIRECTORY_ID, IX_DIRECTORY_001_ID);
    knl_init_index_scan(cursor, GS_TRUE);
    knl_set_scan_key(INDEX_DESC(cursor->index), &cursor->scan_range.l_key, GS_TYPE_STRING, name_buffer,
                     (uint16)strlen(name_buffer), IX_COL_SYS_DIRECTORY_NAME);

    if (knl_fetch(session, cursor) != GS_SUCCESS) {
        CM_RESTORE_STACK(session->stack);
        return GS_ERROR;
    }

    if (cursor->eof) {
        CM_RESTORE_STACK(session->stack);
        return GS_SUCCESS;
    }

    path_buffer = CURSOR_COLUMN_DATA(cursor, SYS_DIRECTORIES_COL_DIRE_PATH);
    path_len = CURSOR_COLUMN_SIZE(cursor, SYS_DIRECTORIES_COL_DIRE_PATH);

    if (dire_path != NULL) {
        ret = memcpy_sp(dire_path, dire_len, path_buffer, path_len);
        knl_securec_check(ret);
    }
    
    *is_found = GS_TRUE;
    CM_RESTORE_STACK(session->stack);
    return GS_SUCCESS;
}


