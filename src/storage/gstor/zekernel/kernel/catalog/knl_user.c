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
 * knl_user.c
 *    kernel user management interface routine
 *
 * IDENTIFICATION
 *    src/storage/gstor/zekernel/kernel/catalog/knl_user.c
 *
 * -------------------------------------------------------------------------
 */
#include "knl_user.h"
#include "knl_context.h"
#include "dc_user.h"
#include "dc_priv.h"
#include "knl_table.h"
#include "knl_sequence.h"
#include "knl_ctlg.h"

#ifdef __cplusplus
extern "C" {
#endif

#define AUTO_INHERIT_ON      (((knl_session_t *)session)->kernel->attr.enable_auto_inherit)
#define AUTO_INHERIT_ROLE_ON (((knl_session_t *)session)->kernel->attr.enable_auto_inherit_role)

static status_t user_check_name_valid(knl_session_t *session, knl_user_def_t *def, knl_user_desc_t *desc)
{
    dc_user_t *user = NULL;
    dc_role_t *role = NULL;
    dc_context_t *ctx = &session->kernel->dc_ctx;
    uint32 i;

    desc->id = GS_INVALID_ID32;

    /* user name can not be the same with roles */
    for (i = 0; i < GS_MAX_ROLES; i++) {
        role = ctx->roles[i];
        if (role != NULL && cm_str_equal_ins(role->desc.name, def->name)) {
            GS_THROW_ERROR(ERR_OBJECT_EXISTS, "role", def->name);
            return GS_ERROR;
        }
    }

    for (i = 0; i < GS_MAX_USERS; i++) {
        user = ctx->users[i];
        if (user == NULL || user->status == USER_STATUS_DROPPED) {
            if (desc->id == GS_INVALID_ID32) {
                desc->id = i;
            }
            continue;
        }

        if (cm_str_equal(user->desc.name, def->name)) {
            GS_THROW_ERROR(ERR_OBJECT_EXISTS, "user", def->name);
            return GS_ERROR;
        }
    }

    if (desc->id == GS_INVALID_ID32) {
        GS_THROW_ERROR(ERR_MAX_ROLE_COUNT, "users", GS_MAX_USERS);
        return GS_ERROR;
    }

    return GS_SUCCESS;
}

static status_t user_insert_history(knl_session_t *session, knl_cursor_t *cursor, knl_user_desc_t *desc,
                                    date_t now)
{
    row_assist_t row;
    uint32 max_size;

    max_size = session->kernel->attr.max_row_size;
    row_init(&row, cursor->buf, max_size, SYSUSER_HISTORY_COLS);
    if (GS_SUCCESS != row_put_int32(&row, desc->id)) {
        return GS_ERROR;
    }

    if (GS_SUCCESS != row_put_str(&row, desc->password)) {
        return GS_ERROR;
    }

    if (GS_SUCCESS != row_put_date(&row, now)) {
        return GS_ERROR;
    }

    return knl_internal_insert(session, cursor);
}

static inline void user_clear_password(char *passwd, uint32 size)
{
    errno_t err;

    err = memset_sp(passwd, size, 0, size);
    knl_securec_check(err);
}

static status_t user_set_profile_id(knl_session_t *session, knl_user_def_t *def, knl_user_desc_t *desc)
{
    profile_t *profile = NULL;

    if (CM_IS_EMPTY(&def->profile)) {
        desc->profile_id = DEFAULT_PROFILE_ID;
    } else {
        if (!profile_find_by_name(session, &def->profile, NULL, &profile)) {
            GS_THROW_ERROR(ERR_PROFILE_NOT_EXIST, T2S(&def->profile));
            return GS_ERROR;
        }
        desc->profile_id = profile->id;
    }
    
    return GS_SUCCESS;
}

static status_t user_prepare_password(knl_session_t *session, knl_user_def_t *def, knl_user_desc_t *desc,
    date_t date)
{
    if (def->is_encrypt) {
        size_t password_len = strlen(def->password);
        errno_t err = strncpy_s(desc->password, GS_PASSWORD_BUFFER_SIZE, def->password, password_len);
        knl_securec_check(err);
    } else {
        if (user_encrypt_password((char *)session->kernel->attr.pwd_alg, session->kernel->attr.alg_iter,
                                  def->password, (uint32)strlen(def->password), desc->password,
                                  GS_PASSWORD_BUFFER_SIZE) != GS_SUCCESS) {
            return GS_ERROR;
        }
    }

    /* first insert user password */
    CM_SAVE_STACK(session->stack);
    knl_cursor_t *cursor = knl_push_cursor(session);
    knl_open_sys_cursor(session, cursor, CURSOR_ACTION_INSERT, SYS_USER_HISTORY_ID, IX_SYS_USER_HISTORY001_ID);
    if (user_insert_history(session, cursor, desc, date) != GS_SUCCESS) {
        CM_RESTORE_STACK(session->stack);
        return GS_ERROR;
    }

    CM_RESTORE_STACK(session->stack);
    return GS_SUCCESS;
}

static status_t user_check_def_space(knl_session_t *session, knl_user_def_t *def, knl_user_desc_t *desc)
{
    text_t space_name;
    space_t *space = NULL;
    dc_tenant_t* tenant = NULL;

    if (strlen(def->default_space) == 0) {
        if (dc_open_tenant_by_id(session, desc->tenant_id, &tenant) != GS_SUCCESS) {
            return GS_ERROR;
        }
        desc->data_space_id = tenant->desc.ts_id;
        dc_close_tenant(session, tenant->desc.id);
    } else {
        cm_str2text(def->default_space, &space_name);
        if (spc_get_space_id(session, &space_name, &desc->data_space_id) != GS_SUCCESS) {
            return GS_ERROR;
        }
        space = SPACE_GET(desc->data_space_id);
        if (spc_check_by_tid(session, &space_name, desc->data_space_id, desc->tenant_id) != GS_SUCCESS) {
            return GS_ERROR;
        }

        if (!IS_USER_SPACE(space)) {
            GS_THROW_ERROR(ERR_DEFAULT_SPACE_TYPE_INVALID, T2S(&space_name));
            return GS_ERROR;
        }

        if (IS_SWAP_SPACE(space)) {
            GS_THROW_ERROR(ERR_DEFAULT_SPACE_TYPE_INVALID);
            return GS_ERROR;
        }
    }

    if (strlen(def->temp_space) != 0) {
        cm_str2text(def->temp_space, &space_name);
        if (spc_get_space_id(session, &space_name, &desc->temp_space_id) != GS_SUCCESS) {
            return GS_ERROR;
        }

        space = SPACE_GET(desc->temp_space_id);
        if (!(IS_TEMP_SPACE(space) && IS_SWAP_SPACE(space))) {
            GS_THROW_ERROR(ERR_TEMP_SPACE_TYPE_INVALID);
            return GS_ERROR;
        }
    }

    return GS_SUCCESS;
}

static status_t user_prepare_desc(knl_session_t *session, knl_user_def_t *def, knl_user_desc_t *desc)
{
    date_t date = cm_now();

    if (user_check_name_valid(session, def, desc) != GS_SUCCESS) {
        return GS_ERROR;
    }

    errno_t err = memcpy_sp(desc->name, GS_NAME_BUFFER_SIZE, def->name, GS_NAME_BUFFER_SIZE);
    knl_securec_check(err);

    if (user_set_profile_id(session, def, desc) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (user_prepare_password(session, def, desc, date) != GS_SUCCESS) {
        return GS_ERROR;
    }

    /*
     * no need to verify pwd reuse, because s1: user duplicate created then return error,
     * s2:user has delete, related USER_HISTORY$_ID record has delete, pwd must can be reused
     */
    desc->temp_space_id = session->kernel->db.ctrl.core.swap_space;
    desc->tenant_id = def->tenant_id;

    if (user_check_def_space(session, def, desc) != GS_SUCCESS) {
        return GS_ERROR;
    }

    desc->astatus = ACCOUNT_STATUS_OPEN;
    desc->ctime = date;
    desc->ptime = date;
    desc->lcount = 0;

    if (def->is_permanent) {
        desc->astatus |= ACCOUNT_SATTUS_PERMANENT;
    }

    if (def->is_expire) {
        desc->exptime = date;
        desc->astatus |= ACCOUNT_STATUS_EXPIRED;
    }

    if (def->is_lock) {
        desc->ltime = date;
        desc->astatus |= ACCOUNT_STATUS_LOCK;
    }

    return GS_SUCCESS;
}

// For compatibility, new users need to be granted INHERIT PRIVILEGES to PUBLIC.
status_t user_create_auto_grant_inherit(knl_session_t *session, dc_context_t *ctx, knl_user_desc_t *desc)
{
    dc_user_priv_entry_t *entry = NULL;
    dc_user_t *user = ctx->users[desc->id];

    if (dc_alloc_user_priv_entry(ctx, &user->user_privs, user->memory, PUBLIC_USER_ID, &entry) != GS_SUCCESS) {
        return GS_ERROR;
    }
    /* add user priv to user dc */
    cm_spin_lock(&entry->bucket->lock, NULL);
    DC_SET_OBJ_PRIV(entry->user_priv_item.privid_map, GS_PRIV_INHERIT_PRIVILEGES);
    entry->user_priv_item.grantor[0] = desc->id;
    cm_spin_unlock(&entry->bucket->lock);

    return GS_SUCCESS;
}

// 2024-9-10
// For basic permisions, new users need to be granted role to CONNECT.
status_t user_create_auto_grant_role_connect(knl_session_t *session, uint32 *grantee_id)
{
    uint32 max_size;
    row_assist_t ra;
    knl_cursor_t *cursor = NULL;
    knl_session_t *knl_session = (knl_session_t *)session;

    CM_SAVE_STACK(knl_session->stack);

    cursor = knl_push_cursor(knl_session);

    cursor->row = (row_head_t *)cursor->buf;

    max_size = knl_session->kernel->attr.max_row_size;
    row_init(&ra, cursor->buf, max_size, 5);

    (void)row_put_int32(&ra, grantee_id);
    (void)row_put_int32(&ra, TYPE_USER);
    (void)row_put_int32(&ra, CONNECT_ROLE_ID);
    (void)row_put_int32(&ra, 0);
    (void)row_put_int32(&ra, 0);

    knl_open_sys_cursor(knl_session, cursor, CURSOR_ACTION_INSERT, SYS_USER_ROLES_ID, GS_INVALID_ID32);

    if (GS_SUCCESS != knl_internal_insert(session, cursor)) {
        CM_RESTORE_STACK(knl_session->stack);
        return GS_ERROR;
    }

    CM_RESTORE_STACK(knl_session->stack);
    return GS_SUCCESS;
}

status_t user_create(knl_session_t *session, knl_user_def_t *def)
{
    knl_cursor_t *cursor = NULL;
    knl_user_desc_t desc = {0};
    dc_context_t *ctx = &session->kernel->dc_ctx;
    space_t *space = NULL;
    rd_user_t redo;
    errno_t err;

    if (DB_NOT_READY(session)) {
        GS_THROW_ERROR(ERR_NO_DB_ACTIVE);
        return GS_ERROR;
    }

    cm_spin_lock(&ctx->paral_lock, NULL);
    knl_set_session_scn(session, GS_INVALID_ID64);

    if (user_prepare_desc(session, def, &desc) != GS_SUCCESS) {
        cm_spin_unlock(&ctx->paral_lock);
        user_clear_password(desc.password, GS_PASSWORD_BUFFER_SIZE);
        return GS_ERROR;
    }

    space = SPACE_GET(desc.data_space_id);

    if (!SPACE_IS_ONLINE(space)) {
        cm_spin_unlock(&ctx->paral_lock);
        user_clear_password(desc.password, GS_PASSWORD_BUFFER_SIZE);
        GS_THROW_ERROR(ERR_SPACE_OFFLINE, space->ctrl->name, "space offline and write to system user failed");
        return GS_ERROR;
    }

    CM_SAVE_STACK(session->stack);

    cursor = knl_push_cursor(session);
    knl_open_sys_cursor(session, cursor, CURSOR_ACTION_INSERT, SYS_USER_ID, IX_SYS_USER_001_ID);

    if (db_insert_sys_user(session, cursor, &desc) != GS_SUCCESS) {
        CM_RESTORE_STACK(session->stack);
        cm_spin_unlock(&ctx->paral_lock);
        user_clear_password(desc.password, GS_PASSWORD_BUFFER_SIZE);
        return GS_ERROR;
    }

    if (AUTO_INHERIT_ON && db_insert_user_privs((knl_handle_t)session, desc.id, desc.id, PUBLIC_USER_ID,
        GS_PRIV_INHERIT_PRIVILEGES) != GS_SUCCESS) {
        CM_RESTORE_STACK(session->stack);
        cm_spin_unlock(&ctx->paral_lock);
        user_clear_password(desc.password, GS_PASSWORD_BUFFER_SIZE);
        return GS_ERROR;
    }

    redo.op_type = RD_CREATE_USER;
    redo.uid = desc.id;
    err = strcpy_sp(redo.name, GS_NAME_BUFFER_SIZE, desc.name);
    knl_securec_check(err);
    log_put(session, RD_LOGIC_OPERATION, &redo, sizeof(rd_user_t), LOG_ENTRY_FLAG_NONE);

    /* add the new user to dc */
    if (!ctx->users[desc.id]) {
        if (dc_add_user(ctx, &desc) != GS_SUCCESS) {
            CM_RESTORE_STACK(session->stack);
            cm_spin_unlock(&ctx->paral_lock);
            user_clear_password(desc.password, GS_PASSWORD_BUFFER_SIZE);
            return GS_ERROR;
        }
    } else {
        /* re use */
        dc_reuse_user(session, &desc);
    }

    if (AUTO_INHERIT_ON && user_create_auto_grant_inherit(session, ctx, &desc) != GS_SUCCESS) {
        CM_RESTORE_STACK(session->stack);
        dc_drop_user(session, desc.id);
        cm_spin_unlock(&ctx->paral_lock);
        user_clear_password(desc.password, GS_PASSWORD_BUFFER_SIZE);
        return GS_ERROR;
    }

    // 2024-9-10
    if (AUTO_INHERIT_ROLE_ON && user_create_auto_grant_role_connect(session, desc.id) != GS_SUCCESS) {
        CM_RESTORE_STACK(session->stack);
        dc_drop_user(session, desc.id);
        cm_spin_unlock(&ctx->paral_lock);
        user_clear_password(desc.password, GS_PASSWORD_BUFFER_SIZE);
        return GS_ERROR;
    }

    CM_RESTORE_STACK(session->stack);
    knl_commit(session);
    cm_spin_unlock(&ctx->paral_lock);
    user_clear_password(desc.password, GS_PASSWORD_BUFFER_SIZE);

    return GS_SUCCESS;
}

status_t user_delete_history_by_id(knl_session_t *session, uint32 id)
{
    knl_cursor_t *cursor = NULL;
    CM_SAVE_STACK(session->stack);

    cursor = knl_push_cursor(session);

    knl_open_sys_cursor(session, cursor, CURSOR_ACTION_DELETE, SYS_USER_HISTORY_ID, IX_SYS_USER_HISTORY001_ID);
    knl_init_index_scan(cursor, GS_FALSE);

    knl_set_scan_key(INDEX_DESC(cursor->index), &cursor->scan_range.l_key,
                     GS_TYPE_INTEGER, &id, sizeof(uint32), IX_COL_SYS_USER_HISTORY001_USER_ID);
    knl_set_scan_key(INDEX_DESC(cursor->index), &cursor->scan_range.r_key,
                     GS_TYPE_INTEGER, &id, sizeof(uint32), IX_COL_SYS_USER_HISTORY001_USER_ID);
    knl_set_key_flag(&cursor->scan_range.l_key, SCAN_KEY_LEFT_INFINITE, IX_COL_SYS_USER_HISTORY001_PASSWORD_DATE);
    knl_set_key_flag(&cursor->scan_range.r_key, SCAN_KEY_RIGHT_INFINITE, IX_COL_SYS_USER_HISTORY001_PASSWORD_DATE);
    if (GS_SUCCESS != knl_fetch(session, cursor)) {
        CM_RESTORE_STACK(session->stack);
        return GS_ERROR;
    }

    while (!cursor->eof) {
        if (GS_SUCCESS != knl_internal_delete(session, cursor)) {
            CM_RESTORE_STACK(session->stack);
            return GS_ERROR;
        }

        if (GS_SUCCESS != knl_fetch(session, cursor)) {
            CM_RESTORE_STACK(session->stack);
            return GS_ERROR;
        }
    }

    CM_RESTORE_STACK(session->stack);
    return GS_SUCCESS;
}

static status_t user_update_role_owner(knl_session_t *session, uint32 uid)
{
    row_assist_t ra;
    knl_cursor_t *cursor = NULL;

    CM_SAVE_STACK(session->stack);

    cursor = knl_push_cursor(session);
    knl_open_sys_cursor(session, cursor, CURSOR_ACTION_UPDATE, SYS_ROLES_ID, IX_SYS_ROLES_002_ID);
    knl_init_index_scan(cursor, GS_TRUE);
    knl_set_scan_key(INDEX_DESC(cursor->index), &cursor->scan_range.l_key,
        GS_TYPE_INTEGER, &uid, sizeof(uint32), IX_COL_SYS_ROLES_002_OWNER_UID);

    if (GS_SUCCESS != knl_fetch(session, cursor)) {
        CM_RESTORE_STACK(session->stack);
        return GS_ERROR;
    }

    while (!cursor->eof) {
        /* set the owner uid to 0 for the role if the owner is dropped */
        row_init(&ra, cursor->update_info.data, HEAP_MAX_ROW_SIZE, 1);
        (void)row_put_int32(&ra, 0);
        cursor->update_info.count = 1;
        cursor->update_info.columns[0] = 1;
        cm_decode_row(cursor->update_info.data, cursor->update_info.offsets, cursor->update_info.lens, NULL);
        if (knl_internal_update(session, cursor) != GS_SUCCESS) {
            CM_RESTORE_STACK(session->stack);
            return GS_ERROR;
        }

        if (GS_SUCCESS != knl_fetch(session, cursor)) {
            CM_RESTORE_STACK(session->stack);
            return GS_ERROR;
        }
    }

    CM_RESTORE_STACK(session->stack);
    return GS_SUCCESS;
}

static status_t db_delete_from_sysdep_by_referenced_owner(knl_session_t *session, uint32 uid)
{
    knl_cursor_t *cursor = NULL;

    CM_SAVE_STACK(session->stack);
    knl_set_session_scn(session, GS_INVALID_ID64);
    cursor = knl_push_cursor(session);
    knl_open_sys_cursor(session, cursor, CURSOR_ACTION_DELETE, SYS_DEPENDENCY_ID, IX_DEPENDENCY1_ID);

    knl_init_index_scan(cursor, GS_FALSE);
    knl_set_key_flag(&cursor->scan_range.l_key, SCAN_KEY_LEFT_INFINITE, IX_COL_DEPENDENCY1_D_OWNER_ID);
    knl_set_key_flag(&cursor->scan_range.l_key, SCAN_KEY_LEFT_INFINITE, IX_COL_DEPENDENCY1_D_OBJ_ID);
    knl_set_key_flag(&cursor->scan_range.l_key, SCAN_KEY_LEFT_INFINITE, IX_COL_DEPENDENCY1_D_TYPE_ID);
    knl_set_key_flag(&cursor->scan_range.l_key, SCAN_KEY_LEFT_INFINITE, IX_COL_DEPENDENCY1_ORDER_ID);
    knl_set_key_flag(&cursor->scan_range.r_key, SCAN_KEY_RIGHT_INFINITE, IX_COL_DEPENDENCY1_D_OWNER_ID);
    knl_set_key_flag(&cursor->scan_range.r_key, SCAN_KEY_RIGHT_INFINITE, IX_COL_DEPENDENCY1_D_OBJ_ID);
    knl_set_key_flag(&cursor->scan_range.r_key, SCAN_KEY_RIGHT_INFINITE, IX_COL_DEPENDENCY1_D_TYPE_ID);
    knl_set_key_flag(&cursor->scan_range.r_key, SCAN_KEY_RIGHT_INFINITE, IX_COL_DEPENDENCY1_ORDER_ID);

    if (knl_fetch(session, cursor) != GS_SUCCESS) {
        CM_RESTORE_STACK(session->stack);
        return GS_ERROR;
    }

    while (!cursor->eof) {
        uint32 referenced_owner = *(uint32 *)CURSOR_COLUMN_DATA(cursor, SYS_DEPENDENCY_P_OWNER);
        if (referenced_owner == uid && knl_internal_delete(session, cursor) != GS_SUCCESS) {
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

static status_t user_drop_common_objects(knl_session_t *session, uint32 uid)
{
    if (user_delete_history_by_id(session, uid) != GS_SUCCESS) {
        return GS_ERROR;
    }
    if (db_delete_all_privs_by_id(session, uid, 0) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (user_update_role_owner(session, uid) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (db_delete_from_sys_user(session, uid) != GS_SUCCESS) {
        return GS_ERROR;
    }

    bool32 HAS_SYS_DEPENDENCY_ID = GS_FALSE;    // 2024-8-6
    if (HAS_SYS_DEPENDENCY_ID) {
        if (db_delete_from_sysdep_by_referenced_owner(session, uid) != GS_SUCCESS) {
            return GS_ERROR;
        }
    }

    return GS_SUCCESS;
}

object_type_t knl_char_pltype_to_objtype(char type)
{
    switch (type) {
        case 'P':
            return OBJ_TYPE_PROCEDURE;

        case 'F':
            return OBJ_TYPE_FUNCTION;

        case 'T':
            return OBJ_TYPE_TRIGGER;

        case 'S':
            return OBJ_TYPE_PACKAGE_SPEC;

        case 'B':
            return OBJ_TYPE_PACKAGE_BODY;

        case 'Y':
            return OBJ_TYPE_TYPE_SPEC;

        case 'O':
            return OBJ_TYPE_TYPE_BODY;

        default:
            return OBJ_TYPE_INVALID;
    }
}

static status_t db_drop_library_by_user(knl_session_t *session, uint32 uid)
{
    text_t path_text, lib_name;
    knl_cursor_t *cursor = NULL;
    char name[GS_NAME_BUFFER_SIZE];
    char path[GS_FILE_NAME_BUFFER_SIZE];
    CM_SAVE_STACK(session->stack);
    cursor = knl_push_cursor(session);

    knl_open_sys_cursor(session, cursor, CURSOR_ACTION_DELETE, SYS_LIBRARY_ID, IDX_LIBRARY_001_ID);

    knl_init_index_scan(cursor, GS_FALSE);
    knl_set_scan_key(INDEX_DESC(cursor->index), &cursor->scan_range.l_key, GS_TYPE_INTEGER, &uid, sizeof(uid),
        IX_COL_SYS_LIBRARY001_OWNER);
    knl_set_scan_key(INDEX_DESC(cursor->index), &cursor->scan_range.r_key, GS_TYPE_INTEGER, &uid, sizeof(uid),
        IX_COL_SYS_LIBRARY001_OWNER);
    knl_set_key_flag(&cursor->scan_range.l_key, SCAN_KEY_LEFT_INFINITE, IX_COL_SYS_LIBRARY001_NAME);
    knl_set_key_flag(&cursor->scan_range.r_key, SCAN_KEY_RIGHT_INFINITE, IX_COL_SYS_LIBRARY001_NAME);

    if (knl_fetch(session, cursor) != GS_SUCCESS) {
        CM_RESTORE_STACK(session->stack);
        return GS_ERROR;
    }

    while (!cursor->eof) {
        lib_name.str = (char *)CURSOR_COLUMN_DATA(cursor, SYS_LIBRARY_NAME);
        lib_name.len = (uint32)CURSOR_COLUMN_SIZE(cursor, SYS_LIBRARY_NAME);
        cm_text2str(&lib_name, name, GS_FILE_NAME_BUFFER_SIZE);

        path_text.str = (char *)CURSOR_COLUMN_DATA(cursor, SYS_LIBRARY_FILE_PATH);
        path_text.len = (uint32)CURSOR_COLUMN_SIZE(cursor, SYS_LIBRARY_FILE_PATH);
        cm_text2str(&path_text, path, GS_FILE_NAME_BUFFER_SIZE);

        if (g_knl_callback.clear_sym_cache((knl_handle_t)session, uid, name, path) != GS_SUCCESS) {
            CM_RESTORE_STACK(session->stack);
            return GS_ERROR;
        }

        if (knl_internal_delete(session, cursor) != GS_SUCCESS) {
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

static status_t user_drop_user_objects(knl_session_t *session, uint32 uid, text_t *owner)
{
    if (db_drop_sequence_by_user(session, owner, uid) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (rb_purge_user(session, uid) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (db_drop_view_by_user(session, owner, uid) != GS_SUCCESS) {
        return GS_ERROR;
    }
    if (db_drop_synonym_by_user(session, uid) != GS_SUCCESS) {
        return GS_ERROR;
    }
    if (db_delete_job_by_user(session, owner) != GS_SUCCESS) {
        return GS_ERROR;
    }

    if (db_drop_table_by_user(session, owner) != GS_SUCCESS) {
        return GS_ERROR;
    }

    bool32 HAS_SYS_LIBRARY_ID = GS_FALSE;   // 2024-9-3
    if (HAS_SYS_LIBRARY_ID) {
        if (db_drop_library_by_user(session, uid) != GS_SUCCESS) {
            return GS_ERROR;
        }
    }

    bool32 HAS_PL_DROP_OBJECT = GS_FALSE;   // 2024-9-24
    if (HAS_PL_DROP_OBJECT) {
        if (g_knl_callback.pl_drop_object(session, uid) != GS_SUCCESS) {
            return GS_ERROR;
        }
    }

    bool32 HAS_SYS_SQL_MAP_ID = GS_FALSE;   // 2024-9-3
    if (HAS_SYS_SQL_MAP_ID) {
        if (db_delete_sql_map_by_user(session, uid) != GS_SUCCESS) {
            return GS_ERROR;
        }
    }
    bool32 HAS_SYS_DISTRIBUTE_RULE_ID = GS_FALSE;   // 2024-9-3
    if (HAS_SYS_DISTRIBUTE_RULE_ID) {
        if (db_delete_dist_rules_by_user(session, uid) != GS_SUCCESS) {
            return GS_ERROR;
        }
    }

    if (user_drop_common_objects(session, uid) != GS_SUCCESS) {
        return GS_ERROR;
    }

    return GS_SUCCESS;
}

static status_t user_process_change_password_failed(knl_session_t *session, knl_user_desc_t *desc)
{
    uint64 limit;
    date_t now = cm_now();
    date_t unlock_dt = 0;

    if (GS_BIT_TEST(desc->astatus, ACCOUNT_STATUS_LOCK_TIMED)) {
        /* if the time exceed the pwd lock time, unlock account */
        if (GS_SUCCESS != profile_get_param_limit(session, desc->profile_id, PASSWORD_LOCK_TIME, &limit)) {
            return GS_ERROR;
        }

        if (PARAM_UNLIMITED != limit) {
            if (GS_SUCCESS != cm_date_add_seconds(desc->ltime, limit, &unlock_dt)) {
                return GS_ERROR;
            }

            if (now > unlock_dt) {
                GS_BIT_RESET(desc->astatus, ACCOUNT_STATUS_LOCK_TIMED);
                desc->lcount = 0;
            }
        }
    }

    if (GS_SUCCESS != profile_get_param_limit(session, desc->profile_id, FAILED_LOGIN_ATTEMPTS, &limit)) {
        return GS_ERROR;
    }

    // the max param in g_resource_map is smaller than uint32
    if (PARAM_UNLIMITED != limit && desc->lcount >= (uint32)limit) {
        /* set account status */
        desc->ltime = now;
        desc->astatus |= ACCOUNT_STATUS_LOCK_TIMED;
        return GS_ERROR;
    }

    desc->lcount++;

    return GS_SUCCESS;
}

static status_t user_change_password(knl_session_t *session, knl_user_def_t *def, dc_user_t *user, 
                                     knl_user_desc_t *desc, uint32 *update_flag)
{
    uint32 alg_iter = session->kernel->attr.alg_iter;
    char *alg = session->kernel->attr.pwd_alg;
    text_t plain_password, cipher_password;

    cm_str2text(def->old_password, &plain_password);
    cm_str2text(desc->password, &cipher_password);

    if (!CM_IS_EMPTY_STR(def->old_password)) {
        if (GS_SUCCESS != cm_check_password(&plain_password, &cipher_password)) {
            GS_THROW_ERROR(ERR_INVALID_OLD_PASSWORD);
            if (GS_SUCCESS != user_process_change_password_failed(session, &user->desc)) {
                cm_reset_error();
                GS_THROW_ERROR(ERR_ACCOUNT_LOCK);
                g_knl_callback.kill_session(session, GS_FALSE);
            }
            return GS_ERROR;
        }
    }

    desc->lcount = 0;
    *update_flag |= UPDATE_LCOUNT_COLUMN;

    // 2024-9-11    if is_encrypt, no need to encrypt through DCC original code again
    if (!def->is_encrypt) {
        if (GS_SUCCESS != user_encrypt_password(alg, alg_iter, def->password, (uint32)strlen(def->password),
            desc->password, GS_PASSWORD_BUFFER_SIZE)) {
            return GS_ERROR;
        }
    } else {
        GS_RETURN_IFERR(memset_s(desc->password, GS_PASSWORD_BUFFER_SIZE, 0, GS_PASSWORD_BUFFER_SIZE));
        GS_RETURN_IFERR(memcpy_s(desc->password, GS_PASSWORD_BUFFER_SIZE, def->password, strlen(def->password)));
    }
    *update_flag |= UPDATE_PASSWORD_COLUMM;
    return GS_SUCCESS;
}

static status_t user_delete_history(knl_session_t *session, knl_cursor_t *cursor, uint32 *uid)
{
    knl_open_sys_cursor(session, cursor, CURSOR_ACTION_DELETE, SYS_USER_HISTORY_ID, IX_SYS_USER_HISTORY001_ID);
    knl_init_index_scan(cursor, GS_FALSE);
    knl_set_scan_key(INDEX_DESC(cursor->index), &cursor->scan_range.l_key, GS_TYPE_INTEGER,
        uid, sizeof(uint32), IX_COL_SYS_USER_HISTORY001_USER_ID);
    knl_set_scan_key(INDEX_DESC(cursor->index), &cursor->scan_range.r_key, GS_TYPE_INTEGER,
        uid, sizeof(uint32), IX_COL_SYS_USER_HISTORY001_USER_ID);
    knl_set_key_flag(&cursor->scan_range.l_key, SCAN_KEY_LEFT_INFINITE,
        IX_COL_SYS_USER_HISTORY001_PASSWORD_DATE);
    knl_set_key_flag(&cursor->scan_range.r_key, SCAN_KEY_RIGHT_INFINITE,
        IX_COL_SYS_USER_HISTORY001_PASSWORD_DATE);
    if (knl_fetch(session, cursor) != GS_SUCCESS) {
        return GS_ERROR;
    }
    while (!cursor->eof) {
        if (knl_internal_delete(session, cursor) != GS_SUCCESS) {
            return GS_ERROR;
        }
        if (knl_fetch(session, cursor) != GS_SUCCESS) {
            return GS_ERROR;
        }
    }
    return GS_SUCCESS;
}

static status_t user_modify_history(knl_session_t *session, knl_cursor_t *cursor, date_t now)
{
    row_assist_t ra;
    knl_update_info_t *ua = &cursor->update_info;

    row_init(&ra, ua->data, HEAP_MAX_ROW_SIZE, 1);
    if (GS_SUCCESS != row_put_date(&ra, now)) {
        return GS_ERROR;
    }
    ua->count = 1;
    ua->columns[0] = SYS_USER_HISTORY_PASSWORD_DATE_ID;
    cm_decode_row(ua->data, ua->offsets, ua->lens, NULL);

    return knl_internal_update(session, cursor);
}

static status_t user_update_history(knl_session_t *session, knl_cursor_t *cursor, knl_user_desc_t *desc,
    date_t history_time, date_t now)
{
    knl_open_sys_cursor(session, cursor, CURSOR_ACTION_UPDATE, SYS_USER_HISTORY_ID, IX_SYS_USER_HISTORY001_ID);
    knl_init_index_scan(cursor, GS_TRUE);

    knl_set_scan_key(INDEX_DESC(cursor->index), &cursor->scan_range.l_key, GS_TYPE_INTEGER, &desc->id, sizeof(uint32),
        IX_COL_SYS_USER_HISTORY001_USER_ID);
    knl_set_scan_key(INDEX_DESC(cursor->index), &cursor->scan_range.l_key, GS_TYPE_DATE, &history_time, sizeof(date_t),
        IX_COL_SYS_USER_HISTORY001_PASSWORD_DATE);

    if (GS_SUCCESS != knl_fetch(session, cursor)) {
        return GS_ERROR;
    }
    knl_panic_log(!cursor->eof, "data is not found, panic info: page %u-%u type %u table %s index %s",
                  cursor->rowid.file, cursor->rowid.page, ((page_head_t *)cursor->page_buf)->type,
                  ((table_t *)cursor->table)->desc.name, ((index_t *)cursor->index)->desc.name);

    if (GS_SUCCESS != user_modify_history(session, cursor, now)) {
        return GS_ERROR;
    }

    return GS_SUCCESS;
}

static status_t user_check_password_reuse(knl_session_t *session, knl_user_def_t *def, knl_user_desc_t *desc)
{
    bool32 found = GS_FALSE;
    text_t cipher_password, plain_password;
    date_t history_time, reuse_time, now;
    uint32 count = 0;
    uint64 reuse_value, reuse_max;
    history_time = cm_now();
    now = cm_now();
    cm_str2text(def->password, &plain_password);

    if (GS_SUCCESS != profile_get_param_limit(session, desc->profile_id, PASSWORD_REUSE_TIME, &reuse_value)) {
        return GS_ERROR;
    }

    if (GS_SUCCESS != profile_get_param_limit(session, desc->profile_id, PASSWORD_REUSE_MAX, &reuse_max)) {
        return GS_ERROR;
    }


    CM_SAVE_STACK(session->stack);
    knl_cursor_t *cursor = knl_push_cursor(session);

    /* If you set both of these parameters to UNLIMITED, we must delete all items and insert into the latest pw */
    if (reuse_value == PARAM_UNLIMITED && reuse_max == PARAM_UNLIMITED) {
        if (GS_SUCCESS != user_delete_history(session, cursor, &desc->id)) {
            CM_RESTORE_STACK(session->stack);
            return GS_ERROR;
        }
        if (GS_SUCCESS != user_insert_history(session, cursor, desc, now)) {
            CM_RESTORE_STACK(session->stack);
            return GS_ERROR;
        }
        CM_RESTORE_STACK(session->stack);
        return GS_SUCCESS;
    }

    knl_open_sys_cursor(session, cursor, CURSOR_ACTION_SELECT, SYS_USER_HISTORY_ID, IX_SYS_USER_HISTORY001_ID);
    knl_init_index_scan(cursor, GS_FALSE);

    knl_set_scan_key(INDEX_DESC(cursor->index), &cursor->scan_range.l_key, GS_TYPE_INTEGER, &desc->id, sizeof(uint32),
        IX_COL_SYS_USER_HISTORY001_USER_ID);
    knl_set_scan_key(INDEX_DESC(cursor->index), &cursor->scan_range.r_key, GS_TYPE_INTEGER, &desc->id, sizeof(uint32),
        IX_COL_SYS_USER_HISTORY001_USER_ID);
    knl_set_key_flag(&cursor->scan_range.l_key, SCAN_KEY_LEFT_INFINITE, IX_COL_SYS_USER_HISTORY001_PASSWORD_DATE);
    knl_set_key_flag(&cursor->scan_range.r_key, SCAN_KEY_RIGHT_INFINITE, IX_COL_SYS_USER_HISTORY001_PASSWORD_DATE);
    if (GS_SUCCESS != knl_fetch(session, cursor)) {
        CM_RESTORE_STACK(session->stack);
        return GS_ERROR;
    }

    while (!cursor->eof) {
        cipher_password.str = CURSOR_COLUMN_DATA(cursor, SYS_USER_HISTORY_PASSOWRD_ID);
        cipher_password.len = CURSOR_COLUMN_SIZE(cursor, SYS_USER_HISTORY_PASSOWRD_ID);
        // the pwd who is equal to the current pwd can't count.
        // must statistic the count between cipher_password andplain_password.   e.g.
        // create user test_pass IDENTIFIED BY A;----not count
        // ALTER user test_pass IDENTIFIED BY B;----count 1
        // ALTER user test_pass IDENTIFIED BY C;----count 2
        // ALTER user test_pass IDENTIFIED BY D;----count 3
        if (found) {
            count++;
        }
        if (GS_SUCCESS == cm_check_password(&plain_password, &cipher_password)) {
            history_time = *(date_t *)CURSOR_COLUMN_DATA(cursor, SYS_USER_HISTORY_PASSWORD_DATE_ID);
            found = GS_TRUE;
        }

        if (GS_SUCCESS != knl_fetch(session, cursor)) {
            CM_RESTORE_STACK(session->stack);
            return GS_ERROR;
        }
    }

    if (!found) {
        if (GS_SUCCESS != user_insert_history(session, cursor, desc, now)) {
            CM_RESTORE_STACK(session->stack);
            return GS_ERROR;
        }
        CM_RESTORE_STACK(session->stack);
        return GS_SUCCESS;
    }

    if (reuse_value == PARAM_UNLIMITED || reuse_max == PARAM_UNLIMITED) {
        CM_RESTORE_STACK(session->stack);
        return GS_ERROR;
    }

    if (GS_SUCCESS != cm_date_add_seconds(history_time, reuse_value, &reuse_time)) {
        CM_RESTORE_STACK(session->stack);
        return GS_ERROR;
    }

    // the max param in g_resource_map is smaller than uint32
    if (count < (uint32)reuse_max || now < reuse_time) {
        CM_RESTORE_STACK(session->stack);
        return GS_ERROR;
    }

    if (user_update_history(session, cursor, desc, history_time, now) != GS_SUCCESS) {
        CM_RESTORE_STACK(session->stack);
        return GS_ERROR;
    }

    CM_RESTORE_STACK(session->stack);

    return GS_SUCCESS;
}

static status_t user_prepare_alter(knl_session_t *session, knl_user_def_t *def, dc_user_t *user,
                                   knl_user_desc_t *desc, uint32 *update_flag)
{
    text_t space_name;
    space_t *space = NULL;
    errno_t err;
    date_t date = cm_now();

    if (GS_BIT_TEST(def->mask, GS_GET_MASK(ALTER_USER_FIELD_PROFILE))) {
        if (CM_IS_EMPTY(&def->profile)) {
            desc->profile_id = DEFAULT_PROFILE_ID;
        } else {
            profile_t *profile = NULL;
            if (!profile_find_by_name(session, &def->profile, NULL, &profile)) {
                GS_THROW_ERROR(ERR_PROFILE_NOT_EXIST, T2S(&def->profile));
                return GS_ERROR;
            }
            desc->profile_id = profile->id;
        }
        *update_flag |= UPDATE_PROFILE_COLUMN;
    }

    if (GS_BIT_TEST(def->mask, GS_GET_MASK(ALTER_USER_FIELD_DATA_SPACE))) {
        cm_str2text(def->default_space, &space_name);

        if (CM_IS_EMPTY(&space_name)) {
            dc_tenant_t* tenant = NULL;
            if (dc_open_tenant_by_id(session, desc->tenant_id, &tenant) != GS_SUCCESS) {
                return GS_ERROR;
            }
            desc->data_space_id = tenant->desc.ts_id;
            dc_close_tenant(session, tenant->desc.id);
        } else {
            if (spc_get_space_id(session, &space_name, &desc->data_space_id) != GS_SUCCESS) {
                GS_THROW_ERROR(ERR_SPACE_NOT_EXIST, def->default_space);
                return GS_ERROR;
            }
            if (spc_check_by_tid(session, &space_name, desc->data_space_id, desc->tenant_id) != GS_SUCCESS) {
                return GS_ERROR;
            }
        }

        space = SPACE_GET(desc->data_space_id);
        if (cm_str_equal_ins(def->name, "sys")) {
            if (!IS_USER_SPACE(space) && !IS_SYSTEM_SPACE(space)) {
                GS_THROW_ERROR(ERR_DEFAULT_SPACE_TYPE_INVALID, T2S(&space_name));
                return GS_ERROR;
            }
        } else {
            if (!IS_USER_SPACE(space)) {
                GS_THROW_ERROR(ERR_DEFAULT_SPACE_TYPE_INVALID, T2S(&space_name));
                return GS_ERROR;
            }
        }

        if (IS_SWAP_SPACE(space)) {
            GS_THROW_ERROR(ERR_DEFAULT_SPACE_TYPE_INVALID, "NOLOGGING");
            return GS_ERROR;
        }
        *update_flag |= UPDATE_DATA_SPACE_COLUMN;
    }

    if (GS_BIT_TEST(def->mask, GS_GET_MASK(ALTER_USER_FIELD_TEMP_SPACE))) {
        cm_str2text(def->temp_space, &space_name);

        if (CM_IS_EMPTY(&space_name)) {
            desc->data_space_id = session->kernel->db.ctrl.core.swap_space;
        } else {
            if (spc_get_space_id(session, &space_name, &desc->temp_space_id) != GS_SUCCESS) {
                GS_THROW_ERROR(ERR_SPACE_NOT_EXIST, def->temp_space);
                return GS_ERROR;
            }
        }

        space = SPACE_GET(desc->temp_space_id);
        if (!IS_TEMP_SPACE(space)) {
            GS_THROW_ERROR(ERR_TEMP_SPACE_TYPE_INVALID);
            return GS_ERROR;
        }
        *update_flag |= UPDATE_TEMP_SPACE_COLUMN;
    }

    if (GS_BIT_TEST(def->mask, GS_GET_MASK(ALTER_USER_FIELD_PASSWORD))) {
        if (user_change_password(session, def, user, desc, update_flag) != GS_SUCCESS) {
            err = memset_sp(def->password, GS_PASSWORD_BUFFER_SIZE, 0, GS_PASSWORD_BUFFER_SIZE);
            knl_securec_check(err);
            err = memset_sp(def->old_password, GS_PASSWORD_BUFFER_SIZE, 0, GS_PASSWORD_BUFFER_SIZE);
            knl_securec_check(err);

            return GS_ERROR;
        }

        if (user_check_password_reuse(session, def, desc) != GS_SUCCESS) {
            err = memset_sp(def->password, GS_PASSWORD_BUFFER_SIZE, 0, GS_PASSWORD_BUFFER_SIZE);
            knl_securec_check(err);
            err = memset_sp(def->old_password, GS_PASSWORD_BUFFER_SIZE, 0, GS_PASSWORD_BUFFER_SIZE);
            knl_securec_check(err);
            GS_THROW_ERROR(ERR_REUSED_PASSWORD_ERROR);
            return GS_ERROR;
        }

        err = memset_sp(def->password, GS_PASSWORD_BUFFER_SIZE, 0, GS_PASSWORD_BUFFER_SIZE);
        knl_securec_check(err);
        err = memset_sp(def->old_password, GS_PASSWORD_BUFFER_SIZE, 0, GS_PASSWORD_BUFFER_SIZE);
        knl_securec_check(err);

        *update_flag |= UPDATE_PASSWORD_COLUMM;
        desc->ptime = date;
        *update_flag |= UPDATE_PTIME_COLUMN;
        if ((GS_BIT_TEST(desc->astatus, ACCOUNT_STATUS_EXPIRED) ||
            GS_BIT_TEST(desc->astatus, ACCOUNT_STATUS_EXPIRED_GRACE)) &&
            !GS_BIT_TEST(def->mask, USER_EXPIRE_MASK)) {    
            desc->exptime = 0;
            *update_flag |= UPDATE_EXPTIME_COLUMN;
            GS_BIT_RESET(desc->astatus, ACCOUNT_STATUS_EXPIRED + ACCOUNT_STATUS_EXPIRED_GRACE);
            *update_flag |= UPDATE_ASTATUS_COLUMN;
        }
    }

    if (GS_BIT_TEST(def->mask, GS_GET_MASK(ALTER_USER_FIELD_EXPIRE))) {
        if (def->is_expire) {
            desc->exptime = date;
            *update_flag |= UPDATE_EXPTIME_COLUMN;
            desc->astatus |= ACCOUNT_STATUS_EXPIRED;
            GS_BIT_RESET(desc->astatus, ACCOUNT_STATUS_EXPIRED_GRACE);
            *update_flag |= UPDATE_ASTATUS_COLUMN;               
        }
    }

    if (GS_BIT_TEST(def->mask, GS_GET_MASK(ALTER_USER_FIELD_EXPIRE_GRACE))) {
        if (def->is_expire_grace) {
            desc->astatus |= ACCOUNT_STATUS_EXPIRED_GRACE;
            *update_flag |= UPDATE_ASTATUS_COLUMN;
        }    
    }

    if (GS_BIT_TEST(def->mask, GS_GET_MASK(ALTER_USER_FIELD_LOCK))) {
        if (def->is_lock) {
            desc->ltime = date;
            *update_flag |= UPDATE_LTIME_COLUMN;
            desc->astatus |= ACCOUNT_STATUS_LOCK;
            GS_BIT_RESET(desc->astatus, ACCOUNT_STATUS_LOCK_TIMED);     
        } else {
            desc->lcount = 0;
            *update_flag |= UPDATE_LCOUNT_COLUMN;
            GS_BIT_RESET(desc->astatus, ACCOUNT_STATUS_LOCK);
            GS_BIT_RESET(desc->astatus, ACCOUNT_STATUS_LOCK_TIMED);
        }
        *update_flag |= UPDATE_ASTATUS_COLUMN;
    }

    if (GS_BIT_TEST(def->mask, GS_GET_MASK(ALTER_USER_FIELD_LOCK_TIMED))) {
        if (def->is_lock_timed) {
            desc->ltime = date;
            *update_flag |= UPDATE_LTIME_COLUMN;
            desc->astatus |= ACCOUNT_STATUS_LOCK_TIMED;
        } else {
            desc->lcount = 0;
            *update_flag |= UPDATE_LCOUNT_COLUMN;
            GS_BIT_RESET(desc->astatus, ACCOUNT_STATUS_LOCK_TIMED);
        }
        *update_flag |= UPDATE_ASTATUS_COLUMN;
    }

    if (GS_BIT_TEST(def->mask, GS_GET_MASK(ALTER_USER_FIELD_LCOUNT))) {
        if (def->is_lcount_clear) {
            desc->lcount = 0;
        } else {
            desc->lcount++;
        }
        *update_flag |= UPDATE_LCOUNT_COLUMN;
    }

    return GS_SUCCESS;
}
static status_t db_check_ddm_by_user(knl_session_t *session, uint32 uid)
{
    knl_cursor_t *cursor = NULL;
    CM_SAVE_STACK(session->stack);
    cursor = knl_push_cursor(session);
    knl_scan_key_t *l_key = NULL;
    knl_scan_key_t *r_key = NULL;

    knl_open_sys_cursor(session, cursor, CURSOR_ACTION_SELECT, SYS_DDM_ID, IX_SYS_DDM_001_ID);
    knl_init_index_scan(cursor, GS_FALSE);
    l_key = &cursor->scan_range.l_key;
    knl_set_scan_key(INDEX_DESC(cursor->index), l_key, GS_TYPE_INTEGER, &uid, sizeof(uint32),
        IX_COL_SYS_DDM_001_UID);
    knl_set_key_flag(l_key, SCAN_KEY_LEFT_INFINITE, IX_COL_SYS_DDM_001_OID);
    knl_set_key_flag(l_key, SCAN_KEY_LEFT_INFINITE, IX_COL_SYS_DDM_001_COLID);
    r_key = &cursor->scan_range.r_key;
    knl_set_scan_key(INDEX_DESC(cursor->index), r_key, GS_TYPE_INTEGER, &uid, sizeof(uint32),
        IX_COL_SYS_DDM_001_UID);
    knl_set_key_flag(r_key, SCAN_KEY_RIGHT_INFINITE, IX_COL_SYS_DDM_001_OID);
    knl_set_key_flag(r_key, SCAN_KEY_RIGHT_INFINITE, IX_COL_SYS_DDM_001_COLID);

    if (knl_fetch(session, cursor) != GS_SUCCESS) {
        CM_RESTORE_STACK(session->stack);
        return GS_ERROR;
    }
    if (cursor->eof == GS_FALSE) {
        CM_RESTORE_STACK(session->stack);
        GS_THROW_ERROR_EX(ERR_INVALID_OPERATION, ", the user has rule, please drop rule firstly.");
        return GS_ERROR;
    }
    CM_RESTORE_STACK(session->stack);
    return GS_SUCCESS;
}

void dc_unlock_user(knl_session_t *session, text_t *username)
{
    (void)dc_set_user_status(session, username, USER_STATUS_NORMAL);
    unlock_tables_directly(session);
    session->drop_uid = GS_INVALID_ID32;
}

status_t user_drop_core(knl_session_t *session, dc_user_t *user, bool32 purge)
{
    uint32 uid = user->desc.id;
    rd_user_t redo;
    text_t username;

    if (dc_lock_user(session, user) != GS_SUCCESS) {
        return GS_ERROR;
    }
    session->drop_uid = uid;
    // if user has ddm policy, please drop policy first
    cm_str2text(user->desc.name, &username);
    if (db_check_ddm_by_user(session, uid) != GS_SUCCESS) {
        dc_unlock_user(session, &username);
        return GS_ERROR;
    }
    /* no session refer the user now and start to drop the user's objects */
    /* find if any objects in user's schema */
    if (!db_user_has_objects(session, uid, &username)) {
        if (user_drop_common_objects(session, uid) != GS_SUCCESS) {
            dc_unlock_user(session, &username);
            return GS_ERROR;
        }

        dc_drop_user(session, uid);
        session->drop_uid = GS_INVALID_ID32;
        knl_commit(session);
        unlock_tables_directly(session);

        redo.op_type = RD_DROP_USER;
        redo.uid = uid;
        log_put(session, RD_LOGIC_OPERATION, &redo, sizeof(rd_user_t), LOG_ENTRY_FLAG_NONE);

        return GS_SUCCESS;
    }

    /* export error, need to specify the CASCADE option */
    if (!purge) {
        dc_unlock_user(session, &username);
        GS_THROW_ERROR(ERR_USER_IS_REFERENCED, "user", "objects", "being used");
        return GS_ERROR;
    }

    if (user_drop_user_objects(session, uid, &username) != GS_SUCCESS) {
        unlock_tables_directly(session);
        dc_free_user_entry(session, uid);
        (void)dc_set_user_status(session, &username, USER_STATUS_NORMAL);
        session->drop_uid = GS_INVALID_ID32;
        return GS_ERROR;
    }

    /* drop the user item in dc */
    dc_drop_user(session, uid);
    session->drop_uid = GS_INVALID_ID32;
    redo.op_type = RD_DROP_USER;
    redo.uid = uid;
    log_put(session, RD_LOGIC_OPERATION, &redo, sizeof(rd_user_t), LOG_ENTRY_FLAG_NONE);
    knl_commit(session);
    unlock_tables_directly(session);
    dc_free_user_entry(session, uid);

    return GS_SUCCESS;
}

/*
 * drop an user
 * @param
 * - session: kernel session
 * - def : drop user definition
 * @return
 * - GS_SUCCESS
 * - GS_ERROR
 * @note null
 * @see null
 */
status_t user_drop(knl_session_t *session, knl_drop_user_t *def)
{
    dc_context_t *ctx = &session->kernel->dc_ctx;
    dc_user_t *user = NULL;

    if (cm_text_str_equal(&def->owner, SYS_USER_NAME) || cm_text_str_equal(&def->owner, PUBLIC_USER)) {
        GS_THROW_ERROR(ERR_USER_IS_REFERENCED, "user", T2S(&def->owner), "system user");
        return GS_ERROR;
    }
    cm_spin_lock(&ctx->paral_lock, NULL);
    knl_set_session_scn(session, GS_INVALID_ID64);

    if (dc_open_user(session, &def->owner, &user) != GS_SUCCESS) {
        cm_spin_unlock(&ctx->paral_lock);
        if (def->options & DROP_IF_EXISTS) {
            int32 code = cm_get_error_code();
            if (code == ERR_USER_NOT_EXIST) {
                cm_reset_error();
                return GS_SUCCESS;
            }
        }
        return GS_ERROR;
    }

    status_t status = user_drop_core(session, user, def->purge);
    cm_spin_unlock(&ctx->paral_lock); 

    return status;
}

status_t user_alter(knl_session_t *session, knl_user_def_t *def)
{
    knl_cursor_t *cursor = NULL;
    knl_user_desc_t desc;
    dc_context_t *ctx = &session->kernel->dc_ctx;
    text_t owner;
    dc_user_t *user = NULL;
    rd_user_t redo;
    errno_t err;
    uint32 update_flag = 0;

    cm_str2text(def->name, &owner);
    cm_spin_lock(&ctx->paral_lock, NULL);
    if (dc_open_user_direct(session, &owner, &user) != GS_SUCCESS) {
        cm_spin_unlock(&ctx->paral_lock);
        return GS_ERROR;
    }

    cm_spin_lock(&user->lock, NULL);
    desc = user->desc;
    if (user_prepare_alter(session, def, user, &desc, &update_flag) != GS_SUCCESS) {
        cm_spin_unlock(&user->lock);
        cm_spin_unlock(&ctx->paral_lock);
        return GS_ERROR;
    }

    CM_SAVE_STACK(session->stack);

    cursor = knl_push_cursor(session);

    knl_open_sys_cursor(session, cursor, CURSOR_ACTION_UPDATE, SYS_USER_ID, IX_SYS_USER_001_ID);
    knl_init_index_scan(cursor, GS_TRUE);
    knl_set_scan_key(INDEX_DESC(cursor->index), &cursor->scan_range.l_key, GS_TYPE_INTEGER, &desc.id, sizeof(uint32),
        0);
    if (!DB_IS_READONLY(session)) {
        if (knl_fetch(session, cursor) != GS_SUCCESS) {
            CM_RESTORE_STACK(session->stack);
            cm_spin_unlock(&user->lock);
            cm_spin_unlock(&ctx->paral_lock);
            return GS_ERROR;
        }
        knl_panic_log(!cursor->eof, "data is not found, panic info: page %u-%u type %u table %s index %s",
                      cursor->rowid.file, cursor->rowid.page, ((page_head_t *)cursor->page_buf)->type,
                      ((table_t *)cursor->table)->desc.name, ((index_t *)cursor->index)->desc.name);
    }

    if (db_alter_user_field(session, &desc, cursor, update_flag) != GS_SUCCESS) {
        CM_RESTORE_STACK(session->stack);
        cm_spin_unlock(&user->lock);
        cm_spin_unlock(&ctx->paral_lock);
        return GS_ERROR;
    }

    cm_spin_unlock(&user->lock);

    if (!DB_IS_READONLY(session)) {
        cm_decode_row(cursor->update_info.data, cursor->update_info.offsets, cursor->update_info.lens, NULL);

        if (GS_SUCCESS != knl_internal_update(session, cursor)) {
            CM_RESTORE_STACK(session->stack);
            cm_spin_unlock(&ctx->paral_lock);
            return GS_ERROR;
        }

        redo.op_type = RD_ALTER_USER;
        err = strcpy_sp(redo.name, GS_NAME_BUFFER_SIZE, def->name);
        knl_securec_check(err);
        log_put(session, RD_LOGIC_OPERATION, &redo, sizeof(rd_user_t), LOG_ENTRY_FLAG_NONE);

        knl_commit(session);
    }

    user->desc = desc;
    CM_RESTORE_STACK(session->stack);
    cm_spin_unlock(&ctx->paral_lock);

    return GS_SUCCESS;
}

status_t user_prepare_role_desc(knl_session_t *session, knl_role_def_t *def, knl_role_desc_t *desc)
{
    uint32 i;
    dc_user_t *user = NULL;
    dc_role_t *role = NULL;
    dc_context_t *ctx = &session->kernel->dc_ctx;
    size_t password_len;
    errno_t err;

    desc->id = GS_INVALID_ID32;

    /* role name can NOT be the same with users */
    for (i = 0; i < GS_MAX_USERS; i++) {
        cm_spin_lock(&ctx->lock, NULL);
        user = ctx->users[i];
        if (user != NULL && user->status != USER_STATUS_DROPPED && cm_str_equal_ins(user->desc.name, def->name)) {
            cm_spin_unlock(&ctx->lock);
            GS_THROW_ERROR(ERR_OBJECT_EXISTS, "user", def->name);
            return GS_ERROR;
        }
        cm_spin_unlock(&ctx->lock);
    }

    /* allocate the free role id */
    for (i = 0; i < GS_MAX_ROLES; i++) {
        cm_spin_lock(&ctx->lock, NULL);
        role = ctx->roles[i];
        if (role == NULL) {
            if (desc->id == GS_INVALID_ID32) {
                desc->id = i;
            }
            cm_spin_unlock(&ctx->lock);
            continue;
        }
        cm_spin_unlock(&ctx->lock);
        if (cm_str_equal_ins(role->desc.name, def->name)) {
            GS_THROW_ERROR(ERR_OBJECT_EXISTS, "role", def->name);
            return GS_ERROR;
        }
    }

    if (desc->id == GS_INVALID_ID32) {
        GS_THROW_ERROR(ERR_MAX_ROLE_COUNT, "roles", GS_MAX_ROLES);
        return GS_ERROR;
    }

    desc->owner_uid = def->owner_uid;
    err = memcpy_sp(desc->name, GS_NAME_BUFFER_SIZE, def->name, GS_NAME_BUFFER_SIZE);
    knl_securec_check(err);
    if (def->password[0] != '\0') {
        if (def->is_encrypt) {
            password_len = strlen(def->password);
            err = strncpy_s(desc->password, GS_PASSWORD_BUFFER_SIZE, def->password, password_len);
            knl_securec_check(err);
        } else {
            if (user_encrypt_password((char *)session->kernel->attr.pwd_alg, session->kernel->attr.alg_iter,
                def->password, (uint32)strlen(def->password), desc->password,
                GS_PASSWORD_BUFFER_SIZE) != GS_SUCCESS) {
                err = memset_sp(def->password, GS_PASSWORD_BUFFER_SIZE, 0, GS_PASSWORD_BUFFER_SIZE);
                knl_securec_check(err);
                return GS_ERROR;
            }
        }
        err = memset_sp(def->password, GS_PASSWORD_BUFFER_SIZE, 0, GS_PASSWORD_BUFFER_SIZE);
        knl_securec_check(err);
    } else {
        err = memset_sp(desc->password, GS_PASSWORD_BUFFER_SIZE, 0, GS_PASSWORD_BUFFER_SIZE);
        knl_securec_check(err);
    }
    return GS_SUCCESS;
}

static status_t user_insert_sys_role(knl_session_t *session, knl_cursor_t *cursor, knl_role_desc_t *desc)
{
    row_assist_t row;
    uint32 max_size;

    max_size = session->kernel->attr.max_row_size;
    row_init(&row, cursor->buf, max_size, 4);
    (void)row_put_int32(&row, desc->id);
    (void)row_put_int32(&row, desc->owner_uid);
    (void)row_put_str(&row, desc->name);
    (void)row_put_str(&row, desc->password);

    return knl_internal_insert(session, cursor);
}

status_t user_create_role(knl_session_t *session, knl_role_def_t *def)
{
    knl_cursor_t *cursor = NULL;
    knl_role_desc_t desc;
    dc_context_t *ctx = &session->kernel->dc_ctx;
    rd_role_t redo;
    errno_t err;

    if (session->kernel->db.status <= DB_STATUS_RECOVERY) {
        GS_THROW_ERROR(ERR_NO_DB_ACTIVE);
        return GS_ERROR;
    }

    if (DB_IS_UPGRADE(session) && !session->kernel->db.has_load_role) {
        if (dc_init_roles(session, ctx) != GS_SUCCESS) {
            return GS_ERROR;
        }
        session->kernel->db.has_load_role = GS_TRUE;
    }

    cm_spin_lock(&ctx->paral_lock, NULL);
    if (user_prepare_role_desc(session, def, &desc) != GS_SUCCESS) {
        cm_spin_unlock(&ctx->paral_lock);
        user_clear_password(desc.password, GS_PASSWORD_BUFFER_SIZE);
        return GS_ERROR;
    }

    CM_SAVE_STACK(session->stack);

    cursor = knl_push_cursor(session);
    knl_open_sys_cursor(session, cursor, CURSOR_ACTION_INSERT, SYS_ROLES_ID, GS_INVALID_ID32);

    if (user_insert_sys_role(session, cursor, &desc) != GS_SUCCESS) {
        cm_spin_unlock(&ctx->paral_lock);
        CM_RESTORE_STACK(session->stack);
        user_clear_password(desc.password, GS_PASSWORD_BUFFER_SIZE);
        return GS_ERROR;
    }

    /* add the new role to dc */
    if (dc_add_role(ctx, &desc) != GS_SUCCESS) {
        cm_spin_unlock(&ctx->paral_lock);
        CM_RESTORE_STACK(session->stack);
        user_clear_password(desc.password, GS_PASSWORD_BUFFER_SIZE);
        return GS_ERROR;
    }

    cm_spin_unlock(&ctx->paral_lock);
    CM_RESTORE_STACK(session->stack);

    redo.op_type = RD_CREATE_ROLE;
    redo.rid = desc.id;
    err = strcpy_sp(redo.name, GS_NAME_BUFFER_SIZE, desc.name);
    knl_securec_check(err);
    log_put(session, RD_LOGIC_OPERATION, &redo, sizeof(rd_role_t), LOG_ENTRY_FLAG_NONE);
    user_clear_password(desc.password, GS_PASSWORD_BUFFER_SIZE);

    // 2024-9-8
    knl_commit(session);
    return GS_SUCCESS;
}

/*
 * drop an role
 * @param
 * - session: kernel session
 * - def : drop role definition
 * @return
 * - GS_SUCCESS
 * - GS_ERROR
 * @note
 *  remove a role from the database. When you drop a role, Database revokes it from all
 *  users and roles to whom it has been granted and removes it from the database. User
 *  sessions in which the role is already enabled are not affected. However, no new user
 *  session can enable the role after it is dropped.
 *
 * @see null
 */
status_t user_drop_role(knl_session_t *session, knl_drop_def_t *def)
{
    uint32 rid;
    dc_context_t *ctx = &session->kernel->dc_ctx;
    rd_role_t redo;

    cm_spin_lock(&ctx->paral_lock, NULL);

    if (!dc_get_role_id(session, &def->name, &rid)) {
        cm_spin_unlock(&ctx->paral_lock);
        GS_THROW_ERROR(ERR_ROLE_NOT_EXIST, T2S(&def->name));
        return GS_ERROR;
    }
    
    if (rid < SYS_ROLE_ID_COUNT) {
        cm_spin_unlock(&ctx->paral_lock);
        GS_THROW_ERROR(ERR_USER_IS_REFERENCED, "role", T2S(&def->name), "system role");
        return GS_ERROR;
    }
    
    if (db_delete_all_privs_by_id(session, rid, 1)) {
        cm_spin_unlock(&ctx->paral_lock);
        return GS_ERROR;
    }

    /* delete the tuple in sysroles */
    if (db_delete_from_sys_roles(session, rid) != GS_SUCCESS) {
        cm_spin_unlock(&ctx->paral_lock);
        return GS_ERROR;
    }

    /* remove the dropped role from the dc */
    if (dc_drop_role(session, rid) != GS_SUCCESS) {
        cm_spin_unlock(&ctx->paral_lock);
        return GS_ERROR;
    }

    cm_spin_unlock(&ctx->paral_lock);

    redo.op_type = RD_DROP_ROLE;
    redo.rid = rid;
    log_put(session, RD_LOGIC_OPERATION, &redo, sizeof(rd_role_t), LOG_ENTRY_FLAG_NONE);

    // 2024-9-8
    knl_commit(session);
    return GS_SUCCESS;
}

status_t user_encrypt_password(const char *alg, uint32 iter_count, char *plain, uint32 plain_len, char *cipher,
    uint32 cipher_len)
{
    if (alg != NULL && cm_str_equal_ins(alg, "PBKDF2")) {
        if (cm_generate_scram_sha256(plain, plain_len, GS_KDF2DEFITERATION, (uchar *)cipher,
            &cipher_len) != GS_SUCCESS) {
            GS_THROW_ERROR(ERR_ENCRYPTION_ERROR);
            return GS_ERROR;
        }
    } else { // SCRAM_SHA256
        if (cm_generate_scram_sha256(plain, plain_len, iter_count, (uchar *)cipher, &cipher_len) != GS_SUCCESS) {
            GS_THROW_ERROR(ERR_ENCRYPTION_ERROR);
            return GS_ERROR;
        }
    }
    return GS_SUCCESS;
}

#ifdef __cplusplus
}
#endif

