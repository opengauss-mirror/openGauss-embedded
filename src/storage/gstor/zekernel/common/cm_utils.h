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
 * cm_utils.h
 *
 *
 * IDENTIFICATION
 *    src/storage/gstor/zekernel/common/cm_utils.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef __CM_UTILS_H__
#define __CM_UTILS_H__
#include <time.h>
#include "cm_defs.h"
#include "cm_error.h"
#ifndef WIN32
#include "dlfcn.h"
#endif

#ifdef _WIN32
#ifndef ENABLE_INTSAFE_SIGNED_FUNCTIONS
#define ENABLE_INTSAFE_SIGNED_FUNCTIONS
#endif
#include <intsafe.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif

status_t cm_verify_password_str(const char *name, const char *passwd, uint32 pwd_min_len);
status_t cm_verify_password_check(const char *pText, uint32 i, uint32 *type_count, bool32 *num_flag,
                                  bool32 *upper_flag, bool32 *lower_flag, bool32 *special_flag);

uint32 cm_random(uint32 range);
uint32 cm_rand_int32(int64 *seed, uint32 range);
void cm_rand_string(uint32 length, char mode, char *buf);

extern uint8 g_nonnaming_chars[];
extern uint8 g_nonnaming_chars_ex[];

static inline bool32 is_nonnaming_char(uint8 c)
{
    return (bool32)g_nonnaming_chars[c];
}

static inline bool32 is_nonnaming_char_ex(uint8 c)
{
    return (bool32)g_nonnaming_chars_ex[c];
}

static inline bool32 contains_nonnaming_char(const char *str)
{
    const uint8 *char_ptr = (uint8 *)str;

    while (*char_ptr) {
        if (is_nonnaming_char(*char_ptr)) {
            return GS_TRUE;
        }
        char_ptr++;
    }
    return GS_FALSE;
}

static inline bool32 contains_nonnaming_char_ex(const char *str)
{
    const uint8 *char_ptr = (uint8 *)str;

    while (*char_ptr) {
        if (is_nonnaming_char_ex(*char_ptr)) {
            return GS_TRUE;
        }
        char_ptr++;
    }
    return GS_FALSE;
}

static inline status_t realpath_file(const char *filename, char *realfile, uint32 real_path_len)
{
#ifdef _WIN32
    if (!_fullpath(realfile, filename, real_path_len - 1)) {
        GS_THROW_ERROR(ERR_OPEN_FILE, filename, errno);
        return GS_ERROR;
    }
#else
    errno_t errcode;
    char resolved_path[PATH_MAX] = { 0 };

    if (!realpath(filename, resolved_path)) {
        // printf("error = %d\n", errno);
        if (errno != ENOENT && errno != EACCES) {
            GS_THROW_ERROR(ERR_OPEN_FILE, filename, errno);
            return GS_ERROR;
        }
    }
    errcode = strncpy_s(realfile, real_path_len, resolved_path, strlen(resolved_path));
    //errcode = strncpy_s(realfile, real_path_len, filename, strlen(filename));
    if (errcode != EOK) {
        GS_THROW_ERROR(ERR_SYSTEM_CALL, (errcode));
        return GS_ERROR;
    }
#endif
    return GS_SUCCESS;
}

typedef struct st_aligned_buf {
    int64 buf_size;
    char *alloc_buf;
    char *aligned_buf;
} aligned_buf_t;

static inline void *cm_aligned_buf(void *buf)
{
    return (char *)buf + (GS_MAX_ALIGN_SIZE_4K - ((uintptr_t)buf) % GS_MAX_ALIGN_SIZE_4K);
}

status_t cm_aligned_malloc(int64 size, const char *name, aligned_buf_t *buf);
status_t cm_aligned_realloc(int64 size, const char *name, aligned_buf_t *buf);
static inline void cm_aligned_free(aligned_buf_t *buf)
{
    if (buf->alloc_buf != NULL) {
        free(buf->alloc_buf);
    }
    buf->alloc_buf = NULL;
    buf->aligned_buf = NULL;
}



status_t cm_load_symbol(void *lib_handle, char *symbol, void **sym_lib_handle);
status_t cm_open_dl(void **lib_handle, char *symbol);
void cm_close_dl(void *lib_handle);

status_t cm_watch_file_init(int32 *i_fd, int32 *e_fd);
status_t cm_add_file_watch(int32 i_fd, const char *dirname, int32 *wd);
status_t cm_rm_file_watch(int32 i_fd, int32 *wd);
status_t cm_watch_file_event(int32 i_fd, int32 e_fd, int32 *wd);

#ifndef WIN32
status_t save_origin_argument(int argc, char ***argv);
status_t init_process_title(const char *title, uint32 len);
#endif

#ifdef __cplusplus
}
#endif

#endif
