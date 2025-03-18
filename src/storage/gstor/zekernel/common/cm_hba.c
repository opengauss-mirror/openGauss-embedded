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
 * cm_hba.c
 *
 *
 * IDENTIFICATION
 * src/storage/gstor/zekernel/common/cm_hba.c
 *
 * -------------------------------------------------------------------------
 */
#include "cm_hba.h"
#include "cm_file.h"

#ifdef __cplusplus
extern "C" {
#endif

static status_t cm_read_hba_file(const char *file_name, char *buf, uint32 *buf_len)
{
    int32 file_fd;
    status_t status;
    uint32 mode = O_RDONLY | O_BINARY;

    if (cm_open_file(file_name, mode, &file_fd) != GS_SUCCESS) {
        return GS_ERROR;
    }

    int64 size = cm_file_size(file_fd);
    if (size == -1) {
        cm_close_file(file_fd);
        GS_THROW_ERROR(ERR_SEEK_FILE, 0, SEEK_END, errno);
        return GS_ERROR;
    }

    if (size > (int64)(*buf_len)) {
        cm_close_file(file_fd);
        GS_THROW_ERROR(ERR_FILE_SIZE_TOO_LARGE, file_name);
        return GS_ERROR;
    }

    if (cm_seek_file(file_fd, 0, SEEK_SET) != 0) {
        cm_close_file(file_fd);
        GS_THROW_ERROR(ERR_SEEK_FILE, 0, SEEK_SET, errno);
        return GS_ERROR;
    }

    status = cm_read_file(file_fd, buf, (int32)size, (int32 *)buf_len);
    cm_close_file(file_fd);
    return status;
}

status_t cm_write_hba_file(const char *file_name, const char *buf, uint32 buf_len, bool32 on_create)
{
    int32 file_fd;
    status_t status;
    uint32 mode = O_RDWR | O_APPEND | O_SYNC;
    if (on_create == GS_TRUE) {
        mode = mode | O_CREAT;
    }

    if (cm_open_file(file_name, mode, &file_fd) != GS_SUCCESS) {
        return GS_ERROR;
    }
    (void)cm_chmod_file(S_IRUSR | S_IWUSR, file_fd);

    int64 size = cm_file_size(file_fd);
    if (size == -1) {
        cm_close_file(file_fd);
        GS_THROW_ERROR(ERR_SEEK_FILE, 0, SEEK_END, errno);
        return GS_ERROR;
    }

    if (size > GS_MAX_HBA_FILE_SIZE - buf_len) {
        cm_close_file(file_fd);
        GS_THROW_ERROR(ERR_FILE_SIZE_TOO_LARGE, file_name);
        return GS_ERROR;
    }

    status = cm_write_file(file_fd, buf, (int32)buf_len);
    cm_close_file(file_fd);
    return status;
}
status_t get_format_user(text_t *user)
{
    bool32 is_exist = GS_FALSE;

    if (user->len == 0) {
        return GS_ERROR;
    }

    /* separate ' " ' from user. */
    if (user->str[0] == '"') {
        is_exist = GS_TRUE;

        if (user->len <= 1) {
            return GS_ERROR;
        }

        if (user->str[user->len - 1] != '"') {
            return GS_ERROR;
        }

        /* only the char in ' " ' is reserved. */
        user->str = user->str + 1;
        user->len -= 2;
    }
    if (user->len > GS_MAX_NAME_LEN) {
        return GS_ERROR;
    }
    /* if  ' " ' not exist,upper user. */
    if (!is_exist) {
        cm_text_upper(user);
    }

    return GS_SUCCESS;
}

static status_t cm_deparse_hba_line(const text_t *line, uint32 line_no, text_t *type, text_t *user, text_t *ip)
{
    text_t remain;

    cm_split_text(line, ' ', '\0', type, &remain);
    cm_trim_text(type);

    cm_trim_text(&remain);
    cm_split_text(&remain, ' ', '\0', user, ip);
    cm_trim_text(user);

    /* format user. */
    if (GS_SUCCESS != get_format_user(user)) {
        GS_THROW_ERROR(ERR_INVALID_HBA_ITEM, "Hba", line_no);
        return GS_ERROR;
    }

    cm_trim_text(ip);

    if (!(cm_text_str_equal_ins(type, "host") || cm_text_str_equal_ins(type, "hostssl")) || CM_IS_EMPTY(user) ||
        CM_IS_EMPTY(ip)) {
        GS_THROW_ERROR(ERR_INVALID_HBA_ITEM, "Hba", line_no);
        return GS_ERROR;
    }

    return GS_SUCCESS;
}

static inline status_t cm_create_uwl_entry(bool32 hostssl, const text_t *user, text_t *ip, uwl_entry_t *uwl_entry)
{
    INIT_UWL_ENTRY(uwl_entry);
    uwl_entry->hostssl = hostssl;
    MEMS_RETURN_IFERR(memcpy_sp(uwl_entry->user, (size_t)GS_MAX_NAME_LEN, user->str, (size_t)user->len));

    if (cm_parse_cidrs(ip, &uwl_entry->white_list) != GS_SUCCESS) {
        cm_destroy_list(&uwl_entry->white_list);
        return GS_ERROR;
    }

    return GS_SUCCESS;
}

static inline void cm_destory_uwl_list(list_t *uwl)
{
    uint32 i;

    for (i = 0; i < uwl->count; i++) {
        uwl_entry_t *uwl_entry = (uwl_entry_t *)cm_list_get(uwl, i);

        cm_destroy_list(&uwl_entry->white_list);
    }

    cm_destroy_list(uwl);
}

static status_t cm_parse_hba_line(text_t *line, uint32 line_no, uwl_entry_t *uwl_entry)
{
    bool32 hostssl = GS_FALSE;
    text_t type, user, ip;

    GS_RETURN_IFERR(cm_deparse_hba_line(line, line_no, &type, &user, &ip));
    hostssl = cm_text_str_equal_ins(&type, "hostssl");
    GS_RETURN_IFERR(cm_create_uwl_entry(hostssl, &user, &ip, uwl_entry));

    return GS_SUCCESS;
}

static status_t cm_parse_hba(white_context_t *ctx, char *buf, uint32 buf_len)
{
    uint32 line_no = 0;
    text_t lines, line;
    list_t new_uwl; // user write list

    CM_POINTER(buf);
    lines.len = buf_len;
    lines.str = buf;

    cm_create_list(&new_uwl, sizeof(uwl_entry_t));
    while (cm_fetch_text(&lines, '\n', '\0', &line)) {
        uwl_entry_t *uwl_entry = NULL;
        line_no++;
        // ignore comment or empty line
        cm_trim_text(&line);
        if (line.len == 0 || line.str[0] == '#') {
            continue;
        }
        if (line.len >= HBA_MAX_LINE_SIZE) {
            cm_destory_uwl_list(&new_uwl);
            GS_THROW_ERROR(ERR_LINE_SIZE_TOO_LONG, line_no);
            return GS_ERROR;
        }
        if (cm_list_new(&new_uwl, (pointer_t *)&uwl_entry) != GS_SUCCESS ||
            cm_parse_hba_line(&line, line_no, uwl_entry) != GS_SUCCESS) {
            cm_destory_uwl_list(&new_uwl);
            return GS_ERROR;
        }
    }

    cm_spin_lock(&ctx->lock, NULL);
    cm_destory_uwl_list(&ctx->user_white_list);
    ctx->user_white_list = new_uwl;
    cm_spin_unlock(&ctx->lock);

    return GS_SUCCESS;
}

status_t cm_load_hba(white_context_t *ctx, const char *file_name)
{
    char *file_buf = (char *)malloc(GS_MAX_HBA_FILE_SIZE);
    if (file_buf == NULL) {
        GS_THROW_ERROR(ERR_ALLOC_MEMORY, (uint64)(GS_MAX_HBA_FILE_SIZE), "hba");
        return GS_ERROR;
    }

    uint32 buf_len = GS_MAX_HBA_FILE_SIZE;
    errno_t ret = memset_sp(file_buf, GS_MAX_HBA_FILE_SIZE, 0, GS_MAX_HBA_FILE_SIZE);
    if (ret != EOK) {
        CM_FREE_PTR(file_buf);
        GS_THROW_ERROR(ERR_SYSTEM_CALL, ret);
        return GS_ERROR;
    }

    if (cm_read_hba_file(file_name, file_buf, &buf_len) != GS_SUCCESS) {
        CM_FREE_PTR(file_buf);
        return GS_ERROR;
    }
    if (cm_parse_hba(ctx, file_buf, buf_len) != GS_SUCCESS) {
        CM_FREE_PTR(file_buf);
        return GS_ERROR;
    }

    CM_FREE_PTR(file_buf);
    return GS_SUCCESS;
}

status_t cm_check_hba_entry_legality(char *hba_str)
{
    text_t line, type, user, ip, remain;
    list_t ip_temp_list;

    cm_str2text_safe(hba_str, (uint32)strlen(hba_str), &line);

    cm_split_text(&line, ' ', '\0', &type, &remain);
    cm_trim_text(&type);

    cm_trim_text(&remain);
    cm_split_text(&remain, ' ', '\0', &user, &ip);
    cm_trim_text(&user);

    if (get_format_user(&user) != GS_SUCCESS) {
        GS_THROW_ERROR(ERR_INVALID_HBA_ITEM, "Hba", 1);
        return GS_ERROR;
    }

    cm_trim_text(&ip);

    if (!(cm_text_str_equal_ins(&type, "host") || cm_text_str_equal_ins(&type, "hostssl")) || CM_IS_EMPTY(&user) ||
        CM_IS_EMPTY(&ip)) {
        GS_THROW_ERROR(ERR_INVALID_HBA_ITEM, "Hba", 1);
        return GS_ERROR;
    }

    cm_create_list(&ip_temp_list, sizeof(cidr_t));
    // check ip legality, ip_temp_list not used any further
    if (cm_parse_cidrs(&ip, &ip_temp_list) != GS_SUCCESS) {
        GS_THROW_ERROR(ERR_INVALID_HBA_ITEM, "Hba", 1);
        cm_destroy_list(&ip_temp_list);
        return GS_ERROR;
    }

    cm_destroy_list(&ip_temp_list);

    return GS_SUCCESS;
}


// !!Caution: Invoker should cm_destroy_list(ip_list) if GS_ERROR returned.
status_t cm_parse_ip_str(text_t *ip_texts, list_t *ip_list)
{
    text_t hba_ip_txt;
    char cidr_str[CM_MAX_IP_LEN] = { 0 };
    hba_ip_entry_t *ip_entry = NULL;

    GS_RETSUC_IFTRUE(ip_texts == NULL || CM_IS_EMPTY(ip_texts));

    if (CM_TEXT_BEGIN(ip_texts) == '(' && CM_TEXT_END(ip_texts) == ')') {
        CM_REMOVE_ENCLOSED_CHAR(ip_texts);
    }

    while (cm_fetch_text(ip_texts, ',', 0, &hba_ip_txt)) {
        GS_CONTINUE_IFTRUE(hba_ip_txt.len == 0);
        GS_RETURN_IFERR(cm_list_new(ip_list, (pointer_t *)&ip_entry));

        cm_trim_text(&hba_ip_txt);
        cm_text2str(&hba_ip_txt, ip_entry->ip, CM_MAX_IP_LEN);

        cm_text2str(&hba_ip_txt, cidr_str, CM_MAX_IP_LEN);
        GS_RETURN_IFERR(cm_str_to_cidr(cidr_str, &ip_entry->cidr, CM_MAX_IP_LEN));

        ip_entry->is_hit = GS_FALSE;
    }

    return GS_SUCCESS;
}

static status_t cm_parse_hba_to_entry(text_t *line, hba_conf_entry_t *hba_node_info)
{
    text_t type, user, ip, remain;

    cm_split_text(line, ' ', '\0', &type, &remain);
    cm_trim_text(&type);

    cm_trim_text(&remain);
    cm_split_text(&remain, ' ', '\0', &user, &ip);
    cm_trim_text(&user);

    if (get_format_user(&user) != GS_SUCCESS) {
        GS_THROW_ERROR(ERR_INVALID_HBA_ITEM, "Hba", 1);
        return GS_ERROR;
    }

    cm_trim_text(&ip);

    if (CM_IS_EMPTY(&ip)) {
        GS_THROW_ERROR(ERR_INVALID_HBA_ITEM, "Hba", 1);
        return GS_ERROR;
    }

    cm_text2str(&type, hba_node_info->host_name, GS_MAX_NAME_LEN);
    cm_text2str(&user, hba_node_info->user_name, GS_MAX_NAME_LEN);

    cm_create_list(&hba_node_info->ip_entry_list, sizeof(hba_ip_entry_t));
    if (cm_parse_ip_str(&ip, &hba_node_info->ip_entry_list) != GS_SUCCESS) {
        GS_THROW_ERROR(ERR_INVALID_HBA_ITEM, "Hba", 1);
        return GS_ERROR;
    }

    hba_node_info->left_count = hba_node_info->ip_entry_list.count;
    return GS_SUCCESS;
}

static inline void cm_destory_zhba_list(list_t *zhba_list)
{
    for (uint32 i = 0; i < zhba_list->count; i++) {
        hba_conf_entry_t *hba_conf_entry = (hba_conf_entry_t *)cm_list_get(zhba_list, i);
        cm_destroy_list(&hba_conf_entry->ip_entry_list);
    }
    cm_destroy_list(zhba_list);
}


static status_t cm_parse_zhba_lines(zhba_context_t *zhba_ctx, char *buf, uint32 buf_len)
{
    uint32 line_no = 0;
    text_t lines, line;

    CM_POINTER(buf);
    lines.len = buf_len;
    lines.str = buf;

    while (cm_fetch_text(&lines, '\n', '\0', &line)) {
        hba_conf_entry_t *hba_conf_entry = NULL;

        if (line.len >= HBA_MAX_LINE_SIZE) {
            GS_THROW_ERROR(ERR_LINE_SIZE_TOO_LONG, line_no);
            return GS_ERROR;
        }

        line_no++;

        // ignore comment or empty line
        cm_trim_text(&line);
        if (line.len == 0 || line.str[0] == '#') {
            continue;
        }

        if (cm_list_new(&zhba_ctx->zhba_list, (pointer_t *)&hba_conf_entry) != GS_SUCCESS ||
            cm_parse_hba_to_entry(&line, hba_conf_entry) != GS_SUCCESS) {
            return GS_ERROR;
        }
    }

    return GS_SUCCESS;
}

static status_t cm_put_zhba_line(hba_conf_entry_t *conf_entry, char *buf, uint32 *offset)
{
    char append_buf[HBA_MAX_LINE_SIZE + 1] = { '\n' };
    int ret_len;
    uint32 tmp_offset = *offset;

    // empty content
    if (conf_entry->left_count == 0) {
        return GS_SUCCESS;
    }

    ret_len = snprintf_s(append_buf, HBA_MAX_LINE_SIZE + 1, HBA_MAX_LINE_SIZE, "%s %s ", conf_entry->host_name,
        conf_entry->user_name);
    PRTS_RETURN_IFERR(ret_len);

    int append_buf_len = HBA_MAX_LINE_SIZE + 1 - (int)ret_len;
    for (uint32 i = 0; i < conf_entry->ip_entry_list.count; ++i) {
        char ip_str[CM_MAX_IP_LEN + 2] = { 0 };

        hba_ip_entry_t *node = (hba_ip_entry_t *)cm_list_get(&conf_entry->ip_entry_list, i);
        if (node->is_hit == GS_TRUE) {
            continue;
        }

        ret_len = snprintf_s(ip_str, CM_MAX_IP_LEN + 2, CM_MAX_IP_LEN + 1, "%s,", node->ip);
        PRTS_RETURN_IFERR(ret_len);

        MEMS_RETURN_IFERR(strncat_s(append_buf, append_buf_len, ip_str, strlen(ip_str)));
        append_buf_len -= (int)ret_len;

        if (append_buf_len < 0) {
            return GS_ERROR;
        }
    }

    // replace last char from ',' to '\n'
    append_buf_len = (int)strlen(append_buf);
    if (append_buf_len == 0) {
        return GS_ERROR;
    }
    append_buf[append_buf_len - 1] = '\n';

    if (*offset + append_buf_len > GS_MAX_HBA_FILE_SIZE) {
        return GS_ERROR;
    }

    MEMS_RETURN_IFERR(
        memcpy_sp(buf + tmp_offset, (size_t)(GS_MAX_HBA_FILE_SIZE - tmp_offset), append_buf, (size_t)append_buf_len));

    *offset += append_buf_len;
    return GS_SUCCESS;
}

static void cm_free_hba_conf_entry(hba_conf_entry_t *hba_conf_entry)
{
    if (hba_conf_entry == NULL) {
        return;
    }
    cm_destroy_list(&hba_conf_entry->ip_entry_list);
    CM_FREE_PTR(hba_conf_entry);
}

static void cm_free_zhba_context(zhba_context_t *zhba_ctx)
{
    for (uint32 i = 0; i < zhba_ctx->zhba_list.count; ++i) {
        hba_conf_entry_t *line = (hba_conf_entry_t *)cm_list_get(&zhba_ctx->zhba_list, i);
        if (line != NULL) {
            cm_destroy_list(&line->ip_entry_list);
        }
    }
    cm_reset_list(&zhba_ctx->zhba_list);
}

static status_t cm_parser_to_zhba_context(zhba_context_t *zhba_ctx, const char *file_name)
{
    char *file_buf = (char *)malloc(GS_MAX_HBA_FILE_SIZE);
    if (file_buf == NULL) {
        GS_THROW_ERROR(ERR_ALLOC_MEMORY, (uint64)(GS_MAX_HBA_FILE_SIZE), "hba");
        return GS_ERROR;
    }

    uint32 buf_len = GS_MAX_HBA_FILE_SIZE;
    errno_t ret = memset_sp(file_buf, GS_MAX_HBA_FILE_SIZE, 0, GS_MAX_HBA_FILE_SIZE);
    if (ret != EOK) {
        CM_FREE_PTR(file_buf);
        GS_THROW_ERROR(ERR_SYSTEM_CALL, ret);
        return GS_ERROR;
    }

    if (cm_read_hba_file(file_name, file_buf, &buf_len) != GS_SUCCESS) {
        CM_FREE_PTR(file_buf);
        return GS_ERROR;
    }
    if (cm_parse_zhba_lines(zhba_ctx, file_buf, buf_len) != GS_SUCCESS) {
        CM_FREE_PTR(file_buf);
        return GS_ERROR;
    }

    CM_FREE_PTR(file_buf);
    return GS_SUCCESS;
}

// match host and user precisely
static status_t cm_hba_key_matched(hba_conf_entry_t *entry_1, hba_conf_entry_t *entry_2)
{
    return (cm_str_equal_ins(entry_1->host_name, entry_2->host_name) &&
        cm_str_equal_ins(entry_1->user_name, entry_2->user_name));
}

static status_t cm_try_hit_hba_line(hba_conf_entry_t *zhba_line_to_color, hba_conf_entry_t *input_line,
    bool32 *ret_is_found)
{
    bool32 is_found;
    uint32 hit_count = 0;

    for (uint32 i = 0; i < input_line->ip_entry_list.count; ++i) {
        hba_ip_entry_t *input_node = (hba_ip_entry_t *)cm_list_get(&input_line->ip_entry_list, i);

        for (uint32 j = 0; j < zhba_line_to_color->ip_entry_list.count; ++j) {
            hba_ip_entry_t *zhba_node = (hba_ip_entry_t *)cm_list_get(&zhba_line_to_color->ip_entry_list, j);

            if (cm_cidr_equals_cidr(&zhba_node->cidr, &input_node->cidr, &is_found) != GS_SUCCESS) {
                cm_reset_error();
                GS_THROW_ERROR(ERR_HBA_ITEM_NOT_FOUND, input_node->ip);
                return GS_ERROR;
            }

            if (is_found) {
                *ret_is_found = GS_TRUE;
                zhba_node->is_hit = GS_TRUE;
                hit_count++;
                break;
            }
        }
    }

    zhba_line_to_color->left_count = zhba_line_to_color->ip_entry_list.count - hit_count;

    return GS_SUCCESS;
}

static status_t cm_new_hba_entry(hba_conf_entry_t **input_entry)
{
    hba_conf_entry_t *new_entry = NULL;
    new_entry = (hba_conf_entry_t *)malloc(sizeof(hba_conf_entry_t));
    if (new_entry == NULL) {
        GS_THROW_ERROR(ERR_ALLOC_MEMORY, (uint64)sizeof(hba_conf_entry_t), "allocate hba_conf_entry failed");
        return GS_ERROR;
    }

    errno_t ret = memset_sp(new_entry, (size_t)sizeof(hba_conf_entry_t), 0, (size_t)sizeof(hba_conf_entry_t));
    if (ret != EOK) {
        CM_FREE_PTR(new_entry);
        GS_THROW_ERROR(ERR_SYSTEM_CALL, ret);
        return GS_ERROR;
    }

    *input_entry = new_entry;
    return GS_SUCCESS;
}

static status_t search_hba_in_zhba_list(zhba_context_t *mem_zhba_ctx, hba_conf_entry_t *parsed_input,
    char *hba_entry_str, char *modified_buf, uint32 *buf_offset)
{
    mem_zhba_ctx->is_found = GS_FALSE;
    for (uint32 i = 0; i < mem_zhba_ctx->zhba_list.count; ++i) {
        hba_conf_entry_t *line = (hba_conf_entry_t *)cm_list_get(&mem_zhba_ctx->zhba_list, i);

        if (cm_hba_key_matched(parsed_input, line) == GS_TRUE) {
            if (cm_try_hit_hba_line(line, parsed_input, &mem_zhba_ctx->is_found) != GS_SUCCESS) {
                GS_THROW_ERROR(ERR_HBA_MOD_FAILED, hba_entry_str);
                return GS_ERROR;
            }
        }
        if (cm_put_zhba_line(line, modified_buf, buf_offset) != GS_SUCCESS) {
            GS_THROW_ERROR(ERR_HBA_MOD_FAILED, hba_entry_str);
            return GS_ERROR;
        }
    }

    if (mem_zhba_ctx->is_found != GS_TRUE) {
        GS_THROW_ERROR(ERR_HBA_ITEM_NOT_FOUND, hba_entry_str);
        return GS_ERROR;
    }
    return GS_SUCCESS;
}

static status_t cm_hba_get_modify_buf(const char *origin_file_name, text_t *hba_entry_txt, char *modified_buf,
    uint32 *buf_offset)
{
    hba_conf_entry_t *parsed_input = NULL;
    zhba_context_t mem_zhba_ctx;

    cm_create_list(&mem_zhba_ctx.zhba_list, sizeof(hba_conf_entry_t));
    if (cm_parser_to_zhba_context(&mem_zhba_ctx, origin_file_name) != GS_SUCCESS) {
        cm_free_zhba_context(&mem_zhba_ctx);
        GS_THROW_ERROR(ERR_HBA_MOD_FAILED, hba_entry_txt->str);
        return GS_ERROR;
    }

    if (cm_new_hba_entry(&parsed_input) != GS_SUCCESS) {
        cm_free_zhba_context(&mem_zhba_ctx);
        GS_THROW_ERROR(ERR_HBA_MOD_FAILED, hba_entry_txt->str);
        return GS_ERROR;
    }

    if (cm_parse_hba_to_entry(hba_entry_txt, parsed_input) != GS_SUCCESS) {
        cm_free_hba_conf_entry(parsed_input);
        cm_free_zhba_context(&mem_zhba_ctx);
        GS_THROW_ERROR(ERR_HBA_MOD_FAILED, hba_entry_txt->str);
        return GS_ERROR;
    }

    if (search_hba_in_zhba_list(&mem_zhba_ctx, parsed_input, hba_entry_txt->str, modified_buf, buf_offset) !=
        GS_SUCCESS) {
        cm_free_hba_conf_entry(parsed_input);
        cm_free_zhba_context(&mem_zhba_ctx);
        GS_THROW_ERROR(ERR_HBA_ITEM_NOT_FOUND, hba_entry_txt->str);
        return GS_ERROR;
    }

    cm_free_hba_conf_entry(parsed_input);
    cm_free_zhba_context(&mem_zhba_ctx);
    return GS_SUCCESS;
}

status_t cm_modify_hba_file(const char *origin_file_name, const char *swap_file_name, char *hba_entry_str)
{
    text_t hba_entry_txt;
    uint32 buf_offset = 0;

    char *modified_buf = (char *)malloc(GS_MAX_HBA_FILE_SIZE + 1);
    if (modified_buf == NULL) {
        GS_THROW_ERROR(ERR_ALLOC_MEMORY, (uint64)(GS_MAX_HBA_FILE_SIZE + 1), "hba");
        return GS_ERROR;
    }

    errno_t ret = memset_sp(modified_buf, GS_MAX_HBA_FILE_SIZE + 1, 0, GS_MAX_HBA_FILE_SIZE + 1);
    if (ret != EOK) {
        CM_FREE_PTR(modified_buf);
        GS_THROW_ERROR(ERR_SYSTEM_CALL, ret);
        return GS_ERROR;
    }

    cm_str2text_safe(hba_entry_str, (uint32)strlen(hba_entry_str), &hba_entry_txt);

    if (cm_hba_get_modify_buf(origin_file_name, &hba_entry_txt, modified_buf, &buf_offset) != GS_SUCCESS) {
        CM_FREE_PTR(modified_buf);
        GS_THROW_ERROR(ERR_HBA_MOD_FAILED, hba_entry_str);
        return GS_ERROR;
    }

    if (cm_write_hba_file(swap_file_name, modified_buf, (uint32)strlen(modified_buf), GS_TRUE) != GS_SUCCESS) {
        CM_FREE_PTR(modified_buf);
        return GS_ERROR;
    }
    if (cm_remove_file(origin_file_name) != GS_SUCCESS) {
        CM_FREE_PTR(modified_buf);
        return GS_ERROR;
    }
    if (cm_rename_file(swap_file_name, origin_file_name) != GS_SUCCESS) {
        CM_FREE_PTR(modified_buf);
        return GS_ERROR;
    }

    CM_FREE_PTR(modified_buf);
    return GS_SUCCESS;
}

status_t cm_free_hba_entry(list_t *mod_list)
{
    if (mod_list != NULL) {
        cm_destroy_list(mod_list);
    }

    return GS_SUCCESS;
}

#ifdef __cplusplus
}

#endif
