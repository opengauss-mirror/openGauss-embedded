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
 * cm_charset.c
 *
 *
 * IDENTIFICATION
 *    src/storage/gstor/zekernel/common/cm_charset.c
 *
 * -------------------------------------------------------------------------
 */
#include "cm_charset.h"

#ifdef __cplusplus
extern "C" {
#endif

charset_t g_charsets[CHARSET_MAX] = {
    { CHARSET_UTF8, "UTF8", NULL, NULL, 6, CODE_PAGE_UTF8 },
    { CHARSET_GBK,  "GBK",  NULL, NULL, 4, CODE_PAGE_GB2312 },
};

collation_t g_collations[COLLATE_MAX] = {
    { COLLATE_UTF8_BIN,        "UTF8_BIN" },
    { COLLATE_UTF8_GENERAL_CI, "UTF8_GENERAL_CI" },
    { COLLATE_UTF8_UNICODE_CI, "UTF8_UNICODE_CI" },
    { COLLATE_GBK_BIN,         "GBK_BIN" },
    { COLLATE_GBK_CHINESE_CI,  "GBK_CHINESE_CI" },
};

#ifdef _ICONV
// [src][dst]
transcode_func_t g_transcode_func[CHARSET_MAX][CHARSET_MAX] = {
    { NULL, cm_utf8_to_gbk },
    { cm_gbk_to_utf8, NULL }
};
#endif

/* order should by same as 'charset_type_t' */
charset_func_t g_charset_func[CHARSET_MAX] = {
    {
        cm_utf8_move_char_forward,
        cm_utf8_move_char_backward,
        cm_utf8_name,
        cm_utf8_has_multibyte,
        cm_utf8_str_bytes,
        #ifdef _ICONV
        cm_utf8_to_unicode,
        #endif
        __cm_utf8_reverse_str_bytes,
        cm_utf8_text_like,
        cm_utf8_text_like_escape,
        cm_utf8_length,
        cm_utf8_length_ignore,
        cm_utf8_length_ignore_truncated_bytes,
        cm_utf8_substr,
        cm_utf8_substr_left,
        cm_utf8_substr_right,
        cm_utf8_instr,
        cm_utf8_get_start_byte_pos,
        cm_utf8_num_instr,
        cm_utf8_max_bytes_per_char,
        cm_utf8_multi_byte,
        cm_utf8_single_byte
    },
    {
        cm_gbk_move_char_forward,
        cm_gbk_move_char_backward,
        cm_gbk_name,
        cm_gbk_has_multibyte,
        cm_gbk_str_bytes,
        #ifdef _ICONV
        cm_gbk_to_unicode,
        #endif
        __cm_gbk_reverse_str_bytes,
        cm_gbk_text_like,
        cm_gbk_text_like_escape,
        cm_gbk_length,
        cm_gbk_length_ignore,
        cm_gbk_length_ignore_truncated_bytes,
        cm_gbk_substr,
        cm_gbk_substr_left,
        cm_gbk_substr_right,
        cm_gbk_instr,
        cm_gbk_get_start_byte_pos,
        cm_gbk_num_instr,
        cm_gbk_max_bytes_per_char,
        cm_gbk_multi_byte,
        cm_gbk_single_byte
    }
};

status_t cm_get_charset(const char *name, charset_t **charset)
{
    for (uint32 i = 0; i < CHARSET_MAX; i++) {
        if (strlen(name) == strlen(g_charsets[i].name) && cm_strcmpni(name, g_charsets[i].name, strlen(name)) == 0) {
            *charset = &g_charsets[i];
            return GS_SUCCESS;
        }
    }
    return GS_ERROR;
}

uint16 cm_get_charset_id(const char *name)
{
    charset_t *charset = NULL;

    if (cm_get_charset(name, &charset) != GS_SUCCESS) {
        return GS_INVALID_ID16;
    }
    return charset->id;
}

status_t cm_get_charset_ex(text_t *name, charset_t **charset)
{
    for (uint32 i = 0; i < CHARSET_MAX; i++) {
        if (cm_text_str_equal_ins(name, g_charsets[i].name)) {
            *charset = &g_charsets[i];
            return GS_SUCCESS;
        }
    }
    return GS_ERROR;
}

uint16 cm_get_charset_id_ex(text_t *name)
{
    charset_t *charset = NULL;

    if (cm_get_charset_ex(name, &charset) != GS_SUCCESS) {
        return GS_INVALID_ID16;
    }
    return charset->id;
}

status_t cm_get_collation(text_t *name, collation_t **collation)
{
    for (uint32 i = 0; i < COLLATE_MAX; i++) {
        if (cm_text_str_equal_ins(name, g_collations[i].name)) {
            *collation = &g_collations[i];
            return GS_SUCCESS;
        }
    }
    return GS_ERROR;
}

uint16 cm_get_collation_id(text_t *name)
{
    collation_t *collation = NULL;

    if (cm_get_collation(name, &collation) != GS_SUCCESS) {
        return GS_INVALID_ID16;
    }
    return collation->id;
}

const char *cm_get_charset_name(charset_type_t id)
{
    return (id >= CHARSET_MAX) ? NULL : g_charsets[id].name;
}

uint32 cm_get_cp_id(charset_type_t id)
{
    return (id >= CHARSET_MAX) ? g_charsets[0].cp_id : g_charsets[id].cp_id;
}

uint32 cm_get_max_size(charset_type_t id)
{
    return (id >= CHARSET_MAX) ? g_charsets[0].max_size : g_charsets[id].max_size;
}

#ifdef _ICONV
transcode_func_t cm_get_transcode_func(uint16 src_id, uint16 dst_id)
{
    return g_transcode_func[src_id][dst_id];
}

transcode_func_t cm_get_transcode_func_ucs2(uint16 src_id)
{
    return src_id == CHARSET_UTF8 ? cm_utf8_to_utf16 : cm_gbk_to_utf16;
}

transcode_func_t cm_from_transcode_func_ucs2(uint16 src_id)
{
    return src_id == CHARSET_UTF8 ? cm_utf16_to_utf8 : cm_utf16_to_gbk;
}
#endif

status_t cm_get_transcode_length(const text_t *src_text, uint16 src_id, uint16 dst_id, uint32 *dst_length)
{
    uint32 src_char_cnt = 0;

    if (src_id == dst_id) {
        *dst_length = src_text->len;
        return GS_SUCCESS;
    }

    GS_RETURN_IFERR(CM_CHARSET_FUNC(src_id).length(src_text, &src_char_cnt));
    
    *dst_length = src_char_cnt * CM_CHARSET_FUNC(dst_id).max_bytes_per_char();
    return GS_SUCCESS;
}

#ifdef _ICONV
status_t cm_transcode(uint16 src_id, uint16 dst_id, void *src, uint32 *src_len, 
                      void *dst, uint32 *dst_len, bool8 force)
{
    uint32 orig_src_len = *src_len;
    bool32 eof;
    int32 len = 0;
    transcode_func_t trans_func = cm_get_transcode_func(src_id, dst_id);
    if (trans_func == NULL) {
        MEMS_RETURN_IFERR(memcpy_s(dst, *dst_len, src, *src_len));
        *dst_len = *src_len;
    } else {
        len = trans_func(src, src_len, dst, *dst_len, &eof);
        if (len < 0) {
            return GS_ERROR;
        }
        
        // if force is setted , trancate src character is allowed
        if ((!force) && (*src_len != 0 || !eof)) {
            return GS_ERROR;
        }

        *src_len = orig_src_len;
        *dst_len = (uint32)len;
    }
    return GS_SUCCESS;
}
#endif

static status_t cm_text_find_wildcard(char **str, const char *str_end, char **wildstr, const char *wildend, 
                                      char escape, int32 *cmp_ret, bool32 *need_return, charset_type_t type)
{
    char s_char, w_char;
    uint32 s_nbytes;

    while (*wildstr != wildend) {
        bool32 escaped = GS_FALSE;

        CM_STR_GET_FIRST(*wildstr, w_char);

        GS_BREAK_IF_TRUE(w_char == '%');

        CM_STR_REMOVE_FIRST(*wildstr);

        if (w_char == escape) {
            if (*wildstr == wildend) {
                GS_THROW_ERROR(ERR_INVALID_OR_LACK_ESCAPE_CHAR);
                return GS_ERROR;
            }
            CM_STR_POP_FIRST(*wildstr, w_char);
            escaped = GS_TRUE;
        }

        if (*str == str_end) {
            *cmp_ret = -1;
            *need_return = GS_TRUE;
            return GS_SUCCESS;
        }
        CM_STR_GET_FIRST(*str, s_char);
        if ((escaped || w_char != '_') && s_char != w_char) {
            *cmp_ret = 1;
            *need_return = GS_TRUE;
            return GS_SUCCESS;
        }
        /* no escaped wild char,str need to skip a char(may include multi bytes) */
        s_nbytes = 1;
        if (!escaped && w_char == '_') {
            GS_RETURN_IFERR(CM_CHARSET_FUNC(type).str_bytes(*str, (uint32)(*str - str_end), &s_nbytes));
        }
        CM_STR_REMOVE_FIRST_N(*str, s_nbytes);
    }
    if (*wildstr == wildend) {
        *cmp_ret = (*str != str_end);
        *need_return = GS_TRUE;
    }
    return GS_SUCCESS;
}

static bool32 cm_text_remove_wildcards(char **str, const char *str_end, char **wildstr, const char *wildend,
                                       int32 *cmp_ret, charset_type_t type)
{
    char w_char;
    uint32 s_nbytes;

    for (; *wildstr != wildend;) {
        CM_STR_GET_FIRST(*wildstr, w_char);
        if (w_char == '%') {
            CM_STR_REMOVE_FIRST(*wildstr);
            continue;
        }

        if (w_char == '_') {
            CM_STR_REMOVE_FIRST(*wildstr);
            if (CM_CHARSET_FUNC(type).str_bytes(*str, (uint32)(str_end - *str), &s_nbytes) != GS_SUCCESS) {
                *cmp_ret = -1;
                return GS_TRUE;
            }
            CM_STR_REMOVE_FIRST_N(*str, s_nbytes);
            continue;
        }
        break;
    }
    return GS_FALSE;
}

/* find str2 in str1 , match_len1 returns how many bytes are matched in str1. */
static int32 cm_in_like(const char *str1, uint32 len1, const char *str2, uint32 len2, uint32 *match_len1,
                        charset_type_t type)
{
    uint32 char_len1, i1, i2, pos;

    i2 = 0;

    for (i1 = 0; i1 < len1;) {
        // try compare str1 at i1 same as str2 ?
        // '_' means any one character
        if (str2[0] == '_' || str1[i1] == str2[0]) {
            pos = i1;
            for (i2 = 0; i2 < len2;) {
                if (i1 >= len1) {
                    break;
                }

                if (str2[i2] != '_' && str1[i1] != str2[i2]) {
                    break;
                }
                // move i1 to next character head position.
                if (str2[i2] == '_') {
                    GS_RETVALUE_IFTRUE((CM_CHARSET_FUNC(type).str_bytes(str1 + i1, len1 - i1,
                        &char_len1) != GS_SUCCESS), -1);
                    i1 += char_len1;
                } else {
                    i1 += 1;
                }
                i2 += 1;
            }
            if (i2 == len2) {
                *match_len1 = i1 - pos;
                return (int32)pos;
            }
            i1 = pos; // resume i1
        }

        if (str2[0] == '_') {
            GS_RETVALUE_IFTRUE((CM_CHARSET_FUNC(type).str_bytes(str1 + i1, len1 - i1, &char_len1) != GS_SUCCESS), -1);
            i1 += char_len1;
        } else {
            i1 += 1;
        }
    }

    return -1;
}

bool32 cm_text_like(const text_t *text1, const text_t *text2, charset_type_t type)
{
    int32 pos;
    uint32 len1, pos1, pos2, part_len1, part_len2, i1, i2;

    pos2 = 0;

    /*
    compare the first piece of text2(piece is split by %) to text1
    eg. text1 is 'abcdefg' text2 is 'a%c%e%g'
    below is compare text1 to text2-part 'a'
    */
    for (i2 = 0, i1 = 0; i2 < text2->len;) {
        if (text2->str[i2] == '%') {
            pos2 = i2 + 1;
            break;
        }

        /* compare text one by one char */
        GS_RETVALUE_IFTRUE((i2 >= text1->len), GS_FALSE);

        if (text1->str[i1] == text2->str[i2]) {
            i1++;
            i2++;
            continue;
        }

        if (text2->str[i2] == '_') {
            GS_RETVALUE_IFTRUE((CM_CHARSET_FUNC(type).str_bytes(text1->str + i1,
                text1->len - i1, &len1) != GS_SUCCESS), GS_FALSE);
            i1 += (int32)len1;
            i2++;
            continue;
        }

        return GS_FALSE;
    }

    if (pos2 == 0) {
        return (text1->len == i1 && text2->len == i2);
    }

    /*
    compare the middle piece of text2(piece is split by %) to text1
    eg. text1 is 'abcdefg' text2 is 'a%c%e%g'
    below is compare text1 to text2-part 'c' & 'e'
    */
    pos1 = i1;

    for (i2 = pos2; i2 < text2->len; i2++) {
        if (text2->str[i2] == '%') {
            if (i2 > pos2) {
                part_len2 = i2 - pos2;

                pos = cm_in_like(text1->str + pos1, (text1->len - pos1), text2->str + pos2, part_len2,
                    &part_len1, type);
                GS_RETVALUE_IFTRUE((pos < 0), GS_FALSE);
                pos1 += (uint32)(pos + part_len1);
            }

            pos2 = i2 + 1;
        }
    }

    part_len2 = text2->len - pos2;
    GS_RETVALUE_IFTRUE((text1->len < part_len2 + pos1), GS_FALSE);

    /*
    compare the last piece of text2(piece is split by %) to text1
    eg. text1 is 'abcdefg' text2 is 'a%c%e%g'
    below is compare text1 to text2-part 'g'
    */
    i1 = text1->len - 1;
    i2 = text2->len - 1;
    for (; i2 >= text2->len - part_len2;) {
        if (text1->str[i1] == text2->str[i2]) {
            i1--;
            i2--;
            continue;
        }

        if (text2->str[i2] == '_') {
            GS_RETVALUE_IFTRUE((CM_CHARSET_FUNC(type).reverse_str_bytes(text1->str + i1,
                i1 + 1, &len1) != GS_SUCCESS), GS_FALSE);
            i1 -= len1;
            i2--;
            continue;
        }

        return GS_FALSE;
    }

    return GS_TRUE;
}

status_t cm_text_like_escape(char *str, const char *str_end, char *wildstr, const char *wildend, char escape,
                             int32 *cmp_ret, charset_type_t type)
{
    char s_char, w_char;
    bool32 need_return = GS_FALSE;

    while (wildstr != wildend) {
        GS_RETURN_IFERR(cm_text_find_wildcard(&str, str_end, &wildstr, wildend, escape, cmp_ret, &need_return,
            type));
        if (need_return) {
            return GS_SUCCESS;
        }

        GS_RETSUC_IFTRUE(cm_text_remove_wildcards(&str, str_end, &wildstr, wildend, cmp_ret, type));
        if (wildstr == wildend || str == str_end) {
            *cmp_ret = (wildstr == wildend ? 0 : -1);
            return GS_SUCCESS;
        }

        CM_STR_POP_FIRST(wildstr, w_char);
        if (w_char == escape) {
            if (wildstr == wildend) {
                GS_THROW_ERROR(ERR_INVALID_OR_LACK_ESCAPE_CHAR);
                return GS_ERROR;
            }
            CM_STR_POP_FIRST(wildstr, w_char);
        }

        while (GS_TRUE) {
            while (str != str_end) {
                CM_STR_GET_FIRST(str, s_char);
                if (s_char == w_char) {
                    break;
                }
                CM_STR_REMOVE_FIRST(str);
            }
            if (str == str_end) {
                *cmp_ret = -1;
                return GS_SUCCESS;
            }
            CM_STR_REMOVE_FIRST(str);
            GS_RETURN_IFERR(cm_text_like_escape(str, str_end, wildstr, wildend, escape, cmp_ret, type));
            GS_RETSUC_IFTRUE(*cmp_ret <= 0);
        }
    }
    *cmp_ret = (str != str_end ? 1 : 0);
    return GS_SUCCESS;
}

status_t cm_substr_left(text_t *src, uint32 start, uint32 size, text_t *dst, charset_type_t type)
{
    uint32 chnum = 1;
    uint32 substr_chnum = 0;
    char *substr_head = NULL;
    char *pos = src->str;
    char *end = src->str + src->len - 1;

    while ((0 <= end - pos) && (substr_chnum < size)) {
        if (chnum == start) {
            substr_head = pos;  // reach the beginning of substr
        }

        if (chnum >= start) {
            ++substr_chnum;
        }

        pos = CM_CHARSET_FUNC(type).move_char_forward(pos, (uint32)(end - pos + 1));
        if (pos != NULL) {
            ++chnum;
        } else {
            return GS_ERROR;
        }
    }

    if (dst != NULL) {
        dst->len = (substr_head == NULL) ? 0 : (uint32)(pos - substr_head);
        if (dst->len > 0) {
            MEMS_RETURN_IFERR(memcpy_sp(dst->str, dst->len, substr_head, dst->len));
        }
    } else {
        src->len = (substr_head == NULL) ? 0 : (uint32)(pos - substr_head);
        src->str = (substr_head == NULL) ? src->str : substr_head;
    }

    return GS_SUCCESS;
}

/*
In mysql:
1. right(string, start): when start > len(string), return the string;
2. substr(string, -start, size): when start > len(string), return empty string;

Both functions use cm_substr_right(), set overflow_allowed=true for 1.
*/
status_t cm_substr_right(text_t *src, uint32 start, uint32 size, text_t *dst, bool32 overflow_allowed, 
                         charset_type_t type)
{
    uint32 chnum = 0;
    uint32 substr_chnum = 0;
    char *pos = src->str + src->len;
    char *substr_head = NULL;

    while (pos > src->str && chnum < start) {
        pos = CM_CHARSET_FUNC(type).move_char_backward(pos, src->str);
        if (pos != NULL) {
            ++chnum;
        } else {
            return GS_ERROR;
        }
    }

    if (SECUREC_UNLIKELY(chnum < start && !overflow_allowed)) {
        if (dst != NULL) {
            dst->len = 0;
        } else {
            src->len = 0;
        }
        return GS_SUCCESS;
    }

    if (size >= start) {
        if (dst != NULL) {
            dst->len = (uint32)(src->str + src->len - pos);
            if (dst->len != 0) {
                MEMS_RETURN_IFERR(memcpy_sp(dst->str, dst->len, pos, dst->len));
            }
        } else {
            src->len = (uint32)(src->str + src->len - pos);
            src->str = pos;
        }
        return GS_SUCCESS;
    }

    substr_head = pos;
    while (substr_chnum < size) {
        pos = CM_CHARSET_FUNC(type).move_char_forward(pos, (uint32)(src->str + src->len - pos));
        if (pos == NULL) {
            return GS_ERROR;
        }
        ++substr_chnum;
    }

    if (dst != NULL) {
        dst->len = (uint32)(pos - substr_head);
        if (dst->len != 0) {
            MEMS_RETURN_IFERR(memcpy_sp(dst->str, dst->len, substr_head, dst->len));
        }
    } else {
        src->len = (uint32)(pos - substr_head);
        src->str = substr_head;
    }
    return GS_SUCCESS;
}

status_t cm_substr(text_t *src, int32 start, uint32 size, text_t *dst, charset_type_t type)
{
    if (start == 0) {
        start = 1;
    }

    if (start > 0) {
        return cm_substr_left(src, (uint32)start, size, dst, type);
    }

    return cm_substr_right(src, (uint32)(-start), size, dst, GS_FALSE, type);
}

status_t cm_get_start_byte_pos(const text_t *text, uint32 char_pos, uint32 *start, charset_type_t charset)
{
    uint32 tmp_char_len = 0;
    uint32 tmp_byte_len;
    uint32 i;

    if (char_pos == 0) {
        *start = 0;
        return GS_SUCCESS;
    }

    if (text->len == 0 || (char_pos > text->len - 1)) {
        GS_THROW_ERROR(ERR_NLS_INTERNAL_ERROR, "start pos");
        return GS_ERROR;
    }

    for (i = 0; i < text->len;) {
        if (tmp_char_len == char_pos) {
            break;
        }

        // one utf-8/GBK char contains multi bytes
        if (CM_CHARSET_FUNC(charset).str_bytes(text->str + i, text->len - i, &tmp_byte_len) != GS_SUCCESS) {
            GS_THROW_ERROR(ERR_NLS_INTERNAL_ERROR, CM_CHARSET_FUNC(charset).name);
            return GS_ERROR;
        }
        i += tmp_byte_len;
        tmp_char_len++;
    }

    *start = i;
    return GS_SUCCESS;
}

static inline uint32 cm_instr_core_forward(const text_t *str, const text_t *substr, uint32 nth, uint32 start)
{
    uint32 occur_times = 0;
    uint32 i;
    uint32 result_pos = 0;
    bool32 find_diff = GS_FALSE;

    for (; start < str->len; start++) {
        if ((str->len - start) < substr->len) {
            return 0;
        }

        find_diff = GS_FALSE;

        for (i = 0; i < substr->len; i++) {
            if (str->str[start + i] != substr->str[i]) {
                find_diff = GS_TRUE;
                break;
            }
        }

        if (find_diff) {
            continue;
        }

        if (++occur_times == nth) {
            result_pos = start + 1;
            break;
        }
    }
    return result_pos;
}
static inline uint32 cm_instr_core_backward(const text_t *str, const text_t *substr, uint32 nth, uint32 start)
{
    uint32 occur_times = 0;
    uint32 i;
    uint32 result_pos = 0;
    bool32 find_diff = GS_FALSE;

    for (;; start--) {
        if ((str->len - start) < substr->len) {
            if (start == 0) {
                break;
            }
            continue;
        }

        find_diff = GS_FALSE;

        for (i = 0; i < substr->len; i++) {
            if (str->str[start + i] != substr->str[i]) {
                find_diff = GS_TRUE;
                break;
            }
        }

        if (find_diff) {
            if (start == 0) {
                break;
            }
            continue;
        }

        if (++occur_times == nth) {
            result_pos = start + 1;
            break;
        }

        if (start == 0) {
            break;
        }
    }
    return result_pos;
}

uint32 cm_instr_core(const text_t *str, const text_t *substr, int32 pos, uint32 nth, uint32 start)
{
    if (pos > 0) {
        // search forward
        return cm_instr_core_forward(str, substr, nth, start);
    } else {
        // search backward
        return cm_instr_core_backward(str, substr, nth, start);
    }
}

uint32 cm_instr(const text_t *str, const text_t *substr, int32 pos, uint32 nth, bool32 *is_char, charset_type_t type)
{
    uint32 start = 0;
    uint32 str1_char_len, str2_char_len;

    GS_RETVALUE_IFTRUE((CM_CHARSET_FUNC(type).length(str, &str1_char_len) != GS_SUCCESS ||
        CM_CHARSET_FUNC(type).length(substr, &str2_char_len) != GS_SUCCESS),
        0);

    // optimize: string contains no utf-8 character
    *is_char = (str->len != str1_char_len || substr->len != str2_char_len);
    if (!*is_char) {
        return cm_instrb(str, substr, pos, nth);
    }

    GS_RETVALUE_IFTRUE(((uint32)abs(pos) > str1_char_len || pos == 0 || str1_char_len < str2_char_len), 0);

    // get start bytes pos
    if (pos > 0) {
        // search forward
        // get start bytes pos with start char pos(pos-1) because one utf-8/GBK char contains multi bytes
        GS_RETVALUE_IFTRUE((cm_get_start_byte_pos(str, (uint32)(pos - 1), &start, type) != GS_SUCCESS), 0);
    } else {
        // search backward
        // get start bytes pos with start char pos(char_len_str1+pos) because one utf-8/GBK char contains multi bytes
        GS_RETVALUE_IFTRUE((cm_get_start_byte_pos(str, (uint32)(str1_char_len + pos), &start, type) != GS_SUCCESS),
            0);
    }

    // get result bytes pos
    return cm_instr_core(str, substr, pos, nth, start);
}

status_t cm_num_instr(const text_t *str, const text_t *substr, text_t *splitchar, uint32 *num, charset_type_t type)
{
    uint32 num_tmp = *num;
    uint32 curr_pos = 1;
    uint32 pre_pos = 1;
    bool32 has_utf8 = GS_FALSE;
    text_t walker = *str;
    text_t item;
    int64 tmp_value = 0;

    if (cm_text2bigint(substr, &tmp_value) != GS_SUCCESS || substr->len == 0) {
        return GS_ERROR;
    }

    while (walker.len > 0) {
        curr_pos = cm_instr(&walker, splitchar, 1, 1, &has_utf8, type);
        if (curr_pos > 0) {
            num_tmp++;
            item.str = walker.str;
            item.len = curr_pos - pre_pos;

            if (cm_text2bigint(&item, &tmp_value) != GS_SUCCESS || item.len == 0) {
                return GS_ERROR;
            }

            if (cm_text_equal_ins(&item, substr)) {
                *num = num_tmp;
                return GS_SUCCESS;
            } else {
                walker.len -= (item.len + 1);
                walker.str += (item.len + 1);
            }
        } else {
            if (cm_text2bigint(&walker, &tmp_value) != GS_SUCCESS || walker.len == 0) {
                return GS_ERROR;
            }

            /* last item */
            if (cm_text_equal_ins(&walker, substr)) {
                num_tmp++;
                *num = num_tmp;
                return GS_SUCCESS;
            } else {
                num_tmp = 0;
                break;
            }
        }
    }

    return GS_SUCCESS;
}

#ifdef __cplusplus
}
#endif


