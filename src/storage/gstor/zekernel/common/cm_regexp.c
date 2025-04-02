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
 * cm_regexp.c
 *
 *
 * IDENTIFICATION
 * src/storage/gstor/zekernel/common/cm_regexp.c
 *
 * -------------------------------------------------------------------------
 */
#include "cm_regexp.h"

#define PCRE_STATIC
#ifdef ENABLE_GCOV
#include "pcre.h"
#endif

regexp_arg_type_t g_instr_arg_types[] = {
    REGEXP_ARG_SOURCE,
    REGEXP_ARG_PATTERN,
    REGEXP_ARG_POSITION,
    REGEXP_ARG_OCCUR,
    REGEXP_ARG_RETURN_OPT,
    REGEXP_ARG_MATCH_PARAM,
    REGEXP_ARG_SUBEXPR,
    REGEXP_ARG_DUMB,
};

regexp_arg_type_t g_substr_arg_types[] = {
    REGEXP_ARG_SOURCE,
    REGEXP_ARG_PATTERN,
    REGEXP_ARG_POSITION,
    REGEXP_ARG_OCCUR,
    REGEXP_ARG_MATCH_PARAM,
    REGEXP_ARG_SUBEXPR,
    REGEXP_ARG_DUMB,
};

regexp_arg_type_t g_count_arg_types[] = {
    REGEXP_ARG_SOURCE,
    REGEXP_ARG_PATTERN,
    REGEXP_ARG_POSITION,
    REGEXP_ARG_MATCH_PARAM,
    REGEXP_ARG_DUMB,
};

regexp_arg_type_t g_replace_arg_types[] = {
    REGEXP_ARG_SOURCE,
    REGEXP_ARG_PATTERN,
    REGEXP_ARG_REPLACE,
    REGEXP_ARG_POSITION,
    REGEXP_ARG_OCCUR,
    REGEXP_ARG_MATCH_PARAM,
    REGEXP_ARG_DUMB,
};
#ifdef ENABLE_GCOV
void cm_regexp_init(regexp_mem_func_t *heap_func, regexp_mem_func_t *stack_func)
{
    pcre_malloc = heap_func->allocator;
    pcre_free = heap_func->deallocator;
    pcre_stack_malloc = stack_func->allocator;
    pcre_stack_free = stack_func->deallocator;
}

void cm_regexp_args_init(regexp_args_t *args)
{
    // default offset and occur begin with 1, default subexpr begin with 0
    args->offset = args->occur = 1;
    args->subexpr = 0;
    args->match_param = NULL;
    args->retopt = GS_FALSE;
    args->var_replace_str.is_null = GS_TRUE;
    args->var_pos.is_null = GS_TRUE;
    args->var_occur.is_null = GS_TRUE;
    args->var_subexpr.is_null = GS_TRUE;
    args->var_retopt.is_null = GS_TRUE;
}

static inline status_t cm_extract_options(int *options, text_t *match_param)
{
    uint32 loop;
    *options = 0;
    if (match_param == NULL) {
        return GS_SUCCESS;
    }
    for (loop = 0; loop < match_param->len; ++loop) {
        switch (match_param->str[loop]) {
            case 'c':
                *options &= ~PCRE_CASELESS;
                break;
            case 'i':
                *options |= PCRE_CASELESS;
                break;
            case 'n':
                *options |= PCRE_DOTALL;
                break;
            case 'm':
                *options |= PCRE_MULTILINE;
                break;
            case 'x':
                *options |= PCRE_EXTENDED;
                break;
            default:
                GS_THROW_ERROR_EX(ERR_INVALID_FUNC_PARAMS, "Invalid match parameter '%c'", match_param->str[loop]);
                return GS_ERROR;
        }
    }
    return GS_SUCCESS;
}

status_t cm_regexp_compile(void **code, const char *regexp, const char **errmsg, int32 *errloc, text_t *match_param,
    charset_type_t charset)
{
    int options;
    GS_RETURN_IFERR(cm_extract_options(&options, match_param));

    if (charset == CHARSET_UTF8) {
        options |= PCRE_UTF8;
    } else {
        // not set PCRE_UTF8, for GBK support;
        options |= PCRE_NO_UTF8_CHECK;
    }

    *code = (void *)pcre_compile(regexp, options, errmsg, errloc, NULL);
    if (*code == NULL) {
        GS_THROW_ERROR(ERR_REGEXP_COMPILE, *errloc, *errmsg);
        return GS_ERROR;
    }
    return GS_SUCCESS;
}

status_t cm_regexp_match(bool32 *matched, const void *code, const text_t *subject)
{
    text_t substr;
    regexp_substr_assist_t assist = {
        .code = code,
        .subject = *subject,
        .offset = 0,
        .occur = 1,
        .subexpr = 0,
        .charset = CHARSET_UTF8
    };

    if (GS_SUCCESS != cm_regexp_substr(&substr, &assist)) {
        return GS_ERROR;
    }

    *matched = substr.str != NULL;
    return GS_SUCCESS;
}

status_t cm_regexp_instr(int32 *pos, regexp_substr_assist_t *assist, bool32 end)
{
    text_t substr;

    if (GS_SUCCESS != cm_regexp_substr(&substr, assist)) {
        return GS_ERROR;
    }
    if (substr.str == NULL) {
        *pos = 0;
        return GS_SUCCESS;
    }
    *pos = (int32)(substr.str - assist->subject.str) + 1;
    if (end) {
        *pos += (int32)substr.len;
    }
    return GS_SUCCESS;
}

#define GS_SIZE_PER_SUBEXPR 3
#define GS_SIZE_OF_OFFSET_PER_SUBEXPR (GS_SIZE_PER_SUBEXPR - 1)
#define GS_MAX_SUBEXPR_COUNT 9
#define GS_MAX_SUBEXPR_VEC_SIZE ((GS_MAX_SUBEXPR_COUNT + 1) * GS_SIZE_PER_SUBEXPR)
static inline status_t cm_regexp_skip_occurs(const void *code, const text_t *subject, int32 *offset, int32 occur)
{
    int ovector[GS_SIZE_PER_SUBEXPR]; // only fetch the entire substring
    int ret;

    while (occur > 1) {
        ret = pcre_exec((const pcre *)code, NULL, subject->str, (int)subject->len, *offset, 0, ovector,
            GS_SIZE_PER_SUBEXPR);
        if (ret < 0) {
            *offset = -1;
            return GS_SUCCESS;
        }

        --occur;
        if (*offset != ovector[1]) {
            *offset = ovector[1];
            continue;
        }
        ++(*offset);
        if (subject->str[*offset - 1] == '\r' && (uint32)*offset < subject->len && subject->str[*offset] == '\n') {
            ++(*offset);
            continue;
        }

        // skip a complete utf8 character
        while ((uint32)*offset < subject->len) {
            if ((subject->str[*offset] & 0xc0) != 0x80) {
                break;
            }
            ++(*offset);
        }
    }
    return GS_SUCCESS;
}

/*
subexpr: 0   return entire string that matched
         1~9 return substring according to the sub patterns in the matched string
         >9  return NULL
offset begin with 0
occur  begin with 1
*/
status_t cm_regexp_substr(text_t *substr, regexp_substr_assist_t *assist)
{
    int ret;
    int capture_count;
    int ovector[GS_MAX_SUBEXPR_VEC_SIZE]; // each sub pattern need 3 int, the entire pattern also should be considered.
    int32 byte_offset = 0;

    substr->str = NULL;
    substr->len = 0;

    if (assist->subexpr > GS_MAX_SUBEXPR_COUNT) {
        return GS_SUCCESS;
    }

    // find out how many sub patterns there are
    capture_count = 0;
    (void)pcre_fullinfo((const pcre *)assist->code, NULL, PCRE_INFO_CAPTURECOUNT, &capture_count);

    // input sub pattern number exceed the count appeared in compiled pattern
    if (assist->subexpr > capture_count) {
        return GS_SUCCESS;
    }

    if (CM_CHARSET_FUNC(assist->charset)
            .get_start_byte_pos(&assist->subject, (uint32)assist->offset, (uint32 *)&byte_offset) != GS_SUCCESS) {
        cm_reset_error();
        return GS_SUCCESS;
    }

    GS_RETURN_IFERR(cm_regexp_skip_occurs(assist->code, &assist->subject, &byte_offset, assist->occur));
    if (byte_offset == -1) {
        return GS_SUCCESS;
    }

    ret = pcre_exec((const pcre *)assist->code, NULL, assist->subject.str, (int)assist->subject.len, byte_offset, 0,
        ovector, GS_MAX_SUBEXPR_VEC_SIZE);
    if (ret < 0) {
        return GS_SUCCESS;
    }
    if (ovector[assist->subexpr * GS_SIZE_OF_OFFSET_PER_SUBEXPR] == -1) {
        return GS_SUCCESS;
    }

    substr->str = assist->subject.str + ovector[assist->subexpr * GS_SIZE_OF_OFFSET_PER_SUBEXPR];
    substr->len = (uint32)(ovector[assist->subexpr * GS_SIZE_OF_OFFSET_PER_SUBEXPR + 1] -
        ovector[assist->subexpr * GS_SIZE_OF_OFFSET_PER_SUBEXPR]);
    return GS_SUCCESS;
}

void cm_regexp_free(void *code)
{
    pcre_free(code);
}

#else

void cm_regexp_init(regexp_mem_func_t *heap_func, regexp_mem_func_t *stack_func)
{
    return;
}
void cm_regexp_args_init(regexp_args_t *args)
{
    return;
}

status_t cm_regexp_compile(void **code, const char *exp, const char **errmsg, int32 *errloc, text_t *match_param,
    charset_type_t charset)
{
    return GS_SUCCESS;
}
status_t cm_regexp_match(bool32 *matched, const void *code, const text_t *subject)
{
    return GS_SUCCESS;
}

status_t cm_regexp_instr(int32 *pos, regexp_substr_assist_t *assist, bool32 end)
{
    return GS_SUCCESS;
}
status_t cm_regexp_substr(text_t *substr, regexp_substr_assist_t *assist)
{
    return GS_SUCCESS;
}
void cm_regexp_free(void *code)
{
    return;
}
#endif