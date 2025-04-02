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
 * var_cmp.h
 *    variant compare
 *
 * IDENTIFICATION
 *    src/storage/gstor/zekernel/common/variant/var_cmp.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef __VAR_CMP_H__
#define __VAR_CMP_H__

#include "storage/gstor/zekernel/common/variant/var_inc.h"

#define NATIVE_CMP(p1, p2) (((p1) > (p2)) ? 1 : (((p1) == (p2)) ? 0 : -1))

typedef struct st_cmp_rule {
    gs_type_t    cmp_type;
    bool32       same_type;   // GS_TRUE, same datatype
    bool32       compatible;  // GS_TRUE, different datatype, but no variant need to convert
}cmp_rule_t;

#define   INVALID_CMP_DATATYPE  0

extern cmp_rule_t g_cmp_rules[VAR_TYPE_ARRAY_SIZE][VAR_TYPE_ARRAY_SIZE];

#define GS_CMP_RULE(lt, rt)  (&g_cmp_rules[GS_TYPE_I(lt)][GS_TYPE_I(rt)])

static inline cmp_rule_t *get_cmp_rule(gs_type_t lt, gs_type_t rt)
{
    return (GS_CMP_RULE(lt, rt)->cmp_type == INVALID_CMP_DATATYPE) ?
        GS_CMP_RULE(rt, lt) : GS_CMP_RULE(lt, rt);
}

static inline gs_type_t get_cmp_datatype(gs_type_t lt, gs_type_t rt)
{
    return get_cmp_rule(lt, rt)->cmp_type;
}

status_t  var_like(variant_t *left, 
    variant_t *right, bool32 *result, bool32 has_escape, char escape, charset_type_t type);
status_t  var_convert(const nlsparams_t *nls, variant_t *var, gs_type_t datatype, text_buf_t *buf);
status_t  var_compare(const nlsparams_t *nls, variant_t *left, variant_t *right, int32 *result);

static status_t inline var_compare_same_type(const variant_t *left, const variant_t *right, int32 *result)
{
    /* with same value types */
    switch (left->type) {
        case GS_TYPE_INTEGER:
        case GS_TYPE_INTERVAL_YM:
            *result = NATIVE_CMP(VALUE(int32, left), VALUE(int32, right));
            break;

        case GS_TYPE_BOOLEAN:
            *result = NATIVE_CMP(VALUE(bool32, left), VALUE(bool32, right));
            break;

        case GS_TYPE_UINT32:
            *result = NATIVE_CMP(VALUE(uint32, left), VALUE(uint32, right));
            break;

        case GS_TYPE_DATE:
        case GS_TYPE_TIMESTAMP:
        case GS_TYPE_TIMESTAMP_LTZ:
        case GS_TYPE_TIMESTAMP_TZ_FAKE:
        case GS_TYPE_BIGINT:
        case GS_TYPE_INTERVAL_DS:
            *result = NATIVE_CMP(VALUE(int64, left), VALUE(int64, right));
            break;

        case GS_TYPE_TIMESTAMP_TZ:
            *result = cm_tstz_cmp(VALUE_PTR(timestamp_tz_t, left), VALUE_PTR(timestamp_tz_t, right));
            break;

        case GS_TYPE_REAL:
            *result = cm_compare_double(VALUE(double, left), VALUE(double, right));
            break;

        case GS_TYPE_NUMBER:
        case GS_TYPE_DECIMAL:
            *result = cm_dec8_cmp(VALUE_PTR(dec8_t, left), VALUE_PTR(dec8_t, right));
            break;

        case GS_TYPE_CHAR:
            if (right->type == GS_TYPE_CHAR) { 
                *result = cm_compare_text_rtrim(VALUE_PTR(text_t, left), VALUE_PTR(text_t, right));
            } else {
                *result = cm_compare_text(VALUE_PTR(text_t, left), VALUE_PTR(text_t, right));
            }
        
            break;

        case GS_TYPE_VARCHAR:
        case GS_TYPE_STRING:
            *result = cm_compare_text(VALUE_PTR(text_t, left), VALUE_PTR(text_t, right));
            break;

        case GS_TYPE_BINARY:
        case GS_TYPE_VARBINARY:
        case GS_TYPE_RAW:
            *result = cm_compare_bin(&left->v_bin, &right->v_bin);
            break;

        case GS_TYPE_ARRAY:
            /* array compare rules:
            1. same elements count
            2. same subscript
            3. same element value according to subscript
            */
        default:
            GS_THROW_ERROR(ERR_INVALID_DATA_TYPE, "comparision");
            return GS_ERROR;
    }

    return GS_SUCCESS;
}

bool32    var_const_equal(const variant_t *v1, const variant_t *v2);
bool32    var_seq_equal(const var_seq_t *v1, const var_seq_t *v2);
#endif
