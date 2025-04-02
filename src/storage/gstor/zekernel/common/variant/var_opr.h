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
 * var_opr.h
 *    variant operate
 *
 * IDENTIFICATION
 *    src/storage/gstor/zekernel/common/variant/var_opr.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef __VAR_OPR_H__
#define __VAR_OPR_H__

#include "storage/gstor/zekernel/common/variant/var_inc.h"
#include "storage/gstor/zekernel/common/variant/var_cast.h"

#define OPR_THROW_ERROR(oper, l_type, r_type) \
    GS_THROW_ERROR(ERR_UNDEFINED_OPER,                       \
        get_datatype_name_str((int32)(l_type)),               \
        oper,                                                \
        get_datatype_name_str((int32)(r_type)))

/** check overflow for real/double type */
#define CHECK_REAL_OVERFLOW(val, inf_is_valid, zero_is_valid) \
    do {                                                       \
        if (isinf(val) && !(inf_is_valid)) {               \
            GS_THROW_ERROR(ERR_TYPE_OVERFLOW, "DOUBLE/REAL");  \
            return GS_ERROR;                                   \
        }                                                      \
        if ((val) == 0.0 && !(zero_is_valid)) {                \
            GS_THROW_ERROR(ERR_TYPE_OVERFLOW, "DOUBLE/REAL");  \
            return GS_ERROR;                                   \
        }                                                      \
    } while (0)

typedef struct st_opr_operand_set {
    nlsparams_t *nls;
    variant_t   *left;
    variant_t   *right;
    variant_t   *result;
} opr_operand_set_t;

#define OP_LEFT   (op_set->left)
#define OP_RIGHT  (op_set->right)
#define OP_RESULT (op_set->result)

typedef status_t (*opr_exec_t)(opr_operand_set_t *op_set);
typedef status_t (*opr_infer_t)(gs_type_t left, gs_type_t right, gs_type_t *result);

typedef struct st_opr_rule {
    opr_exec_t     exec;          // executor 
    gs_type_t      lc_type;       // the type left variant converted to
    gs_type_t      rc_type;       // the type right variant converted to
    gs_type_t      rs_type;       // the type of result
}opr_rule_t;

// declare rule for operator "+"
#define __OPR_DECL(name, lct, rct, rst)  \
    static opr_rule_t g_opr_##name = {.exec = (name), .lc_type = (lct), .rc_type = (rct), .rs_type = (rst)}

#define __OPR_DEF(lt, rt, name) [GS_TYPE_I((lt))][GS_TYPE_I((rt))] = &g_opr_##name

static inline status_t opr_text2dec(variant_t *text, variant_t *dec)
{
    GS_RETURN_IFERR(cm_text_to_dec8(VALUE_PTR(text_t, text), &dec->v_dec));
    dec->ctrl = 0;
    dec->type = GS_TYPE_NUMBER;
    return GS_SUCCESS;
}

extern opr_exec_t  g_opr_execs[];
extern opr_infer_t g_opr_infers[];
extern uint32      g_opr_priority[];

status_t opr_exec(operator_type_t oper,
    const nlsparams_t *nls, variant_t *left, variant_t *right, variant_t *result);
status_t opr_infer_type(operator_type_t oper, gs_type_t left, gs_type_t right, gs_type_t *result);
status_t opr_infer_type_sum(gs_type_t sum_type, typmode_t *typmod);

// process binary as string if the binary variant is not from const hex
// binary data converted to string directly
#define OPR_ANYTYPE_BINARY(oper)\
    do {\
        variant_t var;\
        variant_t *old_right = OP_RIGHT;       \
        if (!OP_RIGHT->v_bin.is_hex_const) {\
            var.v_bin = OP_RIGHT->v_bin;\
            var.type  = GS_TYPE_STRING;\
        }else{\
            GS_RETURN_IFERR(cm_xbytes2bigint(&OP_RIGHT->v_bin, &var.v_bigint));\
            var.type = GS_TYPE_BIGINT;\
        }\
        OP_RIGHT = &var;\
        status_t status = opr_exec_##oper(op_set);\
        OP_RIGHT = old_right;      \
        return status;          \
    } while (0)

// process binary as string if the binary variant is not from const hex
// binary data converted to string directly
#define OPR_BINARY_ANYTYPE(oper)\
    do {\
        variant_t var;\
        variant_t *old_left = OP_LEFT;     \
        if (!OP_LEFT->v_bin.is_hex_const) {\
            var.v_bin = OP_LEFT->v_bin;\
            var.type  = GS_TYPE_STRING;\
        }else{\
            GS_RETURN_IFERR(cm_xbytes2bigint(&OP_LEFT->v_bin, &var.v_bigint));\
            var.type = GS_TYPE_BIGINT;\
        }\
        OP_LEFT = &var; \
        status_t status = opr_exec_##oper(op_set);\
        OP_LEFT = old_left;          \
        return status;          \
    } while (0)


typedef struct st_opr_options {
    bool32 div0_accepted;
} opr_options_t;

extern opr_options_t g_opr_options;

static inline char *var_get_buf(variant_t *var)
{
    char *buf = NULL;
    uint32 len = 0;

    if (var->is_null) {
        return NULL;
    }

    if (GS_IS_VARLEN_TYPE(var->type)) {
        buf = var->v_text.str;
        len = var->v_text.len;
    } else if (GS_IS_LOB_TYPE(var->type)) {
        if (var->v_lob.type == GS_LOB_FROM_KERNEL) {
            buf = (char *)var->v_lob.knl_lob.bytes;
            len = var->v_lob.knl_lob.size;
        } else if (var->v_lob.type == GS_LOB_FROM_NORMAL) {
            buf = var->v_lob.normal_lob.value.str;
            len = var->v_lob.normal_lob.value.len;
        } else {
            return NULL;
        }
    } else {
        return NULL;
    }

    if (len == 0) {
        return NULL;
    }

    return buf;
}




//// add overflow check
//#define opr_int32add_overflow(a, b, res) (Int32Add((a), (b), (res)) == INTSAFE_E_ARITHMETIC_OVERFLOW)
//#define opr_int64add_overflow(a, b, res) (Int64Add((a), (b), (res)) == INTSAFE_E_ARITHMETIC_OVERFLOW)
//#define opr_uint32add_overflow(a, b, res) (UInt32Add((a), (b), (res)) == INTSAFE_E_ARITHMETIC_OVERFLOW)
//#define opr_uint64add_overflow(a, b, res) (UInt64Add((a), (b), (res)) == INTSAFE_E_ARITHMETIC_OVERFLOW)
////  sub overflow check
//#define opr_int32sub_overflow(a, b, res) (Int32Sub((a), (b), (res)) == INTSAFE_E_ARITHMETIC_OVERFLOW)
//#define opr_int64sub_overflow(a, b, res) (Int64Sub((a), (b), (res)) == INTSAFE_E_ARITHMETIC_OVERFLOW)
//#define opr_uint32sub_overflow(a, b, res) (UInt32Sub((a), (b), (res)) == INTSAFE_E_ARITHMETIC_OVERFLOW)
//#define opr_uint64sub_overflow(a, b, res) (UInt64Sub((a), (b), (res)) == INTSAFE_E_ARITHMETIC_OVERFLOW)
//// multiple overflow check
//#define opr_int32mul_overflow(a, b, res) (Int32Mult((a), (b), (res)) == INTSAFE_E_ARITHMETIC_OVERFLOW)
//#define opr_int64mul_overflow(a, b, res) (Int64Mult((a), (b), (res)) == INTSAFE_E_ARITHMETIC_OVERFLOW)
//#define opr_uint32mul_overflow(a, b, res) (UInt32Mult((a), (b), (res)) == INTSAFE_E_ARITHMETIC_OVERFLOW)
//#define opr_uint64mul_overflow(a, b, res) (UInt64Mult((a), (b), (res)) == INTSAFE_E_ARITHMETIC_OVERFLOW)
//

#ifdef _MSC_VER

#define false 0
#define true 1

#define opr_int32add_overflow(a, b, res) \
    (((a) > 0 && (b) > INT32_MAX - (a)) || ((a) < 0 && (b) < INT32_MIN - (a)) ? false : ((res) = (a) + (b), true))
#define opr_int64add_overflow(a, b, res) \
    (((a) > 0 && (b) > INT64_MAX - (a)) || ((a) < 0 && (b) < INT64_MIN - (a)) ? false : ((res) = (a) + (b), true))
#define opr_uint32add_overflow(a, b, res) (((b) > UINT32_MAX - (a)) ? false : ((res) = (a) + (b), true))
#define opr_uint64add_overflow(a, b, res) (((b) > UINT64_MAX - (a)) ? false : ((res) = (a) + (b), true))

#define opr_int64sub_overflow(a, b, res) \
    (((b) > 0 && (a) < INT64_MIN + (b)) || ((b) < 0 && (a) > INT64_MAX + (b)) ? false : ((res) = (a) - (b), true))

#define opr_int32mul_overflow(a, b, res) (((a) != 0 && (b) > INT32_MAX / (a)) ? false : ((res) = (a) * (b), true))
#define opr_int64mul_overflow(a, b, res) \
    (((a) != 0 && (b) > LLONG_MAX / (a)) ? false : ((res) = (long long)(a) * (long long)(b), true))
#define opr_uint32mul_overflow(a, b, res) (((a) != 0 && (b) > UINT32_MAX / (a)) ? false : ((res) = (a) * (b), true))
#define opr_uint64mul_overflow(a, b, res) (((a) != 0 && (b) > UINT64_MAX / (a)) ? false : ((res) = (a) * (b), true))

#else
// add overflow check
#define opr_int32add_overflow(a, b, res) __builtin_sadd_overflow((a), (b), (res))
#define opr_int64add_overflow(a, b, res) __builtin_saddll_overflow((a), (b), (res))
#define opr_uint32add_overflow(a, b, res) __builtin_uadd_overflow((a), (b), (res))
#define opr_uint64add_overflow(a, b, res) __builtin_uaddll_overflow((a), (b), (res))
//   sub overflow check
#define opr_int32sub_overflow(a, b, res) __builtin_ssub_overflow((a), (b), (res))
#define opr_int64sub_overflow(a, b, res) __builtin_ssubll_overflow((a), (b), (res))
#define opr_uint32sub_overflow(a, b, res) __builtin_usub_overflow((a), (b), (res))
#define opr_uint64sub_overflow(a, b, res) __builtin_usubll_overflow((a), (b), (res))
// multiple overflow check
#define opr_int32mul_overflow(a, b, res) __builtin_smul_overflow((a), (b), (res))
#define opr_int64mul_overflow(a, b, res) __builtin_smulll_overflow((a), (b), (res))
#define opr_uint32mul_overflow(a, b, res) __builtin_umul_overflow((a), (b), (res))
#define opr_uint64mul_overflow(a, b, res) __builtin_umulll_overflow((a), (b), (res))
#endif

#endif
