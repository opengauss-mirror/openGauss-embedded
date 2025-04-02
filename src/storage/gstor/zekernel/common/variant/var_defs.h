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
 * var_defs.h
 * variant definitions
 *
 * IDENTIFICATION
 * src/storage/gstor/zekernel/common/variant/var_defs.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef __VAR_DEFS_H__
#define __VAR_DEFS_H__

#include "cm_defs.h"
#include "cm_text.h"
#include "cm_date.h"
#include "cm_decimal.h"
#include "cm_binary.h"
#include "cm_interval.h"
#include "cm_nls.h"
#include "cm_utils.h"
#include "cm_lob.h"
#include "cm_array.h"
#include "cm_charset.h"
#include <math.h>
#include <float.h>
#include "var_plsql.h"
#include "var_column.h"
#include "var_func.h"
#include "var_typmode.h"
#include "cm_nls.h"

#define VAR_DOUBLE_EPSILON (1e-15)
#define VAR_FLOAT_EPSILON (1e-6)
#define VAR_DOUBLE_IS_ZERO(var) (((var) >= -VAR_DOUBLE_EPSILON) && ((var) <= VAR_DOUBLE_EPSILON))
#define VAR_FLOAT_IS_ZERO(var) (((var) >= -VAR_FLOAT_EPSILON) && ((var) <= VAR_FLOAT_EPSILON))

#define VAR_TYPE_ARRAY_SIZE GS_MAX_DATATYPE_NUM

typedef enum en_seq_val {
    SEQ_CURR_VALUE = 1,
    SEQ_NEXT_VALUE = 2,
} seq_mode_t;


typedef struct st_var_seq {
    text_t user;
    text_t name;
    seq_mode_t mode;
} var_seq_t;

typedef struct st_var_object {
    uint32 id;
    pointer_t ptr;
} var_object_t;

typedef struct st_cur_object {
    pointer_t ref_cursor; // cursor slot ptr
    void *input;          // record sys_refcursor's recent input
} cur_object_t;

typedef struct st_var_rowid {
    uint32 res_id; // reserved word id
    uint32 tab_id;
    uint32 ancestor;
} var_rowid_t;

typedef struct st_var_res {
    uint32 res_id; // reserved word id
    bool32 namable;
} var_res_t;

typedef struct st_vm_rowid {
    uint32 vmid;
    uint32 slot;
} vm_rowid_t;

#pragma pack(4)
typedef struct st_var_tstamp {
    double stamp;
    uint8 zone;
    uint8 frac;      // fraction length
    uint8 unused[2]; // not used, for byte alignment
} var_tstamp_t;
#pragma pack()


/*
 * NOTICE:
 * Add info at this structure should modify the deep clone method and
 * the serialize method synchronously.
 */
#pragma pack(4)

typedef enum st_variant_pl_type {
    VAR_PL_DC = 0,
    VAR_UDO = 1
} en_variant_pl_type_t;

typedef struct st_variant {
    union {
        int32 v_int;
        uint32 v_uint32;
        bool32 v_bool;
        int64 v_bigint;
        double v_real;
        date_t v_date;
        int32 v_param_id;
        timestamp_t v_tstamp;
        timestamp_ltz_t v_tstamp_ltz;
        // just for convenient , this element could be treated as v_tstamp in many cases,
        // cause they have same data length
        timestamp_tz_t v_tstamp_tz;
        binary_t v_bin;
        text_t v_text;
        dec8_t v_dec;
        dec4_t v_d4;
        interval_ds_t v_itvl_ds;
        interval_ym_t v_itvl_ym;

        var_lob_t v_lob;
        var_column_t v_col;
        var_seq_t v_seq;
        typmode_t v_type;
        var_object_t v_obj;
        cur_object_t v_cursor;
        var_rowid_t v_rid;
        var_res_t v_res;
        pointer_t v_pointer;   // case expr, v_pointer->case_expr_t
        pointer_t v_json_path; // json_path, v_pointer->json_path_t
        var_func_t v_func;     // function
        pointer_t v_udo;       // user defined object(function/stored procedure)
        pointer_t v_pl_dc;     // user defined object(function/procedure/pkg)
        plv_attr_t v_plattr;
        mtrl_rowid_t v_vmid; // for winsort aggr
        interval_unit_t v_itvl_unit_id;
        var_vm_col_t v_vm_col;
        var_record_t v_record;
        var_array_t v_array; // for array type
        udt_method_t v_method;
        udt_constructor_t v_construct;
        var_address_t v_address; // variant is object or collection, way to call methods
        var_collection_t v_collection;
        udt_var_object_t v_object;
    };

    union {
        struct {
            int16 type;
            bool8 is_null;
            uint8 type_for_pl; // en_variant_pl_type_t
        };

        uint32 ctrl;
    };
} variant_t;

#pragma pack()

#define VAR_SET_NULL(var, datatype) \
    do {                            \
        (var)->is_null = GS_TRUE;   \
        (var)->type = (datatype);   \
    } while (0)

#define VALUE(TYPE, v) (*(TYPE *)(&(v)->v_int))
#define VALUE_PTR(TYPE, v) ((TYPE *)(&(v)->v_int))
#define VAR_TAB(val) ((val)->v_col.tab)
#define VAR_COL(val) ((val)->v_col.col)
#define VAR_ANCESTOR(val) ((val)->v_col.ancestor)
#define VAR_VM_ID(val) ((val)->v_vm_col.id)
#define VAR_VM_ANCESTOR(val) ((val)->v_vm_col.ancestor)
#define VAR_VM_GROUP(val) ((val)->v_vm_col.group_id)
#define VAR_RES_ID(val) ((val)->v_res.res_id)

/* var_copy(a, b)  ===>  *b = *a  */
static inline void var_copy(variant_t *src, variant_t *dst)
{
    /* GS_TYPE_CURSOR type needs to copy its value even if it is null */
    if (src->type <= GS_TYPE_REAL) {
        dst->ctrl = src->ctrl;
        dst->v_real = src->v_real;
    } else if (src->type <= GS_TYPE_DECIMAL) {
        dst->ctrl = src->ctrl;
        if (!src->is_null) {
            cm_dec8_copy(&dst->v_dec, &src->v_dec);
        }
    } else if (src->type <= GS_TYPE_VARBINARY) {
        dst->ctrl = src->ctrl;
        dst->v_bin = src->v_bin;
    } else {
        *dst = *src;
    }
}
struct st_var_malloc_handle;
typedef struct st_var_malloc_handle var_malloc_handle_t; /* type of connection handle */
typedef void *(*var_malloc_t)(var_malloc_handle_t *handle, uint32 size);
status_t var_deep_copy(variant_t *src, variant_t *dst, var_malloc_t func, var_malloc_handle_t *handle);

static inline bool8 _cm_is_finite(double value) {
    return !(isnan(value) || isinf(value));
}

static inline int32 cm_compare_double(double x, double y)
{
    bool8 x_is_nan = isnan(x);
    bool8 y_is_nan = isnan(y);
    if( x_is_nan || y_is_nan ) {
        if( x_is_nan && y_is_nan ) {
            return 0;
        }
        return x_is_nan ? 1 : -1;
    }
    if( !_cm_is_finite(x) || !_cm_is_finite(y) ) {
      if( !_cm_is_finite(x) && !_cm_is_finite(y) ) {
          if(signbit(x) == signbit(y)) {
              return 0;
          }
          return signbit(x) ? -1 : 1;
      }
      if( !_cm_is_finite(x) ) {
          return signbit(x) ? -1 : 1;
      } else {
          return !signbit(y) ? -1 : 1;
      }
    }
    double diff = x - y;

    x = fabs(x);
    y = fabs(y);
    if (fabs(diff) <= VAR_DOUBLE_EPSILON * MAX(x, y)) {
        return 0;
    }

    return (diff > 0) ? 1 : -1;
}

#define CARDINAL_NUMBER 10

#define IS_DZERO(d, dep) (((d) == 0.0) && ((dep) == 0))

static inline int32 cm_compare_double_prec10(double x, double y)
{
    bool8 x_is_nan = isnan(x);
    bool8 y_is_nan = isnan(y);
    if( x_is_nan || y_is_nan ) {
        if( x_is_nan && y_is_nan ) {
            return 0;
        }
        return x_is_nan ? 1 : -1;
    }
    if( !_cm_is_finite(x) || !_cm_is_finite(y) ) {
      if( !_cm_is_finite(x) && !_cm_is_finite(y) ) {
          if(signbit(x) == signbit(y)) {
              return 0;
          }
          return signbit(x) ? -1 : 1;
      }
      if( !_cm_is_finite(x) ) {
          return signbit(x) ? -1 : 1;
      } else {
          return !signbit(y) ? -1 : 1;
      }
    }
    double tmp_x = x;
    double tmp_y = y;
    dec8_t x_dec, y_dec;
    int32 dexp_x, dexp_y;

    (void)frexp(x, &dexp_x);
    dexp_x = (int32)((double)dexp_x * (double)GS_LOG10_2);
    (void)frexp(y, &dexp_y);
    dexp_y = (int32)((double)dexp_y * (double)GS_LOG10_2);
    if (dexp_x > MAX_NUMERIC_EXPN || dexp_y > MAX_NUMERIC_EXPN) {
        // a positive number is greater than a negative number
        if (IS_DZERO(x, dexp_x) && !IS_DZERO(y, dexp_y)) {
            return (y > 0) ? -1 : 1;
        }

        if (IS_DZERO(y, dexp_y) && !IS_DZERO(x, dexp_x)) {
            return (x > 0) ? 1 : -1;
        }

        if (x < 0 && y > 0) {
            return -1;
        }

        if (x > 0 && y < 0) {
            return 1;
        }

        if (dexp_x != dexp_y) {
            // a negative number returns a small power number
            if (dexp_x > dexp_y) {
                return (x > 0) ? 1 : -1;
            } else {
                return (x > 0) ? -1 : 1;
            }
        }

        tmp_x = x * pow(CARDINAL_NUMBER, -dexp_x);
        tmp_y = y * pow(CARDINAL_NUMBER, -dexp_y);
    }

    if (cm_real_to_dec8_prec10(tmp_x, &x_dec) == GS_SUCCESS && cm_real_to_dec8_prec10(tmp_y, &y_dec) == GS_SUCCESS) {
        return cm_dec8_cmp(&x_dec, &y_dec);
    }

    return -1;
}

#define OVERFLOW_NONE 0
#define OVERFLOW_UPWARD 1
#define OVERFLOW_DOWNWARD (-1)


uint32 var_get_size(variant_t *var);
uint32 cm_get_datatype_strlen(gs_type_t type, uint32 strlen);
bool32 cm_datatype_arrayable(gs_type_t type);
bool32 var_num_is_zero(variant_t *var);
status_t var_as_clob(const nlsparams_t *nlsparams, variant_t *var, text_buf_t *buf);
status_t var_as_blob(variant_t *var, text_buf_t *buf);
status_t var_as_image(const nlsparams_t *nlsparams, variant_t *var, text_buf_t *buf);
status_t opr_unary(variant_t *right, variant_t *result);
status_t chk_hex_as_bigint_var(variant_t *var);
status_t var_gen_variant(char *ele_val, uint32 size, uint32 datatype, variant_t *val);
bool32 var_is_negative(variant_t *var);
bool32 var_is_zero(variant_t *var);


#define GS_MAX_DATATYPE_STRLEN 64

#define NUM_DATA_CMP(T, data1, data2) ((*(T *)(data1) < *(T *)(data2)) ? -1 : (*(T *)(data1) > *(T *)(data2)) ? 1 : 0)

static inline int32 var_compare_data(const void *data1, const void *data2, gs_type_t type, uint32 size1, uint32 size2)
{
    text_t text1, text2;

    if (data1 == NULL || data2 == NULL) {
        return (data1 == data2) ? 0 : (data1 == NULL) ? 1 : -1;
    }

    /* with same value types */
    switch (type) {
        case GS_TYPE_UTINYINT:
        case GS_TYPE_USMALLINT:
        case GS_TYPE_UINT32:
            return NUM_DATA_CMP(uint32, data1, data2);
        case GS_TYPE_TINYINT:
        case GS_TYPE_SMALLINT:
        case GS_TYPE_INTEGER:
            return NUM_DATA_CMP(int32, data1, data2);

        case GS_TYPE_BOOLEAN:
            return NUM_DATA_CMP(bool32, data1, data2);

        case GS_TYPE_BIGINT:
            return NUM_DATA_CMP(int64, data1, data2);
        case GS_TYPE_UINT64:
            return NUM_DATA_CMP(uint64, data1, data2);

        case GS_TYPE_DATE:
        case GS_TYPE_TIMESTAMP:
        case GS_TYPE_TIMESTAMP_TZ_FAKE:
        case GS_TYPE_TIMESTAMP_LTZ:
            return NUM_DATA_CMP(int64, data1, data2);

        case GS_TYPE_TIMESTAMP_TZ:
            return cm_tstz_cmp((timestamp_tz_t *)data1, (timestamp_tz_t *)data2);

        case GS_TYPE_REAL:
            return NUM_DATA_CMP(double, data1, data2);

        case GS_TYPE_NUMBER:
        case GS_TYPE_DECIMAL:
            return cm_dec8_cmp((dec8_t *)data1, (dec8_t *)data2);

        default:
            text1.str = (char *)data1;
            text1.len = size1;
            text2.str = (char *)data2;
            text2.len = size2;

            return cm_compare_text(&text1, &text2);
    }
}

static inline int32 var_compare_data_ex(void *data1, uint16 size1, void *data2, uint16 size2, gs_type_t type)
{
    text_t text1, text2;

    if (size1 == 0 || size2 == 0) {
        return (size1 == size2) ? 0 : (size1 == 0) ? -1 : 1;
    }

    /* with same value types */
    switch (type) {
        case GS_TYPE_UTINYINT:
        case GS_TYPE_USMALLINT:
        case GS_TYPE_UINT32:
            return NUM_DATA_CMP(uint32, data1, data2);
        case GS_TYPE_TINYINT:
        case GS_TYPE_SMALLINT:
        case GS_TYPE_INTEGER:
        case GS_TYPE_INTERVAL_YM:
            return NUM_DATA_CMP(int32, data1, data2);

        case GS_TYPE_BOOLEAN:
            return NUM_DATA_CMP(bool32, data1, data2);

        case GS_TYPE_BIGINT:
            return NUM_DATA_CMP(int64, data1, data2);
        case GS_TYPE_UINT64:
            return NUM_DATA_CMP(uint64, data1, data2);
        case GS_TYPE_DATE:
        case GS_TYPE_TIMESTAMP:
        case GS_TYPE_TIMESTAMP_TZ_FAKE:
        case GS_TYPE_TIMESTAMP_LTZ:
        case GS_TYPE_INTERVAL_DS:
            return NUM_DATA_CMP(int64, data1, data2);

        case GS_TYPE_TIMESTAMP_TZ:
            return cm_tstz_cmp((timestamp_tz_t *)data1, (timestamp_tz_t *)data2);

        case GS_TYPE_REAL:
            return NUM_DATA_CMP(double, data1, data2);

        case GS_TYPE_NUMBER:
        case GS_TYPE_DECIMAL:
            return cm_dec4_cmp((dec4_t *)data1, (dec4_t *)data2);

        case GS_TYPE_CHAR:
            text1.str = (char *)data1;
            text1.len = size1;
            text2.str = (char *)data2;
            text2.len = size2;
            return cm_compare_text_rtrim(&text1, &text2);

        case GS_TYPE_VARCHAR:
        case GS_TYPE_STRING:
        default: {
            text1.str = (char *)data1;
            text1.len = size1;
            text2.str = (char *)data2;
            text2.len = size2;
            return cm_compare_text(&text1, &text2);
        }
    }
}

#endif
