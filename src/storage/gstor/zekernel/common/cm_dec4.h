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
 * cm_dec4.h
 *    The head file of two DECIMAL types: dec4_t and dec8_t. The former
 * has a compact format, and is designed for storage. Whereas the latter has an
 * efficient structure, and thus can be applied for numeric computation.
 *
 * IDENTIFICATION
 *    src/storage/gstor/zekernel/common/cm_dec4.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef __CM_DEC4_H__
#define __CM_DEC4_H__
#include "cm_defs.h"
#include "cm_text.h"
#include <math.h>


/* +/- */
#define DEC_SIGN_PLUS (uint8)0
#define DEC_SIGN_MINUS (uint8)1

/* negate a decimal */
#define NEGATE_SIGN(sign) (!(sign))

#define DEC_IS_NEGATIVE(dec) ((dec)->sign == DEC_SIGN_MINUS)

#define ZERO_DEC_HEAD(dec) (dec)->head = 0

/*  The maximal buff size for parsing a decimal, The MAX_NUMERIC_BUFF is
 *  set to be greater than MAX_NUM_PRECISION, which can be captured
 *  more significant digits, and thus can promote high calculation accuracy.
 *  The bigger the value is, the more accuracy it can be improved, but may
 *  weaken the performance.
 */
#define MAX_NUMERIC_BUFF 40

/* The maximal precision for comparing two decimal. Directly compare
 * two decimals may cause failure as several digits in the last may be
 * not too much accuracy,
 * @note  MAX_NUM_CMP_PREC <= MAX_NUMERIC_BUFF, i.e., must less than
 * the number of digits in buff
 */
#define MAX_NUM_CMP_PREC GS_MAX_NUM_SAVING_PREC

/*  The DECIMAL/NUMBER/NUMERIC data type stores zero as well as positive
 *  and negative fixed numbers with absolute values from 1.0*10^MIN_NUMERIC_EXPN
 *  to 1.0*10^MAX_NUMERIC_EXPN. When the exponent of a decimal is
 *  greater than MAX_NUMERIC_EXPN, an error will be returned. If the exponent
 *  is less than MIN_NUMERIC_EXPN, a zero will be returned. */
#define MAX_NUMERIC_EXPN (int32)127
#define MIN_NUMERIC_EXPN (int32) - 127

/* To decide whether a decimal is zero */
#define DECIMAL_IS_ZERO(dec) ((dec)->ncells == 0)

#define GS_PI 3.14159265358979323846        // pi
#define GS_PI_2 1.57079632679489661923      // pi/2
#define GS_PI_4 0.785398163397448309616     // pi/4
#define GS_1_PI 0.318309886183790671538     // 1/pi
#define GS_2_PI 0.636619772367581343076     // 2/pi
#define GS_2_SQRTPI 1.12837916709551257390  // 2/sqrt(pi)

#define GS_LOG10_2 0.30102999566398119521374  // log10(2)

typedef enum en_round_mode {
    ROUND_TRUNC,   /* round towards zero, @see C function *trunc*, <==> (int)dec */
    ROUND_CEILING, /* round towards positive infinity, @see C function *ceil* */
    ROUND_FLOOR,   /* round towards negative infinity, @see C function *floor* */
    /* round towards "nearest neighbor" unless both neighbors are equidistant, in which case round up. */
    ROUND_HALF_UP,
} round_mode_t;

/* overflow check */
#define DEC_OVERFLOW_CHECK_BY_SCIEXP(sciexp)  \
    do {                                      \
        if ((sciexp) > MAX_NUMERIC_EXPN) {    \
            GS_THROW_ERROR(ERR_NUM_OVERFLOW); \
            return GS_ERROR;                  \
        }                                     \
    } while (0)

#define DECIMAL_TRY_CMP(n1, n2, flag) \
    do {                              \
        if ((n1) > (n2)) {            \
            return (flag);            \
        } else if ((n1) < (n2)) {     \
            return -(flag);           \
        }                             \
    } while (0)

/* Compare decimals with different sign
 * (0, N), (N, 0), (P, N), (N, P),
 * these four situations can be decided */
#define dec_cmp_diff_sign(x1, x2)                  \
    do {                                           \
        if (x1->sign != x2->sign) {                \
            return (x1->sign > x2->sign) ? -1 : 1; \
        }                                          \
                                                   \
        if (DECIMAL_IS_ZERO(x1)) {                 \
            return DECIMAL_IS_ZERO(x2) ? 0 : -1;   \
        } else if (DECIMAL_IS_ZERO(x2)) {          \
            return 1;                              \
        }                                          \
    } while (0)

#define dec_cmp_data(dec1, dec2, flag)                                   \
    do {                                                                 \
        uint32 cmp_len = MIN((dec1)->ncells, (dec2)->ncells);            \
        for (uint32 i = 0; i < cmp_len; i++) {                           \
            DECIMAL_TRY_CMP((dec1)->cells[i], (dec2)->cells[i], (flag)); \
        }                                                                \
        DECIMAL_TRY_CMP((dec1)->ncells, (dec2)->ncells, (flag));         \
    } while (0)

/* Compare non-zero x1 and x2 with same sign, (N, N), (P, P) */
#define dec_cmp_same_sign(dec1, dec2)                      \
    do {                                                   \
        int32 flag = DEC_IS_NEGATIVE(dec1) ? -1 : 1;       \
        DECIMAL_TRY_CMP((dec1)->expn, (dec2)->expn, flag); \
        dec_cmp_data((dec1), (dec2), flag);                \
    } while (0)

#define DECIMAL_LEN(buf) cm_dec4_stor_sz((dec4_t *)(buf))
#define DECIMAL_FORMAT_LEN(buf) CM_ALIGN4(DECIMAL_LEN(buf))

/* Compute the (maximal) size of a decimal when specify the precision */
#define MAX_DEC_BYTE_BY_PREC(prec) (DEC4_NCELLS(prec) * sizeof(c4typ_t) + DEC4_HEAD_SIZE)

#define MAX_DEC_BYTE_SZ MAX_DEC_BYTE_BY_PREC(GS_MAX_NUM_SAVING_PREC)

/* The number of cells (an uint32) used to store the decimal type.
The int256 is used to represent a decimal, thus the size is 8 */
#define DEC4_CELL_SIZE (uint8)13

/* The number of digits that an element of an int256, i.e., an uint32
can encode. This indicates each uint32 can record at most DEC_ELEM_DIGIT
digits. Its value 9 is the upper, since 10^(9+1) < 2^32 < 10^11  */
#define DEC4_CELL_DIGIT 4

#define DEC4_EXPN_UNIT 4

#define SEXP_2_D4EXP(sci_exp) (int8)((sci_exp) / DEC4_EXPN_UNIT)

#define D4EXP_2_SEXP(dexp) ((dexp)*DEC4_EXPN_UNIT)

/* The the mask used to handle each cell. It is equal to 10^DEC4_CELL_DIGIT */
#define DEC4_CELL_MASK 10000U

/* Half of DEC4_CELL_MASK */
#define DEC4_HALF_MASK 5000U

/** The format to print a cell */
#define DEC4_CELL_FMT "%04u"

/* DEC4_MAX_ALLOWED_PREC = DEC_CELL_SIZE * DEC4_CELL_DIGIT indicates the maximal
precision that a decimal can capture at most */
#define DEC4_MAX_ALLOWED_PREC (DEC4_CELL_SIZE * DEC4_CELL_DIGIT)

#define DEC4_MAX_EXP_OFFSET DEC4_CELL_SIZE

/* Get the position of n-th digit of an int256, when given precision
 * of u0 (i.e., the position of the dot).
 * @note Both n and the pos begin with 0 */
#define DEC4_POS_N_BY_PREC0(n, prec0) ((n) + (int32)DEC4_CELL_DIGIT - (int32)(prec0))
#define DEC4_POS_N_BY_CELL0(n, cells) ((n) + (int32)DEC4_CELL_DIGIT - cm_count_u16digits((cells)[0]))

#define DEC4_OVERFLOW_CHECK(dec) DEC_OVERFLOW_CHECK_BY_SCIEXP(DEC4_GET_SEXP(dec))

/* This structure is used to represent a big 256-integer. It can be
composed by 8 uint32 or 4 uint64, or 2 uint128 array. The array is
assumed to be in big-endian int-order, i.e., the most significant
digit is in the zeroth element.

Array: | uint32 | uint32 | uint32 | uint32 | uint32 | uint32 | ...
values: |   u0   |   u1   |   u2   |   u3   |   u4   |   u5   |  ...
Then, the bigInteger can be represented by
Integer = u0 * M^7 + u1 * M^6 + u2 * M^5 + ...
where M is equal to 2^32
*/
typedef uint16 c4typ_t;
typedef uint32 cc4typ_t;
typedef c4typ_t cell4_t[DEC4_CELL_SIZE];

#pragma pack(2)
typedef struct st_dec4 {
    union {
        struct {
            uint8 sign : 1;   /* 0: for positive integer; 1: for negative integer */
            uint8 ncells : 7; /* number of cells, 0: for unspecified precision */
            int8 expn;        /* the exponent of the number */
        };
        c4typ_t head;
    };

    cell4_t cells;
} dec4_t;
#pragma pack()

/* == sizeof(expn & sign & ncells) */
#define DEC4_HEAD_SIZE sizeof(uint16)

/* Compute the number of cells used to store the significant digits when
 * given precision. The precision must be greater than zero.
 * **NOTE THAT:** for adjusting the expn to be an integral multiple of
 * DEC4_CELL_DIGIT (for speeding addition and subtraction), You may require
 * an additional cell to store the significant digits. For instance,
 * 2 significant digits could store in one cell, but it may be stored
 * in two cells like | 000X | X000 | * 10^DEC4_CELL_DIGIT. The
 * following equation can provide sufficient but not too much wasteful
 * cells to store fixed-precision digits.
 */
#define DEC4_NCELLS(precision) (((precision) + DEC4_CELL_DIGIT - 2) / DEC4_CELL_DIGIT + 1)

/* Get the scientific exponent of a decimal when given its exponent and precision */
#define DEC4_GET_SEXP_BY_PREC0(sexp, prec0) ((int32)(sexp) + (int32)(prec0)-1)
/* Get the scientific exponent of a decimal when given its exponent and cell0 */
#define DEC4_GET_SEXP_BY_CELL0(sexp, cell0) DEC4_GET_SEXP_BY_PREC0(sexp, cm_count_u16digits(cell0))
/* Get the scientific exponent of a decimal6 */
#define DEC4_GET_SEXP(dec) DEC4_GET_SEXP_BY_CELL0(D4EXP_2_SEXP((dec)->expn), ((dec)->cells[0]))

extern const dec4_t DEC4_MIN_INT64;
extern const dec4_t DEC4_ONE;


#ifdef __cplusplus
extern "C" {
#endif

#define DEC4_DEBUG_PRINT(dec, fmt, ...)

status_t cm_dec4_finalise(dec4_t *dec, uint32 prec, bool32 allow_overflow);

static inline void cm_zero_dec4(dec4_t *dec)
{
    ZERO_DEC_HEAD(dec);
}

static inline uint32 cm_dec4_stor_sz(const dec4_t *d4)
{
    return ((uint32)(1 + (d4)->ncells)) * sizeof(c4typ_t);
}

/* Copy the data a decimal */
static inline void cm_dec4_copy(dec4_t *dst, const dec4_t *src)
{
    if (SECUREC_UNLIKELY(dst == src)) {
        return;
    }

    dst->head = src->head;
    /* Another way to Copy the data of decimals is to use loops, for example:
     *    uint32 i = src->ncells;
     *    while (i-- > 0)
     *        dst->cells[i] = src->cells[i];
     * However, this function is performance sensitive, and not too safe when
     * src->ncells is abnormal. By actural testing, using switch..case here
     * the performance can improve at least 1.5%. The testing results are
     *    WHILE LOOP  : 5.64% cm_dec4_copy
     *    SWITCH CASE : 4.14% cm_dec4_copy
     * Another advantage is that the default branch of SWITCH CASE can be used
     * to handle abnormal case, which reduces an IF statement.
     */
    switch (src->ncells) {
        case 13: /* = DEC_CELL_SIZE */
            dst->cells[12] = src->cells[12];
            /* fall-through */
        case 12: /* = DEC_CELL_SIZE - 1 */
            dst->cells[11] = src->cells[11];
            /* fall-through */
        case 11:
            dst->cells[10] = src->cells[10];
            /* fall-through */
        case 10:
            dst->cells[9] = src->cells[9];
            /* fall-through */
        case 9:
            dst->cells[8] = src->cells[8];
            /* fall-through */
        case 8:
            dst->cells[7] = src->cells[7];
            /* fall-through */
        case 7:
            dst->cells[6] = src->cells[6];
            /* fall-through */
        case 6:
            dst->cells[5] = src->cells[5];
            /* fall-through */
        case 5:
            dst->cells[4] = src->cells[4];
            /* fall-through */
        case 4:
            dst->cells[3] = src->cells[3];
            /* fall-through */
        case 3:
            dst->cells[2] = src->cells[2];
            /* fall-through */
        case 2:
            dst->cells[1] = src->cells[1];
            /* fall-through */
        case 1:
            dst->cells[0] = src->cells[0];
            /* fall-through */
        case 0:
            break;
        default:
            /* if error happens, set the error and write the log */
            GS_THROW_ERROR(ERR_ASSERT_ERROR, "copy_dec4: invalid decimal");
            cm_zero_dec4(dst);
            break;
    }
}

/*
 * Compare the data of two decimal, this function can be used to compare two
 * decimals with same sign and expn
 */
static inline int32 cm_dec4_cmp_data(const dec4_t *dec1, const dec4_t *dec2, int32 flag)
{
    uint32 cmp_len = MIN(dec1->ncells, dec2->ncells);
    for (uint32 i = 0; i < cmp_len; i++) {
        DECIMAL_TRY_CMP(dec1->cells[i], dec2->cells[i], flag);
    }

    DECIMAL_TRY_CMP(dec1->ncells, dec2->ncells, flag);
    return 0;
}

/*
 * x1 > x2: return  1
 * x1 = x2: return  0
 * x1 < x2: return -1
 */
static inline int32 cm_dec4_cmp(const dec4_t *x1, const dec4_t *x2)
{
    dec_cmp_diff_sign(x1, x2);
    dec_cmp_same_sign(x1, x2);
    return 0;
}

#define cm_dec4_equal(dec1, dec2) (cm_dec4_cmp(dec1, dec2) == 0)

static inline void cm_dec4_trim_zeros(dec4_t *dec)
{
    while (dec->ncells > 0 && dec->cells[dec->ncells - 1] == 0) {
        --dec->ncells;
    }
}

EXPORT_API status_t cm_dec4_to_str(const dec4_t *dec, int max_len, char *str);
EXPORT_API status_t cm_dec4_to_text(const dec4_t *dec, int32 max_len, text_t *text);

/*
 * Convert a decimal into a text with all precisions
 */
static inline status_t cm_dec4_to_text_all(const dec4_t *dec, text_t *text)
{
    return cm_dec4_to_text(dec, GS_MAX_DEC_OUTPUT_ALL_PREC, text);
}

EXPORT_API status_t cm_str_to_dec4(const char *str, dec4_t *dec);
EXPORT_API num_errno_t cm_numpart_to_dec4(num_part_t *np, dec4_t *dec);
EXPORT_API status_t cm_text_to_dec4(const text_t *text, dec4_t *dec);
EXPORT_API void cm_uint32_to_dec4(uint32 i32, dec4_t *dec);
EXPORT_API void cm_int32_to_dec4(int32 i32, dec4_t *dec);
EXPORT_API void cm_int64_to_dec4(int64 i64, dec4_t *dec);
EXPORT_API status_t cm_real_to_dec4(double real, dec4_t *dec);
EXPORT_API double cm_dec4_to_real(const dec4_t *dec);
 
static inline status_t cm_dec4_check_valid(const dec4_t *dec)
{
    uint32 cnt = 0;

    for (int i = 0; i < dec->ncells; i++) {
        if (dec->cells[i] > 0) {
            cnt++;
        }
    }

    if (dec->ncells > 0 && cnt == 0) {
        return GS_ERROR;
    }

    return GS_SUCCESS;
}

EXPORT_API status_t cm_dec4_to_uint64(const dec4_t *dec, uint64 *u64, round_mode_t rnd_mode);
EXPORT_API status_t cm_dec4_to_int64(const dec4_t *dec, int64 *val, round_mode_t rnd_mode);
EXPORT_API status_t cm_dec4_to_uint32(const dec4_t *dec, uint32 *i32, round_mode_t rnd_mode);
EXPORT_API status_t cm_dec4_to_int32(const dec4_t *dec, int32 *i32, round_mode_t rnd_mode);
EXPORT_API status_t cm_dec4_to_uint16(const dec4_t *dec, uint16 *i16, round_mode_t rnd_mode);
EXPORT_API status_t cm_dec4_to_int16(const dec4_t *dec, int16 *i16, round_mode_t rnd_mode);
EXPORT_API bool32 cm_dec4_is_integer(const dec4_t *dec);

#ifdef __cplusplus
}
#endif
#endif
