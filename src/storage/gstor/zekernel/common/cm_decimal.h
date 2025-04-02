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
 * cm_decimal.h
 * The head file of two DECIMAL types: dec4_t and dec8_t. The former
 * has a compact format, and is designed for storage. Whereas the latter has an
 * efficient structure, and thus can be applied for numeric computation.
 *
 * IDENTIFICATION
 * src/storage/gstor/zekernel/common/cm_decimal.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef __CM_NUMBER_H_
#define __CM_NUMBER_H_

#include "cm_dec8.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Truncate tailing *prec* number of digits of an int64 into zeros.
 * e.g., cm_truncate_bigint(123123, 3) ==> 123000
 * cm_truncate_bigint(123623, 3) ==> 124000
 * @note prec can not exceed 9  */
EXPORT_API int64 cm_truncate_bigint(int64 val, uint32 prec);

/*
 * Count the number of 10-base digits of an uint16.
 * e.g. 451 ==> 3, 12 ==> 2, abs(-100) ==> 3, 0 ==> 1, 1 ==> 1
 */
EXPORT_API uint32 cm_count_u16digits(uint16 u16);

EXPORT_API uint32 cm_count_u32digits(uint32 u32);

EXPORT_API double cm_round_real(double val, round_mode_t mode);

/**
 * Convert a single cell text into uint32. A single cell text is a text of
 * digits, with the number of text is no more than 9
 * @author Added, 2018/04/16
 */
EXPORT_API uint32 cm_celltext2uint32(const text_t *cellt);

/*
 * Decode a decimal from a void data with size
 */
EXPORT_API status_t cm_dec_4_to_8(dec8_t *d8, const dec4_t *d4, uint32 sz_byte);

EXPORT_API status_t cm_dec_8_to_4(dec4_t *d4, const dec8_t *d8);

/* The actual bytes of a dec8 in storage */
EXPORT_API uint32 cm_dec8_stor_sz(const dec8_t *d8);

EXPORT_API status_t cm_adjust_double(double *val, int32 precision, int32 scale);

/* The arithmetic operations among DECIMAL and BIGINT */
EXPORT_API status_t cm_dec8_add_int64(const dec8_t *dec, int64 i64, dec8_t *result);

EXPORT_API status_t cm_dec8_add_int32(const dec8_t *dec, int32 i32, dec8_t *result);

#define cm_int64_add_dec8(i64, dec, result) cm_dec8_add_int64((dec), (i64), (result))
#define cm_dec8_sub_int64(dec, i64, result) cm_dec8_add_int64((dec), (-(i64)), (result))

EXPORT_API status_t cm_int64_sub_dec(int64 i64, const dec8_t *dec, dec8_t *result);
EXPORT_API status_t cm_dec8_mul_int64(const dec8_t *dec, int64 i64, dec8_t *result);
#define cm_int64_mul_dec8(i64, dec, result) cm_dec8_mul_int64((dec), (i64), (result))

EXPORT_API status_t cm_dec8_div_int64(const dec8_t *dec, int64 i64, dec8_t *result);

EXPORT_API status_t cm_int64_div_dec8(int64 i64, const dec8_t *dec, dec8_t *result);
/* The arithmetic operations among DECIMAL and REAL/DOUBLE */
EXPORT_API status_t cm_dec8_add_real(const dec8_t *dec, double real, dec8_t *result);

#define cm_real_add_dec8(real, dec, result) cm_dec8_add_real((dec), (real), (result))
#define cm_dec8_sub_real(dec, real, result) cm_dec8_add_real((dec), (-(real)), (result))

EXPORT_API status_t cm_real_sub_dec8(double real, const dec8_t *dec, dec8_t *result);

EXPORT_API status_t cm_dec8_mul_real(const dec8_t *dec, double real, dec8_t *result);

#define cm_real_mul_dec8(real, dec, result) cm_dec8_mul_real((dec), (real), (result))

EXPORT_API status_t cm_dec8_div_real(const dec8_t *dec, double real, dec8_t *result);

EXPORT_API status_t cm_real_div_dec8(double real, const dec8_t *dec, dec8_t *result);

#ifdef __cplusplus
}
#endif

#endif
