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
 * cm_base.h
 *
 *
 * IDENTIFICATION
 * src/storage/gstor/zekernel/common/cm_base.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef __CM_BASE__
#define __CM_BASE__

#include "securec.h"

#include <stdio.h>
#include <string.h>

#ifdef _WIN32
#define strcpy_sp   strcpy_s
#define strncpy_sp  strncpy_s
#define strcat_sp   strcat_s
#define strncat_sp  strncat_s
#define memcpy_sp   memcpy_s
#define memset_sp   memset_s
#endif

#ifdef _WIN32
#define EXPORT_API __declspec(dllexport)
#else
#ifndef EXPORT_API
#define EXPORT_API __attribute__((visibility("default")))
#endif
#endif

#endif
