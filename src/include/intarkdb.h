/*
 * Copyright (c) 2022 GC.
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
 * intarkdb.h
 *
 *
 * IDENTIFICATION
 *    src/include/intarkdb.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef __C_API_INTARKDB_H__
#define __C_API_INTARKDB_H__

#include "interface/c/intarkdb_sql.h"
#include "compute/kv/intarkdb_kv.h"
#include "compute/sql/include/common/winapi.h"
#include "../VERSION.h"

EXPORT_API const char* get_version() { return GIT_VERSION;}

EXPORT_API const char* get_commit_id() { return GIT_COMMIT_ID;}

#endif
