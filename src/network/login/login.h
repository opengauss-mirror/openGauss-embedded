/*
* Copyright (c) GBA-NCTI-ISDC. 2022-2024.
*
* openGauss embedded is licensed under Mulan PSL v2.
* You can use this software according to the terms and conditions of the Mulan PSL v2.
* You may obtain a copy of Mulan PSL v2 at:
*
* http://license.coscl.org.cn/MulanPSL2
*
* THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
* EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
* MERCHANTABILITY OR FITFOR A PARTICULAR PURPOSE.
* See the Mulan PSL v2 for more details.
* -------------------------------------------------------------------------
*
* login.h
*
* IDENTIFICATION
* openGauss-embedded/src/network/login/login.h
*
* -------------------------------------------------------------------------
*/

#ifndef __OM_LOGIN_H__
#define __OM_LOGIN_H__

#include "storage/gstor/zekernel/common/cm_defs.h"
#include "interface/c/intarkdb_sql.h"
// #include "dependency/GmSSL/include/gmssl/sm3.h"
// #include "dependency/GmSSL/include/gmssl/sm4.h"
// #include "dependency/GmSSL/include/gmssl/sha2.h"

#ifdef __cplusplus
extern "C" {
#endif

#define PASSWORD_DIGEST_SIZE 32
#define USER_MAX_LEN 32
#define PASSWORLD_MAX_LEN 256

uint64_t get_user_count(intarkdb_connection *conn);
int get_user_password(intarkdb_connection *conn, const char * user_name, char * password);

#ifdef __cplusplus
}
#endif
#endif