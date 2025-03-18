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
* login.c
*
* IDENTIFICATION
* openGauss-embedded/src/network/login/login.c
*
* -------------------------------------------------------------------------
*/

#include "login.h"
#include "interface/c/intarkdb_sql.h"
#include "storage/gstor/zekernel/common/cm_log.h"
#include "common/srv_def.h"
// #include "dependency/GmSSL/include/gmssl/sha2.h"


uint64_t get_user_count(intarkdb_connection *conn)
{
    int count = 0;
    do
    {
        char sql[64] = "select count(*) from \"SYS_LOGIN_USERS\";";
        intarkdb_result result = intarkdb_init_result();
        if (intarkdb_query(*conn, sql, result) != GS_SUCCESS) {
            GS_LOG_RUN_ERR("get user count err");
            break;
        }
        uint64 row_count = intarkdb_row_count(result);
        if (row_count > 0) {
            count = intarkdb_value_uint64(result, 0, 0);
        }
        intarkdb_free_row(result);
    } while(0);
    return count;
}

int get_user_password(intarkdb_connection *conn, const char * user_name, char * password)
{
    status_t rescode = RES_SUCCESS;
    intarkdb_result result = intarkdb_init_result();
    do
    {
        char sql[128];
        sprintf(sql, "select \"PASSWORD#\" from \"SYS_LOGIN_USERS\" where \"USERNAME#\" = '%s';", user_name);
        if (intarkdb_query(*conn, sql, result) != GS_SUCCESS)
        {
            rescode = RES_DB_ERROR;
            GS_LOG_RUN_ERR("db err");
            break;
        }
        uint64 row_count = intarkdb_row_count(result);
        if (row_count > 0)
        {
            strcpy(password,  intarkdb_value_varchar(result, 0, 0));
            rescode = RES_SUCCESS;
        } else {
            rescode = RES_USER_NOT_EXIST;
        }
        intarkdb_free_row(result);
    } while(0);
    return rescode;
}


// void get_hash_SHA256(const char * str, char * dgst)
// {
//     uint8_t int_dgst[PASSWORD_DIGEST_SIZE];
//     uint32_t len = PASSWORD_DIGEST_SIZE * 2 + 1;
//     char str_dgst[ PASSWORD_DIGEST_SIZE * 2 + 1] = "\0";
//     sha256_digest((unsigned char *)str, strlen(str), int_dgst);
//     for (int i = 0; i < PASSWORD_DIGEST_SIZE; i++)
//     {
//         sprintf(str_dgst, "%s%02x", str_dgst, int_dgst[i]);
//     }
//     GS_LOG_DEBUG_INF("dgst:%s", str_dgst);
//     strcpy(dgst, str_dgst);
//     return;
// }

//  void get_hash_str(const char * str, char * dgst)
// {
//     uint8_t int_dgst[SM3_DIGEST_SIZE];
//     uint32_t len = SM3_DIGEST_SIZE * 2 + 1;
//     char str_dgst[ SM3_DIGEST_SIZE * 2 + 1] = "\0";
//     sm3_digest((unsigned char *)str, strlen(str), int_dgst);
//     for (int i = 0; i < SM3_DIGEST_SIZE; i++)
//     {
//         sprintf(str_dgst, "%s%02x", str_dgst, int_dgst[i]);
//     }
//     GS_LOG_DEBUG_INF("dgst:%s", str_dgst);
//     strcpy(dgst, str_dgst);
//     return;
// }
