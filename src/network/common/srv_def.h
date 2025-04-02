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
* svsrv_def.h
*
* IDENTIFICATION
* openGauss-embedded/src/network/common/srv_def.h
*
* -------------------------------------------------------------------------
*/
#ifndef __SVR_DEFS__
#define __SVR_DEFS__

#include "storage/gstor/zekernel/common/cm_defs.h"

#define MAX_BUF_SIZE 2048
#define MAX_SEND_SIZE 4096
#define MAX_KEY_SIZE 128
#define SIZE_256 256
#define MAX_DB_PATH_SIZE 128
#define KV_CMD_SIZE 64   /* kv cmd */
#define RES_MSG_SIZE 64
#define MAX_TYPE_NAME_SIXE 32
#define PATH_SIZE 64
#define SVR_TIME_OUT 500 /* mill-seconds */
#define HEAD_SIZE 4
#define CONFIG_SIZE 2048
#define TIME_CONVERSION 60
#define DATA_CONVERSION 1024
#define THOUSAND 1000
#define DEFAULT_BUFF_LONGTH 1024

#define EXC_BIT_MOVE_TWO_BYTES         (16)
#define EXC_BIT_MOVE_FOUR_BYTES        (32)
#define EXC_BIT_MOVE_SIX_BYTES         (48)
#define EXC_KEY_MAX_SIZE               SIZE_K(4)
#define EXC_DATA_MAX_SIZE              SIZE_M(10)
#define EXC_DIGIT_MAX_SIZE             (32)
#define EXC_STG_ROW_MAX_SIZE           SIZE_M(2)

#define MAX_KV_KEY_LEN  (4 * 1024)
#define MAX_KV_VALUE_LEN (10 * 1024 * 1024)

//default config, config location:output/bin/gstor/cfg/db.ini
#define DEFAULT_OM_AGENT_START 1
#define DEFAULT_OM_AGENT_PORT 3333
#define DEFAULT_SERVER_PORT 9000
#define DEFAULT_HOST "127.0.0.1"

//rescode
typedef enum en_res_code {
    RES_SUCCESS = 0,
    RES_FIELD_ERR = 1,        /* field err or missing */
    RES_CMD_ERR = 2,          /* command err */
    RES_SERVER_ERR = 3,       /* server err */
    RES_NOT_EXIST = 4,        /* not exist */
    RES_COMMIT_ERR = 5,       /* commit err, ignore if select */
    RES_SID_CONFLICT = 6,     /* in transaction sessionId conflict 长连接废弃*/
    RES_SID_NOT_EXIST = 7,    /* in transaction sessionId not exist 长连接废弃*/
    RES_DB_ERROR = 8,         /* DB ERR*/
    RES_USER_NOT_EXIST = 9,   /* user not exist */
    RES_PASSWORLD_ERROR = 10, /* user passworld error */
    RES_NOT_LOGGED = 11,      /* user not logged */
    RES_NOT_CONNECT = 12,     /* not connect */
    RES_STMT_EXIST = 13,      /* statement is exist */
    RES_STMT_NOT_EXIST = 14,  /* statement not exist */
    RES_STMT_IDX_ERR = 15,     /* statement bind value idx error, maybe over size */
    RES_TIMEOUT = 16,         /* not connect */
} rescode_t;


//protoId
#define PROTO_ID_NULL  0         /* error */
#define PROTO_ID_KV  1           /* kv */
#define PROTO_ID_SQL  2          /* sql */
#define PROTO_ID_STATUS  3       /* status */

#define INT32_BUF_SIZE 4
#define INT64_BUF_SIZE 8
#define PROTO_VERSION 1

// Transaction status
typedef enum 
{
    Transaction_Begin = 0,
    Transaction_Normal = 1,
    Transaction_Commit = 2,
    Transaction_Rollback = 3
};



typedef enum en_protocol_type {
    PROTO_TYPE_UNKNOWN = 0,
    PROTO_TYPE_DCC_CMD = 1,
    PROTO_TYPE_JSON = 2,
} protocol_type_t;

typedef enum en_dcc_event_type {
    DCC_WATCH_EVENT_UNKONOW = 0,
    DCC_WATCH_EVENT_PUT,
    DCC_WATCH_EVENT_DELETE,
} dcc_event_type_t;

#endif