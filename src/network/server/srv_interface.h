#ifndef __SRV_INTERFACE_H__
#define __SRV_INTERFACE_H__

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
* srv_interface.c
*
* IDENTIFICATION
* openGauss-embedded/src/network/server/srv_interface.c
*
* -------------------------------------------------------------------------
*/
#include "cm_base.h"
#include "cm_defs.h"
#include "intarkdb_sql.h"

#ifdef __cplusplus
extern "C" {
#endif

#define LSNR_HOST_COUNT 8
#define MAX_IP_LEN 64
#define MAX_DB_NAME_LEN 64
#define MAX_DB_NUM 64

typedef struct st_database_item {
    bool isUsed;
    char name[MAX_DB_NAME_LEN];
    char location[GS_MAX_PATH_LEN];
    intarkdb_database db;
} database_item;

typedef struct st_server_config {
    char host[LSNR_HOST_COUNT][MAX_IP_LEN];
    unsigned short port;
    database_item db_items[MAX_DB_NUM];
} server_config;

EXPORT_API status_t startup_server(const server_config* cfg);
EXPORT_API status_t server_add_database(const char* dbname, const char* location);
EXPORT_API void server_delete_database(char* dbname);
EXPORT_API const database_item* server_list_database();
EXPORT_API void stop_server(void);
EXPORT_API void init_config(server_config *cfg);

#ifdef __cplusplus
}
#endif
#endif /* __SRV_INTERFACE_H__ */