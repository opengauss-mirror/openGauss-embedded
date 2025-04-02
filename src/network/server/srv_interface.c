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

#include <stdio.h> 
#include <stdlib.h> 

#include "srv_interface.h"
#include "srv_instance.h"

status_t startup_server(const server_config* cfg) {
    return srv_instance_startup(cfg);
}

status_t server_add_database(const char* dbname, const char* location) {
    return srv_instance_add_database(dbname, location);
}

void server_delete_database(char* dbname) {
    srv_instance_delete_database(dbname);
}

const database_item* server_list_database() {
    return srv_instance_list_database();
}

void stop_server(void) {
    srv_instance_destroy();
}

void init_config(server_config *cfg) {
    strcpy(cfg->host, DEFAULT_HOST);
    cfg->port = DEFAULT_SERVER_PORT;
    int db_i = 0;
    while (db_i < MAX_DB_NUM) {
        cfg->db_items[db_i].isUsed = false;
        strcpy(cfg->db_items[db_i].name, "\0");
        strcpy(cfg->db_items[db_i].location, "\0");
        db_i++;
    }
    return;
}