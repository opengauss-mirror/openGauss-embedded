/*
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
 * server.c
 *    DCC server main
 *
 * IDENTIFICATION
 * openGauss-embedded/tools/intarkdb_server/server.c
 *
 * -------------------------------------------------------------------------
 */

#include "stdio.h"
#include "cm_error.h"
#include "cm_log.h"
#include "util_error.h"
#include "srv_param.h"
#include "srv_instance.h"
#include "srv_session.h"
#include "cm_timer.h"
#include "cm_file.h"
#include "cm_signal.h"
#include "network/common/json/svr_json.h"


#include "compute/kv/intarkdb_kv.h"
#include "intarkdb_sql.h"


#ifdef __cplusplus
extern "C" {
#endif

typedef struct st_example_result {
    int id;
    char name[64];
    long money;
} example_result_t;

static bool8 g_server_running;
static void srv_usage(void)
{
    (void)printf("Usage: intarkdb [OPTION]\n"
           "   Or: intarkdb [-h|-H]\n"
           "   Or: intarkdb [-v|-V]\n"
           "   Or: intarkdb [mode] -D data_path\n"
           "Option:\n"
           "\t -h/-H                 show the help information.\n"
           "\t -v/-V                 show version information.\n"
           "\t -C                    specify intarkdb server config.\n");
}
#define MAX_DCC_ARG 3
#define SRV_LOOP_SLEEP_5_SECONDS 1
#define DCC_ARG_NUM2 2

static status_t srv_check_args(int argc, char * const argv[])
{
    int i = 1;
    if (argc > MAX_DCC_ARG) {
        (void)printf("too many argument\n");
        return GS_ERROR;
    }

    while (i < argc) {
        if ((strcmp(argv[i], "-C") == 0)) { /* intarkdb_server -C config */
            if (i + 1 >= argc) {
                (void)printf("invalid argument: %s\n", argv[i]);
                return GS_ERROR;
            }
            i++;
            uint32 len = (uint32)strlen((char *)argv[i]);
            if (len <= 1 || len >= (GS_MAX_PATH_LEN - 1)) {
                (void)printf("invalid argument: %s %s\n", argv[i - 1], argv[i]);
                return GS_ERROR;
            }
        } else {
            (void)printf("invalid argument: %s\n", argv[i]);
            return GS_ERROR;
        }
        i++;
    }
    return GS_SUCCESS;
}

static int srv_find_arg(int argc, char * const argv[], const char *find_arg)
{
    for (int i = 1; i < argc; i++) {
        if (cm_str_equal_ins(argv[i], find_arg)) {
            return i;
        }
    }
    return 0;
}

void read_server_config(int argc, char * const argv[], server_config* cfg) {
    char config_path[GS_FILE_NAME_BUFFER_SIZE] = {0};
    int pos = srv_find_arg(argc, argv, "-C");
    if (pos > 0 && (pos + 1) < argc) {
        strcpy(config_path, argv[pos + 1]);
    }
    FILE *fp = fopen(config_path, "r");
    if (fp == NULL) {
        GS_LOG_RUN_INF("config file not exist, use default config");
        return;
    }
    char buf[CONFIG_SIZE];
    int ch;
    int i = 0;
    while ((ch = fgetc(fp)) != EOF) {
        buf[i++] = (char)ch;
    }
    buf[i] = '\0';
    fclose(fp);
    GS_LOG_DEBUG_INF("config:%s len:%ld", buf, strlen(buf));

    double port = 0;
    cJSON* j_config = cJSON_Parse(buf);
    json_get_number(j_config, "port", &port);
    cfg->port = port;
    cJSON *host_array;
    cJSON *dbs_obj;
    cJSON *db_item;
    int size = 0;
    json_get_array(j_config, "host", &host_array, &size);
    if (size > GS_MAX_LSNR_HOST_COUNT) {
        GS_LOG_RUN_INF("server config hosts out of range");
        return GS_FALSE;
    }
    for(int host_i = 0; host_i < size; host_i++) {
        strcpy(cfg->host[host_i], cJSON_GetStringValue(cJSON_GetArrayItem(host_array, host_i)));
    }

    json_get_object(j_config, "dbs", &dbs_obj);
    db_item = dbs_obj->child;
    int db_i = 0;
    while (db_i < MAX_DB_NUM) {
        if (db_item != NULL) {
            cfg->db_items[db_i].isUsed = true;
            strcpy(cfg->db_items[db_i].name, db_item->string);
            strcpy(cfg->db_items[db_i].location, db_item->valuestring);
            printf("db name:%s location:%s %d\n", cfg->db_items[db_i].name, cfg->db_items[db_i].location, cfg->db_items[db_i].isUsed);
            db_item = db_item->next;
        } else {
            cfg->db_items[db_i].isUsed = false;
            strcpy(cfg->db_items[db_i].name, "\0");
            strcpy(cfg->db_items[db_i].location, "\0");
        }
        db_i++;
    }
    return;
}

static void srv_print_version(void)
{
    (void)printf("%s\n", get_version());
}

static void srv_instance_loop(void)
{
    date_t last = g_timer()->systime;
    while (g_server_running) {
        if ((g_timer()->systime - last) > SECONDS_PER_DAY) {
            //(void)srv_chk_ssl_cert_expire();
            last = g_timer()->systime;
        }

        cm_sleep(SRV_LOOP_SLEEP_5_SECONDS);
    }
}

static void signal_handler(int sig_no)
{
    if (sig_no == SIGTERM || sig_no == SIGQUIT || sig_no == SIGINT) {
        GS_LOG_RUN_INF("intarkdb server received signal %d, begin exit gracefully", sig_no);
        g_server_running = GS_FALSE;
    }
}

static status_t srv_main_start(const server_config *cfg)
{
    // handle signal
#ifndef WIN32
        (void)signal(SIGHUP, SIG_IGN);
#endif
    (void)cm_regist_signal(SIGINT, signal_handler);
    (void)cm_regist_signal(SIGTERM, signal_handler);
    (void)cm_regist_signal(SIGQUIT, signal_handler);

    // srv instance startup
    if (startup_server(cfg) != GS_SUCCESS) {
        (void)printf("Instance startup failed, errcode: %d, errmsg: %s\n",
            cm_get_error_code(), cm_get_errormsg(cm_get_error_code()));
        (void)fflush(stdout);
        GS_LOG_RUN_INF("Instance startup failed, errcode: %d, errmsg: %s",
                    cm_get_error_code(), cm_get_errormsg(cm_get_error_code()));
        return GS_ERROR;
    }
    return GS_SUCCESS;
}

int main(int argc, char *argv[])
{

    intarkdb_database db;
    intarkdb_connection conn;
    intarkdb_result intarkdb_result = intarkdb_init_result();
    if (!intarkdb_result) {
        printf("intarkdb_init_result failed!!\n");
        return 0;
    }

    char path[1024] = "db1";
    // open database
    if (intarkdb_open(path, &db) != SQL_SUCCESS) {
        printf("intarkdb_open failed\n");
        intarkdb_close(&db);
        return 0;
    }

    // open database operate handle
    if (intarkdb_connect(db, &conn) != SQL_SUCCESS) {
        printf("intarkdb_connect failed\n");
        intarkdb_disconnect(&conn);
        intarkdb_close(&db);
        return 0;
    }
    if (intarkdb_query(conn,"CREATE TABLE IF NOT EXISTS example_table_c(id int, name varchar(20), money bigint)",
            intarkdb_result) != SQL_SUCCESS) {
        printf("Failed to create table!!, errmsg:%s\n", intarkdb_result_msg(intarkdb_result));
    }

/*     if (intarkdb_query(conn, "CREATE UNIQUE INDEX idx_example_table_c_1 on example_table_c(id)",
            intarkdb_result) != SQL_SUCCESS) {
        printf("Failed to create index!! , errmsg:%s\n", intarkdb_result_msg(intarkdb_result));
    }
    if (intarkdb_query(conn, "INSERT INTO example_table_c(id,name,money) \
            VALUES (1,'小明',168000000), (2,'小天',3880000000)", intarkdb_result) != SQL_SUCCESS) {
        printf("Failed to insert row!!, errmsg:%s\n", intarkdb_result_msg(intarkdb_result));
    } */

    log_param_t *log_param = cm_log_param_instance();
    log_param->log_instance_startup = GS_FALSE;
    if (argc == DCC_ARG_NUM2) {
        if (strcmp(argv[1], "-v") == 0 || strcmp(argv[1], "-V") == 0) {
            srv_print_version();
            return GS_SUCCESS;
        } else if (strcmp(argv[1], "-h") == 0 || strcmp(argv[1], "-H") == 0) {
            srv_usage();
            return GS_SUCCESS;
        }
        else {
            printf("invalid argument: %s\n", argv[1]);
            srv_usage();
            return GS_SUCCESS;
        }
    } else if (argc > 1) {
        GS_RETURN_IFERR(srv_check_args(argc, argv));
    } else if (argc == 1) {
        srv_usage();
        return GS_SUCCESS;
    }
    server_config cfg;
    cfg.port = 9000;
    strcpy(cfg.host[0], "127.0.0.1");
    for (int i = 1; i < GS_MAX_LSNR_HOST_COUNT; i++) {
        strcpy(cfg.host[i], "\0");
    }
    read_server_config(argc, argv, &cfg);

    if (srv_main_start(&cfg) != GS_SUCCESS) {
        return GS_ERROR;
    }
    printf("intarkdb server started\n");
    GS_LOG_RUN_INF("intarkdb server started");
    g_server_running = GS_TRUE;

    if (server_add_database("db1", "db1") != GS_SUCCESS) {
        printf("intarkdb server add db1 error \n");
    }
    
    const database_item* dbs = server_list_database();
    for ( int i = 0; i < MAX_DB_NUM; i++ ) {
        if(dbs[i].isUsed)
            printf("%s:%s \n", dbs[i].name, dbs[i].location);
    }


    srv_instance_loop();
    GS_LOG_RUN_INF("intarkdb server exit");
    
    stop_server();
    GS_LOG_RUN_INF("intarkdb server shutdown");


    if (intarkdb_query(conn, "select * from example_table_c", intarkdb_result) != SQL_SUCCESS) {
        printf("Failed to select row!!, errmsg:%s\n", intarkdb_result_msg(intarkdb_result));
    }

    // print
    int64_t row_count = intarkdb_row_count(intarkdb_result);
    if (row_count > 0) {
        int64_t column_count = intarkdb_column_count(intarkdb_result);
        for (int64_t col = 0; col < column_count; col++) {
            printf("  %s          ", intarkdb_column_name(intarkdb_result, col));
        }
        printf("\n");
        for (int64_t row = 0; row < row_count; row++) {
            example_result_t result_set;
            result_set.id = intarkdb_value_int32(intarkdb_result, row, 0);
            // Note : The pointer returned by intarkdb_value_varchar will automatically release on the next operation!
            strncpy(result_set.name, intarkdb_value_varchar(intarkdb_result, row, 1), sizeof(result_set.name));
            result_set.money = intarkdb_value_int64(intarkdb_result, row, 2);
            printf("  %d            %s            %ld", result_set.id, result_set.name, result_set.money);
            printf("\n");
        }
    }
    return GS_SUCCESS;
}

#ifdef __cplusplus
}
#endif
