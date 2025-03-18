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

#include "include/intarkdb.h"
#include "interface/c/intarkdb_sql.h"


#ifdef __cplusplus
extern "C" {
#endif

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
#define SRV_LOOP_SLEEP_1_SECONDS 1000
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

bool read_server_config(int argc, char * const argv[], server_config* cfg) {
    char config_path[GS_FILE_NAME_BUFFER_SIZE] = {0};
    int pos = srv_find_arg(argc, argv, "-C");
    if (pos > 0 && (pos + 1) < argc) {
        strcpy(config_path, argv[pos + 1]);
    }
    FILE *fp = fopen(config_path, "r");
    if (fp == NULL) {
        printf("config file not exist \n");
        return GS_FALSE;
    }
    fseek(fp,0,SEEK_END);
    long file_size = ftell(fp);
    if (file_size > CONFIG_SIZE) {
        printf("config size over limit:%u \n", CONFIG_SIZE);
        fclose(fp);
        return GS_FALSE;
    }
    fseek(fp,0,SEEK_SET);
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
    if (j_config == NULL) {
        printf("config file error \n");
        return GS_FALSE;
    }
    json_get_number(j_config, "port", &port);
    cfg->port = port;
    cJSON *host_array;
    cJSON *dbs;
    cJSON *db_item;
    int size = 0;
    json_get_array(j_config, "host", &host_array, &size);
    if (size > GS_MAX_LSNR_HOST_COUNT) {
        printf("server config hosts out of range \n");
        return GS_FALSE;
    }
    for(int host_i = 0; host_i < size; host_i++) {
        strcpy(cfg->host[host_i], cJSON_GetStringValue(cJSON_GetArrayItem(host_array, host_i)));
    }
    
    int db_i = 0;
    json_get_array(j_config, "dbs", &dbs, &size);
    if (size > MAX_DB_NUM) {
        printf("server config hosts out of range \n");
        return GS_FALSE;
    }

    while (db_i < MAX_DB_NUM) {
        if (db_i <  size) {
            db_item = cJSON_GetArrayItem(dbs, db_i);
            json_get_string(db_item, "db_name", cfg->db_items[db_i].name);
            json_get_string(db_item, "db_path", cfg->db_items[db_i].location);
            cfg->db_items[db_i].isUsed = true;
        } else {
            cfg->db_items[db_i].isUsed = false;
            strcpy(cfg->db_items[db_i].name, "\0");
            strcpy(cfg->db_items[db_i].location, "\0");
        }
        db_i++;
    }
    return GS_TRUE;
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

        cm_sleep(SRV_LOOP_SLEEP_1_SECONDS);
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
    
    if (read_server_config(argc, argv, &cfg) != GS_TRUE) {
        return GS_ERROR;
    }

    if (srv_main_start(&cfg) != GS_SUCCESS) {
        return GS_ERROR;
    }
    printf("intarkdb server started\n");
    GS_LOG_RUN_INF("intarkdb server started");
    g_server_running = GS_TRUE;

    srv_instance_loop();
    GS_LOG_RUN_INF("intarkdb server exit");

    stop_server();
    GS_LOG_RUN_INF("intarkdb server shutdown");
    return GS_SUCCESS;
}

#ifdef __cplusplus
}
#endif
