#include "cm_log.h"
#include "cm_error.h"
#include "cm_defs.h"
#include "interface/c/gstor.h"
#include "interface/c/intarkdb_sql.h"
#include "network/om_agent/gstor_om_agent.h"
#include "network/common/json/svr_json.h"

#include <stdio.h>
#include <stdlib.h>
#include <sqlite3.h>
#include <time.h>
#include <string.h>
#include <sys/time.h>
#include <signal.h>
#include <pthread.h>
#include <sys/resource.h>
#include <sys/prctl.h>

static const char* DEFAULT_TABLE = "SYS_KV";
#define TIMEOUT 600
#define thread_num  8
static int time_count = 0; // 10分钟超时自动结束
static int is_run = 0;
static int is_start = 0;
static int dbindex = 0;
static int old_index = 0;
static int qps = 0;
static char db_type[16] = "\0";
const static int ReadTotalNum = 100000;
void *instar_handles[32];
void *sqlite_handles[32];
void *sqlitedb;

pthread_t tid[thread_num];

void stop_presstest()
{
    is_start = 0;
    dbindex = 0;
    old_index = 0;
    qps = 0;
    strcpy(db_type, "\0");
}

void get_format_time_st(char *str_time)
{
    struct tm *tm_t;
    struct timeval time;

    gettimeofday(&time, NULL);
    tm_t = localtime(&time.tv_sec);
    if (NULL != tm_t)
    {
        sprintf(str_time, "%04d-%02d-%02d %02d:%02d:%02d.%03ld",
                tm_t->tm_year + 1900,
                tm_t->tm_mon + 1,
                tm_t->tm_mday,
                tm_t->tm_hour,
                tm_t->tm_min,
                tm_t->tm_sec,
                time.tv_usec / 1000);
    }
    return;
}

void timer_thread()
{
    pthread_t tid;
    char str_time[64];
    while (is_start)
    {
        get_format_time_st(str_time);
        qps = dbindex - old_index;
        GS_LOG_RUN_INF("%s qps %d total %d status running", db_type, dbindex - old_index, dbindex);
        old_index = dbindex;

        time_count++;
        if (time_count > TIMEOUT)
            stop_presstest();
        
        sleep(1);
    }
    return;
}

void instar_read_test_thread(void *handle)
{
    prctl(PR_SET_NAME, "instar_read_thread");
    gstorReply *reply_get;
    char str[20];
    while (is_start)
    {
        int tmp_index = (dbindex++) % ReadTotalNum;
        sprintf(str, "%d", tmp_index);
        reply_get = (gstorReply *)gstor_command_get(handle, str); // 测读
        if (reply_get->type != GS_SUCCESS)
        {
            GS_LOG_RUN_INF("InstarDB error: %d",  reply_get->type);
            stop_presstest();
        }
        gstor_freeReplyObject(reply_get);
    }
}

void instar_prepare_data()
{
    gstorReply *reply_set;
    char str[20];
    while (is_run && dbindex < ReadTotalNum)
    {
        int tmp_index = ++dbindex;
        sprintf(str, "%d", tmp_index);
        reply_set = (gstorReply *)gstor_command_set(instar_handles[0], str, str); // 测写入
        gstor_freeReplyObject(reply_set);
        if (tmp_index % 1000 == 0)
        {
            gstor_kv_commit(instar_handles[0]);
        }
    }
    dbindex = 0;
    GS_LOG_RUN_INF("instarDB finish prepare data num:%d", ReadTotalNum);
}

void sqlite_read_test_thread(void *handle)
{
    prctl(PR_SET_NAME, "sqlite_read_thread");
    int result;
    char str[128];
    char *err_msg = NULL;
    int data = 0;

    while (is_start)
    {
        int tmp_index = (dbindex++) % ReadTotalNum;
        sprintf(str, "SELECT * from COMPANY where id =%d", tmp_index);
        result = sqlite3_exec(handle, str, NULL, &data, &err_msg);
        if (result != SQLITE_OK)
        {
            GS_LOG_RUN_INF("SQLite error: %s", err_msg);
            stop_presstest();
        }
        sqlite3_free(err_msg);
    }
}
void sqlite_prepare_data()
{
    gstorReply *reply_set;
    char str[128];
    sqlite3_exec(sqlitedb, "begin", NULL, NULL, NULL);
    while (is_run && dbindex <= ReadTotalNum)
    {
        int tmp_index = ++dbindex;
        sprintf(str, "INSERT INTO COMPANY (name1) VALUES (%d);", tmp_index);
        sqlite3_exec(sqlitedb, str, NULL, NULL, NULL);

        if (tmp_index % 1000 == 0)
        {
            sqlite3_exec(sqlitedb, "commit", NULL, NULL, NULL);
            sqlite3_exec(sqlitedb, "begin", NULL, NULL, NULL);
        }
    }
    dbindex = 0;
    GS_LOG_RUN_INF("sqlite finish prepare data num:%d", ReadTotalNum);
}

status_t instarDB_func(const char* buf_cmd, char* res_msg)
{
    if(strcmp(buf_cmd, "prepare") == 0)
    {
        instar_prepare_data();
    }
    else if (strcmp(buf_cmd, "start") == 0)
    {
        gstorReply *reply_get;
        char str[20];
        sprintf(str, "%d", ReadTotalNum);
        reply_get = (gstorReply *)gstor_command_get(instar_handles[0], str);
        if (reply_get->type != GS_SUCCESS)
        {
            GS_LOG_RUN_INF("InstarDB error: %d",  reply_get->type);
            return RES_SERVER_ERR;
        }
        else if(reply_get->str == NULL)
        {
            instar_prepare_data();
        }
        gstor_freeReplyObject(reply_get);
        is_start = 1;
        pthread_t timer_tid;
        pthread_create(&timer_tid, NULL, timer_thread, NULL); 
        for (int i = 0; i < thread_num; i++)
        {
            pthread_create(&tid[i], NULL, instar_read_test_thread, instar_handles[i]);
        }
    }
    else 
    {
        GS_LOG_DEBUG_WAR("err cmd:%s", buf_cmd);
        sprintf(res_msg, "err cmd:%s", buf_cmd);
        return RES_CMD_ERR;
    }
    return RES_SUCCESS;
}

status_t sqlite_func(const char* buf_cmd, char* res_msg)
{
    if(strcmp(buf_cmd, "prepare") == 0)
    {
        sqlite_prepare_data();
    }
    else if (strcmp(buf_cmd, "start") ==0 )
    {
        //检查是否需要prepare
        char str[128];
        int data = 0;
        int result;
        char *err_msg = NULL;
        int nRow;
        int nCol;
        char** pResult;
        sprintf(str, "SELECT * from COMPANY where id = %d", ReadTotalNum);
        result = sqlite3_get_table(sqlitedb, str, &pResult, &nRow, &nCol, &err_msg);  
        //result = sqlite3_exec(sqlitedb, str, NULL, &data, &err_msg);
        if (result != SQLITE_OK)
        {
            GS_LOG_RUN_INF("SQLite error: %s", err_msg);
        }
        if(nRow == 0)
        {
            sqlite_prepare_data();
        }
        sqlite3_free(err_msg);

        is_start = 1;
        pthread_t timer_tid;
        pthread_create(&timer_tid, NULL, timer_thread, NULL);
        for (int i = 0; i < thread_num; i++)
        {
            pthread_create(&tid[i], NULL, sqlite_read_test_thread, sqlite_handles[i]);
        }
    }
    else 
    {
        GS_LOG_DEBUG_WAR("err cmd:%s", buf_cmd);
        sprintf(res_msg, "err cmd:%s", buf_cmd);
        return RES_CMD_ERR;
    }
    return RES_SUCCESS;
}


status_t pressure_proto_handle(cs_pipe_t *pipe, const cJSON * proto)
{
    GS_LOG_DEBUG_INF("%s proto: %s", __func__, cJSON_PrintUnformatted(proto));
    char buf_cmd[KV_CMD_SIZE];
    cJSON *res_proto = cJSON_CreateObject();
    double seqId = 0;
    uint32 rescode = RES_SUCCESS;
    char res_msg[RES_MSG_SIZE] = "";
    do 
    {
        rescode = RES_FIELD_ERR;
        GS_BREAK_IF_ERROR(json_get_number(proto, "seqId", &seqId));
        GS_BREAK_IF_ERROR(json_get_string(proto, "cmd", buf_cmd));
        if(strcmp(buf_cmd, "status") != 0)
        {
            GS_BREAK_IF_ERROR(json_get_string(proto, "value", db_type));
        }
        rescode = RES_SUCCESS;
    } while(0);
    cJSON_AddItemToObjectCS(res_proto, "seqId", cJSON_CreateNumber((double)seqId));

    if (strcmp(buf_cmd, "status") == 0)
    {
        cJSON *json_rows = cJSON_CreateObject();
        cJSON_AddItemToObjectCS(json_rows, "isRunning", cJSON_CreateString(db_type));
        cJSON_AddItemToObjectCS(json_rows, "qps", cJSON_CreateNumber(qps));
        cJSON_AddItemToObjectCS(res_proto, "value", json_rows);
    }
    else if (strcmp(buf_cmd, "stop") == 0)
    {
        stop_presstest();
    }
    else
    {
        if (strcmp(db_type, "instarDB") == 0)
        {
            rescode = instarDB_func(buf_cmd, res_msg);
        }
        else if (strcmp(db_type, "sqlite") == 0)
        {
            rescode = sqlite_func(buf_cmd, res_msg);
        }
        else
        {
            strcpy(res_msg, "err db type");
            rescode = RES_CMD_ERR;
        }
    }
    cJSON_AddItemToObjectCS(res_proto, "rescode", cJSON_CreateNumber((double)rescode));
    cJSON_AddItemToObjectCS(res_proto, "msg", cJSON_CreateString(res_msg));
    status_t status = tcp_send_set_end(pipe, cJSON_PrintUnformatted(res_proto));
}



status_t sqlite_init(const char* path)
{
    sqlite3 *db;
    char *zErrMsg = 0;
    int rc;
    float task_tps = 0;
    struct timeval start;
    struct timeval end;
    char *sql_create_table;
    
    char db_file[64];
    printf("%s/sqlites.db", path);
    sprintf(db_file, "%s/sqlites.db", path);
    rc = sqlite3_open(db_file, &db);
    if (rc)
    {
        GS_LOG_RUN_INF("Can't open database: %s", sqlite3_errmsg(db));
        exit(0);
    }
    else
    {
        GS_LOG_RUN_INF("Opened database successfully");
    }

    sql_create_table = "CREATE TABLE IF NOT EXISTS COMPANY(id INTEGER PRIMARY KEY AUTOINCREMENT,name1 CHAR(20) NOT NULL);";
    rc = sqlite3_exec(db, sql_create_table, NULL, 0, &zErrMsg);
    if (rc != SQLITE_OK)
    {
        GS_LOG_RUN_INF("SQL error: %s", zErrMsg);
        sqlite3_free(zErrMsg);
    }
    sqlitedb = db;

    for (int i = 0; i < thread_num; i++)
    {
        sqlite3 *tmp_db;
        rc = sqlite3_open_v2(db_file, &tmp_db, SQLITE_OPEN_READONLY | SQLITE_OPEN_NOMUTEX, NULL);
        if (rc)
        {
            GS_LOG_RUN_INF("Can't open database: %s", sqlite3_errmsg(db));
            exit(0);
        }
        sqlite_handles[i] = tmp_db;
    }

    return GS_SUCCESS;
}

status_t instar_init()
{ 
    for (int i = 0; i < thread_num; i++)
    {

        if (alloc_kv_handle(&instar_handles[i]) != GS_SUCCESS)
        {
            GS_LOG_RUN_INF("get instar_handles[%d] failed", i);
        }
        if (create_or_open_kv_table(instar_handles[i], "tb_pressure_test") != GS_SUCCESS)
        {
            GS_LOG_RUN_INF("open table failed i:%d", i);
            return GS_ERROR;
        }
        gstor_kv_begin(instar_handles[i]);
    }
    return GS_SUCCESS;
}

void sig_handler(int signo)
{
    is_run = 0;
}

int main(int argc, char *argv[])
{
    intarkdb_database db;
    char path[1024] = ".";
    if (argc >= 2) 
    {
        memcpy(path, argv[1], strlen(argv[1]));
    }
    GS_LOG_RUN_INF("db path:%s", path);
    if (intarkdb_open(path, &db) != SQL_SUCCESS)
    {
        GS_LOG_RUN_INF("intarkdb_open failed");
        return GS_ERROR;
    }

    if (srv_start_om_agent_by_config(db, "./om_agent_cfg.json", path) != GS_SUCCESS) {
        GS_LOG_RUN_INF("[API] om_agent failed");
        return GS_ERROR;
    }

    GS_RETURN_IFERR(sqlite_init(path));
    GS_RETURN_IFERR(instar_init());

    register_proto_handles(8, pressure_proto_handle);

    GS_LOG_RUN_INF("path=%s thread_num=%d", path, thread_num);

    if (signal(SIGUSR1, sig_handler) == SIG_ERR)
    {
        printf("signal error\n");
    }
    is_run = 1;
    while(is_run)
    {
        sleep(1);
    }

    srv_close_om_agent();
    intarkdb_close(&db);
    sqlite3_close(db);
}