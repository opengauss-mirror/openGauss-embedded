#include "stdio.h"
#include "cm_log.h"
#include "cm_error.h"
#include "cm_defs.h"
#include "compute/kv/intarkdb_kv.h"
#include "interface/c/intarkdb_sql.h"
#include "cm_signal.h"

#define MAX_DCC_ARG 3
#define DCC_ARG_NUM2 2

#define EXC_DCC_KV_TABLE            ((char *)"SYS_KV")

// static int32 g_lock_fd;
// static const char *g_lock_file = "dcc_server.lck";
static int is_run = 0;
intarkdb_database db;

typedef struct st_dcc_text {
    char* value;
    unsigned int len;
} dcc_text_t;

static void srv_usage(void)
{
    (void)printf("Usage: dcc [OPTION]\n"
           "   Or: dcc [-h|-H]\n"
           "   Or: dcc [-v|-V]\n"
           "   Or: dcc [mode] -D data_path\n"
           "Option:\n"
           "\t -h/-H                 show the help information.\n"
           "\t -v/-V                 show version information.\n"
           "\t -D                    specify DCC data path.\n");
}


// static status_t srv_check_args(int argc, char * const argv[])
// {
//     int i = 1;
//     if (argc > MAX_DCC_ARG) {
//         (void)printf("too many argument\n");
//         return GS_ERROR;
//     }

//     while (i < argc) {
//         if ((strcmp(argv[i], "-D") == 0)) { /* dcc_server -D data_path */
//             if (i + 1 >= argc) {
//                 (void)printf("invalid argument: %s\n", argv[i]);
//                 return GS_ERROR;
//             }
//             i++;
//             uint32 len = (uint32)strlen((char *)argv[i]);
//             if (len <= 1 || len >= (GS_MAX_PATH_LEN - 1)) {
//                 (void)printf("invalid argument: %s %s\n", argv[i - 1], argv[i]);
//                 return GS_ERROR;
//             }
//         } else {
//             (void)printf("invalid argument: %s\n", argv[i]);
//             return GS_ERROR;
//         }
//         i++;
//     }
//     return GS_SUCCESS;
// }

// static int srv_find_arg(int argc, char * const argv[], const char *find_arg)
// {
//     for (int i = 1; i < argc; i++) {
//         if (cm_str_equal_ins(argv[i], find_arg)) {
//             return i;
//         }
//     }
//     return 0;
// }

// static status_t srv_process_setup_args(int argc, char *argv[])
// {
//     int pos = srv_find_arg(argc, argv, "-D");
//     if (pos > 0 && (pos + 1) < argc) {
//         GS_RETURN_IFERR(srv_set_param("DATA_PATH", argv[pos + 1]));
//     }

//     return GS_SUCCESS;
// }

// static status_t srv_lock_dcc_server(void)
// {
//     char file_name[GS_FILE_NAME_BUFFER_SIZE] = { 0 };
//     char real_path[GS_FILE_NAME_BUFFER_SIZE] = { 0 };
//     param_value_t param_data_path;
//     GS_RETURN_IFERR(srv_get_param(DCC_PARAM_DATA_PATH, &param_data_path));
//     GS_RETURN_IFERR(realpath_file(param_data_path.str_val, real_path, GS_FILE_NAME_BUFFER_SIZE));
//     PRTS_RETURN_IFERR(snprintf_s(file_name, GS_FILE_NAME_BUFFER_SIZE, GS_FILE_NAME_BUFFER_SIZE - 1, "%s/%s",
//                                  real_path, g_lock_file));

//     if (cm_open_file(file_name, O_CREAT | O_RDWR | O_BINARY, &g_lock_fd) != GS_SUCCESS) {
//         return GS_ERROR;
//     }

//     return cm_lock_fd(g_lock_fd);
// }
void sig_handler(int signo)
{
    // printf("catch the signal SIGUSR1 %d run num:%d\n", signo, kv_index);
    is_run = 0;
}
void *data_collection(void *arg)
{
    int i =0;
    int v =0;
    long long int collect_faild_ID = 0;
    long long int collect_ID = 0;
    char *sql = calloc(1024,sizeof(char));
    intarkdb_connection conn;
    intarkdb_result  intarkdb_result= intarkdb_init_result();
    if(!intarkdb_result) {
        printf("intarkdb_init_result failed!!\n");
        return NULL;
    }
    if (intarkdb_connect(db, &conn) != SQL_SUCCESS) {
        printf("intarkdb_connect failed\n");
        return NULL;
    }
    //sprintf(sql,);
    // sprintf(sql, "CREATE TABLE IF NOT EXISTS s1(ID int NOT NULL AUTOINCREMENT,  \
    //                      ADDR VARCHAR(64),DATA INTEGER,DATE timestamp default now());");
    // if (intarkdb_query(conn, "CREATE TABLE IF NOT EXISTS s1(ID int NOT NULL AUTOINCREMENT,DATE timestamp default now(),ADDR VARCHAR(128),DATA INTEGER,DATE timestamp default now());", intarkdb_result) != SQL_SUCCESS) {
    //     printf("Failed to create table!!\n");
    // }
    if (intarkdb_query(conn,  "CREATE TABLE IF NOT EXISTS s1(ID int NOT NULL AUTOINCREMENT, ADDR VARCHAR(64),DATA INTEGER,DATE timestamp default now())  PARTITION BY RANGE(date)"
                    "TIMESCALE INTERVAL '1d' RETENTION '30d' AUTOPART;", intarkdb_result) != SQL_SUCCESS) {
        printf("Failed to create table!!\n");
    }
    // sprintf(sql,"create index  index_%s on %s(ADDR);","1","s1");
    //     if (intarkdb_query(conn, "", intarkdb_result) != SQL_SUCCESS) {
    //     printf("Failed to create index!!\n");
    // }
    int s = 0;
    while(1)
    {
        
        sprintf(sql, "INSERT INTO s1(ADDR,DATA) VALUES('DB0.DBW1',1);",i,v);
        
        if (intarkdb_query(conn, sql, intarkdb_result) != SQL_SUCCESS) {
            collect_faild_ID++;
            printf("ERROR %s\n",sql);
        }else
        {
            collect_ID++;
            printf("%s %lld\n",sql,collect_ID);
        }
        usleep(30*1000);
        i++;
        v++;
        if(i>100)
        {
            i=0;
            v=0;
        }
    }
    free(sql);
    return NULL;
}
void *data_update(void *arg)
{
    char *sql = calloc(1024,sizeof(char));
    intarkdb_connection conn;
    intarkdb_result  intarkdb_result= intarkdb_init_result();
    if(!intarkdb_result) {
        printf("intarkdb_init_result failed!!\n");
        return NULL;
    }
    if (intarkdb_connect(db, &conn) != SQL_SUCCESS) {
        printf("intarkdb_connect failed\n");
        return NULL;
    }

  int i = 0;
  while(1)
  {
    sprintf(sql, "select data  from s1 where ADDR = 'DB0.DBW%d' order by date desc limit 1;",i);
    if (intarkdb_query(conn, sql, intarkdb_result) != SQL_SUCCESS) {
        printf("ERROR %s!!\n",sql);
    }
    else{
        printf("%s\n",sql);
    }
    i++;
    if(i>100)
        i=0;
    
  }  
    intarkdb_destroy_result(intarkdb_result);
    intarkdb_disconnect(&conn);
    pthread_exit(NULL);
    free(sql);
    return NULL;
}
int main(int argc, char *argv[])
{
    // log_param_t *log_param = cm_log_param_instance();
    // log_param->log_instance_startup = GS_FALSE;
    if (argc == DCC_ARG_NUM2) {
        if (strcmp(argv[1], "-v") == 0 || strcmp(argv[1], "-V") == 0) {
            // srv_print_version();
            printf("[INFO]: dcc version\r\n");
            return GS_SUCCESS;
        } else if (strcmp(argv[1], "-h") == 0 || strcmp(argv[1], "-H") == 0) {
            srv_usage();
            return GS_SUCCESS;
        }
    } else if (argc > 1) {
        // GS_RETURN_IFERR(srv_check_args(argc, argv));
        // GS_RETURN_IFERR(srv_process_setup_args(argc, argv));
    } else if (argc == 1) {
        //srv_usage();
        //return GS_SUCCESS;
    }


    // if (init_config() != GS_SUCCESS) {
    //     printf("init config failed");
    //     return GS_ERROR;
    // }

    // if (srv_lock_dcc_server() != GS_SUCCESS) {
    //     printf("lock dcc server failed");
    //     return GS_ERROR;
    // }

    int db_type = 0;

    char path[1024] = "\0";
    if (argc >= 3) {
        memcpy(path, argv[2], strlen(argv[2]));
    }
    else
    {
        strcpy(path,".");
    } 
    printf("path=%s\n", path);
    if (intarkdb_open(path, &db) != SQL_SUCCESS) {
        printf("intarkdb_open failed\n");
        return GS_ERROR;
    }
    // pthread_t       thread_write,thread_read;

    // pthread_create(&thread_write, NULL, data_collection, NULL);
    // pthread_create(&thread_read, NULL, data_update, NULL);
   
    // pthread_join(thread_write, NULL);
    // pthread_join(thread_read, NULL);
    printf("start success \r\n");
    //test_put();
    if (signal(SIGINT, sig_handler) == SIG_ERR)
    {
        printf("signal error\n");
    }
    is_run = 1;
    while(is_run)
    {
        sleep(1);
    }

    intarkdb_close(&db);

}