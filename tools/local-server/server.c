#include "stdio.h"

#include "compute/kv/intarkdb_kv.h"
#include "interface/c/intarkdb_sql.h"
#include "server_thread/gstor_om_agent.h"
#include "server_thread/srv_def.h"
#include <signal.h>

#define DCC_ARG_NUM1 1
#define DCC_ARG_NUM2 2
#define DCC_ARG_NUM3 3
#define DCC_ARG_NUM4 4

#define EXC_DCC_KV_TABLE            ((char *)"SYS_KV")
static int is_run = 0;


void sig_handler(int signo)
{
    // my_print("catch the signal SIGUSR1 %d run num:%d\n", signo, kv_index);
    is_run = 0;
}

__fortify_function void
my_print(const char *__restrict __fmt, ...)
{
  printf(__fmt, __va_arg_pack ());
  fflush(stdout);
}


int main(int argc, char *argv[])
{
    char db_path[PATH_SIZE] = "./";
    uint16 port = DEFAULT_SERVER_PORT;
    char host[CM_MAX_IP_LEN] = "127.0.0.1";
    if (argc > DCC_ARG_NUM1) {
        strcpy(db_path, argv[DCC_ARG_NUM1]);
    }
    if (argc > DCC_ARG_NUM2) {
        port = atoi(argv[DCC_ARG_NUM2]);
    }
    if (argc > DCC_ARG_NUM3) {
        strcpy(host, argv[DCC_ARG_NUM3]);
    }

    my_print("host:%s port:%d path:%s \r\n", host, port, db_path);

    intarkdb_database db;
    if (intarkdb_open(db_path, &db) != SQL_SUCCESS) {
        my_print("intarkdb_open failed \r\n");
        return GS_ERROR;
    }

    st_om_agent_config cfg;
    init_om_agent_config(&cfg, db_path);
    cfg.om_agent_port = port;
    strcpy(cfg.om_agent_hosts[0], host);


    if (srv_start_om_agent(db, &cfg) != GS_SUCCESS) {
        my_print("[API] om_agent failed\r\n");
        return GS_ERROR;
    }


    my_print("start success \r\n");
    if (signal(SIGUSR1, sig_handler) == SIG_ERR)
    {
        my_print("signal error\n");
    }
    is_run = 1;
    while(is_run)
    {
        sleep(1);
    }

    intarkdb_close(&db);
    srv_close_om_agent();

}