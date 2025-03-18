#include <stdio.h>
#include <stdlib.h>
#include <sqlite3.h>
#include <time.h>
#include <string.h>
#include <sys/time.h>
#include <signal.h>
#include <pthread.h>

#define TIMEOUT 600
static int time_count = 0; // 10分钟超时自动结束
static int is_run = 0;
static int sqlite_index = 0;
static int old_sqlite_index = 0;
char *log_file = "sqlite_test.log";

__fortify_function void mylog(const char *__restrict __fmt, ...)
{
   FILE *fp = fopen(log_file, "a+");
   fprintf(fp, __fmt, __va_arg_pack());
   fclose(fp);
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

void log_stat()
{
   if (is_run == 0)
      return;

   char str_time[64];
   get_format_time_st(str_time);
   mylog("%s qps %d total %d status running\n", str_time, sqlite_index - old_sqlite_index, sqlite_index);
   old_sqlite_index = sqlite_index;
   time_count++;
   if (time_count > TIMEOUT)
      is_run = 0;
   return;
}

void timer_thread()
{
   // printf("timer thread start \n");
   pthread_t tid;
   while (is_run)
   {
      sleep(1);
      pthread_create(&tid, NULL, log_stat, NULL);
   }

   return;
}

void sig_handler(int signo)
{
   //mylog("catch the signal SIGUSR1 %d run num:%d\n", signo, sqlite_index);
   is_run = 0;
}

int exec_handle(void *data, int argc, char **argv, char **colname)
{
   /* 计数器*/
   int i = *(int *)(data);
   *(int *)(data) = i + 1;

   /* 取出结果*/
   printf("NO.%d message: [%s] is [%s], [%s] is [%s]...", *(int *)(data), colname[0], colname[1], argv[0], argv[1]);
   return 0;
}

int sqlite_test()
{
   sqlite3 *db;
   char *zErrMsg = 0;
   int rc;
   int rc2;
   char *sql;
   float time_use = 0;
   float task_tps = 0;
   struct timeval start;
   struct timeval end;
   char *sql_create_table;
   char *sql_create_index;
   char *sql_create_index2;
   char buf[120];
   int count = 0;
   char str[100];
   int i, j, k, h;
   int index;
   int result, ret;
   int data = 0;
   char *err_msg = NULL;
   char **dbResult; // 是 char ** 类型，两个*号
   int nRow, nColumn;

   rc = sqlite3_open("test/sqlites.db", &db);
   if (rc)
   {
      printf("Can't open database: %s\n", sqlite3_errmsg(db));
      exit(0);
   }
   else
   {
      printf("Opened database successfully\n");
   }

   sql_create_table = "CREATE TABLE IF NOT EXISTS COMPANY(id INTEGER PRIMARY KEY AUTOINCREMENT,name1 CHAR(20) NOT NULL);";
   // sql_create_index = "create index index_name1	on COMPANY (name1);";
   rc = sqlite3_exec(db, sql_create_table, NULL, 0, &zErrMsg);
   if (rc != SQLITE_OK)
   {
      printf("SQL error: %s\n", zErrMsg);
      sqlite3_free(zErrMsg);
   }

   // 使用事务提升插入效率，否则每个独立的事务都要打开和关闭数据库文件
   pthread_t tid;
   pthread_create(&tid, NULL, timer_thread, NULL);
   gettimeofday(&start, NULL);
   sqlite3_exec(db, "begin", NULL, NULL, NULL);

   while (is_run)
   {
      sprintf(buf, "INSERT INTO COMPANY (name1) VALUES (%d);", sqlite_index);
      sqlite3_exec(db, buf, NULL, NULL, NULL);
      sqlite3_exec(db, "commit", NULL, NULL, NULL);
      sqlite3_exec(db, "begin", NULL, NULL, NULL);
      sprintf(buf, "SELECT * from COMPANY where id =%d", sqlite_index);
      ret = sqlite3_exec(db, str, NULL, &data, &err_msg);
      sqlite_index++;
      /*if (sqlite_index % 3000 == 0)
      {
         sqlite3_exec(db, "commit", NULL, NULL, NULL);
         sqlite3_exec(db, "begin", NULL, NULL, NULL);
      }*/
   }
   gettimeofday(&end, NULL);
   time_use = (end.tv_sec - start.tv_sec) + (1.0 * (end.tv_usec - start.tv_usec)) / 1000000;
   char str_time[64];
   get_format_time_st(str_time);
   mylog("%s avg_qps %lf total %d status stop\n", str_time, sqlite_index / time_use, sqlite_index);

   sqlite3_close(db);
   return 0;
}
int main(int argc, char *argv[])
{
   printf("run \n");
   if (signal(SIGUSR1, sig_handler) == SIG_ERR)
   {
      printf("signal error\n");
   }
   is_run = 1;
   sqlite_test();
   return 0;
}