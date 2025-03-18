#include "stdio.h"
#include <time.h>
#include <signal.h>
#include <pthread.h>
#include <sys/resource.h>
#include <sys/prctl.h>
#include "interface/c/gstor.h"


#ifdef __cplusplus
extern "C"
{
#endif
    #define TIMEOUT 600
    static int time_count = 0;  //10分钟超时自动结束
    static int is_run = 0;
    static int kv_index = 0;
    static int old_kv_index = 0;
    static char *log_file = "embedded_test.log";
    void *handles[32];
    static uint32 thread_num = 1;
    const static int ReadTotalNum = 100000;

    typedef enum
    {
        ReadTest = 0,
        WriteTest = 1
    } test_type;



    __fortify_function void mylog(const char *__restrict __fmt, ...)
    {
        FILE *fp = fopen(log_file, "a+");
        fprintf(fp, __fmt, __va_arg_pack());
        fclose(fp);
    }

    void get_format_time_st(char * str_time)
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
        int diff = kv_index - old_kv_index;
        mylog("%s qps %d total %d status running\n", str_time, kv_index - old_kv_index, kv_index);
        old_kv_index = kv_index;
        time_count++;
        if(time_count > TIMEOUT)
            is_run = 0;
        return;
    }

    void timer_thread()
    {
        //printf("timer thread start \n");
        prctl(PR_SET_NAME, "my_timer_thread");
        pthread_t tid;
        while (is_run)
        {
            sleep(1);
            pthread_create(&tid, NULL, log_stat, NULL);
        }
        
        return;
    }

    void pressure_read_test_thread(void * handle)
    {
        prctl(PR_SET_NAME, "my_read_thread");
        gstorReply *reply_set;
        gstorReply *reply_get;
        char str[20];
        while (is_run)
        {
            int index = (kv_index++) % ReadTotalNum;
            sprintf(str, "%d", index );
            //mylog("str:%s \n", str);
            reply_get = (gstorReply *)gstor_command_get(handle, str);      // 测读
            gstor_freeReplyObject(reply_get);
        }
    }
    void pressure_write_test_thread(void *handle)
    {
        prctl(PR_SET_NAME, "my_write_thread");
        gstorReply *reply_set;
        char str[20];
        while (is_run)
        {
            int index = kv_index++;
            sprintf(str, "%d", index);
            //mylog("str:%s \n", str);
            reply_set = (gstorReply *)gstor_command_set(handle, str, str); // 测写入
            gstor_freeReplyObject(reply_set);
            //gstor_kv_commit(handle);
            if (kv_index % 1000 == 0)
            {
                gstor_kv_commit(handle);
            }
        }
    }

    void gstor_client(uint32 thread_num, test_type type)
    {
        pthread_t tid[32];

        struct timeval start;
        struct timeval end;

        kv_index = 0;

        pthread_t timer_tid;
        pthread_create(&timer_tid, NULL, timer_thread, NULL);
        gettimeofday(&start, NULL);

        for (int i = 0; i < thread_num; i++)
        {
            if(type == ReadTest)
            {
                pthread_create(&tid[i], NULL, pressure_read_test_thread, handles[i]);
            }
            else
            {
                pthread_create(&tid[i], NULL, pressure_write_test_thread, handles[i]);
            }
            //kv_index++;
        }
        for (int i = 0; i < thread_num; i++)
        {
            pthread_join(tid[i], NULL);
        }
        gettimeofday(&end, NULL); 
        float time_use = (end.tv_sec - start.tv_sec) + (1.0 * (end.tv_usec - start.tv_usec)) / 1000000;
        char str_time[64];
        get_format_time_st(str_time);
        mylog("%s avg_qps %lf total %d status stop\n", str_time, kv_index/time_use, kv_index);
        return;
    }

    void sig_handler(int signo)
    {
        //printf("catch the signal SIGUSR1 %d run num:%d\n", signo, kv_index);
        is_run = 0;
    }

    int main(int argc, char *argv[])
    {
        char path[1024] = "\0";
        char cmd[16] = "\0"; 
        if (argc >= 2)
        {
            memcpy(path, argv[1], strlen(argv[1]) + 1);
        }
        else
        {
            printf("command err!!! ./pressure_test ${path} ${thread_num} eg:./pressure_test test 8");
            return 0;
        }

        int db_type = 0;
        if (gstor_startup_db(db_type, path) != GS_SUCCESS)
        {
            printf("[API] gstor_startup_db failed\n");
            return GS_ERROR;
        }
        if (argc >= 3)
        {
            thread_num = atoi(argv[2]);
        }

        printf("path=%s thread_num=%d\n", path, thread_num);
        for (int i = 0; i < thread_num; i++)
        {

            if (alloc_kv_handle(&handles[i]) != GS_SUCCESS)
            {
                printf("get handles[%d] failed\n", i);
            }
            if (create_or_open_kv_table(handles[i], EXC_DCC_KV_TABLE) != GS_SUCCESS)
            {
                printf("open table failed i:%d\n", i);
                return GS_ERROR;
            }
            gstor_kv_begin(handles[i]);
        }

        if (signal(SIGUSR1, sig_handler) == SIG_ERR)
        {
            printf("signal error\n");
        }
        if (argc >= 4 && strcmp(argv[3], "R") == 0) //不设置的话只创建库文件
        {
            is_run = 1;
            gstor_client(thread_num, ReadTest);       
        }
        else if (argc >= 4 && strcmp(argv[3], "W") == 0) //测写
        {
            is_run = 1;
            gstor_client(thread_num, WriteTest);
        }
        else if (argc >= 4 && strcmp(argv[3], "P") == 0) // 测读前准备数据
        {
            gstorReply *reply_set;
            char str[20];
            pthread_t timer_tid;
            pthread_create(&timer_tid, NULL, timer_thread, NULL);
            while (is_run && kv_index < ReadTotalNum)
            {
                int index = kv_index++;
                sprintf(str, "%d", index);
                // mylog("str:%s \n", str);
                reply_set = (gstorReply *)gstor_command_set(handles[0], str, str); // 测写入
                gstor_freeReplyObject(reply_set);
                if (kv_index % 1000 == 0)
                {
                    gstor_kv_commit(handles[0]);
                }
            }
        }
        
        gstor_shutdown_db();
        return 0; // 指定程序的退出状态码为 0，表示正常退出
    }

#ifdef __cplusplus
}
#endif