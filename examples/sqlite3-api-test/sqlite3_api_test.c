#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#ifdef _MSC_VER
#include <windows.h>
#else
#include <sys/time.h>
#endif

#include "intarkdb_sql.h"
#include "../../interface/sqlite3_api_wrapper/include/sqlite3.h"


int NUM_Threads[20] = {5, 10, 15, 20, 25, 30};
char result[100] = {'\0'}; 
int NUM_Insert = 10;

int *thread_index[100];
void *handle_multi[100];

int exec_handle(void *data, int argc, char **argv, char **colname)
{
    int i = *(int *)(data);
    *(int *)(data) = i + 1;
    printf("[%s] is [%s], [%s] is [%s]...\n", colname[0], argv[0], colname[1], argv[1]);
    return 0;
}

void test_sqlite3_db_filename(sqlite3 *db)
{
   char *name =  sqlite3_db_filename(db,"main");
   printf("%s\n",name);
}

void  test_sqlite3_get_autocommit(sqlite3 *db)
{
    int state = sqlite3_get_autocommit(db);
    printf("autocommit state %d\n",state);
}

int test_for_each(sqlite3 *db)
{
    sqlite3_stmt *stmt;
    const char *sql = "SELECT name, age, salary FROM employees;";
    int rc = 0;
    rc = sqlite3_prepare_v2(db, sql, -1, &stmt, 0);
    if (rc != SQLITE_OK) {
        printf("Failed to prepare statement: %s\n", sqlite3_errmsg(db));
        return rc;
    }
    int ret;

    while ( ret  = sqlite3_step(stmt)== SQLITE_ROW) {
        int cols = sqlite3_data_count(stmt);
        printf("Current row has %d columns:\n", cols);
        for (int i = 0; i < sqlite3_column_count(stmt); i++) {
            int type  = sqlite3_column_type(stmt,i);
            const char *name = sqlite3_column_name(stmt, i);
            printf("Column %s: type:%d ", name,type);
            switch (type) {
                case SQLITE_INTEGER:
                    printf("value:%d\n", sqlite3_column_int(stmt, i));
                    break;
                case SQLITE_FLOAT:
                    printf("value:%f\n", sqlite3_column_double(stmt, i));
                    break;
                case SQLITE_TEXT:
                    printf("value:%s\n", sqlite3_column_text(stmt, i));
                    break;
                case SQLITE_BLOB:
                    printf("BLOB\n");
                    break;
                case SQLITE_NULL:
                    printf("NULL\n");
                    break;
                default:
                    printf("Unknown type\n");
            }
        }
    }
    sqlite3_finalize(stmt);
    return 0;
}

int test_for_expanded_sql(sqlite3 *db)
{
    sqlite3_stmt *stmt;
    const char *sql = "SELECT ?, ?, ? FROM employees;";
    int rc = 0;
    rc = sqlite3_prepare_v2(db, sql, -1, &stmt, 0);
    if (rc != SQLITE_OK) {
        printf("Failed to prepare statement: %s\n", sqlite3_errmsg(db));
        return rc;
    }
    sqlite3_bind_text(stmt, 1, "name", -1, NULL);
    sqlite3_bind_text(stmt, 2, "age", -1, NULL);
    sqlite3_bind_text(stmt, 3, "salary", -1, NULL);


    int ret;
    while ( ret  = sqlite3_step(stmt)== SQLITE_ROW) {
        int cols = sqlite3_data_count(stmt);

        for (int i = 0; i < sqlite3_column_count(stmt); i++) {
            int type  = sqlite3_column_type(stmt,i);
            const char *name = sqlite3_column_name(stmt, i);

            switch (type) {
                case SQLITE_INTEGER:

                    break;
                case SQLITE_FLOAT:

                    break;
                case SQLITE_TEXT:

                    break;
                case SQLITE_BLOB:

                    break;
                case SQLITE_NULL:

                    break;
                default:

                    break;
            }
        }
    }    

    printf("----------------test_for_expanded_sql----------------------\n");
    printf("Original_sql:%s\n",sqlite3_sql(stmt));
    printf("Expanded_sql:%s\n",sqlite3_expanded_sql(stmt));
    printf("-----------------------------------------------------------\n");
    sqlite3_finalize(stmt);
    return 0;
}

int test_for_sqlite3_stmt_readonly(sqlite3 *db)
{
    sqlite3_stmt *stmt;
    sqlite3_stmt *stmt2;
    const char *sql = "SELECT name, age, salary FROM employees;";
    const char* sql2 = "INSERT INTO employees (name, age, salary) VALUES ('xxx', 25, 1000.0)";
    int rc = 0;

    printf("----------------test_for_sqlite3_stmt_readonly----------------------\n");
    rc = sqlite3_prepare_v2(db, sql, -1, &stmt, 0);
    if (rc != SQLITE_OK) {
        printf("Failed to prepare statement: %s\n", sqlite3_errmsg(db));
        return rc;
    }
    sqlite3_step(stmt);
    printf("sql:%s\n", sql);
    printf("sqlite3_stmt_readonly:%d\n",sqlite3_stmt_readonly(stmt));


    rc = sqlite3_prepare_v2(db, sql2, -1, &stmt2, 0);
    if (rc != SQLITE_OK) {
    fprintf(stderr, "SQL prepare error: %s\n", sqlite3_errmsg(db));
    return rc;
    }
    sqlite3_step(stmt2);

    printf("sql:%s\n", sql2);
    printf("sqlite3_stmt_readonly:%d\n",sqlite3_stmt_readonly(stmt2));
    printf("-----------------------------------------------------------\n");
    sqlite3_finalize(stmt);
    sqlite3_finalize(stmt2);
    return 0;
}

int test_for_sqlite3_context_db_handle(sqlite3 *db)
{
    sqlite3_context *context;
    sqlite3 *db_handle;

    printf("----------------test_for_sqlite3_context_db_handle----------------------\n");

    context = sqlite3_create_context(db, 0, 0, NULL);
    if (context == NULL) {
        fprintf(stderr, "Can't create context: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        return 1;
    }
    db_handle = sqlite3_context_db_handle(context);
    if (db_handle == NULL) {
        fprintf(stderr, "Context does not have a database handle\n");
        sqlite3_free(context);
        sqlite3_close(db);
        return 1;
    }

    printf("Database handle: %p\n", db_handle);
    sqlite3_free(context);
    printf("-----------------------------------------------------------\n");
    return 0;
}

int sql_exec(sqlite3 *db,char *str_sql)
{
    int rc = sqlite3_exec(db, str_sql, 0, 0, 0);
    if (rc != SQLITE_OK) {
        printf("Failed to create table: %s\n", sqlite3_errmsg(db));
    }
    return rc;
}
int get_table_info(sqlite3 *db) {
    char *zErrMsg = 0;
    int rc;
    int nRow, nCol;
    char **pazResult;
    sqlite3_stmt *stmt;

    rc = sqlite3_get_table(
        db,
        "SELECT * FROM employees;",
        &pazResult,
        &nRow,
        &nCol,
        &zErrMsg
    );

    if (rc == SQLITE_OK) {
        for (int i = 0; i < (nRow + 1) * nCol; i++) {
            if (i % nCol == 0) {
                printf("\n"); 
            }
            printf("%s ", pazResult[i]); 
        }
        printf("\n");


        sqlite3_free_table(pazResult);
    } else {

        fprintf(stderr, "SQL error: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
    }


    return 0;
}


int test_sqlite3_bind_blob(sqlite3 *db)
{
    printf("======sqlite3_bind_blob 测试 ======\n");

    int rc;
    sqlite3_stmt *stmt;
    unsigned char blob_data[] = {0x01, 0x02, 0x03, 0x04, 0x05};

    char *errorMsg;
    const char *sql = "CREATE TABLE IF NOT EXISTS bind_blob(data BLOB)";
    rc = sqlite3_exec(db, sql, NULL, NULL, &errorMsg);
    if (rc != SQLITE_OK) {
        printf("Failed to create table: %s\n", errorMsg);
        sqlite3_free(errorMsg);
    }
    
    sqlite3_prepare_v2(db, "INSERT INTO bind_blob(data) VALUES (?)", -1, &stmt, NULL);
    rc =  sqlite3_bind_blob(stmt, 1, blob_data, sizeof(blob_data), SQLITE_STATIC);
    rc = sqlite3_step(stmt);
    if (rc != SQLITE_DONE) {
        printf("Execution failed: %s\n", sqlite3_errmsg(db));
        sqlite3_finalize(stmt);
        sqlite3_close(db);
        return rc;
    }
    const char *sql2 = "SELECT data FROM bind_blob";
    rc = sqlite3_prepare_v2(db, sql2, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        printf("Failed to prepare statement: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        return rc;
    }

    // Fetch and print the results
    rc = sqlite3_step(stmt);
    if (rc == SQLITE_ROW) {
        // Get the blob data
        const void *data = sqlite3_column_blob(stmt, 0);
        int dataSize = sqlite3_column_bytes(stmt, 0);

        // Print the blob data
        printf("dataSize :%d sqlite3_bind_blob Blob data: ",dataSize);
        for (int i = 0; i < dataSize; i++) {
            printf("%02X ", ((unsigned char*)data)[i]);
        }
        printf("\n");
    } else if (rc == SQLITE_DONE) {
        printf("No data found.\n");
    } else {
        printf("Execution failed: %s\n", sqlite3_errmsg(db));
    }
    sqlite3_finalize(stmt);
}

int test_zerobind()
{
    sqlite3 *db;
    sqlite3_open("test.db", &db);

    // Create a table with a blob column
    const char *sql = "CREATE TABLE IF NOT EXISTS zerobind (data BLOB)";
    char *errorMsg;
    int rc = sqlite3_exec(db, sql, NULL, NULL, &errorMsg);
    if (rc != SQLITE_OK) {
        printf("Failed to create table: %s\n", errorMsg);
        sqlite3_free(errorMsg);
    }

    // Prepare an INSERT statement with a blob parameter
    sqlite3_stmt *stmt;
    sql = "INSERT INTO zerobind (data) VALUES (?)";
    rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        printf("Failed to prepare statement: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        return rc;
    }

    int paramIndex = 1;
    int blobSize = 10; // Size in bytes
    rc = sqlite3_bind_zeroblob(stmt, paramIndex, blobSize);
    if (rc != SQLITE_OK) {
        printf("Failed to bind zerobind: %s\n", sqlite3_errmsg(db));
        sqlite3_finalize(stmt);
        sqlite3_close(db);
        return rc;
    }
    rc = sqlite3_step(stmt);
    if (rc != SQLITE_DONE) {
        printf("Execution failed: %s\n", sqlite3_errmsg(db));
        sqlite3_finalize(stmt);
        sqlite3_close(db);
        return rc;
    }

    const char *sql2 = "SELECT data FROM zerobind";
    rc = sqlite3_prepare_v2(db, sql2, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        printf("Failed to prepare statement: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        return rc;
    }

    rc = sqlite3_step(stmt);
    if (rc == SQLITE_ROW) {
        const void *data = sqlite3_column_blob(stmt, 0);
        int dataSize = sqlite3_column_bytes(stmt, 0);
        if(dataSize > 0){
            printf("sqlite3_bind_zeroblob  data: ");
            for (int i = 0; i < dataSize; i++) {
                printf("%02X ", ((unsigned char*)data)[i]);
            }
            printf("\n");
        }
    } else if (rc == SQLITE_DONE) {
        printf("No data found.\n");
    } else {
        printf("Execution failed: %s\n", sqlite3_errmsg(db));
    }
    sqlite3_finalize(stmt);
    sqlite3_close_v2(db);

    return 0;
}
int insert_employee(sqlite3* db, const char* name, int age, double salary) {
    char* sql = "INSERT INTO employees (name, age, salary) VALUES (?, ?, ?)";
    sqlite3_stmt* stmt;

    int rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "SQL prepare error: %s\n", sqlite3_errmsg(db));
        return rc;
    }

    sqlite3_bind_text(stmt, 1, name, -1, NULL);
    sqlite3_bind_int(stmt, 2, age);
    sqlite3_bind_double(stmt, 3, salary);
    int bind_parameter_count = sqlite3_bind_parameter_count(stmt);
    printf("bind_parameter_count %d\n",bind_parameter_count);

    rc = sqlite3_step(stmt);
    if (rc != SQLITE_DONE) {
        fprintf(stderr, "SQL step error: %s\n", sqlite3_errmsg(db));
        sqlite3_finalize(stmt);
        return rc;
    }
    
    sqlite3_finalize(stmt);
    return SQLITE_OK;
}

int insert_data(sqlite3 *db)
{
    int rc;
    rc = insert_employee(db, "John Doe", 30, 5000.00);
    rc = insert_employee(db, "Mal Col", 35, 6200.10);
    rc = insert_employee(db, "Merry Csk", 40, 4500.00);
    return rc;
}

int test2()
{
    sqlite3 *db;
    int h = 0;
    int rc;

    float time_use = 0;
    float task_tps = 0;
    struct timeval start;
    struct timeval end;
    const char *data = "Callback function called";

    char str[100];
    char *err_msg = NULL;

    rc = sqlite3_open16("ts2.db", &db);
    if (rc)
    {
        printf("Can't open database: %s\n", "sqlites.db");
        exit(0);
    }
    printf("open database success\n");
    char *sql_create_table;
    char buf[120];

    sql_create_table = "CREATE TABLE IF NOT EXISTS PLC_DATA(ID INTEGER, ACCTID INTEGER, CHGAMT VARCHAR(20), PRIMARY KEY (ID,ACCTID));";

    rc = sqlite3_exec(db, sql_create_table, NULL, 0, &err_msg);
    if (rc != SQLITE_OK)
    {
        fprintf(stdout, "SQL error: %s\n", err_msg);
        sqlite3_free(err_msg);
    } else {
        fprintf(stdout, "Table created successfully\n");
    }
   

    sql_create_table = "INSERT INTO PLC_DATA(ID,ACCTID,CHGAMT) VALUES (1,11,'AAA'), (2,22,'BBB');";
    rc = sqlite3_exec(db, sql_create_table, NULL, 0, &err_msg);
    if (rc != SQLITE_OK){
        printf("Can't insert data \n");
        sqlite3_free(err_msg);
    } else {
        printf("insert data success\n");
    }

    sql_create_table = "SELECT ID,ACCTID,CHGAMT FROM PLC_DATA;";
    rc = sqlite3_exec(db, sql_create_table, exec_handle, &data, &err_msg);
    if (rc != SQLITE_OK){
        printf("Can't select data \n");
        sqlite3_free(err_msg);
    } else {
        printf("select data success\n");
    }


    sqlite3_stmt *pstmt;
    const char *sql = "INSERT INTO PLC_DATA(ID,ACCTID,CHGAMT) VALUES(?,?,?);";
    int nRet = sqlite3_prepare_v2(db, sql, strlen(sql), &pstmt, NULL);
    rc = sqlite3_bind_int(pstmt, 1, 3);
    printf("sqlite3_bind_int rc = %d \n", rc);
    rc = sqlite3_bind_int(pstmt, 2, 33);
    printf("sqlite3_bind_int rc = %d \n", rc);
    rc = sqlite3_bind_text(pstmt, 3,  "Jonn", strlen("Jonn"), NULL);
    printf("sqlite3_bind_text rc = %d \n", rc);

    sqlite3_step(pstmt);
    sqlite3_reset(pstmt);

    sql_create_table = "SELECT ID,ACCTID,CHGAMT FROM PLC_DATA;";
    nRet = sqlite3_prepare_v2(db, sql_create_table, strlen(sql_create_table), &pstmt, NULL);
     
    while (SQLITE_ROW == sqlite3_step(pstmt)) {
        printf("ID = %s\n", (char *)sqlite3_column_text(pstmt, 0));
        printf("ACCTID = %s\n", (char *)sqlite3_column_text(pstmt, 1));
        printf("CHGAMT = %s\n", (char *)sqlite3_column_text(pstmt, 2));
    }
    sqlite3_reset(pstmt);


    sql_create_table = "SELECT ID,ACCTID,CHGAMT FROM PLC_DATA;";
    rc = sqlite3_exec(db, sql_create_table, exec_handle, &data, &err_msg);
    if (rc != SQLITE_OK){
        printf("Can't select data \n");
        sqlite3_free(err_msg);
    } else {
        printf("select data success\n");
    }
    sqlite3_close_v2(db);
    printf("sqlite3_close_v2 success\n");

    return 0;
}
int bind_test(sqlite3 *db)
{

    int rc;
    char *sql;
    sqlite3_stmt *stmt;
    char *err_msg = NULL;
    // 创建一个表
    sql = "CREATE TABLE IF NOT EXISTS persons (id INTEGER, name TEXT, age INTEGER,data BLOB)";
    rc = sqlite3_exec(db, sql, NULL, NULL, NULL);
    if (rc != SQLITE_OK) {
        printf("%d\n",__LINE__);
        fprintf(stderr, "SQL error: %s\n", err_msg);
        sqlite3_free(err_msg);
        return 1;
    }
    sql = "INSERT INTO persons (name, age, data) VALUES (?, ?, ?)";
    rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        printf("%d\n",__LINE__);
        fprintf(stderr, "SQL error: %s\n", err_msg);
        sqlite3_free(err_msg);
        return 1;
    }

    rc = sqlite3_bind_text(stmt, 1, "John Doe", -1, SQLITE_STATIC);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\n", sqlite3_errmsg(db));
        sqlite3_finalize(stmt);
        return 1;
    }

    rc = sqlite3_bind_null(stmt, 2);
    if (rc != SQLITE_OK) {
        printf("%d\n",__LINE__);
        fprintf(stderr, "SQL error: %s\n", sqlite3_errmsg(db));
        sqlite3_finalize(stmt);
        return 1;
    }
    size_t data_size = 100 * 1024 * 1024; // 100 MB
    unsigned char *data = (unsigned char *)malloc(data_size);
    memset(data, 0xA5, data_size);
    rc = sqlite3_bind_blob64(stmt, 3, data, data_size, free);
    if (rc != SQLITE_OK) {
        printf("%d\n",__LINE__);
        free(data);
        return 1;
    }
    rc = sqlite3_step(stmt);
    if (rc != SQLITE_DONE) {
        printf("%d\n",__LINE__);
        fprintf(stderr, "SQL error: %s\n", sqlite3_errmsg(db));
        sqlite3_finalize(stmt);
        return 1;
    }

    rc = sqlite3_clear_bindings(stmt);
    if (rc != SQLITE_OK) {
        sqlite3_finalize(stmt);
        sqlite3_close(db);
        return 1;
    }

    rc = sqlite3_reset(stmt);
    rc = sqlite3_bind_text(stmt, 1, "Mal Col", -1, SQLITE_STATIC);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "SQL error: %s\n", sqlite3_errmsg(db));
        sqlite3_finalize(stmt);
        return 1;
    }
  
    rc = sqlite3_bind_null(stmt, 2);
    if (rc != SQLITE_OK) {
        printf("%d\n",__LINE__);
        fprintf(stderr, "SQL error: %s\n", sqlite3_errmsg(db));
        sqlite3_finalize(stmt);
        return 1;
    }
    unsigned char blob_data[] = {0x01, 0x02, 0x03, 0x04, 0x05};
    rc =  sqlite3_bind_blob(stmt, 3, blob_data, sizeof(blob_data), SQLITE_STATIC);
        if (rc != SQLITE_OK) {
            printf("%d\n",__LINE__);
            free(data);
            return 1;
        }
    rc = sqlite3_step(stmt);
    if (rc != SQLITE_DONE) {
        printf("%d\n",__LINE__);
        fprintf(stderr, "SQL error: %s\n", sqlite3_errmsg(db));
        sqlite3_finalize(stmt);
        return 1;
    }
    sql = "SELECT * FROM persons";
    rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        printf("%d\n",__LINE__);
        fprintf(stderr, "SQL error: %s\n", sqlite3_errmsg(db));
        return 1;
    }
    
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        int id = sqlite3_column_int(stmt, 0);
        const char *name = (const char *)sqlite3_column_text(stmt, 1);
        int age = sqlite3_column_int(stmt, 2);
        if (sqlite3_column_type(stmt, 2) == SQLITE_NULL) {
            printf("ID: %d, Name: %s, Age: NULL\n", id, name);
        } else {
            printf("ID: %d, Name: %s, Age: %d\n", id, name, age);
        }
    }

    sqlite3_finalize(stmt);

    free(data);
    return 0;
}
int test_sqlite3_errstr(sqlite3 *db)
{
    int rc;
     char *err_msg = NULL;
    rc = sqlite3_exec(db, "CREATE TABLE (id INTEGER PRIMARY KEY, name TEXT)", NULL, NULL, &err_msg);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "ERR测试：SQL error: %s\n", err_msg);
        sqlite3_free(err_msg);
        return 1;
    }
    return 0;
}

int test_sqlite3_str()
{
    
    const char *str1 = "Hello World";
    const char *str2 = "hello world";
    const char *str3 = "HELLO WORLD";

    printf("Comparing '%s' and '%s': %d\n", str1, str2, sqlite3_stricmp(str1, str2));
    printf("Comparing '%s' and '%s': %d\n", str1, str3, sqlite3_stricmp(str1, str3));
    printf("Comparing first 5 chars of '%s' and '%s': %d\n", str1, str2, sqlite3_strnicmp(str1, str2, 5));
    printf("Comparing first 5 chars of '%s' and '%s': %d\n", str1, str3, sqlite3_strnicmp(str1, str3, 5));
        // 使用 sqlite3_malloc() 分配 100 字节的内存
    void *memory_block = sqlite3_malloc(100);
    if (memory_block == NULL) {
        printf("Failed to allocate memory.\n");
        return 1;
    }

    //在分配的内存块中写入数据
    sprintf((char *)memory_block, "Hello, SQLite!");
    printf("Initial memory block contents: %s\n", (char *)memory_block);

    // 使用 sqlite3_realloc64() 扩展内存块大小
    void *new_memory_block = sqlite3_realloc64(memory_block, 200);
    if (new_memory_block == NULL) {
        printf("Failed to reallocate memory.\n");
        sqlite3_free(memory_block);
        return 1;
    }

    // 在新的内存块中写入更多数据
    sprintf((char *)new_memory_block + strlen((char *)new_memory_block), ", Welcome to the world of SQLite!");
    printf("Expanded memory block contents: %s\n", (char *)new_memory_block);

    // 使用 sqlite3_realloc() 缩小内存块大小
    new_memory_block = sqlite3_realloc(new_memory_block, 150);
    if (new_memory_block == NULL) {
        printf("Failed to reallocate memory.\n");
        sqlite3_free(new_memory_block);
        return 1;
    }

    // 输出缩小后的内存块内容
    printf("Shrunk memory block contents: %s\n", (char *)new_memory_block);

    // 释放分配的内存
    sqlite3_free(new_memory_block);
    return 0;
}

void last_insert_rowid(sqlite3 *db)
{
    char *err_msg = NULL;
    char *errMsg;
    char *sql_create_table;
    char buf[120];
    sql_create_table = "CREATE TABLE IF NOT EXISTS ROWID_TEST_TABLE(ID INT PRIMARY KEY  AUTOINCREMENT, AA INT);";
    int rc = sqlite3_exec(db, sql_create_table, NULL, 0, &err_msg);
    if (rc != SQLITE_OK)
    {
        fprintf(stdout, "SQL error: %s\n", err_msg);
        
    } else {
        fprintf(stdout, "Table created successfully\n");
    }
   
    sqlite3_int64 lastId  = 0;
    char sql[256];
    for(int i =0 ;i < 10 ;i++)
    {
         
        sprintf(sql,"insert into ROWID_TEST_TABLE(AA) values(%d);",i+1);
        printf("%s\n",sql);
        rc = sqlite3_exec(db, sql, 0, 0, &err_msg);
        if (rc != SQLITE_OK)
        {
            fprintf(stdout, "SQL error: %s\n", err_msg);
            sqlite3_free(err_msg);
            return;
        } else {
            fprintf(stdout, "Insert successfully\n");
        }
        // 获取最后插入的行 ID
        lastId = sqlite3_last_insert_rowid(db);
        printf("最后插入的行 ID: %lld\n", lastId);
    }

}
int main(int argc, char *argv[])
{
    sqlite3 *db;
    char *errMsg;
    int rc;
    char str[100];
    char *err_msg = NULL;
    /********************第一步，打开数据库*************************/
    rc = sqlite3_open("ts.db", &db);
    if (rc != SQLITE_OK) {
        printf("无法打开数据库连接: %s\n", sqlite3_errmsg(db));
        return 1;
    }    
    printf("open database success\n");
    printf("sqlite version:%s\n db_filename:%s\n",sqlite3_libversion(),sqlite3_db_filename(db,"main"));
    printf("sqlite3_libversion_number:%d\n",sqlite3_libversion_number());
    printf("sqlite3_threadsafe:%d\n",sqlite3_threadsafe());
    /********************第二步，创建数据库表*************************/
    char *sql_create_table;
    char buf[120];

    sql_create_table = "CREATE TABLE IF NOT EXISTS  employees(name TEXT, age INTEGER, salary REAL);";

    rc = sqlite3_exec(db, sql_create_table, NULL, 0, &err_msg);
    if (rc != SQLITE_OK)
    {
        fprintf(stdout, "SQL error: %s\n", err_msg);
        sqlite3_free(err_msg);
    } else {
        fprintf(stdout, "Table created successfully\n");
    }
    // last_insert_rowid(db);
    // //插入数据
    insert_data(db);

    //测试获取table信息
    get_table_info(db);
    test_for_each(db);
    // test_for_expanded_sql(db);
    // test_for_sqlite3_stmt_readonly(db);
    // test_for_sqlite3_context_db_handle(db);
    // test_sqlite3_get_autocommit(db);

    // test_sqlite3_bind_blob(db);

    // test_zerobind();   
    
    // test2();
    // bind_test(db);
    // test_sqlite3_errstr(db);
    // test_sqlite3_str();
    
    // 关闭数据库连接
    sqlite3_close(db);
    return 0;
}
