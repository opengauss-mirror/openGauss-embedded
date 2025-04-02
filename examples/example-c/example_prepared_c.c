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
* example_prepared_c.cpp
*
* IDENTIFICATION
* openGauss-embedded/src/examples/example-c/example_prepared_c.cpp
*
* -------------------------------------------------------------------------
*/
#include "intarkdb_sql.h"

void create_example(intarkdb_connection conn, intarkdb_result intarkdb_result);
void prepare_example_insert(intarkdb_connection conn, intarkdb_result intarkdb_result);
void prepare_example_select(intarkdb_connection conn, intarkdb_result intarkdb_result);

int main() {
    intarkdb_database db;
    intarkdb_connection conn;
    intarkdb_result intarkdb_result = intarkdb_init_result();
    if (!intarkdb_result) {
        printf("intarkdb_init_result failed!!\n");
        return SQL_ERROR;
    }

    char path[1024] = "./";
    // open database
    if (intarkdb_open(path, &db) != SQL_SUCCESS) {
        printf("intarkdb_open failed\n");
        intarkdb_close(&db);
        return SQL_ERROR;
    }

    // open database operate handle
    if (intarkdb_connect(db, &conn) != SQL_SUCCESS) {
        printf("intarkdb_connect failed\n");
        intarkdb_disconnect(&conn);
        intarkdb_close(&db);
        return SQL_ERROR;
    }

    // do job
    create_example(conn, intarkdb_result);
    prepare_example_insert(conn, intarkdb_result);
    prepare_example_select(conn, intarkdb_result);

    // clean
    intarkdb_destroy_result(intarkdb_result);
    intarkdb_disconnect(&conn);
    intarkdb_close(&db);

    return SQL_SUCCESS;
}

void create_example(intarkdb_connection conn, intarkdb_result intarkdb_result) {
    if (intarkdb_query(conn,"CREATE TABLE example_table_p(id int, name varchar(20), money bigint, time timestamp)",
            intarkdb_result) != SQL_SUCCESS) {
        printf("Failed to create table!!, errmsg:%s\n", intarkdb_result_msg(intarkdb_result));
    }
}

void prepare_example_insert(intarkdb_connection conn, intarkdb_result intarkdb_result) {
    intarkdb_prepared_statement stmt = intarkdb_init_prepare_stmt();
    // prepare
    if (intarkdb_prepare(conn, "INSERT INTO example_table_p(id,name,money,time) VALUES (1,?,?,now()), (2,?,?,now())",
            &stmt) != SQL_SUCCESS) {
        printf("Failed to prepare insert!! errmsg=%s\n", intarkdb_prepare_errmsg(stmt));
        return;
    }
    // bind
    intarkdb_bind_varchar(stmt, 1, "小美");
    intarkdb_bind_int64(stmt, 2, 100000000);
    intarkdb_bind_varchar(stmt, 3, "小帅");
    intarkdb_bind_int64(stmt, 4, 200000000);
    // execute
    if (intarkdb_execute_prepared(stmt, intarkdb_result) != SQL_SUCCESS) {
        printf("1 intarkdb_execute_prepared error, errmsg=%s\n", intarkdb_result_msg(intarkdb_result));
    }
    // intarkdb_destroy_prepare(&stmt);     // if reuse, do not destroy

    // insert again 
    // bind again
    intarkdb_bind_varchar(stmt, 1, "小白");
    intarkdb_bind_int64(stmt, 2, 300000000);
    intarkdb_bind_varchar(stmt, 3, "小高");
    intarkdb_bind_int64(stmt, 4, 400000000);
    // execute again
    if (intarkdb_execute_prepared(stmt, intarkdb_result) != SQL_SUCCESS) {
        printf("2 intarkdb_execute_prepared error, errmsg=%s\n", intarkdb_result_msg(intarkdb_result));
    }
    intarkdb_destroy_prepare(&stmt);    // if not use again, must be destroyed
}

void prepare_example_select(intarkdb_connection conn, intarkdb_result intarkdb_result) {
    intarkdb_prepared_statement stmt = intarkdb_init_prepare_stmt();
    // prepare
    if (intarkdb_prepare(conn, "select * from example_table_p where money>? and time>?", &stmt) != SQL_SUCCESS) {
        printf("Failed to prepare select!! errmsg=%s\n", intarkdb_prepare_errmsg(stmt));
        return;
    }
    // bind
    intarkdb_bind_int64(stmt, 1, 1000000);
    intarkdb_bind_date(stmt, 2, "2023-12-01 01:01:01");
    // execute
    intarkdb_execute_prepared(stmt, intarkdb_result);
    intarkdb_destroy_prepare(&stmt);

    // print
    int64_t column_count = intarkdb_column_count(intarkdb_result);
    for (int64_t col = 0; col < column_count; col++) {
        printf("  %s          ", intarkdb_column_name(intarkdb_result, col));
    }
    printf("\n");
    while (intarkdb_next_row(intarkdb_result)) {
        for (int64_t col = 0; col < column_count; col++) {
            char* val = intarkdb_column_value(intarkdb_result, col);
            printf("  %s          ", val);
        }
        printf("\n");
    }
}