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
* example_c.cpp
*
* IDENTIFICATION
* openGauss-embedded/src/examples/example-c/example_c.cpp
*
* -------------------------------------------------------------------------
*/
#include "interface/c/intarkdb_sql.h"
#include <string.h>

typedef struct st_example_result {
    int id;
    char name[64];
    int64_t money;
} example_result_t;

void create_example(intarkdb_connection conn, intarkdb_result intarkdb_result);
void insert_example(intarkdb_connection conn, intarkdb_result intarkdb_result);
void select_example(intarkdb_connection conn, intarkdb_result intarkdb_result);

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
    insert_example(conn, intarkdb_result);
    select_example(conn, intarkdb_result);

    // clean
    intarkdb_destroy_result(intarkdb_result);
    intarkdb_disconnect(&conn);
    intarkdb_close(&db);

    return SQL_SUCCESS;
}

void create_example(intarkdb_connection conn, intarkdb_result intarkdb_result) {
    if (intarkdb_query(conn,"CREATE TABLE example_table_c(id int, name varchar(20), money bigint)",
            intarkdb_result) != SQL_SUCCESS) {
        printf("Failed to create table!!, errmsg:%s\n", intarkdb_result_msg(intarkdb_result));
    }

    if (intarkdb_query(conn, "CREATE UNIQUE INDEX idx_example_table_c_1 on example_table_c(id)",
            intarkdb_result) != SQL_SUCCESS) {
        printf("Failed to create index!! , errmsg:%s\n", intarkdb_result_msg(intarkdb_result));
    }
}

void insert_example(intarkdb_connection conn, intarkdb_result intarkdb_result) {
    if (intarkdb_query(conn, "INSERT INTO example_table_c(id,name,money) \
            VALUES (1,'小明',168000000), (2,'小天',3880000000)", intarkdb_result) != SQL_SUCCESS) {
        printf("Failed to insert row!!, errmsg:%s\n", intarkdb_result_msg(intarkdb_result));
    }

    if (intarkdb_query_format(conn, intarkdb_result, "INSERT INTO example_table_c(id,name,money) \
            VALUES (%d,'小明2',168000000), (%d,'小天2',3880000000)", 11, 22) != SQL_SUCCESS) {
        printf("Failed to insert row!!, errmsg:%s\n", intarkdb_result_msg(intarkdb_result));
    }
}

void select_example(intarkdb_connection conn, intarkdb_result intarkdb_result) {
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
            printf("  %d            %s            %lld", result_set.id, result_set.name, result_set.money);
            printf("\n");
        }
    }
}

// default auto_commit is true
void set_autocommit_example(intarkdb_connection conn, intarkdb_result intarkdb_result) {
    if (intarkdb_query(conn, "SET AUTO_COMMIT = FALSE", intarkdb_result) != SQL_SUCCESS) {
        printf("Failed to set auto_commit!!\n");
    }
}

void begin_example(intarkdb_connection conn, intarkdb_result intarkdb_result) {
    if (intarkdb_query(conn, "BEGIN;", intarkdb_result) != SQL_SUCCESS) {
        printf("Failed to begin!!\n");
    }
}

void commit_example(intarkdb_connection conn, intarkdb_result intarkdb_result) {
    if (intarkdb_query(conn, "COMMIT;", intarkdb_result) != SQL_SUCCESS) {
        printf("Failed to commit!!\n");
    }
}

void rollback_example(intarkdb_connection conn, intarkdb_result intarkdb_result) {
    if (intarkdb_query(conn, "ROLLBACK;", intarkdb_result) != SQL_SUCCESS) {
        printf("Failed to rollback!!\n");
    }
}
