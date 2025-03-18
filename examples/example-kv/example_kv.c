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
* example_kv.c
*
* IDENTIFICATION
* openGauss-embedded/src/examples/example-kv/example_kv.c
*
* -------------------------------------------------------------------------
*/
#include "compute/kv/intarkdb_kv.h"

int kv_example_1(intarkdb_connection_kv kvconn);
int kv_example_2(intarkdb_connection_kv kvconn);
int kv_example_3(intarkdb_connection_kv kvconn);

int kv_example_single_db();
int kv_example_multi_db();
int kv_example_memory_table();

int main() {
    kv_example_single_db();

    kv_example_multi_db();

    kv_example_memory_table();

    return 0;
}

int kv_example_single_db() {
    intarkdb_database_kv db;
    intarkdb_connection_kv kvconn;

    char path[1024] = "./";
    // open database
    if (intarkdb_open_kv(path, &db) != 0) {
        printf("intarkdb_open failed\n");
        intarkdb_close_kv(&db);
        return -1;
    }

    // open database operate handle
    if (intarkdb_connect_kv(db, &kvconn) != 0) {
        printf("intarkdb_connect failed\n");
        intarkdb_disconnect_kv(&kvconn);
        intarkdb_close_kv(&db);
        return -1;
    }

    // do job
    kv_example_1(kvconn);

    intarkdb_disconnect_kv(&kvconn);
    intarkdb_close_kv(&db);

    return 0;
}

int kv_example_multi_db() {
    intarkdb_database_kv db1, db2;
    intarkdb_connection_kv kvconn1, kvconn2;

    char path1[1024] = "./aaa";
    char path2[1024] = "./bbb";
    // open database
    if (intarkdb_open_kv(path1, &db1) != 0) {
        printf("intarkdb_open failed\n");
        intarkdb_close_kv(&db1);
        return -1;
    }

    // open database operate handle
    if (intarkdb_connect_kv(db1, &kvconn1) != 0) {
        printf("intarkdb_connect failed\n");
        intarkdb_disconnect_kv(&kvconn1);
        intarkdb_close_kv(&db1);
        return -1;
    }

    // do job
    kv_example_2(kvconn1);

    // ================================================================
    // open database
    if (intarkdb_open_kv(path2, &db2) != 0) {
        printf("intarkdb_open failed\n");
        intarkdb_close_kv(&db2);
        return -1;
    }

    // open database operate handle
    if (intarkdb_connect_kv(db2, &kvconn2) != 0) {
        printf("intarkdb_connect failed\n");
        intarkdb_disconnect_kv(&kvconn2);
        intarkdb_close_kv(&db2);
        return -1;
    }

    // do job
    kv_example_3(kvconn2);

    intarkdb_disconnect_kv(&kvconn1);
    intarkdb_disconnect_kv(&kvconn2);
    intarkdb_close_kv(&db1);
    intarkdb_close_kv(&db2);

    return 0;
}

int kv_example_memory_table() {
    intarkdb_database_kv db;
    intarkdb_connection_kv kvconn;

    char path[1024] = "./";
    // open database
    if (intarkdb_open_kv(path, &db) != 0) {
        printf("intarkdb_open failed\n");
        intarkdb_close_kv(&db);
        return -1;
    }

    // open database operate handle
    if (intarkdb_connect_kv(db, &kvconn) != 0) {
        printf("intarkdb_connect failed\n");
        intarkdb_disconnect_kv(&kvconn);
        intarkdb_close_kv(&db);
        return -1;
    }

    // use memory table
    intarkdb_open_memtable_kv(kvconn, "KV_MEM_TABLE");

    // do job
    kv_example_1(kvconn);

    intarkdb_disconnect_kv(&kvconn);
    intarkdb_close_kv(&db);

    return 0;
}

int kv_example_1(intarkdb_connection_kv kvconn) {
    KvReply * reply;

    // set
    reply = (KvReply *)intarkdb_set(kvconn, "key1", "10000000");
    if (reply->type != 0) {
        printf("set key1 failed, msg:%s\n", reply->str);
    }

    reply = (KvReply *)intarkdb_set(kvconn, "key2", "20000000");
    if (reply->type != 0) {
        printf("set key2 failed, msg:%s\n", reply->str);
    }

    reply = (KvReply *)intarkdb_set(kvconn, "key3", "30000000");
    if (reply->type != 0) {
        printf("set key3 failed, msg:%s\n", reply->str);
    }

    // get
    reply = (KvReply *)intarkdb_get(kvconn, "key1");
    if (reply->type != 0) {
        printf("get key1 failed, msg:%s\n", reply->str);
    } else if (reply->len > 0) {
        printf("key:key1, value:%s\n", reply->str);
    } else {
        printf("key1 not exist\n");
    }

    return 0;
}

int kv_example_2(intarkdb_connection_kv kvconn) {
    KvReply * reply;

    // set
    reply = (KvReply *)intarkdb_set(kvconn, "key4", "40000000");
    if (reply->type != 0) {
        printf("set key4 failed, msg:%s\n", reply->str);
    }

    reply = (KvReply *)intarkdb_set(kvconn, "key5", "50000000");
    if (reply->type != 0) {
        printf("set key5 failed, msg:%s\n", reply->str);
    }

    reply = (KvReply *)intarkdb_set(kvconn, "key6", "60000000");
    if (reply->type != 0) {
        printf("set key6 failed, msg:%s\n", reply->str);
    }

    // get
    reply = (KvReply *)intarkdb_get(kvconn, "key4");
    if (reply->type != 0) {
        printf("get key4 failed, msg:%s\n", reply->str);
    } else if (reply->len > 0) {
        printf("key:key4, value:%s\n", reply->str);
    } else {
        printf("key4 not exist\n");
    }

    return 0;
}

int kv_example_3(intarkdb_connection_kv kvconn) {
    KvReply * reply;

    // set
    reply = (KvReply *)intarkdb_set(kvconn, "key7", "70000000");
    if (reply->type != 0) {
        printf("set key7 failed, msg:%s\n", reply->str);
    }

    reply = (KvReply *)intarkdb_set(kvconn, "key8", "80000000");
    if (reply->type != 0) {
        printf("set key8 failed, msg:%s\n", reply->str);
    }

    reply = (KvReply *)intarkdb_set(kvconn, "key9", "90000000");
    if (reply->type != 0) {
        printf("set key9 failed, msg:%s\n", reply->str);
    }

    // get
    reply = (KvReply *)intarkdb_get(kvconn, "key7");
    if (reply->type != 0) {
        printf("get key7 failed, msg:%s\n", reply->str);
    } else if (reply->len > 0) {
        printf("key:key7, value:%s\n", reply->str);
    } else {
        printf("key7 not exist\n");
    }

    return 0;
}
