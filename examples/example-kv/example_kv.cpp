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
* example_kv.cpp
*
* IDENTIFICATION
* openGauss-embedded/src/examples/example-kv/example_kv.cpp
*
* -------------------------------------------------------------------------
*/
#include "compute/kv/intarkdb_kv.hpp"

#include <iostream>

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
    std::string path = "./";
    KvIntarkDB kvdb_conn;
    kvdb_conn.Connect(path);

    kvdb_conn.Set("key1", "10000000");
    kvdb_conn.Set("key2", "20000000");
    kvdb_conn.Set("key3", "30000000");

    std::cout<<"key1:"<<kvdb_conn.Get("key1")->str<<std::endl;
    std::cout<<"key2:"<<kvdb_conn.Get("key2")->str<<std::endl;
    std::cout<<"key3:"<<kvdb_conn.Get("key3")->str<<std::endl;

    return 0;
}

int kv_example_multi_db() {
    std::string path1 = "./aaa";
    std::string path2 = "./bbb";
    KvIntarkDB kvdb_conn1, kvdb_conn2;
    kvdb_conn1.Connect(path1);
    kvdb_conn2.Connect(path2);

    kvdb_conn1.Set("key1", "100000001");
    kvdb_conn1.Set("key2", "200000001");
    kvdb_conn1.Set("key3", "300000001");

    kvdb_conn2.Set("key11", "110000000");
    kvdb_conn2.Set("key22", "220000000");
    kvdb_conn2.Set("key33", "330000000");

    std::cout<<"key1:"<<kvdb_conn1.Get("key1")->str<<std::endl;
    std::cout<<"key2:"<<kvdb_conn1.Get("key2")->str<<std::endl;
    std::cout<<"key3:"<<kvdb_conn1.Get("key3")->str<<std::endl;
    std::cout<<"key11:"<<kvdb_conn2.Get("key11")->str<<std::endl;
    std::cout<<"key22:"<<kvdb_conn2.Get("key22")->str<<std::endl;
    std::cout<<"key33:"<<kvdb_conn2.Get("key33")->str<<std::endl;

    std::cout<<"key4:"<<kvdb_conn1.Get("key4")->str<<std::endl;
    std::cout<<"key44:"<<kvdb_conn2.Get("key44")->str<<std::endl;

    return 0;
}

int kv_example_memory_table() {
    std::string path = "./";
    KvIntarkDB kvdb_conn;
    kvdb_conn.Connect(path);

    // use memory table
    kvdb_conn.OpenMemoryTable("KV_MEM_TABLE");

    kvdb_conn.Set("key111", "10000000");
    kvdb_conn.Set("key222", "20000000");
    kvdb_conn.Set("key333", "30000000");

    std::cout<<"key111:"<<kvdb_conn.Get("key111")->str<<std::endl;
    std::cout<<"key222:"<<kvdb_conn.Get("key222")->str<<std::endl;
    std::cout<<"key333:"<<kvdb_conn.Get("key333")->str<<std::endl;

    return 0;
}
