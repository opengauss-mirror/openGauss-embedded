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
* benchmark_insert.cpp
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/test/benchmark_insert.cpp
*
* -------------------------------------------------------------------------
*/
#include <benchmark/benchmark.h>

#include <random>
#include <thread>

#include "catalog/catalog.h"
#include "catalog/table_info.h"
#include "main/connection.h"
#include "main/database.h"


std::string rand_str(const int len)  /*参数为字符串的长度*/
{
    /*初始化*/
    std::string str;                 /*声明用来保存随机字符串的str*/
    char c;                     /*声明字符c，用来保存随机生成的字符*/
    int idx;                    /*用来循环的变量*/
    /*循环向字符串中添加随机生成的字符*/
    for(idx = 0;idx < len;idx ++)
    {
        /*rand()%26是取余，余数为0~25加上'a',就是字母a~z,详见asc码表*/
        c = 'a' + rand()%26;
        str.push_back(c);       /*push_back()是string类尾插函数。这里插入随机字符c*/
    }
    return str;                 /*返回生成的随机字符串*/
}

std::shared_ptr<IntarkDB> db_instance = nullptr;
std::unique_ptr<Connection> conn = nullptr;

void setup() {
    db_instance = std::make_shared<IntarkDB>("./");
    // 启动db
    db_instance->Init();
    conn = std::make_unique<Connection>(db_instance);
    conn->Init();
}

uint32 idx = 0;
static void bench_insert(benchmark::State& state)
{
    // 随机数
    std::default_random_engine generator;
    //std::normal_distribution<double> normal_d(70, 3.5);//随机数分布器，均值、方差
    std::uniform_real_distribution<double> score_v(0, 100);//随机数分布器 闭区间
    std::uniform_int_distribution<int> age_v(17, 24);//随机数分布器 闭区间
    std::uniform_int_distribution<int> name_v(5, 10);//随机数分布器 闭区间

    std::string tablename("select_table1");
    do{
        auto table_info = conn->GetTableInfo(tablename);
        if (table_info != NULL)
            break;
        std::string create_table(fmt::format("create table {} (id integer PRIMARY KEY, score real, age integer DEFAULT 20, name varchar(20) NOT NULL);", tablename));
        conn->Query(create_table.c_str());
    }while(0);

    auto length = state.range(0);

    for (auto _: state) {
        for (int i = 1; i <= length; ++i) {
            std::string insert_q(fmt::format("insert into {} values({}, {}, {}, \'{}\');", 
                           tablename, idx, score_v(generator), age_v(generator), rand_str(name_v(generator))));
            conn->Query(insert_q.c_str());
            idx++;
        }
        // state.PauseTiming();
        // state.ResumeTiming();
    }
}

BENCHMARK(bench_insert)->ArgsProduct({
    benchmark::CreateRange(10, 10000*10, 10), // state.range(0) - length
});

/********************************Multithreaded Benchmarks start**********************************/
// uint32 rows = 0;
// static void thread_bench_insert(benchmark::State& state, const std::string tablename, uint32 start_id, uint32 iterations)
// {
//     // 随机数
//     std::default_random_engine generator;
//     //std::normal_distribution<double> normal_d(70, 3.5);//随机数分布器，均值、方差
//     std::uniform_real_distribution<double> score_v(0, 100);//随机数分布器 闭区间
//     std::uniform_int_distribution<int> age_v(17, 24);//随机数分布器 闭区间
//     std::uniform_int_distribution<int> name_v(5, 10);//随机数分布器 闭区间

//     do{
//         auto table_info = conn->GetTableInfo(tablename);
//         if (table_info != NULL)
//             break;
//         std::string create_table(fmt::format("create table {} (id integer PRIMARY KEY, score real, age integer DEFAULT 20, name varchar(20) NOT NULL);", tablename));
//         conn->Query(create_table.c_str());
//     }while(0);

//     uint32 iter_idx = start_id;
//     for (uint32 i = 1; i <= iterations; ++i) {
//         std::string insert_q(fmt::format("insert into {} values({}, {}, {}, \'{}\');", 
//                         tablename, iter_idx, score_v(generator), age_v(generator), rand_str(name_v(generator))));
//         conn->Query(insert_q.c_str());
//         iter_idx++;
//     }
// }


// static void bench_insert_MultiThreaded(benchmark::State& state) {
//     std::string tablename("select_table1");
//     if (state.thread_index() == 0) {
//         auto table_info = conn->GetTableInfo(tablename);
//         if (table_info == NULL){
//             std::string create_table(fmt::format("create table {} (id integer PRIMARY KEY, score real, age integer DEFAULT 20, name varchar(20) NOT NULL);", tablename));
//             conn->Query(create_table.c_str());
//         }
//     }
//     // for (auto _ : state) {
//     //     // Run the test as normal.
//     // }
//     // if (state.thread_index() == 0) {
//     //     // Teardown code here.
//     // }

//     int numThreads = state.range(0);
//     int numIterations = state.range(1);
//     std::vector<std::thread> threads;
//     uint32 start = rows;
//     for (int i = 0; i < numThreads; i++)
//     {
//         start += numIterations*i;
//         std::thread myThread([&state, tablename, start, numIterations](){
//             thread_bench_insert(state, tablename, start, numIterations);
//         });
//         threads.emplace_back(std::move(myThread));
//     }

//     for (auto& thread : threads)
//     {
//         thread.join();
//     }

//     rows += numThreads * numIterations;
// }
// BENCHMARK(bench_insert_MultiThreaded)->ArgsProduct({
//     benchmark::CreateDenseRange(1, 4, 1), // numThreads
//     benchmark::CreateRange(10, 10000*10, 10)  // numIterations
// });
/********************************Multithreaded Benchmarks end**********************************/

//BENCHMARK_MAIN();

int main(int argc, char** argv) {                                     
    char arg0_default[] = "benchmark";                                  
    char* args_default = arg0_default;                                  
    if (!argv) {                                                        
      argc = 1;                                                         
      argv = &args_default;                                             
    }
    setup(); 
    ::benchmark::Initialize(&argc, argv);                               
    if (::benchmark::ReportUnrecognizedArguments(argc, argv)) return 1; 
    ::benchmark::RunSpecifiedBenchmarks();                              
    ::benchmark::Shutdown();                                            
    return 0;                                                           
}       