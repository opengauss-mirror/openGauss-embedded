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
* benchmark_createtable.cpp
*
* IDENTIFICATION
* openGauss-embedded/src/compute/sql/test/benchmark_createtable.cpp
*
* -------------------------------------------------------------------------
*/
#include <benchmark/benchmark.h>
#include <fstream>

#include "catalog/catalog.h"
#include "catalog/table_info.h"
#include "main/connection.h"
#include "main/database.h"
#include "main/capi/gstor-c.h"

gstor_database db{0};
gstor_connection conn{0};


void setup() {
    gstor_open("./", &db);
    gstor_connect(db, &conn);
}

void teardown() {
    gstor_disconnect(&conn);
    gstor_close(&db);
}

uint32 idx = 0;
static void bench_create_table(benchmark::State& state)
{
    std::string basename("create_tb");
    std::string tablename;
    auto numIterations = state.range(0);

    for (auto _: state) {
        for (int i = 1; i <= numIterations; ++i) {
            tablename = basename + std::to_string(idx);
            std::string create_table(fmt::format("create table {} (id integer PRIMARY KEY, score real, age integer DEFAULT 20, name varchar(20) NOT NULL);", tablename));
            gstor_result result{0};
            gstor_query(conn, create_table.c_str(), result);

            state.PauseTiming();
            std::string crop_table(fmt::format("drop table {};", tablename));
            gstor_query(conn, crop_table.c_str(), result);
            idx++;
            state.ResumeTiming();
        }
    }
}

BENCHMARK(bench_create_table)->ArgsProduct({
    benchmark::CreateRange(10, 10000, 10) // state.range(0) - numIterations
});

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
    // 打开输出文件
    std::ofstream output_file("benchmark_createtable.json");
    auto reporter = ::benchmark::ConsoleReporter();
    reporter.SetOutputStream(&output_file);
    ::benchmark::RunSpecifiedBenchmarks(&reporter);                   
    // 关闭输出文件
    output_file.close();    
    teardown();       
    ::benchmark::Shutdown();                                            
    return 0;                                                           
}       